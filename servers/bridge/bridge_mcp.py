#!/usr/bin/env python3
"""Muse Bridge MCP server v2.5.0 (bundled in the muse-bridge Claude Code plugin).

Lets Claude Code delegate tasks to subagents running on the user's Muse VM.
The bridge home on this machine is %USERPROFILE%\\muse-bridge:

  queue.json                 task queue (this server is the only writer)
  claims/<id>.json           master claim written by the worker taking a task
  claims/<id>.a<j>.json      slot claims for multi-worker tasks
  results/<id>.md            finished result (markdown)
  results/<id>.part-<j>.md   per-worker partial outputs (temporary)
  status/worker-<n>.json     worker heartbeats from the Muse side
  messages/<id>.json         messages from home to the worker(s) on a task

ASCII-only output: the Windows console here is cp949, so every tool result
is JSON with ensure_ascii=True.
"""
import json
import os
import sys
import threading
import time
import uuid

try:
    import msvcrt  # Windows only: cross-process file locking
except ImportError:
    msvcrt = None
try:
    import fcntl  # POSIX: cross-process file locking (also used by tests)
except ImportError:
    fcntl = None
# Linux test env: both may be None and the file lock becomes a no-op

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BRIDGE_DIR = os.path.join(os.path.expanduser("~"), "muse-bridge")
QUEUE_PATH = os.path.join(BRIDGE_DIR, "queue.json")
RESULTS_DIR = os.path.join(BRIDGE_DIR, "results")
CLAIMS_DIR = os.path.join(BRIDGE_DIR, "claims")
STATUS_DIR = os.path.join(BRIDGE_DIR, "status")

# Two-level serialization for every queue.json read-modify-write cycle.
# Parallel muse_submit calls used to race here: on Windows the loser's
# os.replace failed with "access denied" (WinError 5), or worse, its save
# silently dropped the other call's task (both loaded the same queue before
# either saved).
#
# Level 1: threading.RLock - serializes threads inside this process.
# Level 2: queue.lock file - serializes across server processes
#   (msvcrt.locking on Windows, fcntl.flock on POSIX). RLock alone is not
#   enough because home may run several Claude Code sessions, i.e. several
#   server processes at once. The two locks are always acquired in this
#   order (thread lock, then file lock); the threading.local depth guard
#   makes a nested acquisition from the same thread a no-op, so
#   _mutate_queue can never deadlock against itself (Windows locks are
#   per-handle, not per-process).
_QUEUE_LOCK = threading.RLock()
_QUEUE_FILE_LOCK_PATH = os.path.join(BRIDGE_DIR, "queue.lock")
_queue_file_lock_depth = threading.local()
_SAVE_RETRIES = 30
_SAVE_RETRY_BASE_SECS = 0.05
_SAVE_RETRY_FACTOR = 1.5
_SAVE_RETRY_MAX_SECS = 1.0
MESSAGES_DIR = os.path.join(BRIDGE_DIR, "messages")

for _d in (BRIDGE_DIR, RESULTS_DIR, CLAIMS_DIR, STATUS_DIR, MESSAGES_DIR):
    os.makedirs(_d, exist_ok=True)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    sys.stderr.write("mcp package is required (pip install 'mcp<2')\n")
    raise

mcp = FastMCP("muse-bridge")

PRUNE_AFTER_SECS = 7 * 86400
MAX_TASKS = 300


def _out(obj):
    return json.dumps(obj, ensure_ascii=True)


def _load_queue():
    try:
        with open(QUEUE_PATH, "r", encoding="utf-8") as f:
            q = json.load(f)
        if not isinstance(q, dict) or not isinstance(q.get("tasks"), list):
            return {"version": 2, "tasks": []}
        return q
    except (FileNotFoundError, ValueError):
        return {"version": 2, "tasks": []}


def _task_terminal(t):
    if t.get("status") == "cancelled":
        return True
    rp = os.path.join(RESULTS_DIR, str(t.get("id")) + ".md")
    return os.path.exists(rp)


def _prune(q, now):
    tasks = q.get("tasks", [])
    kept = [t for t in tasks
            if not _task_terminal(t) or (now - int(t.get("created_at", now))) < PRUNE_AFTER_SECS]
    if len(kept) > MAX_TASKS:
        kept = kept[-MAX_TASKS:]
    q["tasks"] = kept


def _atomic_write_json(path, obj, indent=None):
    """Write obj as JSON to path atomically (tmp + os.replace), tolerating
    transient Windows file locks (AV, indexer, a concurrent reader).

    Retries ~30 times with exponential backoff (0.05s * 1.5, capped at 1s
    per sleep) before giving up and raising the last PermissionError."""
    tmp = "%s.tmp-%d-%d-%d" % (path, os.getpid(),
                               threading.get_ident() % 1000000,
                               int(time.time() * 1000) % 1000000)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=True, indent=indent)
    last_err = None
    delay = _SAVE_RETRY_BASE_SECS
    for attempt in range(_SAVE_RETRIES):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            if attempt < _SAVE_RETRIES - 1:
                time.sleep(delay)
                delay = min(delay * _SAVE_RETRY_FACTOR,
                            _SAVE_RETRY_MAX_SECS)
    try:
        os.remove(tmp)
    except OSError:
        pass
    raise last_err


def _save_queue(q):
    q["version"] = 2
    _prune(q, int(time.time()))
    _atomic_write_json(QUEUE_PATH, q, indent=1)


class _QueueFileLock:
    """Cross-process exclusive lock on queue.lock.

    msvcrt.locking (blocking LK_LOCK) on Windows, fcntl.flock (LOCK_EX) on
    POSIX; no-op where neither is available. The threading.local depth
    guard makes nested acquisition from the same thread a no-op, which is
    required on Windows where locks are per-handle, not per-process.
    """

    def __enter__(self):
        depth = getattr(_queue_file_lock_depth, "depth", 0)
        self._fh = None
        self._locked = False
        if depth == 0:
            self._fh = open(_QUEUE_FILE_LOCK_PATH, "a+b")
            self._fh.seek(0)
            if msvcrt is not None:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
                self._locked = True
            elif fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
                self._locked = True
        _queue_file_lock_depth.depth = depth + 1
        return self

    def __exit__(self, exc_type, exc, tb):
        _queue_file_lock_depth.depth = max(
            getattr(_queue_file_lock_depth, "depth", 1) - 1, 0)
        if self._locked:
            try:
                if msvcrt is not None:
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                elif fcntl is not None:
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._locked = False
        elif self._fh is not None:
            self._fh.close()
        return False


def _mutate_queue(fn):
    """Load the queue, apply fn(q), save it back - atomically w.r.t. other
    threads in this server process AND other server processes on this
    machine (each Claude Code session runs its own server process).
    Every queue mutation must go through here."""
    with _QUEUE_LOCK:
        with _QueueFileLock():
            q = _load_queue()
            fn(q)
            _save_queue(q)


def _find_task(q, task_id):
    tid = str(task_id)
    for t in q.get("tasks", []):
        if str(t.get("id")) == tid:
            return t
    return None


def _norm_instructions(raw, workers):
    items = []
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    items = [str(x) for x in parsed]
            except (ValueError, TypeError):
                items = []
        if not items and s:
            items = [s]
    elif isinstance(raw, list):
        items = [str(x) for x in raw]
    if not items:
        return [""] * workers
    return [items[j % len(items)] for j in range(workers)]


def _claim_files(task_id):
    tid = str(task_id)
    try:
        names = os.listdir(CLAIMS_DIR)
    except OSError:
        return []
    return [n for n in names
            if n == tid + ".json" or (n.startswith(tid + ".a") and n.endswith(".json"))]


def _messages_path(task_id):
    tid = "".join(c for c in str(task_id) if c.isalnum() or c in "-_")[:64]
    return os.path.join(MESSAGES_DIR, tid + ".json")


def _load_messages(task_id):
    try:
        with open(_messages_path(task_id), "r", encoding="utf-8") as f:
            m = json.load(f)
        return m if isinstance(m, list) else []
    except (OSError, ValueError):
        return []


@mcp.tool()
def muse_submit(prompt: str, label: str = "", workers: int = 1,
                worker_instructions: str = "", priority: int = 0,
                timeout_minutes: int = 60) -> str:
    """Submit a task to Muse subagents on the Muse VM.

    prompt: the work order (required).
    label: short label for the task (optional).
    workers: how many parallel workers may collaborate on this task (1-8, default 1).
    worker_instructions: guidance for the workers. Either one string (broadcast
      to every worker) or a JSON array of per-worker instructions, e.g.
      '["research domestic patents", "research foreign papers"]'. Shorter
      arrays are cycled across workers.
    priority: higher runs first (default 0).
    timeout_minutes: worker stops after this long (5-480, default 60).
    Returns JSON with task_id. Then call muse_await(task_id) to wait for
    the result (server-side wait, no polling needed).
    """
    prompt = (prompt or "").strip()
    if not prompt:
        return _out({"ok": False, "error": "prompt is required"})
    try:
        workers = int(workers)
    except (TypeError, ValueError):
        workers = 1
    workers = max(1, min(8, workers))
    try:
        priority = int(priority)
    except (TypeError, ValueError):
        priority = 0
    try:
        timeout_minutes = int(timeout_minutes)
    except (TypeError, ValueError):
        timeout_minutes = 60
    timeout_minutes = max(5, min(480, timeout_minutes))

    task_id = uuid.uuid4().hex[:12]
    task = {
        "id": task_id,
        "prompt": prompt,
        "label": (label or "")[:80],
        "workers": workers,
        "worker_instructions": _norm_instructions(worker_instructions, workers),
        "priority": priority,
        "timeout_minutes": timeout_minutes,
        "status": "pending",
        "created_at": int(time.time()),
    }
    _mutate_queue(lambda q: q["tasks"].append(task))
    return _out({"ok": True, "task_id": task_id, "status": "queued",
                 "workers": workers, "priority": priority,
                 "hint": "Call muse_await(task_id) to wait for the result."})


@mcp.tool()
def muse_result(task_id: str) -> str:
    """Fetch the result of a submitted task.

    Returns status done (with the markdown result), running, pending,
    cancelled, or an error for unknown task_id.
    """
    tid = (task_id or "").strip()
    if not tid:
        return _out({"ok": False, "error": "task_id is required"})
    rp = os.path.join(RESULTS_DIR, tid + ".md")
    if os.path.exists(rp):
        try:
            with open(rp, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except OSError:
            content = ""
        return _out({"ok": True, "task_id": tid, "status": "done", "result": content})
    claimed = bool(_claim_files(tid))
    q = _load_queue()
    t = _find_task(q, tid)
    if t is None and not claimed:
        return _out({"ok": False, "task_id": tid, "error": "unknown task_id"})
    status = (t.get("status") if t else "pending") or "pending"
    if status == "cancelled":
        return _out({"ok": True, "task_id": tid, "status": "cancelled"})
    if claimed:
        return _out({"ok": True, "task_id": tid, "status": "running",
                     "messages": len(_load_messages(tid)),
                     "hint": "Muse is working on it. Poll again in about 30 seconds."})
    return _out({"ok": True, "task_id": tid, "status": "pending",
                 "hint": "Not picked up yet. Poll again in about 30 seconds."})


@mcp.tool()
def muse_await(task_id: str, timeout_secs: int = 600) -> str:
    """Wait for a submitted task to finish, then return its result.

    Blocks server-side until results/<task_id>.md appears, the task is
    cancelled, or timeout_secs elapses. Replaces client-side polling:
    call once instead of polling muse_result every 30 seconds.
    Returns status done (with the markdown result), cancelled, or timeout
    (call again to keep waiting). timeout_secs: 5-3600, default 600.
    """
    tid = (task_id or "").strip()
    if not tid:
        return _out({"ok": False, "error": "task_id is required"})
    try:
        timeout = max(5, min(int(timeout_secs or 600), 3600))
    except (TypeError, ValueError):
        timeout = 600
    rp = os.path.join(RESULTS_DIR, tid + ".md")
    q = _load_queue()
    t = _find_task(q, tid)
    if t is None and not _claim_files(tid) and not os.path.exists(rp):
        return _out({"ok": False, "task_id": tid, "error": "unknown task_id"})
    deadline = time.time() + timeout
    while True:
        if os.path.exists(rp):
            try:
                with open(rp, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()
            except OSError:
                content = ""
            return _out({"ok": True, "task_id": tid, "status": "done",
                         "result": content})
        q = _load_queue()
        t = _find_task(q, tid)
        if t is not None and t.get("status") == "cancelled":
            return _out({"ok": True, "task_id": tid, "status": "cancelled"})
        if time.time() >= deadline:
            return _out({"ok": True, "task_id": tid, "status": "timeout",
                         "hint": "Still not done. Call muse_await again to keep waiting."})
        time.sleep(2)


@mcp.tool()
def muse_status() -> str:
    """Show bridge health: worker pool heartbeats, queued/running tasks.

    Use this to see whether the Muse side is alive and what it is doing.
    """
    now = int(time.time())
    q = _load_queue()
    pending = [t for t in q.get("tasks", [])
               if (t.get("status") or "pending") == "pending"
               and not os.path.exists(os.path.join(RESULTS_DIR, str(t.get("id")) + ".md"))
               and not _claim_files(str(t.get("id")))]
    running = []
    try:
        claim_names = os.listdir(CLAIMS_DIR)
    except OSError:
        claim_names = []
    masters = {}
    for n in claim_names:
        if not n.endswith(".json"):
            continue
        base = n[:-5]
        tid = base.split(".a")[0] if ".a" in base else base
        masters.setdefault(tid, []).append(n)
    for tid, files in sorted(masters.items()):
        if os.path.exists(os.path.join(RESULTS_DIR, tid + ".md")):
            continue
        detail = {"task_id": tid, "assignments": len(files)}
        try:
            with open(os.path.join(CLAIMS_DIR, tid + ".json"), "r", encoding="utf-8") as f:
                mc = json.load(f)
            detail["workers_requested"] = mc.get("workers_effective", mc.get("workers", 1))
            detail["claim_age_secs"] = now - int(mc.get("claimed_at", now))
        except (OSError, ValueError):
            pass
        t = _find_task(q, tid)
        if t:
            detail["label"] = t.get("label", "")
        running.append(detail)
    workers = []
    try:
        status_names = sorted(os.listdir(STATUS_DIR))
    except OSError:
        status_names = []
    for n in status_names:
        if not (n.startswith("worker-") and n.endswith(".json")):
            continue
        try:
            with open(os.path.join(STATUS_DIR, n), "r", encoding="utf-8") as f:
                w = json.load(f)
        except (OSError, ValueError):
            continue
        age = now - int(w.get("updated_at", 0))
        workers.append({"worker": w.get("worker"), "state": w.get("state"),
                        "task_id": w.get("task_id"), "assignment": w.get("assignment"),
                        "heartbeat_age_secs": age, "alive": age < 180})
    alive = [w for w in workers if w["alive"]]
    return _out({"ok": True,
                 "bridge_alive": bool(alive),
                 "workers": workers,
                 "queued": [{"task_id": t.get("id"), "label": t.get("label", ""),
                             "workers": t.get("workers", 1), "priority": t.get("priority", 0)}
                            for t in pending],
                 "running": running,
                 "summary": "workers alive: %d/%d, queued: %d, running: %d"
                            % (len(alive), len(workers), len(pending), len(running))})


@mcp.tool()
def muse_message(task_id: str, text: str) -> str:
    """Send a message to the worker(s) handling a task: changed instructions,
    extra context, new priorities, or a question.

    Delivery is at the worker's next checkpoint (between steps, before
    uploading a slice, before final synthesis) - not a real-time interrupt.
    The worker reads new messages each loop iteration and folds them into
    its ongoing work. Messages to a finished or unknown task are rejected.
    Returns JSON with ok and the message index.
    """
    tid = (task_id or "").strip()
    text = (text or "").strip()
    if not tid:
        return _out({"ok": False, "error": "task_id is required"})
    if not text:
        return _out({"ok": False, "error": "text is required"})
    if os.path.exists(os.path.join(RESULTS_DIR, tid + ".md")):
        return _out({"ok": False, "task_id": tid, "error": "task already finished"})
    q = _load_queue()
    t = _find_task(q, tid)
    if t is None and not _claim_files(tid):
        return _out({"ok": False, "task_id": tid, "error": "unknown task_id"})
    mp = _messages_path(tid)
    msgs = _load_messages(tid)
    msgs.append({"from": "home", "text": text[:2000], "at": int(time.time())})
    msgs = msgs[-50:]
    try:
        _atomic_write_json(mp, msgs)
    except OSError:
        return _out({"ok": False, "task_id": tid, "error": "write failed"})
    return _out({"ok": True, "task_id": tid, "message_index": len(msgs) - 1,
                 "note": "delivered at the worker's next checkpoint"})


@mcp.tool()
def muse_cancel(task_id: str) -> str:
    """Cancel a task. Pending tasks are cancelled immediately; running tasks
    get a cancel request that the worker honors shortly."""
    tid = (task_id or "").strip()
    if not tid:
        return _out({"ok": False, "error": "task_id is required"})
    if os.path.exists(os.path.join(RESULTS_DIR, tid + ".md")):
        return _out({"ok": True, "task_id": tid, "status": "done",
                     "note": "already finished"})
    outcome = {}

    def _do(q):
        t = _find_task(q, tid)
        if t is None:
            outcome["error"] = "unknown task_id"
            return
        if (t.get("status") or "pending") == "pending" and not _claim_files(tid):
            t["status"] = "cancelled"
            t["finished_at"] = int(time.time())
            outcome["status"] = "cancelled"
        else:
            t["cancel_requested"] = True
            outcome["status"] = "cancelling"

    _mutate_queue(_do)
    if "error" not in outcome:
        # Outside the queue lock: drop the master + slot claim files so the
        # worker's next claim-verify fails and it stops itself (per the
        # worker prompt rule "claim-verify false -> stop immediately").
        # Idempotent - missing files are ignored. Result files are never
        # touched here.
        for name in _claim_files(tid):
            try:
                os.remove(os.path.join(CLAIMS_DIR, name))
            except OSError:
                pass
    if "error" in outcome:
        return _out({"ok": False, "task_id": tid, "error": outcome["error"]})
    if outcome["status"] == "cancelled":
        return _out({"ok": True, "task_id": tid, "status": "cancelled"})
    return _out({"ok": True, "task_id": tid, "status": "cancelling",
                 "note": "cancel requested; the worker will stop shortly"})


if __name__ == "__main__":
    mcp.run()
