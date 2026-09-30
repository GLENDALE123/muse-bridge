#!/usr/bin/env python3
"""bw.py -- Muse bridge worker mechanics. Worker-side version 3.0.0.

Used by one-shot bridge worker agents (spawned on demand by the supervisor)
and by the supervisor itself. Handles ONLY the queue mechanics (SSH sync,
claims, heartbeats, uploads); the agent itself does the cognitive work
between these calls.

v3.0.0 changes the operating model: no more standing worker pool. One
supervisor agent runs `supervise` in a loop; it computes, per pending task,
how many one-shot workers are missing and prints them as a JSON launch
plan. The supervisor's agent loop launches one workflow per entry. Workers
do one task and exit; nothing idles.

Usage: bw.py --idx N <command> [args...]

Commands:
  sync                          download queue/claims/results listing
  heartbeat <idle|working> [task_id] [assignment] [--progress-at <epoch>] [--stalled]
  goodbye                     delete my heartbeat (local+home) on terminal exit
  run-hb <task_id> <command...>  run a long command with auto-heartbeat
  hb-daemon <task_id>         detached 60s heartbeat loop for one claim
                              (started automatically by claim-new; exits when
                              the claim/task ends)
  progress <task_id> [note]   record an explicit progress timestamp
  pick                          print JSON {action: none|new|join, ...}
  claim-new <task_id>           bid election for the master claim
  claim-verify <task_id>        {"ok": true} iff I still own the master claim
  claim-slot <task_id>          take a free slot on a multi-worker task
  mark-failed <task_id> <reason> drop a failed-task request file for the
                              MCP server (bw.py never writes queue.json
                              directly; the server applies it)
  slice-brief <task_id> <slot>  JSON {prompt, instruction, timeout_minutes, ...}
  slot-instructions <task_id> <slot>
  upload-part <task_id> <slot> <localfile>
  upload-result <task_id> <localfile>
  wait-for-work [interval_secs] blocking wait until pick finds work
  wait-parts <task_id> <K> <timeout_secs>
  check-messages <task_id>  new messages from home since last check (JSON)
  ws-init <task_id>                 create isolated task workspace on home
  ws-pull <task_id> <remote> [name] stage existing home file into workspace
  ws-get <task_id> <name> <local>   download workspace file for local editing
  ws-put <task_id> <local> <name>   upload edited file back to workspace
  ws-ls <task_id>                   list workspace files (JSON)
  ws-apply <task_id> <name> <remote> backup original + overwrite + audit log
  ws-log <task_id>                  print audit log lines (JSONL)
  ws-revert <task_id> <remote>      restore latest backup of a remote file
  ws-clean <task_id>                delete task workspace (backups+log kept)
  reclaim                       delete stale claims
  supervise [block_secs]        print JSON launch plan for missing workers
  task-status <task_id>         fresh status/cancel_requested from queue
  cleanup-parts <task_id>       delete part files after synthesis

Exit codes: 0 ok. wait-parts: 0 all parts present, 2 timeout, 3 stalled,
4 cancelled.
"""
import json
import os
import fcntl
import shutil
import subprocess
import sys
import time

# All human-readable log timestamps are KST (Asia/Seoul, UTC+9): the VM
# runs on UTC but the bridge is operated in the user's timezone. Epoch
# values (claimed_at, updated_at, etc.) stay timezone-agnostic.
KST_OFFSET = 9 * 3600


def _kst_now():
    return time.time() + KST_OFFSET


def _kst_str(fmt="%Y-%m-%d %H:%M:%S", ts=None):
    return time.strftime(fmt, time.gmtime((ts if ts is not None
                                          else time.time()) + KST_OFFSET))


def _kst_hm(ts):
    return _kst_str("%H:%M:%S", ts) if ts else "-"

SSH_CONFIG = "/home/hatch/.ssh/config"
REMOTE = "home"
BRIDGE = "muse-bridge"  # relative to home dir on the home PC
STALE_CLAIM_SECS = 1800
STALE_HB_SECS = 600
# A claim whose owner is alive but whose own heartbeat shows it is no longer
# working on the task (idle, or working on another task) is abandoned, not
# just stale. Only applied to claims older than this grace period, so a
# worker still in its claim-new election (heartbeat not yet refreshed) is
# never mistaken for having abandoned the task.
ABANDON_GRACE_SECS = 120
MAX_EFFECTIVE_WORKERS = 4
WORKER_VERSION = "3.0.0"
# --- on-demand supervisor (v3.0.0) ---
# How fresh a worker heartbeat must be to count that worker as live on its
# task. One-shot workers refresh every ~2-3 minutes during work, so 420s
# leaves ample margin; a crashed worker is detected within ~7 minutes.
LIVE_HB_SECS = 420
# Launch intents (supervisor-local): a worker that was launched but has no
# heartbeat/claim yet. TTL must cover agent boot + claim election (~2 min).
INTENT_TTL_SECS = 240
SUPERVISOR_INTENTS = "/tmp/bw-supervisor-intents.json"
# One-shot worker indices live far above the old pool range (0-7) and the
# supervisor's own idx (99) so heartbeat files never collide.
ONESHOT_IDX_MIN = 100
ONESHOT_IDX_MAX = 199
MAX_CONCURRENT_WORKERS = 50
# --- size budget (2026-09-30, home/Muse agreement) ---
# A single worker task may reference at most this many source lines.
# Oversized tasks enter a churn spiral (missed heartbeat -> replacement ->
# progress reset -> slow -> replaced again). muse_submit enforces this on
# home; measure-source lets the worker double-check tasks that slipped
# through (submitted without source_paths).
SIZE_BUDGET_LINES = 2000
# Runs on home via `python -c` (base64-wrapped to survive cmd.exe quoting).
# Measures files/lines for the given home-side paths; argv[1] is a base64
# JSON list of paths. Prints one JSON line.
_MEASURE_CODE = r'''
import base64, json, os, sys
EXTS = {".ts",".tsx",".js",".jsx",".mjs",".cjs",".py",".java",".kt",".go",".rs",".rb",".php",".swift",".cs",".cpp",".c",".h",".hpp",".vue",".svelte",".css",".scss",".less",".html",".xml",".json",".yaml",".yml",".toml",".md",".txt",".sh",".sql"}
SKIP = {"node_modules",".git","dist","build",".next","out","coverage","__pycache__",".venv","venv","target"}
def walk(root):
    files = lines = 0
    for dp, dn, fn in os.walk(root):
        dn[:] = [d for d in dn if d not in SKIP]
        for f in fn:
            if os.path.splitext(f)[1].lower() not in EXTS:
                continue
            p = os.path.join(dp, f)
            try:
                fh = open(p, "r", encoding="utf-8", errors="replace")
                n = sum(1 for _ in fh)
                fh.close()
            except OSError:
                continue
            files += 1
            lines += n
    return files, lines
paths = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
per = []
tf = 0
tl = 0
for raw in paths:
    p = os.path.expanduser(raw)
    if os.path.isfile(p):
        try:
            fh = open(p, "r", encoding="utf-8", errors="replace")
            n = sum(1 for _ in fh)
            fh.close()
        except OSError:
            n = 0
        per.append({"path": raw, "files": 1, "lines": n})
        tf += 1
        tl += n
    elif os.path.isdir(p):
        f, n = walk(p)
        per.append({"path": raw, "files": f, "lines": n})
        tf += f
        tl += n
    else:
        per.append({"path": raw, "files": 0, "lines": 0, "missing": True})
print(json.dumps({"ok": True, "paths": per, "total_files": tf, "total_lines": tl}, ensure_ascii=True))
'''
# Bid-election stabilization: how long a bidder waits for rival bids to
# become visible before deciding the winner. Must comfortably exceed the
# slowest single scp_put (~a few seconds over the proxy); 3s proved too
# short and caused a double master-claim on 2026-09-29.
ELECTION_SETTLE_SECS = 8
# Runs on home via `python -c` (base64-wrapped to survive cmd.exe quoting).
# Locked queue.json status flip for _mark_task_done: executes ON HOME so it
# can take the SAME inter-process queue.lock the MCP server uses
# (open a+b, msvcrt.locking LK_LOCK 1 byte at offset 0 — see
# bridge_mcp.py _QueueFileLock). argv[1] is the task id. Prints one JSON line.
_MARKDONE_CODE = r'''
import base64, json, msvcrt, os, sys
BRIDGE_DIR = "C:\\Users\\ghfud\\muse-bridge"
tid = sys.argv[1]
lockp = os.path.join(BRIDGE_DIR, "queue.lock")
qp = os.path.join(BRIDGE_DIR, "queue.json")
fd = os.open(lockp, os.O_RDWR | os.O_CREAT)
changed = False
try:
    msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
    try:
        with open(qp, encoding="utf-8") as f:
            q = json.load(f)
        for t in q.get("tasks", []):
            if str(t.get("id")) == tid and (t.get("status") or "pending") == "pending":
                t["status"] = "done"
                changed = True
        if changed:
            tmp = qp + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(q, f, ensure_ascii=False, indent=1)
            os.replace(tmp, qp)
    finally:
        try:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    print(json.dumps({"ok": True, "changed": changed}, ensure_ascii=True))
finally:
    os.close(fd)
'''
# After writing a master/slot claim, re-read it after this delay; if another
# worker overwrote it in the meantime, concede instead of executing.
CLAIM_WRITEBACK_CHECK_SECS = 3
# --- hb-daemon (2026-09-30, claim-scoped deterministic heartbeat) ---
# Interval between daemon heartbeats. The daemon is a detached process
# spawned by claim-new; it owns "liveness" so the worker agent can forget
# the heartbeat discipline without looking dead. Progress (real work) is
# tracked separately via `progress` / the structural alog.
HB_DAEMON_INTERVAL_SECS = 60
# Stalled window: no progress for min(20 min, timeout_minutes/3) counts as
# stalled. 20 minutes is the absolute ceiling.
STALLED_MAX_SECS = 1200
# Structured per-command log (JSON lines), shared across all worker
# indices on this VM. fcntl-guarded append; best effort, never fails the
# caller. Used for claim-verify / task-status / ws-get / ws-ls forensics.
STRUCTLOG_PATH = os.path.expanduser(
    "~/workspace/muse-bridge/worker/logs/bw-structured.log")
# Replacement bookkeeping lives under claims/meta/ on home (NOT directly
# under claims/): files matching claims/*.json are parsed as master/slot
# claims by claim_names(), so meta files must stay out of that namespace.
CLAIMS_META_DIR = "claims/meta"
# A task replaced this many times with no progress is failed, not
# relaunched (failed_churn).
MAX_REPLACEMENTS_NO_PROGRESS = 3


def sh(args, timeout=60):
    try:
        p = subprocess.run(
            ["ssh", "-F", SSH_CONFIG, "-o", "BatchMode=yes",
             "-o", "ConnectTimeout=15", REMOTE] + args,
            capture_output=True, text=True, errors="replace", timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except (subprocess.TimeoutExpired, OSError) as e:
        return 99, "", str(e)


def scp_get(remote, local, recursive=False, timeout=90):
    cmd = ["scp", "-F", SSH_CONFIG, "-o", "BatchMode=yes",
           "-o", "ConnectTimeout=15"]
    if recursive:
        cmd.append("-r")
    cmd += ["%s:%s" % (REMOTE, remote), local]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def scp_put(local, remote, timeout=90):
    try:
        p = subprocess.run(
            ["scp", "-F", SSH_CONFIG, "-o", "BatchMode=yes",
             "-o", "ConnectTimeout=15", local, "%s:%s" % (REMOTE, remote)],
            capture_output=True, text=True, timeout=timeout)
        return p.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def ssh_del(remote_pattern):
    # best effort delete; pattern may include wildcards
    rc, _, _ = sh(["del", "/f", "/q", remote_pattern.replace("/", "\\")], timeout=30)
    return rc == 0


def now():
    return int(time.time())


class BW:
    def __init__(self, idx):
        self.idx = idx
        self.dir = "/tmp/bw-%d" % idx
        os.makedirs(self.dir, exist_ok=True)
        os.makedirs(os.path.join(self.dir, "claims"), exist_ok=True)

    def qpath(self):
        return os.path.join(self.dir, "queue.json")

    # ---------- sync ----------
    def _sync(self):
        ok = scp_get(BRIDGE + "/queue.json", self.qpath())
        # claims dir (best effort)
        shutil.rmtree(os.path.join(self.dir, "claims"), ignore_errors=True)
        scp_get(BRIDGE + "/claims", self.dir, recursive=True)
        rc, out, _ = sh(["cmd", "/c",
                         "dir", "/b", "C:\\Users\\ghfud\\muse-bridge\\results"], timeout=30)
        try:
            with open(os.path.join(self.dir, "results.txt"), "w") as f:
                f.write(out if rc == 0 else "")
        except OSError:
            pass
        return ok

    def cmd_sync(self, a=None):
        print("SYNC_OK" if self._sync() else "SYNC_FAIL")

    def _sync_status(self):
        """Download home's status/ (worker heartbeat files) to the local
        cache. Best effort; used by the stale-heartbeat reaper. Kept
        separate from _sync() so high-frequency worker commands don't pay
        for it — only supervise/reclaim need the full status dir."""
        shutil.rmtree(os.path.join(self.dir, "status"), ignore_errors=True)
        scp_get(BRIDGE + "/status", self.dir, recursive=True)

    def load_queue(self):
        try:
            with open(self.qpath(), encoding="utf-8") as f:
                q = json.load(f)
            if isinstance(q, dict) and isinstance(q.get("tasks"), list):
                return q
        except (OSError, ValueError):
            pass
        return {"tasks": []}

    def find_task(self, task_id):
        for t in self.load_queue().get("tasks", []):
            if str(t.get("id")) == str(task_id):
                return t
        return None

    def claim_names(self):
        d = os.path.join(self.dir, "claims")
        try:
            return [n for n in os.listdir(d) if n.endswith(".json")]
        except OSError:
            return []

    def result_names(self):
        try:
            with open(os.path.join(self.dir, "results.txt"), encoding="utf-8",
                      errors="replace") as f:
                return [l.strip() for l in f if l.strip()]
        except OSError:
            return []

    def read_claim(self, name):
        try:
            with open(os.path.join(self.dir, "claims", name), encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    def heartbeat_of(self, worker):
        # read from the last synced?? heartbeats live on home; fetch directly
        rc, out, _ = sh(["cmd", "/c", "type",
                         "C:\\Users\\ghfud\\muse-bridge\\status\\worker-%s.json" % worker],
                        timeout=30)
        if rc != 0:
            return None
        try:
            return json.loads(out)
        except ValueError:
            return None

    # ---------- heartbeat ----------
    def cmd_heartbeat(self, a):
        a = list(a or [])
        # optional trailing flags (parsed from the end so a free-text note
        # is unaffected): --progress-at <epoch> --stalled
        progress_at = None
        stalled = None
        rest = []
        i = 0
        while i < len(a):
            if a[i] == "--progress-at" and i + 1 < len(a):
                try:
                    progress_at = int(a[i + 1])
                except (ValueError, TypeError):
                    progress_at = None
                i += 2
            elif a[i] == "--stalled":
                stalled = True
                i += 1
            else:
                rest.append(a[i])
                i += 1
        state = rest[0] if len(rest) > 0 else "idle"
        task_id = rest[1] if len(rest) > 1 else None
        assignment = int(rest[2]) if len(rest) > 2 else None
        note = rest[3] if len(rest) > 3 else None
        if not self._heartbeat(state, task_id, assignment, note,
                               progress_at=progress_at, stalled=stalled):
            print("HB_FAIL")
            return
        # pure report, no side effects: tell the caller whether home has
        # cancelled this task so it can stop on its own terms.
        if task_id and self._task_cancelled(task_id):
            print("HB_OK_CANCELLED")
        else:
            print("HB_OK")

    def cmd_goodbye(self, a):
        """goodbye — terminal exit: delete this worker's heartbeat file both
        locally and on home, so the supervisor sees the worker as gone
        immediately instead of waiting out the silence window (420s) or the
        stale reaper (840s/24h). Call once on every terminal exit path
        (task done, claim lost, cancelled, timeout) instead of
        'heartbeat idle'. The idle heartbeat is for pauses; goodbye is for
        exits. Prints GOODBYE_OK, or GOODBYE_LOCAL_OK if the remote delete
        failed (local file still removed)."""
        self._stop_all_hb_daemons()
        for p in (os.path.join(self.dir, "hb.json"), self._note_path()):
            try:
                os.unlink(p)
            except OSError:
                pass
        ok = ssh_del(BRIDGE + "/status/worker-%d.json" % self.idx)
        print("GOODBYE_OK" if ok else "GOODBYE_LOCAL_OK")

    def cmd_run_hb(self, a):
        """run-hb <task_id> <command...>  Run a long command with auto-heartbeat.

        Structural fix for false-death: a worker busy in a long phase
        (tests, builds, big transfers) no longer needs to remember to
        heartbeat. The heartbeat fires inside this call's wait loop every
        60s, so even a distracted worker stays visibly alive and its claim
        is never reclaimed mid-work. Prints the command's combined output
        (truncated past 20k chars) and exits with the command's code.

        The child's output goes to a temp file, never a PIPE, so large
        output cannot deadlock the wait loop. Cancel-aware: every 60s tick
        also checks home's queue.json for a cancel; on cancel the child is
        SIGTERM'd (SIGKILL after 5s), an idle heartbeat is sent,
        RUN_HB_CANCELLED is printed, and the exit code is 3.
        """
        if len(a) < 2:
            print("usage: run-hb <task_id> <command...>", file=sys.stderr)
            return 2
        tid, cmd = a[0], a[1:]
        import subprocess
        import time
        out_path = os.path.join(self.dir, "runhb-%s.out" % tid)
        try:
            out_f = open(out_path, "wb")
        except OSError as e:
            print("RUN_HB_SPAWN_FAIL: %s" % e, file=sys.stderr)
            return 2
        try:
            try:
                p = subprocess.Popen(cmd, stdout=out_f,
                                     stderr=subprocess.STDOUT)
            except OSError as e:
                print("RUN_HB_SPAWN_FAIL: %s" % e, file=sys.stderr)
                return 2
            # the child holds its own fd now; close ours so a lingering
            # writer cannot keep the file (or the read) open
            out_f.close()
            out_f = None
            # code-generated progress note: the running command, so a long
            # silent phase (tests, builds) is visible without trusting the
            # worker to narrate itself.
            run_note = "run: " + " ".join(cmd)[:120]
            self._alog(tid, run_note)
            self._heartbeat("working", tid, note=run_note)
            # 10s ticks so we notice child exit promptly; heartbeat every 60s.
            ticks = 0
            while p.poll() is None:
                time.sleep(10)
                ticks += 1
                if p.poll() is None and ticks % 6 == 0:
                    if self._task_cancelled(tid):
                        # structural cancel: stop the child instead of
                        # burning a worker slot on a dead task
                        try:
                            p.terminate()
                        except OSError:
                            pass
                        for _ in range(50):
                            if p.poll() is not None:
                                break
                            time.sleep(0.1)
                        if p.poll() is None:
                            try:
                                p.kill()
                            except OSError:
                                pass
                        p.wait()
                        self._heartbeat("idle", tid)
                        print("RUN_HB_CANCELLED")
                        sys.stdout.flush()
                        return 3
                    self._heartbeat("working", tid, note=run_note)
            p.wait()
            out = ""
            try:
                with open(out_path, "rb") as f:
                    out = f.read().decode("utf-8", errors="replace")
            except OSError:
                pass
            if out:
                if len(out) > 20000:
                    out = out[:20000] + "\n...[truncated %d chars]" % (len(out) - 20000)
                sys.stdout.write(out)
                sys.stdout.flush()
            print("RUN_HB_EXIT=%d" % p.returncode)
            return p.returncode
        finally:
            try:
                if out_f is not None:
                    out_f.close()
            except OSError:
                pass
            try:
                os.unlink(out_path)
            except OSError:
                pass

    # ---------- pick ----------
    def _pick_action(self):
        """Decision core of pick: returns the action dict (no printing)."""
        q = self.load_queue()
        results = set(self.result_names())
        # index claims by task
        masters = {}   # tid -> master claim dict
        slots = {}     # tid -> set of slot numbers taken
        for n in self.claim_names():
            base = n[:-5]
            if ".bid-" in base or ".slotbid-" in base:
                continue  # in-flight election bids, not real claims
            if ".a" in base:
                tid, _, s = base.partition(".a")
                try:
                    slots.setdefault(tid, set()).add(int(s))
                except ValueError:
                    pass
            else:
                c = self.read_claim(n)
                if c:
                    masters[base] = c
        # 1) joinable multi-worker tasks first
        best = None
        for tid, mc in masters.items():
            if tid + ".md" in results:
                continue
            t = next((x for x in q.get("tasks", []) if str(x.get("id")) == tid), None)
            if t and (t.get("status") or "pending") == "cancelled":
                continue
            if t and t.get("cancel_requested"):
                continue
            k = int(mc.get("workers_effective", 1) or 1)
            taken = set(slots.get(tid, set())) | {0}
            if len(taken) >= k:
                continue
            # don't join twice
            mine = False
            for n in self.claim_names():
                base = n[:-5]
                c = self.read_claim(n)
                if c and str(c.get("worker")) == str(self.idx) and (
                        base == tid or base.startswith(tid + ".a")):
                    mine = True
                    break
            if mine:
                continue
            free = [j for j in range(k) if j not in taken]
            if not free:
                continue
            cand = (len(taken), int(mc.get("claimed_at", 0)), tid)
            if best is None or cand < best[0]:
                best = (cand, {"action": "join", "task_id": tid,
                               "slot": min(free), "workers_effective": k,
                               "task": t})
        if best:
            return best[1]
        # 2) new tasks: pending, no result — by (-priority, created_at).
        # A task with slot claims but no master claim is an orphan (its
        # master died or was reclaimed while slot workers were still alive).
        # It is offered as "new" so a fresh master election adopts the live
        # slots and synthesizes from their parts instead of leaving the task
        # stuck forever. claim-new's bid election + writeback check still
        # guard against a double master from a transient read failure.
        cands = []
        for t in q.get("tasks", []):
            tid = str(t.get("id"))
            if (t.get("status") or "pending") != "pending":
                continue
            if tid + ".md" in results:
                continue
            if tid in masters:
                continue
            if tid in slots:
                # orphan slots: don't re-master a task I already hold a
                # slot on (I'd wait on my own part forever)
                mine = False
                for n in self.claim_names():
                    base = n[:-5]
                    c = self.read_claim(n)
                    if c and str(c.get("worker")) == str(self.idx) and (
                            base == tid or base.startswith(tid + ".a")):
                        mine = True
                        break
                if mine:
                    continue
            cands.append((-(int(t.get("priority", 0) or 0)),
                          int(t.get("created_at", 0) or 0), t))
        if cands:
            cands.sort(key=lambda x: (x[0], x[1]))
            t = cands[0][2]
            return {"action": "new", "task_id": str(t["id"]), "task": t}
        return {"action": "none"}

    def cmd_pick(self, a=None):
        print(json.dumps(self._pick_action(), ensure_ascii=True))

    # ---------- wait-for-work ----------
    def _quick_queue(self):
        """One SSH call fetching just queue.json. Returns the parsed queue
        dict, or None on failure. Hot-loop check - ~1s, no LLM involved."""
        rc, out, _ = sh(["cmd", "/c", "type",
                         "C:\\Users\\ghfud\\muse-bridge\\queue.json"],
                        timeout=30)
        if rc != 0:
            return None
        try:
            q = json.loads(out)
        except ValueError:
            return None
        if isinstance(q, dict) and isinstance(q.get("tasks"), list):
            return q
        return None

    def _seen_path(self):
        return os.path.join(self.dir, "seen-tasks.json")

    def _load_seen(self):
        try:
            with open(self._seen_path(), encoding="utf-8") as f:
                d = json.load(f)
            s = d.get("seen")
            return set(s) if isinstance(s, list) else set()
        except (OSError, ValueError):
            return set()

    def _save_seen(self, seen):
        try:
            with open(self._seen_path(), "w", encoding="utf-8") as f:
                json.dump({"seen": sorted(seen)}, f)
        except OSError:
            pass

    @staticmethod
    def _pending_ids(q):
        return [str(t.get("id")) for t in q.get("tasks", [])
                if (t.get("status") or "pending") == "pending" and t.get("id")]

    def cmd_wait_for_work(self, a=None):
        """Block until there is work. Hot loop = one SSH call (~1s) reading
        only queue.json; the expensive full sync + pick decision runs only
        when a *new* pending task appears (tracked in seen-tasks.json) or on
        the periodic sweep every ~5 minutes (catches join opportunities and
        recovers missed races). No LLM involvement while idle.
        Heartbeats as idle and reclaims stale claims periodically.
        Prints the pick JSON (action new|join) and exits when work appears.
        """
        try:
            interval = float(a[0]) if a else 2.0
        except (ValueError, TypeError, IndexError):
            interval = 2.0
        interval = max(1.0, min(30.0, interval))
        seen = self._load_seen()
        n = 0
        fails = 0
        force_pick = False
        while True:
            n += 1
            q = self._quick_queue()
            if q is None:
                fails += 1
                if fails >= 3:
                    time.sleep(60)
                    fails = 0
                else:
                    time.sleep(interval)
                continue
            fails = 0
            pending = self._pending_ids(q)
            new_ids = [tid for tid in pending if tid not in seen]
            periodic = (n % 150 == 0)  # ~5 min sweep at 2s interval
            woke = False
            if (new_ids or periodic or force_pick) and self._sync():
                force_pick = False
                d = self._pick_action()
                if d.get("action") != "none":
                    tid = str(d.get("task_id", ""))
                    if tid:
                        seen.add(tid)
                    # drop tasks that are no longer pending
                    seen = {t for t in seen if t in pending} | ({tid} if tid else set())
                    self._save_seen(seen)
                    print(json.dumps(d, ensure_ascii=True), flush=True)
                    return
                woke = True
            if woke or periodic:
                # pick found nothing actionable: remember what we saw so the
                # hot loop stays quiet; the periodic sweep retries later
                seen = seen | set(pending)
                self._save_seen(seen)
            if n == 1 or n % 20 == 0:
                self._heartbeat("idle")
            if n % 60 == 0:
                try:
                    if self._reclaim():
                        # stale/abandoned claims were freed: re-evaluate
                        # immediately instead of waiting for the next
                        # 5-minute sweep
                        force_pick = True
                except Exception:
                    pass
            time.sleep(interval)

    # ---------- replacements + first_claimed_at ----------
    # The timeout clock must survive worker replacement: claim-new records
    # first_claimed_at (inherited, never overwritten) and every replacement
    # is journaled in claims/meta/<tid>.replacements.json on home. The meta/
    # subdir keeps these files out of the claims/*.json namespace, which
    # claim_names() parses as master/slot claims.
    def _replacements_remote(self, tid):
        return BRIDGE + "/" + CLAIMS_META_DIR + "/%s.replacements.json" % tid

    def _ensure_claims_meta(self):
        sh(["cmd", "/c", "if", "not", "exist",
            "C:\\Users\\ghfud\\muse-bridge\\claims\\meta", "mkdir",
            "C:\\Users\\ghfud\\muse-bridge\\claims\\meta"], timeout=30)

    def _read_replacements(self, tid, fresh=False):
        """Read the replacements record. fresh=True re-downloads from home
        (claim-new needs it: the local cache may predate the supervise pass
        that wrote it); otherwise the pass-local synced copy is used."""
        if fresh:
            lp = os.path.join(self.dir, "repl-%s.json" % tid)
            if not scp_get(self._replacements_remote(tid), lp):
                return None
        else:
            lp = os.path.join(self.dir, "claims", "meta",
                              "%s.replacements.json" % tid)
        try:
            with open(lp, encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else None
        except (OSError, ValueError):
            return None

    def _write_replacements(self, tid, rep):
        lp = os.path.join(self.dir, "repl-%s.json" % tid)
        try:
            with open(lp, "w", encoding="utf-8") as f:
                json.dump(rep, f, ensure_ascii=True)
        except OSError:
            return False
        self._ensure_claims_meta()
        return scp_put(lp, self._replacements_remote(tid))

    def _resolve_first_claimed_at(self, tid, task):
        """first_claimed_at for a new claim, by priority:
        1) the replacements record (a previous worker was here),
        2) a still-present old claim (reclaim hasn't run),
        3) the queue task's created_at,
        4) now."""
        rep = self._read_replacements(tid, fresh=True)
        if rep:
            try:
                v = int(rep.get("first_claimed_at") or 0)
                if v > 0:
                    return v, True
            except (ValueError, TypeError):
                pass
        mc = self._read_master_claim(tid)
        if mc:
            try:
                v = int(mc.get("first_claimed_at")
                        or mc.get("claimed_at") or 0)
                if v > 0:
                    return v, True
            except (ValueError, TypeError):
                pass
        if task:
            try:
                v = int(task.get("created_at") or task.get("created") or 0)
                if v > 0:
                    return v, False
            except (ValueError, TypeError):
                pass
        return now(), False

    # ---------- claim-new ----------
    # Distributed election via bid files: upload a uniquely-named bid, wait
    # for rival bids to settle, then the earliest (claimed_at, worker) bid
    # wins and writes the master claim. Two-phase commit against the
    # 2026-09-29 double-win: (1) the "no master claim" check happens AFTER
    # the bid listing, immediately before the write; (2) after writing, the
    # winner re-reads the claim and concedes if it was overwritten.
    # Workers must ALSO run claim-verify before each mutation phase.
    def _list_bids(self, tid, pattern):
        """Download every bid file matching pattern; return sorted list of
        (claimed_at, worker, filename)."""
        rc, out, _ = sh(["cmd", "/c", "dir", "/b", pattern], timeout=30)
        bids = []
        if rc == 0:
            for line in out.splitlines():
                bn = line.strip()
                if not bn:
                    continue
                dest = os.path.join(self.dir, "bid.json")
                if scp_get(BRIDGE + "/claims/" + bn, dest):
                    try:
                        with open(dest, encoding="utf-8") as f:
                            b = json.load(f)
                        bids.append((int(b.get("claimed_at", 0)),
                                     int(b.get("worker", 999)), bn))
                    except (OSError, ValueError):
                        pass
        bids.sort()
        return bids

    def _read_master_claim(self, tid):
        lp = os.path.join(self.dir, "master.json")
        if not scp_get(BRIDGE + "/claims/%s.json" % tid, lp):
            return None
        try:
            with open(lp, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    def cmd_claim_new(self, a):
        tid = a[0]
        self._alog(tid, "claim-new")
        t = self.find_task(tid)
        if t is None:
            print(json.dumps({"ok": False, "reason": "task gone"}))
            return
        # someone already won? (fast path; repeated after the election too)
        if self._read_master_claim(tid) is not None:
            print(json.dumps({"ok": False, "reason": "already claimed"}))
            return
        epoch = now()
        bidname = "%s.bid-%d-%d.json" % (tid, self.idx, epoch)
        bid = {"id": tid, "worker": self.idx, "claimed_at": epoch}
        lp = os.path.join(self.dir, "master.json")
        with open(lp, "w", encoding="utf-8") as f:
            json.dump(bid, f, ensure_ascii=True)
        if not scp_put(lp, BRIDGE + "/claims/" + bidname):
            print(json.dumps({"ok": False, "reason": "bid upload failed"}))
            return
        # let slow rival bid uploads land before electing
        time.sleep(ELECTION_SETTLE_SECS)
        bids = self._list_bids(
            tid, "C:\\Users\\ghfud\\muse-bridge\\claims\\%s.bid-*.json" % tid)
        if not bids:
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "no bids visible"}))
            return
        if bids[0][1] != self.idx:
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "conflict"}))
            return
        # I won the bid round: re-check the master claim immediately before
        # writing (a rival may have won a parallel round and written already)
        if self._read_master_claim(tid) is not None:
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "already claimed"}))
            return
        # I won: write the master claim, clean up all bid files.
        # first_claimed_at is inherited (never reset): the timeout clock
        # survives replacement. See _resolve_first_claimed_at.
        k = max(1, min(MAX_EFFECTIVE_WORKERS, int(t.get("workers", 1) or 1)))
        instr = t.get("worker_instructions") or []
        if not isinstance(instr, list):
            instr = [str(instr)]
        first_claimed_at, inherited = self._resolve_first_claimed_at(tid, t)
        claim = {"id": tid, "assignment": 0, "worker": self.idx,
                 "workers_requested": int(t.get("workers", 1) or 1),
                 "workers_effective": k, "claimed_at": now(),
                 "first_claimed_at": first_claimed_at,
                 "instructions": instr,
                 "prompt": t.get("prompt", ""),
                 "label": t.get("label", ""),
                 "timeout_minutes": int(t.get("timeout_minutes", 60) or 60)}
        with open(lp, "w", encoding="utf-8") as f:
            json.dump(claim, f, ensure_ascii=True)
        if not scp_put(lp, BRIDGE + "/claims/%s.json" % tid):
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "claim upload failed"}))
            return
        for _, _, bn in bids:
            ssh_del(BRIDGE + "/claims/" + bn)
        # write-back check: if a rival overwrote the claim in the meantime,
        # concede instead of executing as a phantom second master
        time.sleep(CLAIM_WRITEBACK_CHECK_SECS)
        mc = self._read_master_claim(tid)
        if mc is None or int(mc.get("worker", -1)) != self.idx:
            print(json.dumps({"ok": False, "reason": "overwritten"}))
            return
        # claim won: start the detached liveness daemon, and seed the
        # replacements record (count 0) so supervise can journal later
        # replacements and inherit first_claimed_at across them.
        if not inherited:
            self._write_replacements(
                tid, {"task_id": tid,
                      "first_claimed_at": first_claimed_at,
                      "count": 0, "history": []})
        self._start_hb_daemon(tid)
        print(json.dumps({"ok": True, "effective_k": k,
                          "prompt": t.get("prompt", ""),
                          "label": t.get("label", ""),
                          "timeout_minutes": int(t.get("timeout_minutes", 60) or 60),
                          "claimed_at": claim["claimed_at"],
                          "first_claimed_at": first_claimed_at,
                          "source_size": t.get("source_size")}))

    # ---------- claim-verify ----------
    # Fresh ownership check: {"ok": true} iff I still own the task — via the
    # master claim OR via one of my slot claims (slot-aware: a slot worker
    # owns the task even though the master claim names someone else).
    # Workers call this after claim-new / claim-slot AND before every
    # mutation phase; a phantom second master or a reclaimed slot aborts.
    # The upload/ws commands additionally enforce the same check internally
    # (write fence), so a mutation is blocked even if the agent skips this.
    def cmd_claim_verify(self, a):
        tid = a[0]
        role = self._owns_task(tid)
        if role is None:
            self._slog("claim-verify", tid, {"ok": False, "reason": "not owner"})
            print(json.dumps({"ok": False, "reason": "not owner"}))
            return
        self._slog("claim-verify", tid, {"ok": True, "role": role})
        print(json.dumps({"ok": True, "role": role}))

    # ---------- claim-slot ----------
    # Same bid election as claim-new, scoped to one slot number. Same
    # two-phase protection: slot-taken is re-checked after the election,
    # and the written slot claim is read back before reporting ok.
    def cmd_claim_slot(self, a):
        tid = a[0]
        self._alog(tid, "claim-slot")
        mc = self._read_master_claim(tid)
        if mc is None:
            print(json.dumps({"ok": False, "reason": "no master claim"}))
            return
        k = int(mc.get("workers_effective", 1) or 1)
        # refresh taken slots
        rc, out, _ = sh(["cmd", "/c", "dir", "/b",
                         "C:\\Users\\ghfud\\muse-bridge\\claims\\%s.a*.json" % tid],
                        timeout=30)
        taken = {0}
        if rc == 0:
            for line in out.splitlines():
                base = line.strip()[:-5]
                _, _, s = base.partition(".a")
                try:
                    taken.add(int(s))
                except ValueError:
                    pass
        free = [j for j in range(k) if j not in taken]
        if not free:
            print(json.dumps({"ok": False, "reason": "no free slot"}))
            return
        j = min(free)
        epoch = now()
        bidname = "%s.slotbid-%d-%d-%d.json" % (tid, j, self.idx, epoch)
        bid = {"id": tid, "assignment": j, "worker": self.idx,
               "claimed_at": epoch}
        lp = os.path.join(self.dir, "master.json")
        with open(lp, "w", encoding="utf-8") as f:
            json.dump(bid, f, ensure_ascii=True)
        if not scp_put(lp, BRIDGE + "/claims/" + bidname):
            print(json.dumps({"ok": False, "reason": "bid upload failed"}))
            return
        time.sleep(ELECTION_SETTLE_SECS)
        bids = self._list_bids(
            tid, "C:\\Users\\ghfud\\muse-bridge\\claims\\%s.slotbid-%d-*.json" % (tid, j))
        if not bids:
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "no bids visible"}))
            return
        if bids[0][1] != self.idx:
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "taken"}))
            return
        # re-check the slot immediately before writing
        rc, out, _ = sh(["cmd", "/c", "dir", "/b",
                         "C:\\Users\\ghfud\\muse-bridge\\claims\\%s.a%d.json" % (tid, j)],
                        timeout=30)
        if rc == 0 and out.strip():
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "slot taken"}))
            return
        claim = {"id": tid, "assignment": j, "worker": self.idx,
                 "claimed_at": now()}
        with open(lp, "w", encoding="utf-8") as f:
            json.dump(claim, f, ensure_ascii=True)
        if not scp_put(lp, BRIDGE + "/claims/%s.a%d.json" % (tid, j)):
            ssh_del(BRIDGE + "/claims/" + bidname)
            print(json.dumps({"ok": False, "reason": "claim upload failed"}))
            return
        for _, _, bn in bids:
            ssh_del(BRIDGE + "/claims/" + bn)
        time.sleep(CLAIM_WRITEBACK_CHECK_SECS)
        rc, out, _ = sh(["cmd", "/c", "type",
                         "C:\\Users\\ghfud\\muse-bridge\\claims\\%s.a%d.json" % (tid, j)],
                        timeout=30)
        try:
            back = json.loads(out) if rc == 0 else {}
        except ValueError:
            back = {}
        if int(back.get("worker", -1)) != self.idx:
            print(json.dumps({"ok": False, "reason": "overwritten"}))
            return
        print(json.dumps({"ok": True, "slot": j}))

    # ---------- slot-instructions ----------
    def cmd_slot_instructions(self, a):
        tid, slot = a[0], int(a[1])
        lp = os.path.join(self.dir, "master.json")
        if not scp_get(BRIDGE + "/claims/%s.json" % tid, lp):
            print("")
            return
        try:
            with open(lp, encoding="utf-8") as f:
                mc = json.load(f)
            instr = mc.get("instructions") or []
            print(instr[slot] if slot < len(instr) else "")
        except (OSError, ValueError, IndexError):
            print("")

    # ---------- uploads ----------
    def cmd_upload_part(self, a):
        tid, slot, local = a[0], a[1], a[2]
        self._alog(tid, "upload-part slot %s" % slot)
        # write fence: the master may only upload part 0, a slot worker only
        # its own slot's part. Holds even when the agent skips claim-verify.
        try:
            slot_n = int(slot)
        except (ValueError, TypeError):
            slot_n = -1
        role = self._owns_task(tid)
        allowed = (role == "master" and slot_n == 0) or role == "slot:%d" % slot_n
        if not allowed:
            print(json.dumps({"ok": False, "error": "not owner",
                              "reason": "upload-part blocked: no claim"}))
            return
        ok = scp_put(local, BRIDGE + "/results/%s.part-%s.md" % (tid, slot))
        self._ship_alog(tid)
        print("UPLOAD_OK" if ok else "UPLOAD_FAIL")

    def cmd_upload_result(self, a):
        tid, local = a[0], a[1]
        self._alog(tid, "upload-result")
        # write fence: only the live master may publish the final result
        if self._owns_task(tid) != "master":
            print(json.dumps({"ok": False, "error": "not owner",
                              "reason": "upload-result blocked: not master"}))
            return
        ok = scp_put(local, BRIDGE + "/results/%s.md" % tid)
        if ok:
            # mark the task done in queue.json so the queue view stays
            # accurate (previously the status stayed "pending" forever)
            self._mark_task_done(tid)
            # the claim's life is over: stop the liveness daemon now
            # instead of waiting for its next 60s tick to notice
            self._stop_hb_daemon(tid)
        self._ship_alog(tid)
        print("UPLOAD_OK" if ok else "UPLOAD_FAIL")

    def _mark_task_done(self, tid):
        """Flip a task's queue.json status to "done" after its result is
        published.

        OWNERSHIP: queue.json is owned by the home MCP server
        (bridge_mcp.py). bw.py must NOT write it directly as a rule — new
        code paths must use request files (e.g. mark-failed) and let the
        server apply them. This method is the one legacy exception
        (upload-result -> done flip), kept because the queue view would
        otherwise stay "pending" forever despite the result file existing.

        Because it still touches queue.json, it goes through the SAME
        inter-process queue.lock protocol the MCP server uses: the
        read-modify-write executes ON HOME (python -c via SSH) holding
        queue.lock with msvcrt.locking(LK_LOCK, 1 byte at offset 0) —
        see bridge_mcp.py _QueueFileLock. A concurrent muse_submit /
        muse_cancel can no longer interleave a lost update (the old
        scp_get -> local edit -> scp_put race). There is deliberately NO
        unlocked fallback: if the home-side script fails we return False
        and leave the task pending — an unlocked direct write would
        violate the queue.lock protocol (2026-09-30 requirement). The
        supervise pass already excludes tasks with results from launch
        plans, so the worst case is a stale "pending" label, never a
        corrupted queue.
        """
        import base64 as _b64
        blob = _b64.b64encode(_MARKDONE_CODE.encode("utf-8")).decode("ascii")
        outer = ("import base64,sys;"
                 "exec(base64.b64decode('%s').decode('utf-8'))" % blob)
        rc, out, _ = sh(["python", "-c", '"%s"' % outer, tid], timeout=60)
        if rc == 0:
            for line in out.splitlines():
                line = line.strip()
                if line.startswith("{"):
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    if d.get("ok"):
                        return True
        return False

    def _note_path(self):
        return os.path.join(self.dir, "hb-note.txt")

    # ---------- structural activity log ----------
    # Free, trustworthy "what is the worker doing": every task-scoped bw.py
    # call appends a phase line locally; the buffer ships to home's
    # activity/<tid>.bw-<idx>.log on heartbeat/uploads (overwrite is safe:
    # the local file is the complete append-only source, one file per
    # worker so slot workers never clobber each other). This is code-
    # generated ground truth, not prompt-dependent self-reporting. The
    # DB-derived <tid>.log keeps the per-tool-call forensic detail.
    def _alog_path(self, tid):
        return os.path.join(self.dir, "activity-%s.log" % tid)

    def _alog(self, tid, event):
        if not tid or not self._ws_ok(tid):
            return
        line = "[%s] [w%d] %s\n" % (
            _kst_str("%H:%M:%S"), self.idx, event)
        try:
            with open(self._alog_path(tid), "a", encoding="utf-8") as f:
                f.write(line)
        except OSError:
            pass

    def _ship_alog(self, tid):
        if not tid:
            return
        lp = self._alog_path(tid)
        if not os.path.exists(lp):
            return
        scp_put(lp, BRIDGE + "/activity/%s.bw-%d.log" % (tid, self.idx))

    def _slog(self, cmd, tid, result):
        """Structured one-line JSON log for forensics. Shared file across
        all worker indices on this VM; fcntl-guarded append so concurrent
        workers never interleave lines. Best effort — never fails the
        caller. Fields: ts, time (local), cmd, task_id, worker, result."""
        try:
            os.makedirs(os.path.dirname(STRUCTLOG_PATH), exist_ok=True)
            line = json.dumps({"ts": now(),
                               "time": _kst_str(),
                               "cmd": cmd, "task_id": tid, "worker": self.idx,
                               "result": result}, ensure_ascii=True)
            with open(STRUCTLOG_PATH, "a", encoding="utf-8") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(line + "\n")
                f.flush()
        except OSError:
            pass

    def _heartbeat(self, state, task_id=None, assignment=None, note=None,
                   progress_at=None, stalled=None):
        # Note persistence: an explicit note is saved; a later call without
        # a note re-attaches the last one (so _maybe_heartbeat never wipes
        # it); going idle clears it.
        if state == "idle":
            note = None
            try:
                os.unlink(self._note_path())
            except OSError:
                pass
        elif note is not None:
            try:
                with open(self._note_path(), "w", encoding="utf-8") as f:
                    f.write(note[:200])
            except OSError:
                pass
        else:
            try:
                with open(self._note_path(), encoding="utf-8") as f:
                    note = f.read()[:200] or None
            except (OSError, ValueError):
                pass
        # last_progress_at / stalled separate "alive" from "making progress".
        # The hb-daemon fills them from the progress record; ordinary calls
        # leave them None (unknown), which consumers must not read as False.
        hb = {"worker": self.idx, "state": state, "task_id": task_id,
              "assignment": assignment, "note": note, "updated_at": now(),
              "last_progress_at": progress_at, "stalled": stalled}
        lp = os.path.join(self.dir, "hb.json")
        try:
            tmp = lp + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(hb, f)
            os.replace(tmp, lp)
        except OSError:
            return False
        ok = scp_put(lp, BRIDGE + "/status/worker-%d.json" % self.idx)
        if ok:
            self._ship_alog(task_id)
        return ok

    def _maybe_heartbeat(self, task_id, assignment=None, min_gap=300):
        """Refresh the working heartbeat at most every min_gap seconds.
        Called from long mutation phases (ws-pull/ws-apply) so a busy worker
        is never mistaken for dead. Best effort; never fails the caller."""
        cp = os.path.join(self.dir, "hb-ts.txt")
        try:
            with open(cp, encoding="utf-8") as f:
                last = int(f.read().strip())
        except (OSError, ValueError):
            last = 0
        if now() - last < min_gap:
            return
        try:
            with open(cp, "w", encoding="utf-8") as f:
                f.write(str(now()))
        except OSError:
            pass
        try:
            self._heartbeat("working", task_id, assignment)
        except Exception:
            pass

    # ---------- progress + hb-daemon ----------
    # Liveness ("the worker process is alive") and progress ("real work is
    # happening") are separate signals. The hb-daemon owns liveness: a
    # detached 60s loop calling the same _heartbeat path as everything else,
    # so the format never diverges. Progress is recorded explicitly by the
    # worker via `progress <tid>` at real milestones; where the worker
    # forgets, the structural alog's write time is the fallback. Stalled =
    # no progress for min(20 min, timeout_minutes/3).
    def _progress_path(self, tid):
        return os.path.join(self.dir, "progress-%s.txt" % tid)

    def cmd_progress(self, a):
        """progress <task_id> [note] — record an explicit progress
        timestamp for the task. The hb-daemon reads this to fill the
        heartbeat's last_progress_at; supervise reads the home-side copy
        for stalled/churn decisions. Prints PROGRESS_OK."""
        if not a:
            print("usage: progress <task_id> [note]", file=sys.stderr)
            return 2
        tid = a[0]
        note = " ".join(a[1:])[:120] if len(a) > 1 else ""
        epoch = now()
        try:
            with open(self._progress_path(tid), "w", encoding="utf-8") as f:
                f.write(str(epoch))
        except OSError:
            print("PROGRESS_FAIL")
            return
        self._alog(tid, "progress" + (" " + note if note else ""))
        # ship a task-level marker home (best effort) so the supervisor —
        # which cannot see this worker's /tmp — can judge progress/churn
        lp = os.path.join(self.dir, "progress-ship.json")
        try:
            with open(lp, "w", encoding="utf-8") as f:
                json.dump({"task_id": tid, "at": epoch, "worker": self.idx,
                           "note": note}, f, ensure_ascii=True)
            scp_put(lp, BRIDGE + "/activity/%s.progress.json" % tid)
        except OSError:
            pass
        print("PROGRESS_OK")

    def _last_progress_at(self, tid):
        """Local progress epoch: explicit `progress` record first, then the
        structural alog's mtime as fallback. None if neither exists."""
        try:
            with open(self._progress_path(tid), encoding="utf-8") as f:
                v = int(f.read().strip())
            if v > 0:
                return v
        except (OSError, ValueError):
            pass
        try:
            return int(os.path.getmtime(self._alog_path(tid)))
        except OSError:
            return None

    def _progress_at_remote(self, tid):
        """Task-level last-progress epoch from home-side signals (for the
        supervisor): 1) activity/<tid>.progress.json (explicit progress
        calls), 2) newest mtime of activity/<tid>.bw-*.log (shipped alog).
        None if nothing is found."""
        rc, out, _ = sh(["cmd", "/c", "type",
                         "C:\\Users\\ghfud\\muse-bridge\\activity\\%s.progress.json" % tid],
                        timeout=30)
        if rc == 0:
            try:
                v = int(json.loads(out).get("at", 0) or 0)
                if v > 0:
                    return v
            except (ValueError, TypeError):
                pass
        rc, out, _ = sh(["powershell", "-NoProfile", "-Command",
                         "(Get-ChildItem 'C:\\Users\\ghfud\\muse-bridge\\activity\\%s.bw-*.log' "
                         "-ErrorAction SilentlyContinue | ForEach-Object { "
                         "([datetimeoffset]$_.LastWriteTimeUtc).ToUnixTimeSeconds() } | "
                         "Measure-Object -Maximum).Maximum" % tid],
                        timeout=30)
        if rc == 0 and out.strip():
            try:
                v = int(out.strip().split()[-1])
                return v if v > 0 else None
            except (ValueError, IndexError):
                pass
        return None

    def _claim_timeout_info(self, claim, task=None):
        """(first_claimed_at, timeout_minutes) for a claim dict. The timeout
        clock is anchored at first_claimed_at — inherited across
        replacements — so repeated replacement can never reset it (the
        2026-09-30 Layer-task bug: claimed_at was rewritten on every
        claim-new). Falls back to the queue task's created_at."""
        first = 0
        try:
            first = int(claim.get("first_claimed_at")
                        or claim.get("claimed_at") or 0)
        except (ValueError, TypeError):
            first = 0
        if not first and task:
            try:
                first = int(task.get("created_at") or task.get("created") or 0)
            except (ValueError, TypeError):
                first = 0
        try:
            tm = int((claim.get("timeout_minutes")
                      or (task or {}).get("timeout_minutes") or 60))
        except (ValueError, TypeError):
            tm = 60
        return first, max(1, tm)

    def _stalled_window_secs(self, timeout_minutes):
        """Stalled iff no progress for min(20 min, timeout_minutes/3)."""
        return min(STALLED_MAX_SECS, max(1, timeout_minutes) * 60 // 3)

    def _is_stalled(self, tid, progress_at, first_claimed_at,
                    timeout_minutes):
        """True when (now - last progress) exceeds the stalled window. A
        task with no progress record at all counts from first_claimed_at —
        a claim that never produced anything for a full window is stalled,
        not merely young."""
        ref = progress_at or first_claimed_at or None
        if not ref:
            return False
        return now() - ref > self._stalled_window_secs(timeout_minutes)

    def _task_timed_out(self, tid):
        """Fresh check: has first_claimed_at + timeout_minutes*60 passed?"""
        mc = self._read_master_claim(tid)
        if not mc:
            return False
        first, tm = self._claim_timeout_info(mc)
        return bool(first) and now() > first + tm * 60

    # ----- hb-daemon lifecycle -----
    def _hb_daemon_pidfile(self, tid):
        return "/tmp/bw-hbdaemon-%d-%s.pid" % (self.idx, tid)

    def _start_hb_daemon(self, tid):
        """Spawn the detached 60s heartbeat daemon for this claim. Called
        on claim-new success. Idempotent: any previous daemon for this
        idx+task is stopped first (never two)."""
        self._stop_hb_daemon(tid)
        try:
            p = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__),
                 "--idx", str(self.idx), "hb-daemon", tid],
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError:
            return False
        try:
            with open(self._hb_daemon_pidfile(tid), "w") as f:
                f.write(str(p.pid))
        except OSError:
            pass
        self._alog(tid, "hb-daemon started pid=%d" % p.pid)
        return True

    def _stop_hb_daemon(self, tid):
        pf = self._hb_daemon_pidfile(tid)
        try:
            with open(pf, encoding="utf-8") as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 15)  # SIGTERM; the daemon has no cleanup
            except OSError:
                pass  # already gone
        except (OSError, ValueError):
            pass
        try:
            os.unlink(pf)
        except OSError:
            pass

    def _stop_all_hb_daemons(self):
        import glob as _glob
        for pf in _glob.glob("/tmp/bw-hbdaemon-%d-*.pid" % self.idx):
            try:
                with open(pf, encoding="utf-8") as f:
                    pid = int(f.read().strip())
                try:
                    os.kill(pid, 15)
                except OSError:
                    pass
            except (OSError, ValueError):
                pass
            try:
                os.unlink(pf)
            except OSError:
                pass

    def _daemon_should_live(self, tid, claim, tnow, probe_queue):
        """False when the daemon must exit: claim lost/overwritten, task
        done (result file), cancelled, or timeout reached."""
        if claim is None or int(claim.get("worker", -1)) != self.idx:
            return False  # claim lost or stolen: stop heartbeating
        if probe_queue:
            # result-file check rides the same SSH call as the claim read
            # (see cmd_hb_daemon); queue is probed less often (cancel).
            if self._task_cancelled(tid):
                return False
        first, tm = self._claim_timeout_info(claim)
        if first and tnow > first + tm * 60:
            return False  # timeout: heartbeats would only mask a dead task
        return True

    def cmd_hb_daemon(self, a):
        """hb-daemon <task_id> — detached liveness loop. NOT for interactive
        use: claim-new spawns this (start_new_session, stdio to DEVNULL).
        Every HB_DAEMON_INTERVAL_SECS it sends the standard working
        heartbeat with last_progress_at/stalled filled in, then exits when
        the claim is no longer valid (lost, done, cancelled, timeout)."""
        if not a:
            print("usage: hb-daemon <task_id>", file=sys.stderr)
            return 2
        tid = a[0]
        pf = self._hb_daemon_pidfile(tid)
        try:
            with open(pf, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
        except OSError:
            pass
        n = 0
        fails = 0
        try:
            while True:
                n += 1
                tnow = now()
                # one SSH: claim content + result-file marker
                rc, out, _ = sh(["cmd", "/c",
                                 "type \"C:\\Users\\ghfud\\muse-bridge\\claims\\%s.json\""
                                 " & echo ===BW_R=== & dir /b "
                                 "\"C:\\Users\\ghfud\\muse-bridge\\results\\%s.md\""
                                 % (tid, tid)], timeout=45)
                claim = None
                done = False
                # the echo always runs (&-chained): a missing marker means
                # the transport itself broke, not that the claim is gone
                if "===BW_R===" in out:
                    fails = 0
                    head, _, tail = out.partition("===BW_R===")
                    head, tail = head.strip(), tail.strip()
                    try:
                        c = json.loads(head)
                        claim = c if isinstance(c, dict) else None
                    except ValueError:
                        claim = None
                    done = any(ln.strip().endswith(".md") for ln in
                               tail.splitlines() if ln.strip()
                               and "File Not Found" not in ln)
                else:
                    fails += 1
                    if fails >= 3:
                        break  # home unreachable: stop masking the worker
                    time.sleep(HB_DAEMON_INTERVAL_SECS)
                    continue
                if done:
                    break
                # cancel is queue-side; probe it every 3rd tick (3 min)
                if not self._daemon_should_live(tid, claim, tnow,
                                                probe_queue=(n % 3 == 0)):
                    break
                first, tm = self._claim_timeout_info(claim or {})
                pa = self._last_progress_at(tid)
                stalled = self._is_stalled(tid, pa, first, tm)
                try:
                    self._heartbeat("working", tid, progress_at=pa,
                                    stalled=stalled)
                except Exception:
                    pass
                time.sleep(HB_DAEMON_INTERVAL_SECS)
        finally:
            try:
                os.unlink(pf)
            except OSError:
                pass

    # ---------- check-messages ----------
    def _load_messages(self, tid):
        lp = os.path.join(self.dir, "msgs-%s.json" % tid)
        if not scp_get(BRIDGE + "/messages/%s.json" % tid, lp):
            return []
        try:
            with open(lp, encoding="utf-8") as f:
                m = json.load(f)
            return m if isinstance(m, list) else []
        except (OSError, ValueError):
            return []

    def _new_messages(self, tid):
        """Messages since this worker's local cursor; advances the cursor."""
        msgs = self._load_messages(tid)
        cp = os.path.join(self.dir, "seen-%s.txt" % tid)
        try:
            with open(cp, encoding="utf-8") as f:
                seen = int(f.read().strip())
        except (OSError, ValueError):
            seen = -1
        new = msgs[seen + 1:]
        try:
            with open(cp, "w", encoding="utf-8") as f:
                f.write(str(len(msgs) - 1))
        except OSError:
            pass
        return [{"index": seen + 1 + i, "text": str(m.get("text", "")),
                 "at": int(m.get("at", 0) or 0)} for i, m in enumerate(new)
                if isinstance(m, dict)]

    def cmd_check_messages(self, a):
        tid = a[0]
        msgs = self._new_messages(tid)
        print(json.dumps({"ok": True, "messages": msgs}, ensure_ascii=True))

    def _drain_to_inbox(self, tid):
        """During blocking waits: stash new messages in a local inbox file
        for the agent to read after the wait returns."""
        msgs = self._new_messages(tid)
        if not msgs:
            return
        ip = os.path.join(self.dir, "inbox-%s.txt" % tid)
        try:
            with open(ip, "a", encoding="utf-8") as f:
                for m in msgs:
                    f.write("[msg %d @%d] %s\n" % (m["index"], m["at"], m["text"]))
        except OSError:
            pass

    def _task_cancelled(self, tid):
        """Fresh cancel check straight from home's queue.json (one SSH call).
        True if the task is cancelled or cancel was requested."""
        q = self._quick_queue()
        if not q:
            return False
        for t in q.get("tasks", []):
            if str(t.get("id")) == str(tid):
                return (t.get("status") or "pending") == "cancelled" or bool(t.get("cancel_requested"))
        return False

    # ---------- wait-parts ----------
    def cmd_wait_parts(self, a):
        tid, k, timeout_s = a[0], int(a[1]), int(a[2])
        self._alog(tid, "wait-parts K=%d" % k)
        start = now()
        last_progress = start
        seen = set()
        while True:
            # keep the heartbeat fresh while waiting so reclaim
            # does not treat this claim as abandoned
            self._heartbeat("working", tid, 0)
            # collect any messages from home into the local inbox
            self._drain_to_inbox(tid)
            # stop waiting promptly if home cancelled the task
            if self._task_cancelled(tid):
                print("WAIT_CANCELLED")
                return 4
            rc, out, _ = sh(["cmd", "/c", "dir", "/b",
                             "C:\\Users\\ghfud\\muse-bridge\\results\\%s.part-*.md" % tid],
                            timeout=30)
            cur = set()
            if rc == 0:
                for line in out.splitlines():
                    cur.add(line.strip())
            if cur != seen:
                seen = cur
                last_progress = now()
            have = set()
            for name in seen:
                base = name[:-3]  # strip .md
                _, _, s = base.partition(".part-")
                try:
                    have.add(int(s))
                except ValueError:
                    pass
            if all(j in have for j in range(k)):
                print("WAIT_DONE")
                return 0
            el = now() - start
            if el > timeout_s:
                print("WAIT_TIMEOUT")
                return 2
            if now() - last_progress > 600:
                print("WAIT_STALL")
                return 3
            time.sleep(20)
        return 0

    def _ws_log_active(self, tid, within_secs):
        """True if the task's ws audit log was written within within_secs.
        A worker actively mutating files keeps this fresh even when its
        heartbeat path is broken; reclaim must not steal its claim."""
        rc, out, _ = sh(["powershell", "-NoProfile", "-Command",
                         "(Get-Item 'C:\\muse-workspace\\_log\\%s.jsonl' "
                         "-ErrorAction SilentlyContinue | ForEach-Object { "
                         "([datetimeoffset]$_.LastWriteTimeUtc).ToUnixTimeSeconds() })" % tid],
                        timeout=30)
        if rc != 0 or not out.strip():
            return False
        try:
            return now() - int(out.strip().split()[-1]) < within_secs
        except (ValueError, IndexError):
            return False

    # ---------- reclaim ----------
    # How long a heartbeat file may sit before the reaper treats it as a
    # corpse or litter. Live workers heartbeat every 2-3 min (run-hb every
    # 60s), so 2*LIVE_HB_SECS without a beat means the worker is dead;
    # deleting the file is self-healing (the worker's next heartbeat, if
    # it is somehow still alive, recreates it) and the supervisor already
    # treats such a worker as dead past LIVE_HB_SECS anyway.
    CORPSE_HB_SECS = 2 * 420
    LITTER_HB_SECS = 86400

    def _reap_stale_heartbeats(self, tnow):
        """Delete corpse/litter worker status files on home, based on the
        local status cache (call _sync_status() first):
        - state=working but updated_at older than CORPSE_HB_SECS: the
          worker died without a final heartbeat (the 7 zombies of
          2026-09-30 were exactly this).
        - state=idle older than LITTER_HB_SECS: exited workers' litter.
        - state not in (working, idle): corrupt write (e.g. the
          worker-112 file whose state field held a task_id); atomic
          heartbeat writes prevent new ones, this cleans old ones.
        A worker actively writing the ws audit log is spared even with a
        stale heartbeat (same guard as _reclaim: its heartbeat path may
        be broken while it is alive). Returns the number deleted."""
        d = os.path.join(self.dir, "status")
        try:
            names = [n for n in os.listdir(d) if n.endswith(".json")]
        except OSError:
            return 0
        n = 0
        for name in names:
            try:
                with open(os.path.join(d, name), encoding="utf-8") as f:
                    hb = json.load(f)
            except (OSError, ValueError):
                continue
            if not isinstance(hb, dict):
                continue
            state = hb.get("state")
            try:
                age = tnow - int(hb.get("updated_at", 0) or 0)
            except (ValueError, TypeError):
                continue
            if state == "working":
                if age <= self.CORPSE_HB_SECS:
                    continue
                tid = hb.get("task_id")
                if tid and self._ws_log_active(str(tid), self.CORPSE_HB_SECS):
                    continue
            elif state == "idle":
                if age <= self.LITTER_HB_SECS:
                    continue
            elif age <= 3600:
                continue
            if ssh_del(BRIDGE + "/status/" + name):
                n += 1
        return n

    def _claim_abandoned(self, tid, c, hb, tnow):
        """True if the claim's owner is alive but its own heartbeat shows it
        is no longer working on this task (idle, or working on another task).
        This catches the master-idle-with-claim stuck case: the worker dropped
        the task without releasing the claim, yet its heartbeat stays fresh
        so the stale-heartbeat path never fires. Only applied past
        ABANDON_GRACE_SECS, so a worker still in its claim election
        (heartbeat not yet refreshed) is never flagged."""
        try:
            age = tnow - int(c.get("claimed_at", tnow))
        except (ValueError, TypeError):
            age = 0
        if age < ABANDON_GRACE_SECS:
            return False
        if not hb:
            return False  # dead/unknown worker: the stale path decides
        return not (hb.get("state") == "working"
                    and str(hb.get("task_id") or "") == str(tid))

    def _owns_task(self, tid):
        """Fresh ownership check for this worker: "master", "slot:<n>", or
        None. A slot worker owns the task via its slot claim even though the
        master claim names someone else. Used by claim-verify and by the
        write fences in upload-part / upload-result / cleanup-parts /
        ws-apply, so a mutation is blocked even when the agent skips the
        prompt's claim-verify discipline."""
        mc = self._read_master_claim(tid)
        if mc is not None and int(mc.get("worker", -1)) == self.idx:
            return "master"
        rc, out, _ = sh(["cmd", "/c", "dir", "/b",
                         "C:\\Users\\ghfud\\muse-bridge\\claims\\%s.a*.json" % tid],
                        timeout=30)
        if rc == 0:
            for line in out.splitlines():
                bn = line.strip()
                if not bn.endswith(".json"):
                    continue
                _, _, s = bn[:-5].partition(".a")
                try:
                    slot = int(s)
                except ValueError:
                    continue
                lp = os.path.join(self.dir, "slotcheck.json")
                if not scp_get(BRIDGE + "/claims/" + bn, lp):
                    continue
                try:
                    with open(lp, encoding="utf-8") as f:
                        c = json.load(f)
                except (OSError, ValueError):
                    continue
                if int(c.get("worker", -1)) == self.idx:
                    return "slot:%d" % slot
        return None

    def _reclaim(self, stale_hb_secs=None, stale_claim_secs=None):
        tnow = now()
        shs = stale_hb_secs or STALE_HB_SECS
        scs = stale_claim_secs or STALE_CLAIM_SECS
        results = set(self.result_names())
        reclaimed = []
        for n in self.claim_names():
            base = n[:-5]
            if ".bid-" in base or ".slotbid-" in base:
                # stale election leftovers: delete if older than 5 minutes
                # and no master claim exists for the task
                tid = base.split(".bid-")[0].split(".slotbid-")[0]
                if os.path.exists(os.path.join(self.dir, "claims", tid + ".json")):
                    continue
                c = self.read_claim(n)
                if c and tnow - int(c.get("claimed_at", tnow)) > 300:
                    if ssh_del(BRIDGE + "/claims/" + n):
                        reclaimed.append(n)
                continue
            if ".a" in base:
                tid, _, s = base.partition(".a")
                try:
                    slot = int(s)
                except ValueError:
                    continue
                part = "%s.part-%d.md" % (tid, slot)
            else:
                tid, slot, part = base, 0, None
            if tid + ".md" in results:
                # task complete: the claim is litter, remove it
                if ssh_del(BRIDGE + "/claims/" + n):
                    reclaimed.append(n)
                continue
            if part and part in results:
                continue
            c = self.read_claim(n)
            if not c:
                continue
            # heartbeat stale or missing: still don't steal the claim of a
            # worker that is actively writing the ws audit log (its
            # heartbeat path may be broken while it is alive and working)
            if self._ws_log_active(tid, STALE_HB_SECS):
                continue
            age = tnow - int(c.get("claimed_at", tnow))
            hb = self.heartbeat_of(c.get("worker"))
            abandoned = self._claim_abandoned(tid, c, hb, tnow)
            if not abandoned:
                if age < scs:
                    continue
                if hb and tnow - int(hb.get("updated_at", 0)) < shs:
                    continue
            # Deleting a master claim orphans its live slots; that is
            # deliberate — _pick_action re-masters orphaned slots so the
            # task recovers instead of staying stuck forever.
            if ssh_del(BRIDGE + "/claims/" + n):
                reclaimed.append(n)
        # drop the deleted files from the local claims cache too, so the
        # caller's next decision sees the same view as the remote side
        for n in reclaimed:
            try:
                os.remove(os.path.join(self.dir, "claims", n))
            except OSError:
                pass
        return reclaimed

    def cmd_reclaim(self, a=None):
        # Never decide on the local cache alone: a stale cache makes
        # reclaim a no-op at best and a live-claim stealer at worst.
        if not self._sync():
            print("RECLAIM_SYNC_FAIL")
            return
        reclaimed = self._reclaim()
        self._sync_status()
        reaped = self._reap_stale_heartbeats(now())
        if reclaimed or reaped:
            print("RECLAIMED " + " ".join(reclaimed) +
                  (" REAPED_HB=%d" % reaped if reaped else ""))
        else:
            print("RECLAIM_NONE")

    # ---------- supervise ----------
    # On-demand worker management (v3.0.0). The supervisor agent runs
    # `supervise [block_secs]` in a loop; each pass syncs the queue, reaps
    # abandoned/stale claims, and computes how many one-shot workers are
    # missing per pending task. Prints a JSON list with one entry per
    # worker to launch: [{"task_id": tid, "idx": 101}, ...]. The agent loop
    # launches one workflow per entry. Prints [] when nothing needs
    # launching. Launch intents (supervisor-local) cover workers that are
    # still booting and have no heartbeat/claim yet, so they are never
    # double-launched. Each pass holds the single intent flock for its
    # whole duration (sync -> reclaim -> plan -> intent write), so two
    # concurrent dispatchers serialize and can never compute overlapping
    # plans. With block_secs, waits (5s granularity, zero LLM cost) until
    # something needs launching or the timeout expires.
    _INTENT_LOCK_PATH = "/tmp/bw-supervisor-intents.lock"

    def _intent_lock(self):
        """Exclusive flock on the intent lock file. bw.py is the single
        writer code path for intents; dispatchers record via
        `record-intent`, never by manual file append (the old manual
        append raced with supervise's read-modify-write and lost entries).
        Returns the open file; closing it releases the lock."""
        f = open(self._INTENT_LOCK_PATH, "w")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        return f

    def _load_intents(self):
        try:
            with open(SUPERVISOR_INTENTS, encoding="utf-8") as f:
                items = json.load(f)
        except (OSError, ValueError):
            return []
        tnow = now()
        return [e for e in items if isinstance(e, dict)
                and tnow - int(e.get("at", 0) or 0) < INTENT_TTL_SECS]

    def _write_intents_locked(self, intents):
        """Atomic write; caller must hold the intent lock."""
        tmp = SUPERVISOR_INTENTS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(intents, f)
        os.replace(tmp, SUPERVISOR_INTENTS)

    def cmd_record_intent(self, a):
        """record-intent <task_id> <idx> — dispatcher entry point for launch
        intents. Locked read-modify-write; replaces the old manual-append
        instruction in dispatcher prompts."""
        if len(a) < 2:
            print("usage: record-intent <task_id> <idx>", file=sys.stderr)
            return 2
        tid = a[0]
        try:
            idx = int(a[1])
        except (ValueError, TypeError):
            print("INTENT_FAIL bad idx", file=sys.stderr)
            return 2
        try:
            lock = self._intent_lock()
        except OSError:
            print("INTENT_FAIL")
            return
        try:
            items = self._load_intents()
            items = [e for e in items
                     if not (str(e.get("tid")) == str(tid)
                             and int(e.get("idx", -1) or -1) == idx)]
            items.append({"tid": str(tid), "idx": idx, "at": now()})
            self._write_intents_locked(items)
            print("INTENT_OK")
        except (OSError, ValueError):
            print("INTENT_FAIL")
        finally:
            lock.close()

    def _alloc_idx(self, intents):
        used = set()
        for e in intents:
            try:
                used.add(int(e.get("idx", -1)))
            except (ValueError, TypeError):
                pass
        for i in range(ONESHOT_IDX_MIN, ONESHOT_IDX_MAX + 1):
            if i in used:
                continue
            hb = self.heartbeat_of(i)
            if hb and now() - int(hb.get("updated_at", 0) or 0) < LIVE_HB_SECS:
                continue
            return i
        return None

    def _master_claim(self, tid):
        for n in self.claim_names():
            if n == tid + ".json":
                return self.read_claim(n)
        return None

    def _master_live(self, tid, mc):
        if not mc:
            return False
        hb = self.heartbeat_of(mc.get("worker"))
        return bool(hb and hb.get("state") == "working"
                    and str(hb.get("task_id") or "") == str(tid)
                    and now() - int(hb.get("updated_at", 0) or 0) < LIVE_HB_SECS)

    def _master_stalled(self, tid, mc, rep, task):
        """True if the master is live (fresh heartbeat) but making no
        progress. This is the orphaned-hb-daemon case: the worker's agent
        died, but its hb-daemon survived and keeps the claim looking
        healthy, so the stale-heartbeat path never fires and the task
        stalls forever. Checked in the supervise planning loop; a stalled
        master is reclaimed and replaced like a dead one."""
        if not mc:
            return False
        hb = self.heartbeat_of(mc.get("worker"))
        if hb and hb.get("stalled"):
            return True
        try:
            first = int((rep or {}).get("first_claimed_at") or 0)
        except (ValueError, TypeError):
            first = 0
        tm = int((task or {}).get("timeout_minutes", 60) or 60)
        prog_at = self._progress_at_remote(tid)
        return self._is_stalled(tid, prog_at, first, tm)

    def _task_live_workers(self, tid):
        """Distinct workers holding a claim on tid with a fresh 'working on
        tid' heartbeat. Claims come from the synced local dir; heartbeats
        are one SSH read per claiming worker."""
        workers = set()
        for n in self.claim_names():
            base = n[:-5]
            if ".bid-" in base or ".slotbid-" in base:
                continue
            if base.split(".a")[0] != tid:
                continue
            c = self.read_claim(n)
            if not c:
                continue
            w = c.get("worker")
            if w in workers:
                continue
            hb = self.heartbeat_of(w)
            if (hb and hb.get("state") == "working"
                    and str(hb.get("task_id") or "") == str(tid)
                    and now() - int(hb.get("updated_at", 0) or 0) < LIVE_HB_SECS):
                workers.add(w)
        return workers

    def _reap_abandoned_masters(self, tnow):
        """Fast path for the stuck case the pool hit on 2026-09-29: a master
        claim whose owner is alive but no longer working the task (idle, or
        on another task). Removed immediately (past the 120s grace) instead
        of waiting for the next reclaim sweep, so a replacement can elect
        without delay. Returns the number of claims removed."""
        n = 0
        for name in self.claim_names():
            base = name[:-5]
            if ".a" in base or ".bid-" in base or ".slotbid-" in base:
                continue
            c = self.read_claim(name)
            if not c:
                continue
            hb = self.heartbeat_of(c.get("worker"))
            if self._claim_abandoned(base, c, hb, tnow):
                if ssh_del(BRIDGE + "/claims/" + name):
                    n += 1
        return n

    # ---------- replacement journaling + churn gate ----------
    # Every worker replacement is journaled: claims/meta/<tid>.replacements.json
    # on home (count + history with reason), plus one human-readable line in
    # home's activity/<tid>.supervise.log. A task replaced
    # MAX_REPLACEMENTS_NO_PROGRESS times with no progress is failed, not
    # relaunched (failed_churn) — the 2026-09-30 Layer task was replaced 7
    # times with the timeout clock resetting on every claim-new.
    def _mark_failed_remote(self, tid):
        return BRIDGE + "/" + CLAIMS_META_DIR + "/%s.mark_failed.json" % tid

    def _failed_marker(self, tid):
        """True if the task is already failed/being failed: a mark_failed
        request file exists, or the replacements record has failed set.
        Uses the pass-local synced claims/meta/ copy (synced at pass start)."""
        lp = os.path.join(self.dir, "claims", "meta",
                          tid + ".mark_failed.json")
        if os.path.exists(lp):
            return True
        rep = self._read_replacements(tid)
        return bool(rep and rep.get("failed"))

    def cmd_mark_failed(self, a):
        """mark-failed <task_id> <reason> — request the MCP server to flip
        the task to failed.

        CONTRACT (for the MCP-server implementer): bw.py never writes
        queue.json directly — it is MCP-owned. This drops
        claims/meta/<tid>.mark_failed.json on home:
          {"task_id", "reason" ("churn"|"timeout"), "at", "by",
           "worker_idx", "note"}
        The server applies it on its next queue-touching call: set the
        task's status="failed", reason=<reason>, finished_at=now, then
        consume the request (delete it or rename to .applied). muse_result /
        muse_await then surface status=failed + reason to the caller.
        The file lives under claims/meta/ (not claims/) so it never
        collides with the claims/*.json master/slot-claim namespace."""
        if not a:
            print("usage: mark-failed <task_id> <reason>", file=sys.stderr)
            return 2
        tid = a[0]
        reason = a[1] if len(a) > 1 else "churn"
        req = {"task_id": tid, "reason": reason, "at": now(),
               "by": "bw-supervise", "worker_idx": self.idx,
               "note": ("MCP server: set status=failed, reason=%s, "
                        "finished_at=now, then consume this file" % reason)}
        lp = os.path.join(self.dir, "markfailed.json")
        try:
            with open(lp, "w", encoding="utf-8") as f:
                json.dump(req, f, ensure_ascii=True)
        except OSError:
            print("MARK_FAILED_FAIL")
            return
        self._ensure_claims_meta()
        print("MARK_FAILED_OK"
              if scp_put(lp, self._mark_failed_remote(tid))
              else "MARK_FAILED_FAIL")

    def _append_supervise_log(self, tid, line):
        """Append one line to home's activity/<tid>.supervise.log via
        download-append-upload. The supervise pass holds the intent flock,
        so no other supervise writer interleaves; home's MCP server never
        touches this file."""
        remote = BRIDGE + "/activity/%s.supervise.log" % tid
        lp = os.path.join(self.dir, "svlog-%s.txt" % tid)
        try:
            if not scp_get(remote, lp):
                with open(lp, "w", encoding="utf-8") as f:
                    f.write("")
            with open(lp, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            return False
        return scp_put(lp, remote)

    def _replacement_reason(self, tid, prev_worker, rep, task):
        """Classify why the previous worker is being replaced. Returns
        (reason, last_hb_at, last_progress_at). Codes: hb_silent (heartbeat
        gone/stale), stalled (no progress within the window), timeout
        (first_claimed_at + timeout_minutes passed), claim_lost (worker
        alive but its claim is gone), ssh_fail (our transport to home
        broke — not the worker's fault). duplicate_dispatch is logged
        separately when live workers exceed the crew size."""
        tnow = now()
        prog_at = self._progress_at_remote(tid)
        try:
            first = int(rep.get("first_claimed_at") or 0)
        except (ValueError, TypeError):
            first = 0
        tm = int((task or {}).get("timeout_minutes", 60) or 60)
        if first and tnow > first + tm * 60:
            return "timeout", None, prog_at
        hb_at = None
        if prev_worker is None:
            return "hb_silent", None, prog_at
        rc, out, _ = sh(["cmd", "/c", "type",
                         "C:\\Users\\ghfud\\muse-bridge\\status\\worker-%s.json"
                         % prev_worker], timeout=30)
        if rc == 99:
            # transport broke (timeout/exception), not the worker
            return "ssh_fail", None, prog_at
        hb = None
        if rc == 0:
            try:
                hb = json.loads(out)
            except ValueError:
                hb = None
        if isinstance(hb, dict):
            try:
                hb_at = int(hb.get("updated_at", 0) or 0) or None
            except (ValueError, TypeError):
                hb_at = None
            if hb.get("stalled"):
                return "stalled", hb_at, prog_at
            if not hb_at or tnow - hb_at > LIVE_HB_SECS:
                return "hb_silent", hb_at, prog_at
        else:
            return "hb_silent", None, prog_at
        # heartbeat alive: stalled by the progress clock?
        if self._is_stalled(tid, prog_at, first, tm):
            return "stalled", hb_at, prog_at
        # alive but the claim is gone (reclaimed from under it)
        if self._read_master_claim(tid) is None:
            return "claim_lost", hb_at, prog_at
        return "hb_silent", hb_at, prog_at

    def _task_has_progress(self, tid, rep):
        """True if any progress was recorded after first_claimed_at."""
        try:
            first = int(rep.get("first_claimed_at") or 0)
        except (ValueError, TypeError):
            first = 0
        pa = self._progress_at_remote(tid)
        return bool(pa and pa > first)

    def _record_replacement(self, tid, prev_worker, reason, hb_at, prog_at,
                            new_worker, rep):
        """Journal one replacement: bump count, append history, write the
        replacements record home, and append the one-line supervise log."""
        rep = rep or {"task_id": tid, "count": 0, "history": []}
        if not rep.get("first_claimed_at"):
            rep["first_claimed_at"] = now()
        rep["count"] = int(rep.get("count", 0) or 0) + 1
        rep["last_reason"] = reason
        rep.setdefault("history", []).append({
            "at": now(), "prev_worker": prev_worker, "reason": reason,
            "last_hb_at": hb_at, "last_progress_at": prog_at,
            "new_worker": new_worker})
        self._write_replacements(tid, rep)
        ts = _kst_str()

        def _hm(v):
            return _kst_hm(v)

        line = "%s | w%s | %s | hb=%s progress=%s | w%s" % (
            ts, prev_worker, reason, _hm(hb_at), _hm(prog_at), new_worker)
        self._append_supervise_log(tid, line)
        return rep

    def _fail_task_churn(self, tid, reason, rep, task):
        """Stop relaunching: the task was replaced
        MAX_REPLACEMENTS_NO_PROGRESS times with no progress. Writes the
        failed summary result (results/<tid>.md), journals the failure,
        and drops the mark_failed request for the MCP server to apply."""
        rep = rep or {"task_id": tid, "count": 0, "history": []}
        rep["failed"] = reason
        rep["failed_at"] = now()
        self._write_replacements(tid, rep)
        self.cmd_mark_failed([tid, reason])
        label = (task or {}).get("label", "")
        lines = ["# task %s — FAILED (%s)" % (tid, reason), "",
                 ("supervise stopped relaunching: %d replacements with no "
                  "progress since first claim."
                  % int(rep.get("count", 0))), "",
                 "## replacement history", ""]
        for h in rep.get("history", []):
            at = _kst_str("%Y-%m-%d %H:%M:%S", h.get("at", 0))
            lines.append("- %s: w%s -> w%s (%s)" % (
                at, h.get("prev_worker"), h.get("new_worker"),
                h.get("reason")))
        lines += ["",
                  "## remaining artifacts",
                  "- workspace: C:\\muse-workspace\\%s\\" % tid,
                  "- activity: muse-bridge/activity/%s.* "
                  "(bw logs, progress, supervise)" % tid,
                  "- ws backups: C:\\muse-workspace\\_backups\\%s\\" % tid,
                  "- audit log: C:\\muse-workspace\\_log\\%s.jsonl" % tid,
                  "",
                  "label: %s" % label]
        lp = os.path.join(self.dir, "failed-%s.md" % tid)
        try:
            with open(lp, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            scp_put(lp, BRIDGE + "/results/%s.md" % tid)
        except OSError:
            pass
        ts = _kst_str()
        self._append_supervise_log(
            tid, "%s | - | failed_%s | - | -" % (ts, reason))

    def _supervise_once(self):
        """One pass. Returns the launch plan list, or None if the sync
        failed (caller retries).

        The whole pass — sync, reclaim, plan computation, intent
        reservation — runs under the single intent flock, so two
        concurrent dispatchers (hook + sweep) can never compute
        overlapping plans: the second one blocks on the lock, then sees
        the first one's reserved intents and excludes those tasks. The
        lock is per-pass (released between block-mode iterations); flock
        auto-releases if the process dies, so no stale-lock cleanup is
        ever needed."""
        try:
            lock = self._intent_lock()
        except OSError:
            return None
        try:
            return self._supervise_pass_locked()
        finally:
            lock.close()

    def _supervise_pass_locked(self):
        """Body of _supervise_once. Caller must hold the intent lock, so
        the intent read-compute-write is atomic and no merge is needed —
        every intent writer (supervise, record-intent) goes through this
        same lock."""
        if not self._sync():
            return None
        tnow = now()
        self._reap_abandoned_masters(tnow)
        if self.claim_names():
            # crashed workers' claims are freed here; the supervisor uses
            # shorter windows than the old pool defaults because one-shot
            # workers heartbeat every ~2-3 minutes during work
            self._reclaim(stale_hb_secs=LIVE_HB_SECS, stale_claim_secs=600)
        # corpse/litter heartbeat files on home (dead workers' "working"
        # files, exited workers' old "idle" files, corrupt writes)
        self._sync_status()
        self._reap_stale_heartbeats(tnow)
        q = self.load_queue()
        results = set(self.result_names())
        intents = self._load_intents()
        ordered = sorted(
            (t for t in q.get("tasks", [])
             if (t.get("status") or "pending") == "pending"
             and not t.get("cancel_requested")
             and str(t.get("id")) + ".md" not in results
             and t.get("id")),
            key=lambda t: (-(int(t.get("priority", 0) or 0)),
                           int(t.get("created_at", 0) or 0)))
        # never relaunch a task the churn gate already failed (its
        # mark_failed request is still waiting for the MCP server, or the
        # replacements record carries failed)
        ordered = [t for t in ordered
                   if not self._failed_marker(str(t.get("id")))]
        pending_ids = {str(t.get("id")) for t in ordered}
        # drop intents for tasks that are no longer actionable so they
        # neither consume the concurrency cap nor block idx reuse
        intents = [e for e in intents if str(e.get("tid")) in pending_ids]
        live_by_task = {}
        for t in ordered:
            live_by_task[str(t.get("id"))] = self._task_live_workers(str(t.get("id")))

        def current_total():
            return (sum(len(v) for v in live_by_task.values())
                    + len(intents))

        plan = []
        for t in ordered:
            tid = str(t.get("id"))
            if current_total() >= MAX_CONCURRENT_WORKERS:
                break
            needed = max(1, min(MAX_EFFECTIVE_WORKERS,
                                int(t.get("workers", 1) or 1)))
            launching = sum(1 for e in intents if str(e.get("tid")) == tid)
            live = live_by_task[tid]
            mc = self._master_claim(tid)
            stalled_prev = None  # worker idx of a stalled master we reclaimed
            if mc and self._master_live(tid, mc):
                # master healthy: only refill genuinely free slots (no claim
                # file and no part uploaded yet)
                k = int(mc.get("workers_effective", 1) or 1)
                claimed_slots = set()
                for name in self.claim_names():
                    base = name[:-5]
                    if base.startswith(tid + ".a"):
                        try:
                            claimed_slots.add(int(base.split(".a")[1]))
                        except (ValueError, IndexError):
                            pass
                free = [j for j in range(1, k)
                        if j not in claimed_slots
                        and "%s.part-%d.md" % (tid, j) not in results]
                short = max(0, len(free) - launching)
                replacement = False
                # A live-but-stalled master is NOT healthy: its agent died
                # but the hb-daemon survived, so the heartbeat looks fresh
                # while no progress is made. Reclaim the stale claim and
                # replace the master; otherwise the task stalls forever.
                rep = self._read_replacements(tid)
                if self._master_stalled(tid, mc, rep, t):
                    cn = tid + ".json"
                    if ssh_del(BRIDGE + "/claims/" + cn):
                        try:
                            os.remove(os.path.join(self.dir, "claims", cn))
                        except OSError:
                            pass
                        if rep is None:
                            try:
                                first = int(mc.get("first_claimed_at") or 0)
                            except (ValueError, TypeError):
                                first = 0
                            rep = {"task_id": tid,
                                   "first_claimed_at": first or now(),
                                   "count": 0, "history": []}
                            self._write_replacements(tid, rep)
                        short = max(0, 1 - launching)
                        replacement = True
                        # remember the stalled master so the replacement
                        # is journaled with reason="stalled" (not hb_silent)
                        stalled_prev = mc.get("worker")
                    # if the claim delete failed, leave the master alone;
                    # a failed delete means the transport broke, not that
                    # the worker is replaceable right now
            elif live:
                # workers alive but no live master (orphaned slots): only a
                # master is missing; one launch elects it and adopts them
                short = max(0, 1 - launching)
                replacement = True
            else:
                # fresh task or everyone gone: launch the full crew
                short = max(0, needed - launching)
                replacement = True
            short = min(short, MAX_CONCURRENT_WORKERS - current_total())
            if short <= 0:
                continue
            # duplicate-dispatch diagnostic: more live workers than the crew
            # size means an earlier pass double-launched; journal it (this
            # is not a replacement, just a note for forensics)
            if len(live) > needed:
                self._append_supervise_log(
                    tid, "%s | %s | duplicate_dispatch | - | -" % (
                        _kst_str(),
                        ",".join("w%s" % w for w in sorted(live))))
            rep = self._read_replacements(tid)
            # a launch counts as a replacement when the task was claimed
            # before (replacements record exists) and this pass is filling
            # a gap rather than staffing a fresh task
            is_replacement = bool(replacement and rep is not None)
            prev_worker = None
            reason = None
            hb_at = prog_at = None
            if is_replacement:
                # churn gate: replaced MAX_REPLACEMENTS_NO_PROGRESS times
                # with no progress -> fail the task, do not relaunch
                if (int(rep.get("count", 0) or 0)
                        >= MAX_REPLACEMENTS_NO_PROGRESS
                        and not self._task_has_progress(tid, rep)):
                    try:
                        first = int(rep.get("first_claimed_at") or 0)
                    except (ValueError, TypeError):
                        first = 0
                    tm = int((t or {}).get("timeout_minutes", 60) or 60)
                    fail_reason = ("timeout"
                                   if first and now() > first + tm * 60
                                   else "churn")
                    self._fail_task_churn(tid, fail_reason, rep, t)
                    continue
                # previous worker: the most recent launched worker that is
                # no longer live (in the orphaned-master case that is the
                # dead master, not a live slot worker). For a stalled
                # master we reclaimed above, use its worker directly —
                # history is empty on the first replacement.
                hist = rep.get("history") or []
                for h in reversed(hist):
                    w = h.get("new_worker")
                    if w not in live:
                        prev_worker = w
                        break
                if prev_worker is None and hist:
                    prev_worker = hist[-1].get("new_worker")
                if prev_worker is None and stalled_prev is not None:
                    prev_worker = stalled_prev
                reason, hb_at, prog_at = self._replacement_reason(
                    tid, prev_worker, rep, t)
            new_idxs = []
            for _ in range(short):
                idx = self._alloc_idx(intents)
                if idx is None:
                    break
                intents.append({"tid": tid, "idx": idx, "at": tnow})
                plan.append({"task_id": tid, "idx": idx})
                new_idxs.append(idx)
            if is_replacement and new_idxs:
                self._record_replacement(tid, prev_worker, reason, hb_at,
                                         prog_at, new_idxs[0], rep)
        # Lock is held for the whole pass, so a plain atomic write is
        # correct here: no other writer can interleave between the load
        # above and this write (record-intent blocks on the same lock).
        self._write_intents_locked(intents)
        return plan

    def cmd_supervise(self, a=None):
        block = 0.0
        if a:
            try:
                block = float(a[0])
            except (ValueError, TypeError):
                block = 0.0
        deadline = now() + block if block > 0 else 0
        try:
            self._heartbeat("idle")
        except Exception:
            pass
        while True:
            try:
                plan = self._supervise_once()
            except Exception:
                plan = None
            if plan:
                print(json.dumps(plan, ensure_ascii=True))
                return
            if plan is None:
                time.sleep(10)
                continue
            if deadline and now() < deadline:
                time.sleep(5)
                continue
            print("[]")
            return

    # ---------- task-status ----------
    def cmd_task_status(self, a):
        tid = a[0]
        scp_get(BRIDGE + "/queue.json", self.qpath())
        t = self.find_task(tid)
        if t is None:
            self._slog("task-status", tid, {"status": "unknown"})
            print(json.dumps({"status": "unknown"}))
        else:
            status = t.get("status", "pending")
            self._slog("task-status", tid,
                       {"status": status,
                        "cancel_requested": bool(t.get("cancel_requested"))})
            print(json.dumps({"status": status,
                              "cancel_requested": bool(t.get("cancel_requested"))}))

    # ---------- measure-source ----------
    def cmd_measure_source(self, a):
        """Measure source files/lines on home for the given home-side paths.
        Usage: measure-source <path1> [<path2> ...]
        Prints JSON: {ok, paths: [{path, files, lines}], total_files,
        total_lines, budget_lines, over_budget}. Used by the worker as a
        fallback size check for tasks submitted without source_paths."""
        import base64 as _b64
        paths = [p for p in (a or []) if p]
        if not paths:
            print(json.dumps({"ok": False, "error": "no paths given"}))
            return
        blob = _b64.b64encode(_MEASURE_CODE.encode("utf-8")).decode("ascii")
        arg = _b64.b64encode(json.dumps(paths).encode("utf-8")).decode("ascii")
        outer = ("import base64,sys;"
                 "exec(base64.b64decode('%s').decode('utf-8'))" % blob)
        rc, out, err = sh(["python", "-c", '"%s"' % outer, arg], timeout=120)
        if rc != 0:
            print(json.dumps({"ok": False, "error": "measure failed on home",
                              "detail": (err or "")[-200:]}))
            return
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    data = json.loads(line)
                except ValueError:
                    continue
                data["budget_lines"] = SIZE_BUDGET_LINES
                data["over_budget"] = (data.get("total_lines", 0)
                                       > SIZE_BUDGET_LINES)
                print(json.dumps(data, ensure_ascii=True))
                return
        print(json.dumps({"ok": False, "error": "no JSON from home"}))

    # ---------- cleanup-parts ----------
    def cmd_cleanup_parts(self, a):
        tid = a[0]
        self._alog(tid, "cleanup-parts")
        # write fence: only the master deletes a task's part files
        if self._owns_task(tid) != "master":
            print(json.dumps({"ok": False, "error": "not owner",
                              "reason": "cleanup-parts blocked: not master"}))
            return
        ssh_del(BRIDGE + "/results/%s.part-*.md" % tid)
        self._ship_alog(tid)
        print("CLEANUP_OK")

    # ---------- slice-brief ----------
    def cmd_slice_brief(self, a):
        tid, slot = a[0], int(a[1])
        self._alog(tid, "slice-brief slot %d" % slot)
        lp = os.path.join(self.dir, "master.json")
        if not scp_get(BRIDGE + "/claims/%s.json" % tid, lp):
            print(json.dumps({"ok": False}))
            return
        try:
            with open(lp, encoding="utf-8") as f:
                mc = json.load(f)
            instr = mc.get("instructions") or []
            print(json.dumps({
                "ok": True,
                "prompt": mc.get("prompt", ""),
                "label": mc.get("label", ""),
                "instruction": instr[slot] if slot < len(instr) else "",
                "timeout_minutes": int(mc.get("timeout_minutes", 60) or 60),
                "workers_effective": int(mc.get("workers_effective", 1) or 1),
            }, ensure_ascii=True))
        except (OSError, ValueError):
            print(json.dumps({"ok": False}))

    # ---------- workspace: safe home file modification ----------
    # Isolated per-task staging dir on home (C:\muse-workspace\<task_id>).
    # Real files are never touched except through ws-apply, which always
    # backs up the original first and appends an audit-log entry.
    WSROOT = "C:\\muse-workspace"

    def _ws_ok(self, s):
        import re as _re
        return bool(_re.match(r"^[A-Za-z0-9_.\-][A-Za-z0-9_.\-]{0,80}$", s or "")) and ".." not in s and s != "."

    def _ws_ensure(self, tid):
        base = self.WSROOT + "\\" + tid
        for d in (self.WSROOT, self.WSROOT + "\\_backups", self.WSROOT + "\\_log",
                  base, self.WSROOT + "\\_backups\\" + tid):
            sh(["cmd", "/c", "if", "not", "exist", d, "mkdir", d], timeout=30)

    def _ws_remote_exists(self, rpath):
        rc, out, _ = sh(["cmd", "/c", "if", "exist", rpath, "echo", "EXISTS"], timeout=30)
        return rc == 0 and "EXISTS" in out

    def _ws_sha(self, rpath):
        rc, out, _ = sh(["cmd", "/c", "certutil", "-hashfile", rpath, "SHA256"], timeout=30)
        if rc != 0:
            return None
        for ln in out.splitlines():
            s = ln.strip().replace(" ", "")
            if len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s):
                return s.lower()
        return None

    def _ws_log(self, tid, entry):
        import base64 as _b64
        e = dict(entry)
        e.update({"ts": now(), "task_id": tid, "worker": self.idx})
        b64 = _b64.b64encode(json.dumps(e, ensure_ascii=True).encode("utf-8")).decode("ascii")
        code = ("import base64;open('C:/muse-workspace/_log/%s.jsonl','a',encoding='utf-8')"
                ".write(base64.b64decode('%s').decode('utf-8')+chr(10))") % (tid, b64)
        rc, _, _ = sh(["python", "-c", '"%s"' % code], timeout=30)
        return rc == 0

    def cmd_ws_init(self, a):
        tid = a[0]
        self._alog(tid, "ws-init")
        if not self._ws_ok(tid):
            print(json.dumps({"ok": False, "error": "bad task_id"}))
            return
        self._ws_ensure(tid)
        print(json.dumps({"ok": True, "workspace": "C:\\muse-workspace\\" + tid}))

    def cmd_ws_pull(self, a):
        tid, rpath = a[0], a[1]
        name = a[2] if len(a) > 2 else rpath.replace("/", "\\").rsplit("\\", 1)[-1]
        self._alog(tid, "ws-pull %s" % name)
        if not self._ws_ok(tid) or not self._ws_ok(name):
            print(json.dumps({"ok": False, "error": "bad args"}))
            return
        if not self._ws_remote_exists(rpath):
            print(json.dumps({"ok": False, "error": "remote not found"}))
            return
        self._ws_ensure(tid)
        wsp = self.WSROOT + "\\" + tid + "\\" + name
        rc, _, _ = sh(["cmd", "/c", "copy", "/y", rpath, wsp], timeout=60)
        ok = rc == 0
        logged = self._ws_log(tid, {"op": "pull", "remote": rpath, "workspace_file": name}) if ok else False
        if ok:
            self._maybe_heartbeat(tid)
        print(json.dumps({"ok": ok, "workspace_file": name if ok else None, "logged": logged}))

    def cmd_ws_get(self, a):
        tid, name, local = a[0], a[1], a[2]
        if not (self._ws_ok(tid) and self._ws_ok(name)):
            self._slog("ws-get", tid, {"ok": False, "reason": "bad args"})
            print("GET_FAIL")
            return
        ok = scp_get("C:/muse-workspace/%s/%s" % (tid, name), local)
        self._slog("ws-get", tid, {"ok": ok, "file": name})
        print("GET_OK" if ok else "GET_FAIL")

    def cmd_ws_put(self, a):
        tid, local, name = a[0], a[1], a[2]
        self._alog(tid, "ws-put %s" % name)
        if not (self._ws_ok(tid) and self._ws_ok(name)):
            print("PUT_FAIL")
            return
        print("PUT_OK" if scp_put(local, "C:/muse-workspace/%s/%s" % (tid, name)) else "PUT_FAIL")

    def cmd_ws_ls(self, a):
        tid = a[0]
        if not self._ws_ok(tid):
            self._slog("ws-ls", tid, {"ok": False, "reason": "bad task_id"})
            print(json.dumps([]))
            return
        rc, out, _ = sh(["cmd", "/c", "dir", "/b", self.WSROOT + "\\" + tid], timeout=30)
        files = [ln.strip() for ln in out.splitlines() if ln.strip()] if rc == 0 else []
        self._slog("ws-ls", tid, {"ok": rc == 0, "files": len(files)})
        print(json.dumps(files))

    def cmd_ws_apply(self, a):
        tid, name, rpath = a[0], a[1], a[2]
        self._alog(tid, "ws-apply %s" % name)
        if not (self._ws_ok(tid) and self._ws_ok(name)):
            print(json.dumps({"ok": False, "error": "bad args"}))
            return
        # write fence: only a worker that owns the task (master or slot)
        # may modify home files for it
        if self._owns_task(tid) is None:
            print(json.dumps({"ok": False, "error": "not owner",
                              "reason": "ws-apply blocked: no claim"}))
            return
        wsp = self.WSROOT + "\\" + tid + "\\" + name
        if not self._ws_remote_exists(wsp):
            print(json.dumps({"ok": False, "error": "workspace file not found"}))
            return
        self._ws_ensure(tid)
        backup = None
        sha_before = None
        if self._ws_remote_exists(rpath):
            sha_before = self._ws_sha(rpath)
            base = rpath.replace("/", "\\").rsplit("\\", 1)[-1]
            safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in base) or "file"
            backup = "%s\\_backups\\%s\\%d_%s" % (self.WSROOT, tid, now(), safe)
            rc, _, _ = sh(["cmd", "/c", "copy", "/y", rpath, backup], timeout=60)
            if rc != 0:
                print(json.dumps({"ok": False, "error": "backup failed"}))
                return
        else:
            parent = rpath.replace("/", "\\").rsplit("\\", 1)[0]
            if parent and parent != rpath:
                sh(["cmd", "/c", "if", "not", "exist", parent, "mkdir", parent], timeout=30)
        rc, _, _ = sh(["cmd", "/c", "copy", "/y", wsp, rpath], timeout=60)
        if rc != 0:
            print(json.dumps({"ok": False, "error": "apply failed", "backup": backup}))
            return
        sha_after = self._ws_sha(rpath)
        logged = self._ws_log(tid, {"op": "apply", "workspace_file": name, "remote": rpath,
                                    "sha_before": sha_before, "sha_after": sha_after, "backup": backup})
        self._maybe_heartbeat(tid)
        print(json.dumps({"ok": True, "backup": backup, "logged": logged,
                          "sha_before": sha_before, "sha_after": sha_after}))

    def cmd_ws_log(self, a):
        tid = a[0]
        if not self._ws_ok(tid):
            print("LOG_EMPTY")
            return
        lp = os.path.join(self.dir, "wslog-%s.jsonl" % tid)
        if scp_get("C:/muse-workspace/_log/%s.jsonl" % tid, lp):
            try:
                with open(lp, encoding="utf-8") as f:
                    txt = f.read().strip()
                print(txt if txt else "LOG_EMPTY")
                return
            except OSError:
                pass
        print("LOG_EMPTY")

    def cmd_ws_revert(self, a):
        tid, rpath = a[0], a[1]
        if not self._ws_ok(tid):
            print(json.dumps({"ok": False, "error": "bad task_id"}))
            return
        lp = os.path.join(self.dir, "wslog-%s.jsonl" % tid)
        backup = None
        if scp_get("C:/muse-workspace/_log/%s.jsonl" % tid, lp):
            try:
                with open(lp, encoding="utf-8") as f:
                    for ln in f:
                        try:
                            e = json.loads(ln)
                        except ValueError:
                            continue
                        if e.get("op") == "apply" and e.get("remote") == rpath and e.get("backup"):
                            backup = e["backup"]
            except OSError:
                pass
        if not backup:
            print(json.dumps({"ok": False, "error": "no backup found"}))
            return
        rc, _, _ = sh(["cmd", "/c", "copy", "/y", backup, rpath], timeout=60)
        if rc != 0:
            print(json.dumps({"ok": False, "error": "revert failed"}))
            return
        logged = self._ws_log(tid, {"op": "revert", "remote": rpath, "backup": backup,
                                    "sha_after": self._ws_sha(rpath)})
        print(json.dumps({"ok": True, "restored_from": backup, "logged": logged}))

    def cmd_ws_clean(self, a):
        tid = a[0]
        if not self._ws_ok(tid):
            print("CLEAN_FAIL")
            return
        rc, _, _ = sh(["cmd", "/c", "rmdir", "/s", "/q", self.WSROOT + "\\" + tid], timeout=60)
        print("CLEAN_OK" if rc == 0 else "CLEAN_FAIL")



def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--idx", type=int, required=True)
    ap.add_argument("command")
    ap.add_argument("args", nargs="*")
    ns = ap.parse_args()
    bw = BW(ns.idx)
    fn = getattr(bw, "cmd_" + ns.command.replace("-", "_"), None)
    if fn is None:
        print("UNKNOWN_COMMAND", file=sys.stderr)
        sys.exit(1)
    rc = fn(ns.args)
    sys.exit(rc if isinstance(rc, int) else 0)


if __name__ == "__main__":
    main()
