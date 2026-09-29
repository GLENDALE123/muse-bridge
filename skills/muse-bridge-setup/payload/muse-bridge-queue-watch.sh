#!/usr/bin/env bash
# muse-bridge-queue-watch: near-real-time dispatcher trigger.
# Polls home's queue.json every N seconds with ONE cheap SSH call (no LLM).
# Wakes a dispatcher agent when genuinely new pending tasks appear, and
# also when an already-announced pending task looks stuck (no claim file
# for a while, or pending for a very long time) so dead workers / stale
# claims don't wait for the 15-min sweep. Still exactly one SSH call.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

STATE_DIR="$HOME/hooks/state/muse-bridge-queue-watch"
STATE_FILE="$STATE_DIR/announced.json"
mkdir -p "$STATE_DIR"
[ -f "$STATE_FILE" ] || echo '{}' > "$STATE_FILE"

# Stuck heuristics (seconds). A task being legitimately worked keeps its
# claim file, so NO_CLAIM_AFTER only fires when nothing holds the task.
# LONG_AFTER is a backstop for the stale-claim-with-dead-worker case the
# claim check can't see; the dispatcher re-verifies via supervise anyway.
NO_CLAIM_AFTER=300
NO_CLAIM_REWAKE=900
LONG_AFTER=1800
LONG_REWAKE=3600

# One SSH call: queue.json + claims listing. BatchMode: never hang on auth.
RAW="$(ssh -F /home/hatch/.ssh/config -o BatchMode=yes -o ConnectTimeout=15 home \
  'cmd /c type "C:\Users\ghfud\muse-bridge\queue.json" & echo ===BW_CLAIMS=== & dir /b "C:\Users\ghfud\muse-bridge\claims\*.json" 2>nul & exit 0' 2>/dev/null)" || {
  log "queue-fetch-failed" '{}'
  silent "queue fetch failed"
  exit 0
}

QJSON="$(printf '%s' "$RAW" | awk '/===BW_CLAIMS===/{sub(/===BW_CLAIMS===.*/, ""); print; exit} {print}')"
CLAIMS="$(printf '%s' "$RAW" | awk '/===BW_CLAIMS===/{f=1; sub(/.*===BW_CLAIMS===/, ""); if ($0) print; next} f')"

# Pending task ids (cancel_requested excluded here; supervise also excludes),
# space-separated. Empty string on parse failure.
PENDING="$(printf '%s' "$QJSON" | python3 -c '
import json, sys
try:
    q = json.load(sys.stdin)
except Exception:
    print("")
    sys.exit()
ids = [str(t.get("id")) for t in q.get("tasks", [])
       if (t.get("status") or "pending") == "pending" and t.get("id")
       and not t.get("cancel_requested")]
print(" ".join(ids))
' 2>/dev/null)"

if [ -z "${PENDING}" ]; then
  # prune state to nothing pending (skip state writes on dry runs)
  if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then
    echo '{}' > "$STATE_FILE"
  fi
  silent "no pending tasks"
  exit 0
fi

# Classify pending ids: NEW (never announced) vs STUCK (announced long ago
# with no claim, or pending very long). State format: {tid: {"seen": epoch,
# "stuck_wake": epoch}}; old {tid: 1} entries are migrated to seen=now.
RESULT="$(python3 - "$STATE_FILE" $PENDING <<PYEOF 2>/dev/null
import json, sys, time
state_path, pending = sys.argv[1], sys.argv[2:]
claims = """$CLAIMS""".split()
now = int(time.time())
try:
    state = json.load(open(state_path))
except Exception:
    state = {}
def entry(tid):
    e = state.get(tid)
    if isinstance(e, dict) and "seen" in e:
        return e
    return {"seen": now, "stuck_wake": 0}  # migrate old format / first sight
def has_claim(tid):
    return any(c == tid + ".json" or c.startswith(tid + ".") for c in claims)
new, stuck = [], []
for tid in pending:
    e = entry(tid)
    if tid not in state:
        new.append(tid)
    else:
        age = now - int(e.get("seen", now))
        since_wake = now - int(e.get("stuck_wake", 0))
        if age > $NO_CLAIM_AFTER and not has_claim(tid) and since_wake > $NO_CLAIM_REWAKE:
            stuck.append(tid)
        elif age > $LONG_AFTER and since_wake > $LONG_REWAKE:
            stuck.append(tid)
print("NEW:" + " ".join(new))
print("STUCK:" + " ".join(stuck))
PYEOF
)"
NEW="$(printf '%s' "$RESULT" | sed -n 's/^NEW://p' | xargs)"
STUCK="$(printf '%s' "$RESULT" | sed -n 's/^STUCK://p' | xargs)"

if [ -z "${NEW}" ] && [ -z "${STUCK}" ]; then
  silent "no new tasks"
  exit 0
fi

# Record announced + stuck_wake (skip state writes on dry runs), prune to
# current pending.
if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then
  python3 - "$STATE_FILE" $PENDING <<PYEOF 2>/dev/null || true
import json, sys, time
state_path, pending = sys.argv[1], sys.argv[2:]
stuck = """$STUCK""".split()
now = int(time.time())
try:
    state = json.load(open(state_path))
except Exception:
    state = {}
out = {}
for tid in pending:
    e = state.get(tid)
    if not (isinstance(e, dict) and "seen" in e):
        e = {"seen": now, "stuck_wake": 0}
    if tid in stuck:
        e["stuck_wake"] = now
    out[tid] = e
json.dump(out, open(state_path, "w"))
PYEOF
fi

if [ -n "${NEW}" ]; then
  log "new-tasks" "{\"task_ids\": \"$NEW\"}"
  wake "new bridge tasks: $NEW" "{\"task_ids\": \"$NEW\", \"reason\": \"new\"}"
fi
if [ -n "${STUCK}" ]; then
  log "stuck-tasks" "{\"task_ids\": \"$STUCK\"}"
  wake "bridge tasks look stuck (no live worker?): $STUCK" "{\"task_ids\": \"$STUCK\", \"reason\": \"stuck\"}"
fi
exit 0
