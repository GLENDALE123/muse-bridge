#!/usr/bin/env bash
# muse-bridge-queue-watch: near-real-time dispatcher trigger.
# Polls home's queue.json every N seconds with ONE cheap SSH call (no LLM).
# Wakes a dispatcher agent only when genuinely new pending tasks appear.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

STATE_DIR="$HOME/hooks/state/muse-bridge-queue-watch"
STATE_FILE="$STATE_DIR/announced.json"
mkdir -p "$STATE_DIR"
[ -f "$STATE_FILE" ] || echo '{}' > "$STATE_FILE"

# One SSH call fetching just queue.json (~1s). BatchMode: never hang on auth.
QJSON="$(ssh -F /home/hatch/.ssh/config -o BatchMode=yes -o ConnectTimeout=15 home \
  'cmd /c type "C:\Users\ghfud\muse-bridge\queue.json"' 2>/dev/null)" || {
  log "queue-fetch-failed" '{}'
  silent "queue fetch failed"
  exit 0
}

# Pending task ids, space-separated. Empty string on parse failure.
PENDING="$(printf '%s' "$QJSON" | python3 -c '
import json, sys
try:
    q = json.load(sys.stdin)
except Exception:
    print("")
    sys.exit()
ids = [str(t.get("id")) for t in q.get("tasks", [])
       if (t.get("status") or "pending") == "pending" and t.get("id")]
print(" ".join(ids))
' 2>/dev/null)"

if [ -z "${PENDING}" ]; then
  silent "no pending tasks"
  exit 0
fi

# Diff against announced state -> genuinely new ids only.
NEW="$(python3 - "$STATE_FILE" $PENDING <<'PYEOF' 2>/dev/null
import json, sys
state_path = sys.argv[1]
pending = sys.argv[2:]
try:
    announced = json.load(open(state_path))
except Exception:
    announced = {}
new = [tid for tid in pending if tid not in announced]
print(" ".join(new))
PYEOF
)"
NEW="$(echo "$NEW" | xargs)"  # trim

if [ -z "${NEW}" ]; then
  silent "no new tasks"
  exit 0
fi

# Record announced (skip state writes on dry runs) and prune to current pending.
if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then
  python3 - "$STATE_FILE" $PENDING <<'PYEOF' 2>/dev/null || true
import json, sys
state_path = sys.argv[1]
pending = sys.argv[2:]
json.dump({tid: 1 for tid in pending}, open(state_path, "w"))
PYEOF
fi

log "new-tasks" "{\"task_ids\": \"$NEW\"}"
wake "new bridge tasks: $NEW" "{\"task_ids\": \"$NEW\"}"
exit 0
