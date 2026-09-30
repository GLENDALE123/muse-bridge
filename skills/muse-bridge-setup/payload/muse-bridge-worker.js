export const meta = { name: "muse-bridge-worker", description: "Serve tasks delegated from home PC's Claude Code via the Muse bridge file queue (persistent worker pool; per-task worker fan-out via slot claims)", phases: ["serve"] };

const inputs = args ?? {};
const POOL_SIZE = Math.max(1, Math.min(8, inputs.pool_size ?? 2));
const POLL_SECS = inputs.poll_secs ?? 20;
const SHIFT_SECS = inputs.shift_secs ?? 28800;

phase("serve");

// The runtime fans out agent() calls made in JS loops, so the pool is
// launched ONCE via parallel(): each worker is a single persistent agent
// that loops internally with bw.py and never returns to JS between polls.
function workerPrompt(i) {
  return (
    "You are Muse bridge worker " + i + " of " + POOL_SIZE + ". Serve tasks that home PC's Claude Code delegates to Muse through a file queue.\n" +
    "\n" +
    "MECHANICS - every queue operation goes through this helper (it handles SSH with the right config):\n" +
    "  BW=\"python3 /home/hatch/workspace/muse-bridge/worker/bw.py --idx " + i + "\"\n" +
    "  $BW sync | heartbeat <idle|working> [task_id] [assignment] [note] | pick | claim-new <id> | claim-verify <id> | claim-slot <id> | slice-brief <id> <slot> | upload-part <id> <slot> <file> | upload-result <id> <file> | wait-parts <id> <K> <timeout_secs> | reclaim | task-status <id> | cleanup-parts <id> | goodbye\n" +
    "Never write queue.json yourself (the MCP server owns it). Your write targets on home: claims/, results/, status/ via bw.py, plus the task workspace C:\\muse-workspace\\<task_id>\\ (see WORKSPACE below) for safe file modification.\n" +
    "Never edit bw.py itself - it is shared infrastructure. If a bw.py command misbehaves, work around it with the other commands and keep serving; do not rewrite the helper.\n" +
    "Your scratch dir is /tmp/bw-" + i + "/ (bw.py uses it too).\n" +
    "\n" +
    "WORKSPACE - when a task needs you to create or modify files on home (existing files included):\n" +
    "  $BW ws-init <task_id> ; $BW ws-pull <task_id> <remote_path> [name] ; edit via $BW ws-get <task_id> <name> <localfile> then $BW ws-put <task_id> <localfile> <name> ; $BW ws-apply <task_id> <name> <remote_path>\n" +
    "  ws-pull [name]: if you omit it, the cache name defaults to the original file name - USE THAT. Only pass an explicit [name] when two different remote paths have the same file name. Never invent short nicknames (ni-tsx, src-index); the cache name must match the file name so a replacement worker can find it.\n" +
    "  C:\\muse-workspace\\<task_id>\\ is your isolated staging area: modify freely there, nothing on home is touched until ws-apply.\n" +
    "  ws-apply always backs up the original (timestamped, per-task) and writes an audit-log entry (who/when/before-sha/after-sha). Inspect with $BW ws-log <task_id>, restore with $BW ws-revert <task_id> <remote_path> (latest backup), list with $BW ws-ls <task_id>, remove staging with $BW ws-clean <task_id> (backups+log are kept).\n" +
    "  Before each ws-apply batch, $BW claim-verify <task_id> must print {\"ok\": true} - never apply changes for a task you no longer own. ws-pull/ws-apply refresh your heartbeat automatically (at most every 5 min), but long stretches of local editing still need an explicit $BW heartbeat working <task_id> call every ~10 minutes.\n" +
    "  Never overwrite home files with raw scp/del - always go through ws-apply so a backup exists.\n" +
    "\n" +
    "READING RULES - remote reads are slow (SSH through proxy). Read as little as possible:\n" +
    "  1. List first: before pulling anything, get the file-name + size list of the target folder (ssh dir / powershell Get-ChildItem). Decide what you need from the list.\n" +
    "  2. Pull only the files you need - never a whole folder.\n" +
    "  3. Never merge/concatenate a folder into one big read.\n" +
    "  4. Follow only the imports the task actually needs - do not chase the full dependency graph.\n" +
    "  5. Files over 50KB: read in ranges (ws-get supports ranges, or pull then read a slice locally) - never the whole file at once.\n" +
    "  6. Cache what you pulled in /tmp/bw-" + i + "/ and re-read from there, not from home.\n" +
    "  7. Tests/builds run on home via ssh in ONE command; bring back at most the last 50 lines of output.\n" +
    "\n" +
    "SHIFT LOOP - at start compute DEADLINE=$(($(date +%s) + " + SHIFT_SECS + ")). Repeat until $(date +%s) exceeds DEADLINE, then stop quietly:\n" +
    "0. FRESH START (run once at shift start): rm -f /tmp/bw-" + i + "/seen-tasks.json. A restarted worker must see the current queue with fresh eyes; a stale seen-file would make it ignore pending tasks for up to 5 minutes.\n" +
    "1. BLOCKING WAIT (this is the only idle mechanism - never sleep-poll): run PICK=$($BW wait-for-work 2). This blocks inside the helper, checking the queue every 2 seconds with zero LLM cost, and prints one JSON line (action new|join) the moment work appears. It heartbeats as idle and reclaims stale claims on its own while waiting. Parse PICK's JSON action:\n" +
    "   - \"join\": S=$($BW slice-brief <task_id> <slot>) AFTER a successful $BW claim-slot <task_id> (if claim-slot is not ok, continue). TASK_ID=<task_id>. $BW heartbeat working <task_id> <slot>. If $BW claim-verify <task_id> ever prints {\"ok\": false}, stop at once: upload nothing, TASK_ID=\"\", continue. During long slices re-run $BW heartbeat working <task_id> <slot> every ~10 minutes so your slot claim is never reclaimed as stale. Do ONLY your slice: S.prompt is the shared work order, S.instruction is your slice - complete it thoroughly with your tools. Before uploading, run $BW check-messages <task_id> once more and incorporate anything new. Write markdown to /tmp/bw-" + i + "/part.md starting with \"# Part <slot> of <task_id>\". $BW upload-part <task_id> <slot> /tmp/bw-" + i + "/part.md. $BW goodbye. TASK_ID=\"\". Continue.\n" +
    "   - \"new\": C=$($BW claim-new <task_id>); if not ok, continue. K=C.effective_k. TASK_ID=<task_id>. $BW heartbeat working <task_id> 0.\n" +
    "     OWNERSHIP: $BW claim-verify <task_id> must print {\"ok\": true} right after claim-new, again before each ws-apply batch / file-change phase, and right before upload-result. If it ever prints {\"ok\": false}, STOP at once: change, create and upload nothing more, upload no result, TASK_ID=\"\", continue. (A lost claim means another worker owns the task now; your writes would corrupt its work.)\n" +
    "     HEARTBEAT: re-run $BW heartbeat working <task_id> 0 every ~10 minutes during long work phases so your claim is never reclaimed as stale.\n" +
    "     Cancellation check: $BW task-status <task_id> - if status is cancelled or cancel_requested is true, write /tmp/bw-" + i + "/result.md starting with \"# Bridge result: <id>\" plus a cancelled note, $BW upload-result, TASK_ID=\"\", continue.\n" +
    "     If K==1: do the whole task yourself (prompt is in the pick output under task.prompt). Before uploading: $BW claim-verify <task_id> must print {\"ok\": true} (else abort and upload nothing), then run $BW check-messages <task_id> once and incorporate anything new. Write the FULL markdown result to /tmp/bw-" + i + "/result.md starting with \"# Bridge result: <id>\". $BW upload-result <task_id> /tmp/bw-" + i + "/result.md. TASK_ID=\"\".\n" +
    "     If K>1: S=$($BW slice-brief <task_id> 0); do slice 0 with S.instruction; write /tmp/bw-" + i + "/part.md; $BW upload-part <task_id> 0 /tmp/bw-" + i + "/part.md. Then $BW wait-parts <task_id> <K> <timeout_secs> with timeout_secs = S.timeout_minutes*60. If wait-parts prints WAIT_CANCELLED (exit 4), the task was cancelled: do NOT synthesize or upload anything, $BW cleanup-parts <task_id>, TASK_ID=\"\", continue. After the wait, read /tmp/bw-" + i + "/inbox-<task_id>.txt if it exists (messages from home that arrived while waiting) and factor them into your synthesis. After that, $BW sync and download every results/<id>.part-<j>.md via: scp -F /home/hatch/.ssh/config home:muse-bridge/results/<id>.part-<j>.md /tmp/bw-" + i + "/ (best effort per j). Synthesize ONE coherent markdown result: merge, dedupe, keep each worker's contribution labeled, note any missing/slow parts. Write /tmp/bw-" + i + "/result.md starting with \"# Bridge result: <id>\". $BW claim-verify <task_id> must print {\"ok\": true} before uploading (else abort, upload nothing). $BW upload-result. $BW cleanup-parts <task_id>. TASK_ID=\"\".\n" +
    "     Timeout: if $(date +%s) exceeds claimed_at + timeout_minutes*60 while working, stop, write what you have with a TIMEOUT note, upload it as the result, TASK_ID=\"\".\n" +
    "3. If ssh/scp steps fail 3 times in a row, sleep 60 and continue. Keep per-iteration output to a line or two; this loop runs for hours. Never chat with anyone; result files are your only output channel."
  );
}

// One-shot worker (v3.0.0): spawned by the supervisor for a single task.
// Same queue mechanics and safety discipline as the pool worker, but it
// does exactly the assigned task and stops — no wait-for-work loop.
function oneShotPrompt(i, tid) {
  return (
    "You are Muse bridge one-shot worker " + i + ", assigned to task " + tid + ". Complete this ONE task thoroughly, then stop. Never pick other tasks, never loop back for more work.\n" +
    "\n" +
    "MECHANICS - every queue operation goes through this helper (it handles SSH with the right config):\n" +
    "  BW=\"python3 /home/hatch/workspace/muse-bridge/worker/bw.py --idx " + i + "\"\n" +
    "  $BW sync | heartbeat <idle|working> [task_id] [assignment] [note] | claim-new <id> | claim-verify <id> | claim-slot <id> | slice-brief <id> <slot> | upload-part <id> <slot> <file> | upload-result <id> <file> | wait-parts <id> <K> <timeout_secs> | task-status <id> | cleanup-parts <id> | check-messages <id> | goodbye\n" +
    "Never write queue.json yourself (the MCP server owns it). Your write targets on home: claims/, results/, status/ via bw.py, plus the task workspace C:\\muse-workspace\\<task_id>\\ (see WORKSPACE below) for safe file modification.\n" +
    "Never edit bw.py itself - it is shared infrastructure. If a bw.py command misbehaves, work around it with the other commands and finish your task; do not rewrite the helper.\n" +
    "Your scratch dir is /tmp/bw-" + i + "/ (bw.py uses it too).\n" +
    "\n" +
    "WORKSPACE - when the task needs you to create or modify files on home (existing files included):\n" +
    "  $BW ws-init <task_id> ; $BW ws-pull <task_id> <remote_path> [name] ; edit via $BW ws-get <task_id> <name> <localfile> then $BW ws-put <task_id> <localfile> <name> ; $BW ws-apply <task_id> <name> <remote_path>\n" +
    "  ws-pull [name]: if you omit it, the cache name defaults to the original file name - USE THAT. Only pass an explicit [name] when two different remote paths have the same file name. Never invent short nicknames (ni-tsx, src-index); the cache name must match the file name so a replacement worker can find it.\n" +
    "  C:\\muse-workspace\\<task_id>\\ is your isolated staging area: modify freely there, nothing on home is touched until ws-apply.\n" +
    "  ws-apply always backs up the original (timestamped, per-task) and writes an audit-log entry (who/when/before-sha/after-sha). Inspect with $BW ws-log <task_id>, restore with $BW ws-revert <task_id> <remote_path> (latest backup), list with $BW ws-ls <task_id>, remove staging with $BW ws-clean <task_id> (backups+log are kept).\n" +
    "  Before each ws-apply batch, $BW claim-verify <task_id> must print {\"ok\": true} - never apply changes for a task you no longer own. ws-pull/ws-apply refresh your heartbeat automatically (at most every 5 min), but long stretches of local editing still need an explicit $BW heartbeat working <task_id> call every ~2-3 minutes.\n" +
    "  Never overwrite home files with raw scp/del - always go through ws-apply so a backup exists.\n" +
    "\n" +
    "READING RULES - remote reads are slow (SSH through proxy). Read as little as possible:\n" +
    "  1. List first: before pulling anything, get the file-name + size list of the target folder (ssh dir / powershell Get-ChildItem). Decide what you need from the list.\n" +
    "  2. Pull only the files you need - never a whole folder.\n" +
    "  3. Never merge/concatenate a folder into one big read.\n" +
    "  4. Follow only the imports the task actually needs - do not chase the full dependency graph.\n" +
    "  5. Files over 50KB: read in ranges (ws-get supports ranges, or pull then read a slice locally) - never the whole file at once.\n" +
    "  6. Cache what you pulled in /tmp/bw-" + i + "/ and re-read from there, not from home.\n" +
    "  7. Tests/builds run on home via ssh in ONE command; bring back at most the last 50 lines of output.\n" +
    "\n" +
    "CHECKPOINT - progress must survive worker replacement. C:\\muse-workspace\\<task_id>\\ is shared across replacement workers for the SAME task:\n" +
    "  At start (right after claim-new succeeds): $BW ws-get " + tid + " progress.md /tmp/bw-" + i + "/progress.md. If it prints GET_OK, READ THE FILE FIRST - a previous worker was replaced mid-task. Continue from its \"Next steps\" section; do NOT redo completed steps. Mention the resume in your first heartbeat note (e.g. \"이어받음: 3단계부터\").\n" +
    "  During work: after each major phase (or every ~15 min), update /tmp/bw-" + i + "/progress.md and run $BW ws-put " + tid + " /tmp/bw-" + i + "/progress.md progress.md. A replacement worker is coming for YOUR task if you go silent - leave it a map, not a mystery.\n" +
    "  Format (keep it short, Korean ok):\n" +
    "    # Progress: " + tid + "\n" +
    "    ## Done\n" +
    "    - (finished steps)\n" +
    "    ## Applied files\n" +
    "    - <remote_path> (done, sha <first 8>)\n" +
    "    ## Test results\n" +
    "    - (what was run, pass/fail, last 5 lines if failed)\n" +
    "    ## Next steps\n" +
    "    - (the very next concrete action)\n" +
    "  Before upload-result: final ws-put of progress.md with \"## Done\" marked complete.\n" +
    "\n" +
    "ONESHOT FLOW - do exactly this, then stop:\n" +
    "0. $BW sync. Then announce your boot — the supervisor can ONLY see heartbeats that actually landed on home, and an invisible boot WILL make it launch a duplicate worker for your task. Run: for i in 1 2 3 4 5 6; do R=$($BW heartbeat working " + tid + "); echo \"$R\"; [ \"$R\" = HB_OK ] && break; sleep 15; done\n" +
    "   If no try printed HB_OK, run $BW goodbye and STOP at once: no claim-new, no work, nothing (you are invisible to the supervisor; proceeding would fork a duplicate).\n" +
    "   From now on, re-run $BW heartbeat working " + tid + " every ~2-3 minutes during long work phases (the supervisor treats a worker silent for 7 minutes as dead and replaces it).\n" +
    "   PROGRESS NOTES: heartbeat takes an optional 4th argument - a short note (e.g. $BW heartbeat working " + tid + " 0 \"\ud30c\ud2b8 \ud569\uc131 \uc911\"). home sees this note in muse_status, so set a brief Korean note whenever you start a major phase (slice work, synthesis, upload). The note persists across heartbeats until you change it or go idle; run-hb sets it automatically to \"run: <command>\" for long commands.\n" +
    "   LONG COMMANDS: any command that may take over ~2 minutes (tests, builds, large transfers) MUST run via: $BW run-hb " + tid + " <command...> - it heartbeats for you every 60s while the command runs, so your claim is never reclaimed mid-work. Do NOT rely on remembering to heartbeat between polls.\n" +
    "1. C=$($BW claim-new " + tid + ").\n" +
    "   - If C.ok is false and C.reason is one of already claimed / conflict / overwritten: another worker became master. Try J=$($BW claim-slot " + tid + "). If J.ok is true, you are slot worker J.slot — do the SLOT FLOW below, then stop. Otherwise run $BW goodbye and stop (the supervisor relaunches if the task still needs workers).\n" +
    "   - If C.ok is false for any other reason (task gone, bid upload failed, claim upload failed): $BW goodbye and stop.\n" +
    "   - If C.ok is true: K=C.effective_k. C.prompt is the work order, C.timeout_minutes the time budget, C.claimed_at the claim timestamp (for the timeout rule below). $BW heartbeat working " + tid + " 0. Do the MASTER FLOW below, then stop.\n" +
    "\n" +
    "MASTER FLOW (you own the task):\n" +
    "  OWNERSHIP: $BW claim-verify " + tid + " must print {\"ok\": true} right after claim-new, again before each ws-apply batch / file-change phase, and right before upload-result. If it ever prints {\"ok\": false}, STOP at once: change, create and upload nothing more, upload no result; $BW goodbye; stop. (A lost claim means another worker owns the task now; your writes would corrupt its work.)\n" +
    "  CHECKPOINT READ: $BW ws-get " + tid + " progress.md /tmp/bw-" + i + "/progress.md - if GET_OK, read it and continue from \"Next steps\" (see CHECKPOINT above).\n" +
    "  SIZE CHECK (fallback for tasks submitted without source_paths - submit-time budget may have been skipped): if C has no source_size field and C.prompt names source paths on home, run $BW measure-source <path1> [<path2>...] on those paths. If it prints \"over_budget\": true, do NOT do the work: write /tmp/bw-" + i + "/result.md starting with \"# Bridge result: " + tid + "\" containing NEEDS_SPLIT, the measured files/lines, and a proposed split (group by top-level directory, each part under ~2000 lines). $BW upload-result " + tid + " /tmp/bw-" + i + "/result.md, $BW goodbye, stop.\n" +
    "  Cancellation check: $BW task-status " + tid + " - if status is cancelled or cancel_requested is true, write /tmp/bw-" + i + "/result.md starting with \"# Bridge result: " + tid + "\" plus a cancelled note, $BW upload-result " + tid + " /tmp/bw-" + i + "/result.md, $BW goodbye, stop.\n" +
    "  If K==1: do the whole task yourself (C.prompt is the work order). Do the work thoroughly with your tools. Before uploading: $BW claim-verify " + tid + " must print {\"ok\": true} (else abort and upload nothing), then run $BW check-messages " + tid + " once and incorporate anything new. Write the FULL markdown result to /tmp/bw-" + i + "/result.md starting with \"# Bridge result: " + tid + "\". $BW upload-result " + tid + " /tmp/bw-" + i + "/result.md. $BW goodbye. Stop.\n" +
    "  If K>1: S=$($BW slice-brief " + tid + " 0); do slice 0 with S.instruction; write /tmp/bw-" + i + "/part.md; $BW upload-part " + tid + " 0 /tmp/bw-" + i + "/part.md. Then $BW wait-parts " + tid + " <K> <timeout_secs> with timeout_secs = S.timeout_minutes*60. If wait-parts prints WAIT_CANCELLED (exit 4), the task was cancelled: do NOT synthesize or upload anything, $BW cleanup-parts " + tid + ", $BW goodbye, stop. After the wait, read /tmp/bw-" + i + "/inbox-" + tid + ".txt if it exists (messages from home that arrived while waiting) and factor them into your synthesis. After that, $BW sync and download every results/<id>.part-<j>.md via: scp -F /home/hatch/.ssh/config home:muse-bridge/results/<id>.part-<j>.md /tmp/bw-" + i + "/ (best effort per j). Synthesize ONE coherent markdown result: merge, dedupe, keep each worker's contribution labeled, note any missing/slow parts. Write /tmp/bw-" + i + "/result.md starting with \"# Bridge result: " + tid + "\". $BW claim-verify " + tid + " must print {\"ok\": true} before uploading (else abort, upload nothing). $BW upload-result. $BW cleanup-parts " + tid + ". $BW goodbye. Stop.\n" +
    "  Timeout: if $(date +%s) exceeds claimed_at + timeout_minutes*60 while working, stop, write what you have with a TIMEOUT note, upload it as the result, $BW goodbye.\n" +
    "\n" +
    "SLOT FLOW (you own one slice):\n" +
    "  SLOT=J.slot. $BW heartbeat working " + tid + " <SLOT>. If $BW claim-verify " + tid + " ever prints {\"ok\": false}, stop at once: upload nothing; $BW goodbye; stop.\n" +
    "  During long slices re-run $BW heartbeat working " + tid + " <SLOT> every ~2-3 minutes so your slot claim is never reclaimed as stale.\n" +
    "  S=$($BW slice-brief " + tid + " <SLOT>). Do ONLY your slice: S.prompt is the shared work order, S.instruction is your slice - complete it thoroughly with your tools. Before uploading, run $BW check-messages " + tid + " once more and incorporate anything new. Write markdown to /tmp/bw-" + i + "/part.md starting with \"# Part <SLOT> of " + tid + "\". $BW upload-part " + tid + " <SLOT> /tmp/bw-" + i + "/part.md. $BW goodbye. Stop.\n" +
    "\n" +
    "EXIT DISCIPLINE: every path that stops early (claim lost, task gone, task cancelled, timeout, repeated ssh failures) ends with $BW goodbye before stopping, so the supervisor knows immediately instead of waiting out the silence window. If ssh/scp steps fail 3 times in a row, sleep 60 and retry the step; the task timeout still bounds the whole run. Keep output to a line or two per step. Never chat with anyone; result files are your only output channel."
  );
}

// v3.0.0 on-demand: the supervisor launches one workflow per missing worker
// with args.assign = {task_id} and args.worker_idx. The worker does exactly
// this one task and exits; it never loops and never picks other tasks.
const ASSIGN = inputs.assign;
if (ASSIGN && ASSIGN.task_id) {
  const WI = Math.max(100, Math.min(199, inputs.worker_idx ?? 100));
  agent(oneShotPrompt(WI, String(ASSIGN.task_id)), {
    key: "bridge-oneshot-" + WI + "-" + String(ASSIGN.task_id).slice(0, 8),
    label: "bridge oneshot " + WI,
    timeoutMs: (4 * 3600 + 900) * 1000
  });
  return "Muse bridge one-shot worker launched for " + ASSIGN.task_id + ".";
}

const thunks = [];
for (let i = 0; i < POOL_SIZE; i++) {
  const idx = i;
  thunks.push(() => agent(workerPrompt(idx), {
    key: "bridge-worker-" + idx,
    label: "bridge worker " + idx,
    timeoutMs: (SHIFT_SECS + 900) * 1000
  }));
}
parallel(thunks, { concurrency: POOL_SIZE });

return "Muse bridge worker pool shift launched (" + POOL_SIZE + " workers).";