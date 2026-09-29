---
description: Delegate a task to Muse subagents (submit + poll until done)
argument-hint: "<task prompt>"
---
# /muse-bridge:muse

Delegate the following task to Muse subagents running on the user's Muse VM.

Task: $ARGUMENTS

Steps:
1. If no task was given, ask the user what to delegate and stop.
2. Call `muse_submit` with the task as `prompt`. Decide `workers` (1-8):
   use 1 for a single coherent piece of work, 2-4 when it splits into
   parallel threads. When workers > 1, pass `worker_instructions` as a JSON
   array of per-worker slice instructions (concrete, non-overlapping).
   Use `priority` 1+ for urgent tasks.
3. Poll `muse_result` with the returned task_id about every 30 seconds until
   status is `done`, `failed`, or `cancelled`. Do other useful work between
   polls rather than busy-waiting.
4. Present the result, noting it was produced by a Muse subagent. If the
   bridge looks dead (`muse_status()` shows no alive workers), say so
   instead of polling forever.
