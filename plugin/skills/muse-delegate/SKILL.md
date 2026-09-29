---
name: muse-delegate
description: Delegate work to Muse subagents on the user's Muse VM via the muse-bridge plugin. Use when a task is long-running, benefits from parallel investigation, or should run in the background while you continue.
---

# Delegating to Muse subagents

The `muse-bridge` plugin connects this Claude Code session to subagents
running on the user's Muse VM. The bridge is a file queue: you submit, a
worker is spawned on demand for your task, you await the markdown result.
There is no standing pool to manage — workers appear when tasks arrive and
disappear when the work is done. Up to 8 workers run concurrently.

## Tools (MCP: muse-bridge)

- `muse_submit(prompt, label, workers, worker_instructions, priority, timeout_minutes)` → `{task_id}`
- `muse_await(task_id, timeout_secs=600)` → wait server-side until the task
  finishes, then return `{status: done|cancelled|timeout, result?}`. Call
  once instead of polling `muse_result`. On `timeout`, call again to keep
  waiting.
- `muse_result(task_id)` → `{status: done|running|pending|cancelled, result?}` (polling fallback)
- `muse_status()` → bridge health, queued and running tasks
- `muse_cancel(task_id)` → cancel a queued or running task
- `muse_message(task_id, text)` → send a message to the worker(s) on a task:
  changed instructions, extra context, new priorities. Delivered at the
  worker's next checkpoint (between steps, before slice upload, before final
  synthesis) — not a real-time interrupt. Rejected for finished/unknown tasks.

## Mid-task messaging

While a task is running, use `muse_message` to steer it: correct a
misunderstanding, add a constraint you forgot, reprioritize, or ask the
worker to cover an extra angle. Keep messages short and actionable — the
worker folds them into its ongoing work at the next checkpoint. One message
per change; the worker sees them in order.

## When to delegate

Good fits: deep research, multi-angle investigation, design-doc review,
long syntheses, background monitoring tasks, and file work on this PC
(see below). Bad fits: anything needing credentials or interactive tools.

## File operations on this PC

Workers CAN read, create, and modify files on this machine. Just name the
absolute path in the prompt, e.g. "fix the typo on line 3 of
C:\scripts\deploy.ps1". Every modification goes through a safe workspace
flow: the worker stages the file in an isolated per-task directory, edits
there, then applies the change — the original is always backed up
(timestamped) and every change is audit-logged, so it can be reverted.
No extra setup needed; describe the file and the desired change in the
task prompt.

## Two delegation patterns

**Direct (recommended, the default).** You split, workers execute, you
merge — exactly like the Agent tool. Submit one task per independent
thread with `workers: 1` and a self-contained prompt; each task gets its
own worker and its own result file. Await each `task_id` and combine the
results yourself.

```
muse_submit(prompt="...", label="korean-prior-art", workers=1)  -> id A
muse_submit(prompt="...", label="foreign-prior-art", workers=1) -> id B
muse_await(A); muse_await(B)   # merge the two markdown results yourself
```

**Fan-out (only when you want ONE synthesized result).** Submit a single
task with `workers: 2-4` and `worker_instructions` (one string broadcast,
or a JSON array of per-worker instructions). The workers split the slices
and one of them merges the parts into a single result file. Costs one
extra synthesis pass, and you cannot steer individual slices mid-task —
prefer Direct unless the merged document is the deliverable.

## How to submit well

- `prompt` is the whole work order. Write it self-contained: goal, scope,
  constraints, and the exact shape of the expected output (e.g. "return a
  markdown report with sections ..."). The worker has no other context.
- `workers` (1-8): use 1 (Direct pattern). Use 2-4 only for the Fan-out
  pattern, when the task splits into parallel threads (e.g. domestic vs
  foreign prior art) and you want one merged result. Each worker only sees
  its own slice instruction plus the shared prompt.
- `worker_instructions`: one string broadcast to all workers, or a JSON
  array of per-worker instructions, e.g.
  `'["survey Korean patents", "survey foreign papers", "survey OSS implementations"]'`.
  Keep each slice instruction concrete and non-overlapping; one worker
  synthesizes the parts into one result.
- `priority`: default 0; use 1+ for urgent tasks.
- `timeout_minutes`: default 60; raise for genuinely long investigations.

## Waiting for results

1. `muse_submit` → keep the `task_id`.
2. Call `muse_await(task_id)` once — it waits server-side until the task
   finishes and returns the result. No polling loop needed. On `timeout`,
   call it again to keep waiting, or do other work between calls. For the
   Direct pattern, await each task id (calls are independent — await them
   one after another or interleave other work between them).
3. Fallback: poll `muse_result` about every 30 seconds if you need to check
   status without blocking.
4. On `done`, read `result` (markdown) and present it. On `cancelled`,
   say so. On repeated `running` with no progress for a long time, check
   `muse_status()` — a dead bridge shows no supervisor and no workers.

## Slash commands

- `/muse-bridge:muse <task>` — submit and wait until done.
- `/muse-bridge:status` — show bridge health.
