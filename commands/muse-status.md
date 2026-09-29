---
description: Show Muse bridge health (worker pool, queue, running tasks)
---
# /muse-bridge:status

Call `muse_status` and present a compact summary: how many workers are
alive and what each is doing, queued tasks, running tasks, and anything
that looks stuck (old claims, no heartbeat).
