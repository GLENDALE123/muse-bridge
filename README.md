# muse-bridge

Delegate Claude Code tasks to **Muse subagents** and get the results back in
your Claude Code session. Your PC's Claude Code submits work; your Muse (in the
cloud) runs it on real subagents and drops the results back.

```
Claude Code (your PC)  --queue.json-->  Muse VM  --subagents-->  results
        ^                                                      |
        └─────────────── muse_result / muse_await ─────────────┘
```

- **Real-time dispatch, zero idle cost.** A 10-second file watcher (one SSH
  call, no LLM) wakes a dispatcher only when new work arrives. No polling
  loop burns tokens while idle.
- **On-demand workers.** One worker per task, spawned when needed, exits when
  done. Up to 8 concurrent.
- **Home-file workspace.** Workers edit your real files via backup + audit
  log (`bw.py ws-*`), never blind overwrites.

## Contents

| Path | What |
|---|---|
| `plugin/` | Claude Code plugin `muse-bridge` (MCP server + skill + commands) |
| `cli/setup.js` | `npx muse-bridge setup` — one-command PC preparation |
| `skills/muse-bridge-setup/` | Muse-side installer skill (payloads: `bw.py`, worker workflow, hook script, SSH proxy) |

## Install

Two sides, in this order:

**1. Muse side first — get the SSH public key.**
In your Muse chat say: *"install the muse bridge"*. It prints its SSH public
key. Keep it.

**2. PC side — one command (Administrator terminal recommended).**
```powershell
npx muse-bridge setup --pubkey "ssh-ed25519 AAAA..."
```
This creates `%USERPROFILE%\muse-bridge\`, checks Tailscale + OpenSSH Server,
registers the Muse key, installs the Claude Code plugin, and prints your
PC's tailnet IP.

**3. Back in Muse chat**, give it the tailnet IP. It finishes SSH pairing
(first connection needs your approval in the Muse app), installs the worker
+ watcher hook + fallback crons, and runs an end-to-end test.

Then in Claude Code: `/muse-bridge:muse` → describe the task → collect the
result with `muse_result` (or block with `muse_await`).

## Adding another PC

Run step 2 on the new PC (same pubkey), then in Muse chat say
*"pair another PC"* with its tailnet IP. Workers are shared; each PC gets its
own watcher.

## Updating

- PC: `npx -y muse-bridge@latest setup` (idempotent), then
  `claude plugin update` + restart Claude Code.
- Muse: re-run the installer skill; it re-copies payloads.

## How it works (short)

The plugin's MCP server (`muse_submit`) appends tasks to
`%USERPROFILE%\muse-bridge\queue.json`. Every 10s a hook script on the Muse VM
checks that file over SSH. On new pending tasks it wakes a dispatcher agent,
which runs `bw.py supervise` (claims, dedup, max-8 concurrency) and launches
one-shot `muse-bridge-worker` workflows per task. A 15-minute sweep cron is
the fallback; a 2-hour keeper cron watches the sweep.

## License

MIT
