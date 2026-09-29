# muse-bridge-setup

Install the **Muse side** of the muse-bridge on this Muse VM and pair it with
the user's PC. The PC side is prepared separately with `npx muse-bridge setup`
(see the repo README). This skill is executed by a Muse agent; several steps
require agent tools (`hooks.add`, `cron.add`) that no shell script can call —
that is why the Muse side is a skill, not a CLI.

## What gets installed

| File | Destination | Purpose |
|---|---|---|
| `payload/bw.py` | `~/workspace/muse-bridge/worker/bw.py` | worker CLI (supervise, claims, results, ws-*) |
| `payload/muse-bridge-worker.js` | `~/workspace/.jarvis/workflows/muse-bridge-worker.js` | saved workflow: one-shot worker (dropping the file registers it) |
| `payload/queue-watch.sh` | `~/hooks/scripts/muse-bridge-queue-watch.sh` | 10s queue watcher (no LLM cost) |
| `payload/tailscale-proxy.sh` | `~/workspace/bin/tailscale-proxy.sh` | SSH ProxyCommand via the egress proxy |

Plus, per paired PC: an SSH host alias, one event hook, and the shared
fallback crons (sweep 15m, keeper 2h — created once, not per PC).

## Procedure

### 0. Preconditions

- Payloads: use this skill's `payload/` dir. If it is missing, fetch the npm
  tarball instead: `npm pack muse-bridge` (or `npx -y muse-bridge --version`)
  and use its `skills/muse-bridge-setup/payload/`.
- The PC must have run `npx muse-bridge setup --pubkey "<key>"` — but the
  public key comes from step 1 below, so do step 1 first, hand the key to the
  user, and continue while they run the PC setup.

### 1. SSH keypair (Muse side)

```bash
[ -f ~/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub
```

Print the public key and tell the user: run the PC setup with
`--pubkey "<that key>"` in an Administrator terminal. The PC CLI registers it
in `C:\ProgramData\ssh\administrators_authorized_keys` and fixes the ACL
automatically; without admin it prints manual steps.

### 2. Collect pairing facts (ask the user)

- `alias`: short SSH host name, e.g. `home`, `laptop` (default `home`)
- `tailnet_ip`: the PC's Tailscale IPv4 (the PC setup prints it)
- `pc_user`: the Windows username (the PC setup prints it)

### 3. SSH client config (Muse side)

Append to `~/.ssh/config` (create it if missing):

```
Host <alias>
    HostName <tailnet_ip>
    User <pc_user>
    IdentityFile /home/hatch/.ssh/id_ed25519
    ProxyCommand /home/hatch/workspace/bin/tailscale-proxy.sh %h %p
```

Rules (learned the hard way):
- `IdentityFile` must be an **absolute** path: the ssh process runs as root,
  so `~` expands to `/root`, not `/home/hatch`.
- Shell `exec` runs as root and reads `/root/.ssh/config`, not
  `$HOME/.ssh/config`. After writing the config, ensure the symlink exists:
  `ln -sf /home/hatch/.ssh/config /root/.ssh/config` (recreate after VM
  reboot/replacement if `ssh <alias>` breaks — `/root` is ephemeral).
- The proxy script reads auth from `${HTTPS_PROXY}` at runtime; never hardcode
  or print it.

### 4. First connection (egress approval)

```bash
ssh -F /home/hatch/.ssh/config -o BatchMode=yes -o ConnectTimeout=15 <alias> "echo ok"
```

The first connection to a new tailnet IP needs the user's approval in the
Muse app (it then covers all TCP to that IP). Tell the user to approve, then
retry. `BatchMode=yes` so it never hangs on a password prompt. Do not
proceed until `echo ok` returns `ok`.

### 5. Install payload files

```bash
mkdir -p ~/workspace/muse-bridge/worker ~/workspace/bin
cp payload/bw.py ~/workspace/muse-bridge/worker/bw.py
cp payload/muse-bridge-worker.js ~/workspace/.jarvis/workflows/muse-bridge-worker.js
cp payload/queue-watch.sh ~/hooks/scripts/muse-bridge-queue-watch.sh
cp payload/tailscale-proxy.sh ~/workspace/bin/tailscale-proxy.sh
chmod +x ~/workspace/muse-bridge/worker/bw.py ~/hooks/scripts/muse-bridge-queue-watch.sh ~/workspace/bin/tailscale-proxy.sh
python3 ~/workspace/muse-bridge/worker/bw.py --help  # sanity check
```

### 6. Queue-watch hook (per PC)

The watcher script polls `<alias>`'s `queue.json` every 10s over one SSH call
(zero LLM cost) and wakes a dispatcher only on genuinely new pending tasks.
If the script hardcodes a host, parameterize it per alias (env var or a
per-alias copy, e.g. `muse-bridge-queue-watch-<alias>.sh`).

Register with `hooks.add`:
- `id`: `muse-bridge-queue-watch` (first PC) or `muse-bridge-queue-watch-<alias>`
- `script_path`: `~/hooks/scripts/muse-bridge-queue-watch.sh`
- `poll_interval_secs`: `10`
- `prompt`: the dispatcher instruction below (keep it verbatim; it encodes
  the hard-won rules):

```text
You are the on-demand dispatcher for the Muse bridge (PC <-> this Muse VM).
A file-watcher hook woke you because new pending bridge tasks were submitted.
The wake payload contains space-separated task_ids.

Do exactly this:
1. Run: python3 /home/hatch/workspace/muse-bridge/worker/bw.py --idx 99 supervise 0
   It prints a JSON plan: a list of {"task_id": ..., "worker_idx": ...} entries
   to spawn, or [] when nothing is needed.
   IMPORTANT: supervise takes 40-90s (multiple SSH round-trips). The exec tool
   WILL background it and return a session id instead of the result. That is
   normal, not a failure. Use process.poll (repeatedly, timeout up to 120000ms
   per call) until it completes, then read the JSON plan from its output. NEVER
   conclude "supervise failed" just because the result did not arrive
   immediately — wait for the session to finish and verify the output first.
2. For each plan entry, call workflow.launch_async with
   name="muse-bridge-worker" and
   args={"assign": {"task_id": "<id>"}, "worker_idx": <idx>}.
   Issue all launches in one parallel block (independent calls).
3. Trust the supervise plan: it enforces max 8 concurrent workers, skips tasks
   with live workers, boot-duplicate protection (intent TTL 240s), and skips
   tasks that already have results. Never spawn anything not in the plan.
4. SSH to a paired PC always as: ssh -F /home/hatch/.ssh/config <alias>

Stay silent to the user on success. In your execute summary report: tasks
seen, workers launched (task_id + worker_idx), or "no action needed".
Surface to the main agent only if supervise errored or a launch failed.
```

Then `hooks.dry_run` to verify the script, then `hooks.enable`.

### 7. Fallback crons (once, shared by all PCs)

- `muse-bridge-sweep` (interval `15m`, title `Muse bridge sweep (fallback)`):
  one supervise pass + `workflow.launch_async` per plan entry, exactly as the
  dispatcher does. Never writes `queue.json` (owned by the PC's MCP server).
  Silent on success; report only repeated failures or a stalled queue.
- `muse-bridge-keeper` (interval `2h`, title `Muse bridge keeper`): verifies
  the sweep cron/hook still exist and are enabled (repair if not), stops
  stale `muse-bridge-worker` runs without `assign` args (legacy pool
  remnants), never touches one-shot workers. Silent on success.

Give each an owner (a tracked item or goal for the bridge); a bare cron with
no owner is a routing error.

### 8. End-to-end test

Ask the user to submit a test task from Claude Code on the PC
(`/muse-bridge:muse` → "reply with the word PONG", workers=1). Within ~10s
the hook should wake the dispatcher; the one-shot worker claims it, runs,
and `results/<id>.md` appears on the PC. Verify with
`muse_result` from the PC side. If the hook does not fire within 30s, run
`hooks.dry_run` and inspect `~/hooks/logs/`.

## Adding another PC later

Repeat steps 2–6 with a new alias. Worker pool (`bw.py` max 8 concurrent) is
shared across PCs. Give each PC its own hook id
(`muse-bridge-queue-watch-<alias>`); the sweep/keeper crons stay shared.

## Updating

PC side: `npx -y muse-bridge@latest setup` (re-run; idempotent).
Muse side payloads: re-fetch the tarball and re-copy the four files, then
restart any running one-shot workers only if `bw.py` changed incompatibly.
The plugin on the PC updates via `claude plugin update` + a new Claude Code
session.
