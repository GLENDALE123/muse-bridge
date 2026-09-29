#!/usr/bin/env node
/* muse-bridge PC setup: prepares a Windows PC for the Muse bridge.
 *
 *   npx muse-bridge setup --pubkey "ssh-ed25519 AAAA..." [--alias home]
 *
 * Does: create %USERPROFILE%\muse-bridge dirs, verify Tailscale + OpenSSH
 * Server, register the Muse VM's public key (admin), install the Claude Code
 * plugin, and print the pairing info the Muse-side installer needs.
 * No third-party dependencies.
 */
"use strict";
const { execSync } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

const BRIDGE_DIR = path.join(os.homedir(), "muse-bridge");
const MARKETPLACE_DIR = path.join(os.homedir(), "muse-marketplace", "muse-bridge");
const PLUGIN_SRC = path.join(__dirname, "..", "plugin");
const AUTH_KEYS = "C:\\ProgramData\\ssh\\administrators_authorized_keys";

function sh(cmd, opts = {}) {
  return execSync(cmd, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], ...opts }).trim();
}
function ok(msg) { console.log("  [ok] " + msg); }
function warn(msg) { console.log("  [!!] " + msg); }
function info(msg) { console.log("  " + msg); }

function isAdmin() {
  try { sh("net session"); return true; } catch { return false; }
}

function parseArgs(argv) {
  const out = { pubkey: null, alias: "home" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--pubkey" && argv[i + 1]) out.pubkey = argv[++i];
    else if (argv[i] === "--alias" && argv[i + 1]) out.alias = argv[++i];
    else if (argv[i] === "--help" || argv[i] === "-h") out.help = true;
  }
  return out;
}

function usage() {
  console.log([
    "muse-bridge setup — prepare this PC for the Muse bridge",
    "",
    "  npx muse-bridge setup --pubkey \"ssh-ed25519 AAAA...\" [--alias home]",
    "",
    "Get the --pubkey value from your Muse first: in Muse chat say",
    "\"install the muse bridge\" and it will print its SSH public key.",
    "Run this in an Administrator terminal for fully automatic setup.",
  ].join("\n"));
}

function ensureDirs() {
  for (const d of ["claims", "results", "status", "messages"]) {
    fs.mkdirSync(path.join(BRIDGE_DIR, d), { recursive: true });
  }
  const qpath = path.join(BRIDGE_DIR, "queue.json");
  try {
    fs.writeFileSync(qpath, JSON.stringify({ version: 2, tasks: [] }, null, 1) + "\n", { flag: "wx" });
  } catch (e) { if (e.code !== "EEXIST") throw e; }
  ok("bridge home: " + BRIDGE_DIR);
}

function checkTailscale() {
  let ip = null;
  try { ip = sh("tailscale ip -4").split("\n")[0].trim(); } catch {}
  if (ip) { ok("tailscale ip: " + ip); return ip; }
  warn("Tailscale not found or not logged in.");
  info("Install Tailscale (https://tailscale.com/download), run `tailscale up`,");
  info("then re-run this setup. The Muse side needs this PC's tailnet IP.");
  return null;
}

function checkSshServer() {
  try {
    const out = sh('powershell -NoProfile -Command "Get-Service sshd | Select-Object -ExpandProperty Status"');
    ok("OpenSSH Server status: " + out);
    return true;
  } catch {
    warn("OpenSSH Server (sshd) not found.");
    info("Install it: Settings > Apps > Optional features > Add > OpenSSH Server,");
    info("then: Start-Service sshd; Set-Service sshd -StartupType Automatic");
    return false;
  }
}

function registerKey(pubkey) {
  if (!pubkey) { warn("no --pubkey given; skipping key registration."); return false; }
  const key = pubkey.trim();
  if (!/^ssh-(ed25519|rsa|ecdsa)/.test(key)) {
    warn("the --pubkey value does not look like an SSH public key; skipping.");
    return false;
  }
  if (!isAdmin()) {
    warn("not running as Administrator — cannot write " + AUTH_KEYS);
    info("Manual step (Administrator terminal):");
    info(`  echo ${key} >> "${AUTH_KEYS}"`);
    info(`  icacls "${AUTH_KEYS}" /inheritance:r /grant SYSTEM:F /grant Administrators:F`);
    return false;
  }
  let cur = "";
  try { cur = fs.readFileSync(AUTH_KEYS, "utf8"); } catch {}
  if (!cur.includes(key)) {
    fs.appendFileSync(AUTH_KEYS, (cur.endsWith("\n") || cur === "" ? "" : "\n") + key + "\n");
  }
  sh(`icacls "${AUTH_KEYS}" /inheritance:r /grant SYSTEM:F /grant Administrators:F`);
  ok("registered Muse SSH key in administrators_authorized_keys");
  return true;
}

function copyDir(src, dst) {
  fs.mkdirSync(dst, { recursive: true });
  for (const e of fs.readdirSync(src, { withFileTypes: true })) {
    if (e.name === "__pycache__") continue;
    const s = path.join(src, e.name), d = path.join(dst, e.name);
    if (e.isDirectory()) copyDir(s, d);
    else fs.copyFileSync(s, d);
  }
}

function installPlugin() {
  try {
    copyDir(PLUGIN_SRC, MARKETPLACE_DIR);
    ok("plugin copied to " + MARKETPLACE_DIR);
  } catch (e) {
    warn("plugin copy failed: " + e.message);
    return;
  }
  const marketRoot = path.dirname(MARKETPLACE_DIR);
  const steps = [
    `claude plugin marketplace add "${marketRoot}"`,
    `claude plugin install muse-bridge@muse-marketplace`,
  ];
  for (const c of steps) {
    try { sh(c, { timeout: 120000 }); ok("ran: " + c); }
    catch {
      warn("could not run: " + c);
      info("Run it manually in a terminal where `claude` is on PATH, then restart Claude Code.");
      return;
    }
  }
  ok("plugin installed — restart Claude Code (or open a new session) to load it.");
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const cmd = process.argv[2];
  if (args.help || cmd !== "setup") { usage(); process.exit(cmd === "setup" ? 0 : 1); }

  console.log("muse-bridge PC setup\n");
  ensureDirs();
  const ip = checkTailscale();
  checkSshServer();
  registerKey(args.pubkey);
  installPlugin();

  console.log("\nPairing info for the Muse-side installer:");
  console.log("  alias:        " + args.alias);
  console.log("  tailnet ip:   " + (ip || "(see above — install Tailscale first)"));
  console.log("  windows user: " + os.userInfo().username);
  console.log("\nIn Muse chat say: \"install the muse bridge\" and give it the");
  console.log("tailnet IP above. The first SSH connection needs your approval in Muse.");
}

main();
