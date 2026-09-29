#!/bin/bash
# SSH to the Tailscale tailnet via the tunnel proxy (hatch-egress-proxy:3130).
# Proxy auth is read from env at runtime - never hardcode it here.
url="${HTTPS_PROXY:-$https_proxy}"
creds=$(printf '%s' "$url" | sed -E 's|http://([^@]+)@.*|\1|')
exec socat - "PROXY:hatch-egress-proxy:$1:$2,proxyport=3130,proxyauth=$creds"
