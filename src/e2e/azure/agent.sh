#!/usr/bin/env bash
# agent.sh — per-VM peer agent for the geo suite (Task 8), run by the
# orchestrator over SSH (cleaner lifecycle than a systemd unit: the
# Chromium instance lives exactly as long as the run that owns it, and a
# dropped SSH session tears both Chromium and its CDP relay down with it).
#
# Usage (on the VM):  agent.sh <js|dart>
#   js   -> the JS demo page   http://<vm-ip>/        (flash-demo,  port 80)
#   dart -> the dart-web page  http://<vm-ip>:8090/  (flash-demo-dart)
#
# This is the Azure analogue of the netns rig's child mode
# (src/e2e/scripts/netns-up.sh __child), minus everything netns-specific:
#   - no --allow-loopback-in-peer-connection and no --host-resolver-rules:
#     Azure VMs have real interfaces and real DNS, so host + srflx
#     candidates just work.
#   - --unsafely-treat-insecure-origin-as-secure stays: the demo origin is
#     plain http://<vm-ip> and secure-context APIs (crypto.randomUUID,
#     getUserMedia) are required (the demo also has a uuidV4 fallback).
#
# CDP: new headless ignores --remote-debugging-address and always binds
# 127.0.0.1 (verified empirically on the netns rig), so this script also
# starts scripts/cdp-relay.mjs — a plain TCP pipe from 0.0.0.0:9223 to
# 127.0.0.1:9222 — and the orchestrator connects over CDP to
# http://<vm-ip>:9223 (NSG: 9222-9225 open to the orchestrator IP only).
set -euo pipefail

role=${1:-}
case "$role" in
  js)   demo_port=80 ;;
  dart) demo_port=8090 ;;
  *) echo "usage: agent.sh <js|dart>" >&2; exit 2 ;;
esac

OPT_ROOT=/opt/flash
PLAYWRIGHT_ROOT=/opt/ms-playwright
PROFILE_ROOT="$OPT_ROOT/agent/profile-$role"

# --- this VM's public IP (Azure IMDS, non-routable from outside; falls
# back to the first private address, which still works VM-to-VM over the
# vnet if the topology is ever switched to private endpoints).
vm_ip=$(curl -fsS -m 5 -H Metadata:true \
  'http://169.254.169.254/metadata/instance/network/interface/0/ipv4/ipAddress/0/publicIpAddress?api-version=2021-02-01&format=text' \
  || true)
[ -n "$vm_ip" ] || vm_ip=$(hostname -I | awk '{print $1}')
[ -n "$vm_ip" ] || { echo "agent: cannot determine this VM's IP" >&2; exit 1; }

# --- chromium: the Playwright-managed build installed by cloud-init.
# Prefer the full chromium binary (the headless-shell build lacks some
# media paths the fake-device flag relies on).
chrome_bin=$(ls -1 "$PLAYWRIGHT_ROOT"/chromium-*/chrome-linux/chrome 2>/dev/null | head -n 1 || true)
[ -n "$chrome_bin" ] && [ -x "$chrome_bin" ] ||
  { echo "agent: no Playwright chromium under $PLAYWRIGHT_ROOT" >&2; exit 1; }

origin="http://${vm_ip}:${demo_port}"
# The JS demo is on the default port: keep the origin bare (http://ip, not
# http://ip:80) — the treated-secure origin must match the address bar.
[ "$demo_port" = 80 ] && origin="http://${vm_ip}"

# --- CDP relay: 0.0.0.0:9223 -> 127.0.0.1:9222 (see header comment).
node "$OPT_ROOT/src/e2e/scripts/cdp-relay.mjs" 0.0.0.0 9223 9222 &
relay_pid=$!

# --- headless Chromium. Mirrors netns-up.sh's child flags minus the
# netns-specific ones (see header). A per-role profile dir so a js and a
# dart agent can coexist on the same VM during debugging.
mkdir -p "$OPT_ROOT/agent"
"$chrome_bin" \
  --headless \
  --remote-debugging-port=9222 \
  --remote-debugging-address=0.0.0.0 \
  --user-data-dir="$PROFILE_ROOT" \
  --no-sandbox \
  --disable-gpu \
  --disable-dev-shm-usage \
  --no-first-run \
  --no-default-browser-check \
  --use-fake-ui-for-media-stream \
  --use-fake-device-for-media-stream \
  --autoplay-policy=no-user-gesture-required \
  --disable-features=WebRtcHideLocalIpsWithMdns \
  --remote-allow-origins='*' \
  --no-proxy-server \
  --unsafely-treat-insecure-origin-as-secure="$origin" \
  about:blank &
chrome_pid=$!

# If the relay dies early, take Chromium down with it (the SSH session
# closing handles the normal teardown path via SIGHUP).
if ! kill -0 "$relay_pid" 2>/dev/null; then
  kill "$chrome_pid" 2>/dev/null || true
  exit 1
fi

# --- readiness: poll the CDP endpoint (loopback — that's what the relay
# pipes), then announce so the orchestrator's ssh stdout can gate on it.
ready=0
for _ in $(seq 1 60); do
  if curl -fsS -m 2 http://127.0.0.1:9222/json/version 2>/dev/null | grep -q '"Browser"'; then
    ready=1
    break
  fi
  if ! kill -0 "$chrome_pid" 2>/dev/null; then
    echo "agent: chromium exited before CDP came up" >&2
    exit 1
  fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "agent: CDP endpoint never came up" >&2; exit 1; }

echo "agent ready: role=$role origin=$origin cdp=9222 relay=9223 pid=$chrome_pid"

# Stay in the foreground: Chromium (and the relay) die when the orchestrator
# closes the SSH session — the lifecycle the geo suite wants.
wait "$chrome_pid"