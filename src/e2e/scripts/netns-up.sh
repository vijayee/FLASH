#!/usr/bin/env bash
# netns-up.sh — deterministic per-region latency rig, 100% unprivileged
# (Task 3). One Linux network namespace per region, each running headless
# Chromium with its own `tc netem` egress delay, reachable from the host
# through slirp4netns's forwarded CDP port.
#
# Rootlesskit could not be used on this machine: its multi-subuid mapping
# needs newuidmap(1) from uid-runtime, which is not installed — verified:
# `newuidmap: No such file or directory`). The same architecture is
# realized directly with the two primitives that DO work unprivileged
# here (both verified empirically on this machine):
#
#   1. `unshare -Urn --map-root-user` — a user+network namespace whose
#      mapped root holds CAP_NET_ADMIN: it creates tap0 and addresses it.
#   2. `slirp4netns -c -r 3 --api-socket=<sock> <childPid> tap0` with
#      --ready-fd — attaches user-mode networking to that netns from the
#      host as the same unprivileged user (it brings the tap up + assigns
#      10.0.2.100/24 + sets the default route), and its raw-JSON API
#      socket (scripts/cdp-relay.mjs) adds the host->guest forwarding
#      that puts Chromium's CDP endpoint (9222 inside the netns) on a
#      host port.
#
# Netem goes on tap0 INSIDE each namespace — that delays the region's
# egress, so the RTT between two regions is the sum of their half
# delays (plus the slirp overhead, verified empirically on this
# machine).
#
# ONE slirp subnet for every region: slirp4netns 1.0.1 ignores --cidr
# (its --configure always assigns 10.0.2.100/24 with the 10.0.2.2
# gateway, verified empirically: us/asia guests raised with
# --cidr=10.0.3.0/24 etc. ended up with 10.0.2.100/24 AND their
# computed address, and their outbound to the computed gateway failed
# with net::ERR_ADDRESS_UNREACHABLE). Every region therefore shares
# tap0 = 10.0.2.100/24, gateway 10.0.2.2 -> the host's loopback, which
# is verified working end to end (HTTP 200 from inside a namespace).
#
# Chromium's DevTools server binds 127.0.0.1 INSIDE the netns (new
# headless ignores --remote-debugging-address), so the slirp
# host-forward targets the in-netns CDP relay (scripts/cdp-relay.mjs:
# 0.0.0.0:9223 -> 127.0.0.1:9222) instead of Chromium's port directly.
#
# The netns has no usable DNS (its resolv.conf is the host's
# systemd-resolved stub 127.0.0.53, which inside the netns is the
# namespace's own loopback), so Chromium maps the library's
# stun:stun.l.google.com:19302 to a host-resolved IPv4 via
# --host-resolver-rules (ahostsv4, not `hosts`: `hosts` may return the
# IPv6 address first, and slirp4netns 1.0.1 NATs IPv4 only — an IPv6
# STUN target would be unreachable from the netns). The resulting
# server-reflexive candidates (the host's egress IP + slirp-mapped
# ports) pair across the netns through the host's NAT hairpin — the
# host candidates (10.0.2.100, identical in every netns) are
# self-referential and fail their DTLS check, which ICE discards.
#
# Idempotent: every raise first sweeps this region's orphaned
# processes (marker scan over /proc/*/cmdline, so a recycled pid can
# never take out an unrelated process) and drops the region's stale
# state dir — stale child.pid/tap-ip.txt files race the fresh raise
# (the host-side wait loops break instantly on a stale non-empty pid
# file and attach slirp4netns to a dead child). Requires no sudo
# anywhere.
#
# NOTE ON DELAYS: the scripted half delays are eu=60ms, us=20ms,
# asia=100ms -> RTT eu<->us ~80ms, eu<->asia ~160ms, us<->asia ~120ms
# (+0-2ms slirp overhead). This corrects the plan's netem 20/20/60ms
# sketch: netem on a device delays only that namespace's transmitted
# packets (egress), so RTT(a<->b) = halfDelay_a + halfDelay_b, and
# 20/20/60 would have measured 40/80/80ms — landing the us and asia
# peers a ring lower than the latency assertions expect.
set -euo pipefail

CDP_GUEST_PORT=9222
# In-netns CDP relay listen port (-> Chromium's 127.0.0.1:9222).
CDP_RELAY_PORT=9223
STATE_ROOT=/tmp/mrd-netns
SLIRP_API="${0%/*}/slirp-api.mjs"
CDP_RELAY="${0%/*}/cdp-relay.mjs"
UDP_PROBE="${0%/*}/udp-probe.mjs"

# The slirp gateway inside the netns (maps to the host's loopback).
GATEWAY=10.0.2.2
TAP_IP=10.0.2.100

die() {
  echo "netns-up: $*" >&2
  # Forensics: the rig's own logs (chromium/slirp/hostfwd/relay/tc),
  # read before any teardown sweep can remove them. Bounded tails so a
  # huge log cannot flood the run output.
  local region=${2:-}
  if [ -n "$region" ] && [ -d "$STATE_ROOT/$region" ]; then
    for log in chromium.log slirp.log hostfwd.log relay.log tc.txt diag.txt; do
      if [ -s "$STATE_ROOT/$region/$log" ]; then
        echo "--- $region/$log (tail):" >&2
        tail -n 12 "$STATE_ROOT/$region/$log" >&2
      fi
    done
  fi
  exit 1
}

json_num() { # <jsonFile> <field> -> the field's number (empty if absent)
  sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p" "$1" |
    head -n 1
}

sweep_orphans() { # <region> — kill any live rig process of this region
  local region=$1 pid pass cmdline
  # Marker scan (not pid files): catches rigs that died before writing
  # their state. Chromium (the exec'd unshare child), its slirp4netns
  # attachment and the CDP relay all carry /tmp/mrd-netns/<region> on
  # their command line; the rig's own host-mode shell does not.
  local self=$$
  for pass in TERM KILL; do
    for cmdline in /proc/[0-9]*/cmdline; do
      pid=${cmdline#/proc/}; pid=${pid%/cmdline}
      [ -r "$cmdline" ] || continue
      [ "$pid" != "$self" ] || continue
      if tr '\0' ' ' <"$cmdline" | grep -q "mrd-netns/$region"; then
        if [ "$pass" = TERM ]; then kill "$pid" 2>/dev/null; fi
        if [ "$pass" = KILL ]; then kill -9 "$pid" 2>/dev/null; fi
      fi
    done
    # NOT `A && sleep` — under `set -e` a failed A (the KILL pass) would
    # exit the whole rig with code 1 before it raised anything.
    if [ "$pass" = TERM ]; then sleep 0.5; fi
  done
}

# --- child mode (runs INSIDE the user+net namespace as its mapped root) ---
if [ "${1:-}" = "__child" ]; then
  shift
  region_dir=$1 half_ms=$2 chrome_bin=$3 stun_ip=$4
  echo $$ >"$region_dir/child.pid"

  ip link set lo up
  # tap0 is pre-created here so the tc/netem below has a device before
  # Chromium starts; slirp4netns (attached from the host right after)
  # finds it already present and --configure brings it up + assigns
  # 10.0.2.100/24 + sets the default route via 10.0.2.2.
  ip tuntap add tap0 mode tap

  # Wait for slirp4netns --configure to assign the guest address and
  # bring the tap up (it races with us: it attaches once our pid is on
  # file). NO fallback route/addr setup here — slirp4netns
  # --configure's own route-add would hit EEXIST against ours and take
  # slirp4netns down (verified empirically on this machine).
  for _ in $(seq 1 75); do
    if ip -4 addr show dev tap0 | grep -q "$TAP_IP"; then
      break
    fi
    sleep 0.2
  done
  ip -4 addr show dev tap0 | grep -q "$TAP_IP" ||
    die "tap0 was never configured by slirp4netns" "$region"

  # The region's one-way egress delay; RTT to another region is the sum
  # of the two regions' half delays (verified egress-only model).
  tc qdisc add dev tap0 root netem delay "${half_ms}ms"

  tc qdisc show dev tap0 >"$region_dir/tc.txt" 2>&1

  # Task 10 (forced-relay TURN spec, MRD_UDP_ALLOW=<ip>): starve the netns
  # of outbound UDP so host/srflx candidates can never pair and ICE must
  # fall back to the relay — everything on tap0's OUTPUT except UDP to the
  # TURN server is dropped. The mapped root holds CAP_NET_ADMIN (verified:
  # iptables-nft works inside this unshare), TCP is untouched (signaling,
  # demo, CDP), and the TURN server's relayed address IS its own public IP,
  # so the single ACCEPT rule covers the data channel too. Evidence (rules
  # + one probe allowed / one probe blocked) lands in the region dir.
  if [ -n "${MRD_UDP_ALLOW:-}" ]; then
    iptables -A OUTPUT -o tap0 -p udp -d "$MRD_UDP_ALLOW" -j ACCEPT
    iptables -A OUTPUT -o tap0 -p udp -j DROP
    iptables -S OUTPUT >"$region_dir/iptables.txt" 2>&1
    # Allowed path: the TURN server answers STUN binding requests.
    node "$UDP_PROBE" "$MRD_UDP_ALLOW" "${MRD_TURN_PORT:-3478}" 4000 \
      >"$region_dir/udp-allow.txt" 2>&1 || true
    # Blocked path: a STUN probe anywhere else times out (here: the rig's
    # own loopback mini-STUN through the slirp gateway).
    node "$UDP_PROBE" "$GATEWAY" "${MRD_STUN_PORT:-3478}" 2000 \
      >"$region_dir/udp-block.txt" 2>&1 || true
  fi

  # The guest address as slirp4netns --configure assigned it (the
  # host-side script reads it for the hostfwd guest_addr).
  ip -4 -o addr show dev tap0 | awk '{print $4}' >"$region_dir/tap-ip.txt"

  # Relay host->guest CDP through to Chromium's loopback-bound DevTools
  # server (parented to $$ so it exits when Chromium, the exec'd child
  # below, is gone). The region dir rides along as the sweep's cmdline
  # marker.
  node "$CDP_RELAY" 0.0.0.0 $CDP_RELAY_PORT $CDP_GUEST_PORT $$ "$region_dir" \
    >"$region_dir/relay.log" 2>&1 &
  echo $! >"$region_dir/relay.pid"

  if [ "${NETNS_DIAG:-0}" = 1 ]; then
    {
      echo "== qdisc"; tc qdisc show dev tap0
      echo "== addr"; ip -4 addr show dev tap0
      echo "== ping $GATEWAY (one netem egress expected)"
      ping -c 3 -q "$GATEWAY" 2>&1 | tail -3 || true
      echo "== host demo via gw ($GATEWAY:8090 -> host 127.0.0.1:8090)"
      curl -s -m 8 "http://$GATEWAY:8090/" -o /dev/null \
        -w 'http %{http_code} connect %{time_connect}s total %{time_total}s\n' || true
    } >"$region_dir/diag.txt" 2>&1
  fi

  # The netns demo origin is plain http via slirp — treat it as secure so
  # secure-context APIs (crypto.randomUUID, getUserMedia) exist. The demo
  # also carries its own uuidV4 fallback; this flag covers both.
  exec "$chrome_bin" \
    --headless \
    --remote-debugging-port=$CDP_GUEST_PORT \
    --user-data-dir="$region_dir/profile" \
    --no-sandbox \
    --disable-gpu \
    --disable-dev-shm-usage \
    --no-first-run \
    --no-default-browser-check \
    --use-fake-ui-for-media-stream \
    --use-fake-device-for-media-stream \
    --autoplay-policy=no-user-gesture-required \
    --disable-features=WebRtcHideLocalIpsWithMdns \
    --allow-loopback-in-peer-connection \
    --remote-allow-origins='*' \
    --no-proxy-server \
    --host-resolver-rules="MAP stun.l.google.com ${stun_ip:-74.125.250.129}" \
    --unsafely-treat-insecure-origin-as-secure="${E2E_DEMO_ORIGIN:-http://10.0.2.2:8090}" \
    about:blank
fi

# --- host mode -------------------------------------------------------------
if [ "${1:-}" = "--all" ]; then
  chrome_bin=${2:-}
  # Mirrors REGIONS in src/netns-launch.ts (single source of truth there).
  set -- eu 60 9223 us 20 9224 asia 100 9225
  while [ $# -ge 3 ]; do
    "$0" "$1" "$2" "$3" "$chrome_bin"
    shift 3
  done
  exit 0
fi

region=${1:?usage: netns-up.sh <region> <halfDelayMs> <cdpHostPort> [chromePath] | --all [chromePath]}
half_ms=$2 cdp_host_port=$3 chrome_bin=${4:-} stun_ip=${5:-74.125.250.129}
region_dir="$STATE_ROOT/$region"
state_json="$STATE_ROOT/$region.json"

# Chromium: default to the Playwright-managed build (resolved through the
# e2e package so the script's cwd never matters).
if [ -z "$chrome_bin" ]; then
  chrome_bin=$(
    cd "$(dirname "$0")/.." && node -p "require('playwright-core').chromium.executablePath()"
  )
fi
[ -x "$chrome_bin" ] ||
  { echo "netns-up: chrome binary not executable: $chrome_bin" >&2; exit 1; }

# Idempotency: sweep this region's orphaned rig processes first, then
# drop its stale state dir — stale child.pid/tap-ip.txt files race the
# fresh raise (the host-side wait loops break instantly on a stale
# non-empty pid file and attach slirp4netns to a dead child).
sweep_orphans "$region"
rm -rf "$region_dir" "$state_json"
mkdir -p "$region_dir"

# 1. The child: user+net namespace, tap0, netem, then Chromium (exec'd).
env NETNS_DIAG="${NETNS_DIAG:-0}" \
  unshare -Urn --map-root-user \
  bash "$0" __child "$region_dir" "$half_ms" "$chrome_bin" "$stun_ip" \
  >"$region_dir/chromium.log" 2>&1 &
parent=$!

# 2. Wait for the child to announce itself, then attach slirp4netns to it.
for _ in $(seq 1 50); do
  [ -s "$region_dir/child.pid" ] && break
  kill -0 "$parent" 2>/dev/null ||
    die "child died before writing its pid" "$region"
  sleep 0.2
done
child=$(cat "$region_dir/child.pid")

: >"$region_dir/ready"
slirp4netns -c -r 3 --api-socket="$region_dir/api.sock" \
  --netns-type=pid "$child" tap0 \
  >"$region_dir/slirp.log" 2>&1 3>"$region_dir/ready" &
slirp=$!
echo "$slirp" >"$region_dir/slirp.pid"

for _ in $(seq 1 75); do
  [ -s "$region_dir/ready" ] && break
  kill -0 "$slirp" 2>/dev/null ||
    die "slirp4netns exited early" "$region"
  sleep 0.2
done
[ -s "$region_dir/ready" ] ||
  die "slirp4netns never signaled ready" "$region"

# 3. The actual guest address (slirp --configure assigns the CIDR's
# .100; whatever it settled on is authoritative for the hostfwd
# guest_addr). The child writes it after slirp's --configure brings
# the tap up (30s budget: the child's own slirp-wait is up to 15s).
for _ in $(seq 1 150); do
  [ -s "$region_dir/tap-ip.txt" ] && break
  kill -0 "$child" 2>/dev/null ||
    die "chromium died before the tap address settled" "$region"
  sleep 0.2
done
tap_actual=$(head -n 1 "$region_dir/tap-ip.txt" 2>/dev/null | cut -d/ -f1)
[ -n "$tap_actual" ] || tap_actual="$TAP_IP"

# 4. Forward host CDP port -> the guest's relay port (raw-JSON API, see
# cdp-relay.mjs); the relay pipes through to Chromium's 9222 loopback.
node "$SLIRP_API" "$region_dir/api.sock" add_hostfwd \
  "{\"proto\":\"tcp\",\"host_addr\":\"0.0.0.0\",\"host_port\":$cdp_host_port,\"guest_addr\":\"$tap_actual\",\"guest_port\":$CDP_RELAY_PORT}" \
  >"$region_dir/hostfwd.log" 2>&1 ||
  die "add_hostfwd failed" "$region"

# 5. Wait for Chromium's CDP endpoint through the forward. The forward is
# on the host's loopback at the region's cdp_host_port.
endpoint="http://127.0.0.1:${cdp_host_port}"
for _ in $(seq 1 240); do
  if curl -s -m 3 "$endpoint/json/version" 2>/dev/null | grep -q '"Browser"'; then
    break
  fi
  kill -0 "$child" 2>/dev/null ||
    die "chromium died before CDP came up" "$region"
  sleep 0.5
done
curl -s -m 3 "$endpoint/json/version" 2>/dev/null | grep -q '"Browser"' ||
  die "CDP endpoint never answered" "$region"

printf '{\n' >"$state_json"
printf '  "region": "%s",\n' "$region" >>"$state_json"
printf '  "pid": %s,\n' "$child" >>"$state_json"
printf '  "slirpPid": %s,\n' "$slirp" >>"$state_json"
printf '  "relayPid": %s,\n' "$(cat "$region_dir/relay.pid" 2>/dev/null)" >>"$state_json"
printf '  "endpoint": "%s",\n' "$endpoint" >>"$state_json"
printf '  "halfDelayMs": %s,\n' "$half_ms" >>"$state_json"
printf '  "tapIp": "%s",\n' "$tap_actual" >>"$state_json"
printf '  "gateway": "%s",\n' "$GATEWAY" >>"$state_json"
printf '  "cdpHostPort": %s,\n' "$cdp_host_port" >>"$state_json"
printf '  "startedAt": "%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$state_json"
printf '}\n' >>"$state_json"

echo "netns-up: $region up: chromium pid $child, slirp pid $slirp, $endpoint (netem ${half_ms}ms egress)"
cat "$state_json"
