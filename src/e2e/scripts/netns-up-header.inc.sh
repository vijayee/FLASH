#!/usr/bin/env bash
# netns-up.sh — deterministic per-region latency rig, 100% unprivileged
# (Task 3). One Linux network namespace per region, each running headless
# Chromium with its own `tc netem` egress delay, reachable from the host
# through slirp4netns's forwarded CDP port.
#
# Rootlesskit could not be used on this machine (its multi-subuid mapping
# needs newuidmap(1) from uid-runtime, which is not installed — verified:
# `newuidmap: executable file not found in $PATH`). The same architecture is
# realized directly with the two primitives that DO work unprivileged here
# (both verified empirically on this machine):
#
#   1. `unshare -Urn --map-root-user` — a user+network namespace whose
#      mapped root holds CAP_NET_ADMIN: it creates tap0, addresses it and
#      applies `tc qdisc add dev tap0 root netem delay <halfDelayMs>ms`.
#   2. `slirp4netns -c -r 3 --api-socket=... <childPid> tap0` — attaches
#      user-mode networking to that netns from the host as the same
#      unprivileged user, and its raw-JSON API socket (scripts/slirp-api.mjs)
#      adds the host->guest forwarding that puts Chromium's CDP endpoint
#      (9222 inside the netns) on a host port.
#
# Netem goes on tap0 INSIDE each namespace — that delays the region's
# egress, so all of the region's traffic (demo page, signaling, WebRTC) is
# delayed by its half delay in each direction it originates.
#
# ONE slirp subnet for every region: slirp4netns 1.0.1 ignores --cidr (its
# -c always assigns 10.0.2.100/24 with the 10.0.2.2 gateway, verified
# empirically: us/asia guests raised with --cidr=10.0.3.0/24 etc. ended up
# with 10.0.2.100/24 AND their computed address, and their outbound to the
# computed gateway failed with net::ERR_ADDRESS_UNREACHABLE). Every region
# therefore shares tap0 = 10.0.2.100/24, gateway 10.0.2.2 -> the host's
# loopback, which is verified working end to end (HTTP 200 from inside a
# namespace).
#
# Chromium's DevTools server binds 127.0.0.1 INSIDE the netns (new headless
# ignores --remote-debugging-address), so the slirp host-forward targets the
# in-netns CDP relay (scripts/cdp-relay.mjs: 0.0.0.0:9223 ->
# 127.0.0.1:9222) instead of Chromium's port directly.
#
# Usage:
#   netns-up.sh <region> <halfDelayMs> <cdpHostPort> [chromePath]
#       — one region (this is what src/netns-launch.ts drives)
#   netns-up.sh --all [chromePath]
#       — the whole scripted rig (mirrors REGIONS in src/netns-launch.ts)
#
# State (read by netns-down.sh and src/netns-launch.ts):
#   /tmp/mrd-netns/<region>.json      {pid, slirpPid, relayPid, endpoint,
#                                     halfDelayMs, tapIp, gateway,
#                                     cdpHostPort, startedAt}
#   /tmp/mrd-netns/<region>/          api.sock, ready, profile/, logs
#
# Idempotent: a live rig is reused as-is; a stale one is swept (marker
# scan over /proc/*/cmdline, so a recycled pid can never take out an
# unrelated process) and re-raised. Requires no sudo anywhere.
#
# NOTE ON DELAYS: the scripted half delays are eu=60ms, us=20ms,
# asia=100ms -> RTT eu<->us ~80ms, eu<->asia ~160ms, us<->asia ~120ms
# (+0-2ms slirp overhead). This corrects the plan's netem 20/20/60ms
# sketch: netem on a device delays only that namespace's transmitted
# packets (egress), so RTT(a<->b) = halfDelay_a + halfDelay_b, and
# 20/20/60 would have measured 40/80/80ms — landing the us and asia peers
# a ring lower than the latency assertions expect.
set -u

CDP_GUEST_PORT=9222
# In-netns CDP relay listen port (-> Chromium's 127.0.0.1:9222).
CDP_RELAY_PORT=9223
STATE_ROOT=/tmp/mrd-netns
SLIRP_API="${0%/*}/slirp-api.mjs"

# The slirp gateway inside the netns (maps to the host's loopback).
GATEWAY=10.0.2.2
TAP_IP=10.0.2.100

die() {
  echo "netns-up: $*" >&2
  # Forensics: the rig's own logs (chromium/slirp/hostfwd/relay/tc), read
  # before any teardown sweep can remove them. Bounded tails so a huge
  # log cannot flood the run output.
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

# Resolve the ICE STUN server host-side (the netns has no usable DNS: its
# resolv.conf is the host's systemd-resolved stub 127.0.0.53, which inside
# the netns is the namespace's own loopback). Chromium maps the library's
# stun:stun.l.google.com:19302 to this IP via --host-resolver-rules, so
# srflx candidates can still form.
resolve_stun_ip() {
  local ip=${NETNS_STUN_IP:-}
  if [ -z "$ip" ]; then
    ip=$(getent hosts stun.l.google.com 2>/dev/null | awk 'NR==1 {print $1; exit}')
  fi
  # Fallback: a long-lived public STUN anycast address.
  printf '%s' "${ip:-74.125.250.129}"
}

endpoint_url() { # <cdpHostPort> -> http://127.0.0.1:<port>
  printf 'http://127.0.0.1:%s' "$1"
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
    [ "$pass" = TERM ] && sleep 0.5
  done
}