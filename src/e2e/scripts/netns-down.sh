#!/usr/bin/env bash
# netns-down.sh — tears down the Task 3 latency rig, 100% unprivileged.
#
# For each region with a state file (/tmp/mrd-netns/<region>.json):
# sweeps the region's orphaned rig processes by marker scan (Chromium via
# --user-data-dir=<dir>/profile, slirp4netns via --api-socket=<dir>/api.sock,
# the CDP relay via its region-dir argument — only after confirming the
# pid is still ours through /proc/<pid>/cmdline, so a recycled pid can
# never take out an unrelated process), preserves the region dir's logs
# in a sibling .done dir (bounded to a few), and drops the state file.
#
# Regions without a state file get an orphan sweep only (a rig that died
# before writing its state leaves its processes behind with the cmdline
# marker). Idempotent: missing state is a no-op beyond the sweep.
#
# Requires no sudo anywhere.
set -uo pipefail

STATE_ROOT=/tmp/mrd-netns

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
      if tr '\0' ' ' <"$cmdline" 2>/dev/null | grep -q "mrd-netns/$region"; then
        if [ "$pass" = TERM ]; then kill "$pid" 2>/dev/null; fi
        if [ "$pass" = KILL ]; then kill -9 "$pid" 2>/dev/null; fi
      fi
    done
    # NOT `A && sleep` — under `set -e` a failed A (the KILL pass) would
    # exit the whole down script mid-teardown.
    if [ "$pass" = TERM ]; then sleep 0.5; fi
  done
}

preserve_logs() { # <dir> — move the dir to a sibling .done dump
  [ -d "$1" ] || return 0
  mv "$1" "$1.done-$(date +%s)" 2>/dev/null || true
}

down_region() { # <region>
  local region=$1 json="$STATE_ROOT/$region.json" dir="$STATE_ROOT/$region"
  local pid pass
  # Orphan sweep FIRST — a rig that died before writing its state leaves
  # no pid file, but Chromium (the exec'd unshare child), its slirp4netns
  # attachment and the CDP relay all carry /tmp/mrd-netns/<region> on
  # their command line.
  sweep_orphans "$region"
  if [ ! -f "$json" ]; then
    # Preserve the rig's forensics (its logs die with the dir in the rm
    # below) in a sibling .failed dir.
    if [ -d "$dir" ]; then
      rm -rf "${dir}.failed"
      mv "$dir" "${dir}.failed" 2>/dev/null || true
    fi
    echo "netns-down: $region: no state; swept orphans"
    return 0
  fi
  for pass in TERM KILL; do
    for field in slirpPid relayPid pid; do
      pid=$(json_num "$json" "$field")
      [ -n "$pid" ] || continue
      [ "$pid" != "$$" ] || continue
      marker="mrd-netns/$region"
      if [ -r "/proc/$pid/cmdline" ] &&
        tr '\0' ' ' <"/proc/$pid/cmdline" | grep -q "$marker"; then
        if [ "$pass" = TERM ]; then kill "$pid" 2>/dev/null; fi
        if [ "$pass" = KILL ]; then kill -9 "$pid" 2>/dev/null; fi
      fi
    done
    if [ "$pass" = TERM ]; then sleep 1; fi
  done
  # Preserve the teardown logs for forensics, bounded to the newest 3 dumps.
  mv "$dir" "$dir.done-$(date +%s)" 2>/dev/null || true
  ls -d "$dir".done-* 2>/dev/null | sort | head -n -3 | xargs -r rm -rf
  rm -rf "$json"
  echo "netns-down: $region down"
}

if [ "${1:-}" = "--all" ]; then
  for region in eu us asia; do
    down_region "$region"
  done
  # Non-standard region dirs (leftover rigs without state): sweep their
  # orphaned processes and preserve their logs too.
  for dir in "$STATE_ROOT"/*; do
    [ -d "$dir" ] || continue
    region=$(basename "$dir")
    case "$region" in eu|us|asia) continue;; esac
    sweep_orphans "$region"
    preserve_logs "$dir"
    echo "netns-down: $region down"
  done
  exit 0
fi

if [ $# -eq 0 ]; then
  echo "usage: netns-down.sh [--all|<region>...]" >&2
  exit 2
fi
for region in "$@"; do
  down_region "$region"
done
