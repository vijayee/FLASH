# FLASH e2e + network testing

Playwright suite for the Meridian-WebRTC overlay: local two-peer discovery,
a 100% unprivileged netns latency rig, media/frame-flow, failure + recovery,
Dart web interop, and a native Flutter desktop peer. Run everything with
`npm test` from this directory (`src/e2e`).

## Suites

| Spec | What it proves |
| --- | --- |
| `test/*.spec.ts` | discovery, latency-ordered queries, media frame flow, failure/re-election, Dart interop, native desktop peer (see the per-file headers) |
| `src/netns-launch.ts` + `scripts/netns-up.sh` | per-region latency rig: one user+net namespace per region, `tc netem` egress delays, slirp4netns host-forwarded CDP — no root anywhere |
| `scripts/build-dart-web.sh` | stages the Dart example's Flutter web bundle to `build/dart-web` (serve.mjs `DART_ROOT` mode) |
| `scripts/build-dart-linux.sh` | stages the Dart example's Linux desktop bundle to `build/dart-linux` (the native peer binary) |

## Azure geo suite (Task 8+)

Real-geography variant of the netns rig: 3 regional peer VMs (eastus,
westeurope, southeastasia) + 1 lab VM (signaling, Flutter desktop peer,
later coturn), each a `Standard_B2s` Ubuntu 24.04 VM. Instead of netem
delays, the latency is *real* — the peers are on different continents.

### Prereqs

- `az` CLI installed and logged in: `az login` (then
  `az account set --subscription <id>` if needed). `provision.sh` fails
  fast with instructions otherwise.
- Build artifacts present: `scripts/build-dart-web.sh` and
  `scripts/build-dart-linux.sh` must have run (provision.sh checks).
- This machine is the **orchestrator**: its egress IP (auto-detected via
  `ifconfig.me`, override with `ORCHESTRATOR_IP`) is the only source
  allowed to SSH (22) and reach CDP (9222-9225) on any VM.

### Provision flow

```
src/e2e/azure/provision.sh            # add --dry-run to print the plan only
```

Per VM it: creates the resource group + B2s VM (`Canonical:ubuntu-24_04-lts:server:latest`,
64GB os disk), applies NSG rules (SSH/CDP orchestrator-only; 80 +
8080-8091 open; UDP 1024-65535 inbound for ICE — Chromium can't restrict
its ICE port range), passes a role-templated `cloud-init.yaml` via
`--custom-data` (Node 20, Playwright Chromium, xvfb; Flutter SDK + GTK
toolchain on the lab VM only), then scps the demo stack into
`/opt/flash/` (mirroring the repo layout so `serve.mjs`'s path bases
resolve unmodified) and starts the services:

- `:80` JS demo (`flash-demo.service`), `:8090` dart-web
  (`flash-demo-dart.service`) — every VM
- `:8080` signaling (`flash-signaling.service`) — lab VM only

`cloud-init.yaml` and `agent.sh` are parameterized (the `__FLASH_ROLE__`
placeholder; the agent's `js|dart` argument). Provision ends by curling
each VM's demo roots (expect HTTP 200) and the lab's signaling port
(expect 426).

### How the orchestrator reaches agents

Chromium instances are **not** provisioned artifacts: at run time (Task 9)
the orchestrator ssh'es each VM and runs `agent.sh <js|dart>`, which
launches one headless Chromium (fake media, no-sandbox, `--unsafely-treat-
insecure-origin-as-secure=http://<vm-ip>` so the plain-http demo origin is
a secure context) plus a CDP relay (`scripts/cdp-relay.mjs`,
`0.0.0.0:9223 -> 127.0.0.1:9222`, because new headless ignores
`--remote-debugging-address`), then Playwright attaches with
`chromium.connectOverCDP('http://<vm-ip>:9223')`. Closing the SSH session
tears Chromium and the relay down with it.

### Teardown + cost

```
src/e2e/azure/teardown.sh             # az group delete --yes --no-wait
```

B2s x4 ≈ **$120/month** running 24/7 (plus disk/IP churn) — tear the
group down whenever the rig is idle; re-provisioning is one command
(`provision.sh` is idempotent: existing VMs are skipped, NSG re-applied).

## Run artifacts (Task 12)

Specs that call `startArtifacts(testInfo)` (opt-in; see `src/artifacts.ts`)
get a per-run dir under `runs/` (gitignored):

```
runs/<UTC ts>-w<worker>-<spec basename>/
├── peers/<peerId>.jsonl      500ms state snapshots (`kind:"state"`) + wirelog
│                             events beyond a watermark (`kind:"wire"`), each
│                             line with a peer-local `seq` + ms `ts`.
│                             Heartbeat-class keep-alives (raft
│                             append_entries/votes — several hundred frames/s
│                             once the supernode cluster forms) are NOT
│                             recorded line-by-line; they are aggregated per
│                             tick into the state line's `wireSkipped` counter.
├── console/<peerId>.log      page console + pageerror streams
├── ice-stats/<peerId>.jsonl  1s getStats() digests for media-carrying
│                             PeerConnections (PC tracker init script; pages
│                             without visible media PCs record nothing —
│                             graceful skip)
├── signaling.jsonl           the signaling server's own JSONL log (the spec
│                             spawns it with `startSignaling(port, { logFile })`;
│                             one line per relayed/dropped message +
│                             register/disconnect/peers_list, ISO timestamps)
├── topology.json             the region/delay config actually in force
└── result.json               per-test name/status/duration/error
```

The standalone signaling server can also be run with
`LOG=flash-signaling LOG_FILE=path node server.js` to produce the same
JSONL log outside the suite.

## Failure playbook

Which artifact answers which failure class:

| Failure class | Where to look |
| --- | --- |
| Discovery never converged | `peers/<peerId>.jsonl`: does `state.knownPeers` grow and flip `status` to `connected`? Cross-check `signaling.jsonl`: did each peer `register`, and do `peers_list` lines show the others (`count > 0`)? If registers exist but knownPeers stalls, gossip (not signaling) is broken; if `peers_list` counts stay 0, registration never reached the server (check `console/<peerId>.log` for WS errors). |
| ICE fails (media never establishes) | `ice-stats/<peerId>.jsonl`: watch `pc.connectionState`/`iceConnectionState` and the selected pair's `localCandidateType`/`remoteCandidateType`. `srflx↔srflx` stuck at `checking` → the STUN mapping or the netns UDP path is broken; candidates present but pairs never `succeeded` → reachability/firewall; no `ice-stats` lines at all → the tracker was installed after page load (see the graceful-skip note above). |
| Query timed out (`findClosestNode` rejects) | `peers/<peerId>.jsonl` `kind:"wire"` lines: correlate by `payload.queryId` — is the `probe_request` sent (`dir:"send"`, right `peerId`), does a matching `probe_result`/`probe_answer` come back, and how long between `wireTs` values? Request sent but no relay → check `signaling.jsonl` for a `relay_dropped` line naming the target. |
| Ring placement wrong | `peers/<peerId>.jsonl`: the `ringIndex` timeline per known peer, against `topology.json`'s `rttMatrixMs`. First check `rtt` in the same snapshots — if the measured RTT disagrees with the scripted matrix, the rig (netem) is wrong, not the ring logic; if RTT is right but `ringIndex` doesn't match the bounds, it's a library bug. |
| Supernode election picked the wrong peer | `peers/*.jsonl` final snapshots (`clusterLeader`, `isSupernode`) + each candidate's RTT to the others; `signaling.jsonl` shows the relayed probe offers that measured them. |
| Crash/browser-level failure | `console/<peerId>.log` (pageerror entries) first, then `result.json` for which test failed with which message. |