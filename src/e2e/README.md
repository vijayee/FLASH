# FLASH e2e + network testing

Playwright suite for the Meridian-WebRTC overlay: local two-peer discovery,
a 100% unprivileged netns latency rig, media/frame-flow, failure + recovery,
Dart web interop, and a native Flutter desktop peer. Run everything with
`npm test` from this directory (`src/e2e`).

## Suites

| Spec | What it proves |
| --- | --- |
| `test/*.spec.ts` | discovery, latency-ordered queries, media frame flow, failure/re-election, Dart interop, native desktop peer (see the per-file headers) |
| `src/netns-launch.ts` + `scripts/netns-up.sh` | per-region latency rig: one user+net namespace per region, `tc netem` egress delays, slirp4netns host-forwarded CDP — no root anywhere (plus the TURN spec's `MRD_UDP_ALLOW` iptables mode) |
| `scripts/build-dart-web.sh` | stages the Dart example's Flutter web bundle to `build/dart-web` (serve.mjs `DART_ROOT` mode) |
| `scripts/build-dart-linux.sh` | stages the Dart example's Linux desktop bundle to `build/dart-linux` (the native peer binary) |

Two more specs are **@geo opt-in** (they need the Azure estate, `E2E_GEO=1`;
locally they skip via the same title-level guard geo.spec.ts carries):

- `test/turn.spec.ts` — the forced-relay TURN test (see below).
- `test/desktop-geo.spec.ts` — the native Flutter desktop peer, built and run
  ON the lab VM (see below).

### Running the local suite

```
cd src/e2e && npx tsc --noEmit   # type check (also `npm run lint`)
npx playwright test              # everything @local (6 specs, 21 tests, ~4 min)
npx playwright test test/local-media.spec.ts   # one spec
```

**Port overrides.** The suite binds its own signaling server and a mini-STUN
responder (`scripts/mini-stun.mjs`, the loopback STUN the netns rig peers
point at via the demo's `?stun=` affordance). Defaults follow the plan —
**signaling 8080, STUN 3478** — and every spec overrides them from the
environment:

```
E2E_SIGNALING_PORT=18080 E2E_STUN_PORT=3479 npx playwright test
```

On **this machine** the overrides are mandatory, not optional: a Docker
daemon's published services squat 8080 and 3478, so the defaults collide at
bind time (or worse, answer — the mini-STUN readiness probe can get a
foreign UDP reply). Use 18080/3479 unless you know the defaults are free.

### What each spec proves

- `test/local-two-peer.spec.ts` — two peers discover each other through the
  real signaling server and gossip within 2 gossip periods, and
  `findClosestNode` resolves the other peer from each side (the deadlock
  guard: the query path works before any supernode exists).
- `test/local-query-routing.spec.ts` — the netns rig's scripted matrix
  (eu↔us ~80ms ⇒ ring 7, eu↔asia ~160ms ⇒ ring 8, us↔asia ~120ms ⇒ ring 7)
  orders real query routing correctly and places every peer in the ring the
  RTT bounds dictate; assertions are inequality/threshold-based, never
  exact-value (slirp adds small overhead on top of netem, never below).
- `test/local-media.spec.ts` — real frame flow (a looping 720p file via
  `video.captureStream()`, asserted by `requestVideoFrameCallback` counts,
  not track events), plus the middle-RTT peer winning the supernode election
  deterministically and acting as the SFU relay path for a third peer.
- `test/local-failure.spec.ts` — the real election picks `us` (lowest avg
  RTT), its tab death triggers re-election to a SURVIVOR, a fresh peer's tab
  death strips it from rings/knownPeers, and pure silence (no close frames)
  is pruned by the stale-peer path.
- `test/dart-interop.spec.ts` — the compiled Flutter web bundle and a JS
  peer discover each other, answer closest-node queries across languages,
  and stream media both directions (JS→Dart, then Dart→JS after a clean
  close).
- `test/desktop-peer.spec.ts` — the native `flutter build linux` binary
  (flutter_webrtc's libwebrtc + `dart:io` signaling paths) under `xvfb-run`
  joins real Chromium peers, gets discovered, answers queries, and receives
  a JS→native media stream.
- `test/turn.spec.ts` (@geo) — the SAME netns rig with outbound UDP starved
  (iptables inside each namespace, only the TURN server's IP allowed) pointed
  at the lab VM's real coturn: media still connects and getStats() shows the
  selected pair running through the relay — ICE fallback under a
  symmetric-UDP firewall, end-to-end across the public internet.
- `test/desktop-geo.spec.ts` (@geo) — Task 7's desktop peer built ON the lab
  VM (`flutter build linux --release` there) and joined to the geo topology
  under xvfb: a swedencentral JS agent discovers it over real inter-region
  latency, routes a closest-node query to it, and streams media into its
  native libwebrtc receive path.

## Known flake (honest)

- **Loopback-scale query self-answers.** Spec §3.6's `findClosestNode`
  candidate window `[myRtt/2, myRtt*2]` compares a FRESH query measurement
  against the STORED gossip-averaged RTT. At loopback RTTs (sub-ms to a few
  ms) the two drift across the window boundary between gossip refreshes, so
  a query can legitimately answer "self" — observed as three self-answers
  in a row on an early run. The affected specs (`dart-interop`,
  `desktop-peer`) therefore assert query-result *membership*
  (`{self, target}`) with bounded retries and wait for the stored RTT to
  move between attempts; the exact identity assertion is only falsifiable
  at production-scale latencies (the netns specs, where netem keeps the
  windows far apart).
- **Full-suite rig boots under memory pressure.** Six specs in one process
  each boot 1-3 Chromium instances (some inside netns with slirp4netns) and
  the artifacts collectors; on a loaded machine a boot deadline (CDP
  `/json/version` handshake) can be exceeded with no bug behind it. If a
  spec fails on a rig-boot deadline with artifacts showing nothing wrong,
  rerun the spec alone before investigating.
- **The 8080 Docker squatter.** See the port-override note above — running
  without `E2E_SIGNALING_PORT`/`E2E_STUN_PORT` on this machine is a flake
  source in itself.

## Not covered by automation (manual)

- **Glare over real-ICE races.** Beyond the signaling-server glare unit
  tests and fake-transport fakes, no automated test races two concurrent
  offers through a real ICE stack.
- **SFU media re-transmission.** The library forwards media *signals*
  (spec §6.2 as specified: `forwarded_stream` signaling, SFU relay path in
  `local-media.spec.ts`); NACK/RTX/keyframe re-request behavior of a real
  SFU is not exercised — peers relay via WebRTC, not an RTP-level SFU.

## Verified (2026-09-22/23, this machine)

| Suite | Command | Result |
| --- | --- | --- |
| e2e type check | `npx tsc --noEmit` | clean |
| e2e suite (run 1) | `E2E_SIGNALING_PORT=18080 E2E_STUN_PORT=3479 npx playwright test` | 21 passed (3.9m) |
| e2e suite (run 2, flake check) | same | 21 passed (4.1m) |
| e2e suite (after Task 10) | same | 21 passed — `turn`/`desktop-geo` skip locally via their `E2E_GEO` guards |
| JS library | `npm test` / `npm run lint` | 60 passed / eslint clean |
| JS syntax | `node --check` on all 17 `.js` under `src/js/src` + `src/signaling-server` | clean |
| Dart | `flutter analyze` / `flutter test` / `dart format --output=none --set-exit-if-changed .` | 0 issues / 57 passed / clean |
| Signaling server | `npm test` | 15 passed |
| geo suite (Task 9) | `E2E_GEO=1 E2E_SIGNALING_PORT=18080 npx playwright test test/geo.spec.ts` | 5 passed (58.5s) — measured RTTs: centralus↔swedencentral ~130ms, centralus↔koreacentral ~160ms, swedencentral↔koreacentral ~250-290ms; media ICE connected `srflx`↔`srflx` (156ms pair RTT) |
| TURN forced-relay (Task 10) | `E2E_GEO=1 ... npx playwright test test/turn.spec.ts` | 2 passed — in-netns iptables block verified (STUN probe timed out, coturn answered), media connected with the selected pair through the relay (`srflx`↔`relay` / `relay`↔`srflx`, 159-181ms pair RTT) |
| desktop geo peer (Task 10) | `E2E_GEO=1 ... npx playwright test test/desktop-geo.spec.ts` | 3 passed — desktop binary built on the lab VM, discovered with a real 129-145ms RTT, closest-node query routed, JS→native media received |
| TURN reachability (manual) | `node scripts/udp-probe.mjs 172.173.102.12 3478` + raw Allocate probe | STUN binding answered; long-term-cred Allocate returned `relayed=172.173.102.12:58958` |

## Azure geo suite (Task 8+)

Real-geography variant of the netns rig: 3 regional peer VMs
(flash-e2e-centralus, flash-e2e-swedencentral, flash-e2e-koreacentral) + 1
lab VM (flash-e2e-lab: signaling, coturn, the Flutter toolchain + desktop
peer), each
a `Standard_B2s` Ubuntu 24.04 VM. Instead of netem delays, the latency is
*real* — the peers are on different continents (approx RTT matrix:
centralus↔swedencentral ~130ms, centralus↔koreacentral ~160ms,
swedencentral↔koreacentral ~250-290ms measured).

### Run

```
E2E_GEO=1 E2E_SIGNALING_PORT=18080 npx playwright test test/geo.spec.ts
# plus the Task 10 additions (also @geo opt-in):
E2E_GEO=1 E2E_SIGNALING_PORT=18080 npx playwright test test/turn.spec.ts
E2E_GEO=1 E2E_SIGNALING_PORT=18080 npx playwright test test/desktop-geo.spec.ts
```

Opt-in only (`@geo` titles; the config's `testIgnore` excludes
`geo.spec.ts` from local runs, and the spec itself skips without
`E2E_GEO=1`). Requires the provisioned estate; nothing is re-provisioned —
agents are raised over SSH for the run and torn down after it (see
`src/remote.ts`). Note the demo pages load at the agent's ready-line origin
(the VM's PRIVATE address, which is what Chromium was launched with as
`--unsafely-treat-insecure-origin-as-secure`): Chrome 140 blocks navigation
to insecure IP origins that are not in that flag's list, and IMDS reports
an empty publicIpAddress on the estate — the treated-secure origin must
match the address bar exactly.

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

### TURN forced-relay (Task 10)

The lab VM runs **coturn** for `test/turn.spec.ts` (config: `azure/turnserver.conf`,
applied to `/etc/turnserver.conf`; long-term credentials `flash` /
`flash-e2e-cred`, realm `flash.test`, relay ports 49152-65535,
`external-ip=<lab public IP>/<private IP>`). The NSG opens 3478 tcp+udp
(`allow-turn` / `allow-turn-udp`); the relay range rides the existing
`allow-ice-udp` rule (inbound UDP 1024-65535).

Future provisions get this automatically (cloud-init lab role installs the
package, `provision.sh upload_lab` applies the templated conf + restarts —
the VM's public IP is not known at custom-data render time, so the conf
lands post-boot). The FIRST estate was configured live over SSH:

```
ssh azureuser@172.173.102.12 'sudo apt-get install -y coturn'
scp azure/turnserver.conf /tmp/flash-turnserver.conf   # sed the public IP in
ssh azureuser@172.173.102.12 'sudo tee /etc/turnserver.conf < /tmp/... && sudo systemctl restart coturn'
az network nsg rule create -g flash-e2e-rg --nsg-name flash-e2e-labNSG \
  --name allow-turn --priority 1050 --protocol Tcp --destination-port-ranges 3478 ...
az network nsg rule create -g flash-e2e-rg --nsg-name flash-e2e-labNSG \
  --name allow-turn-udp --priority 1051 --protocol Udp --destination-port-ranges 3478 ...
```

Verified by hand (2026-09-23): a raw STUN binding probe answers, and a raw
TURN Allocate with the long-term credentials returns
`relayed=172.173.102.12:<relay port>` — the public address the relay
advertises. In the suite, `turn.spec.ts` proves the same path from a
UDP-starved net (see the spec's header for the observed shape: libwebrtc
also derives an srflx candidate from the TURN Allocate's mapped address,
so the selected pair is typically `srflx<->relay` from one vantage and
`relay<->srflx` from the other — the same pair, still through coturn).

### Native desktop peer on the lab VM (Task 10)

`test/desktop-geo.spec.ts` uploads `src/dart` (package + example) to
`/opt/flash/src-dart-geo` over ssh, runs `flutter pub get` +
`flutter build linux --release` THERE (the cloud-init lab role installs the
Flutter SDK + GTK toolchain; a content hash of the source set short-circuits
the build while it matches), then runs the binary under `xvfb-run` with
`MRD_SIGNALING` + `MRD_STATUS_FILE` streamed back through `ssh tail -F`.
Two headless-host constraints it works around (both documented in the spec
and handled by cloud-init on the lab role): libwebrtc aborts in ADM init
without a userspace audio daemon, so a headless PulseAudio null sink is
installed + started; and the status file is remote, so the local
desktop-peer spec's readFile seam is replaced by the tail stream.

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