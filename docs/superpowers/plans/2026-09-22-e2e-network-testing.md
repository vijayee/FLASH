# E2E + Network Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the Meridian-WebRTC overlay works end-to-end in real browsers and on native Flutter desktop — locally (deterministic netns/netem latency rig) and across 3 Azure regions (real geographic latency) — including interop between the JS and Dart libraries, supernode failover, and TURN-forced relay.

**Architecture:** A `src/e2e` TypeScript/Playwright package drives real Chromium peers. Locally, peers are Chromium instances launched into Linux **network namespaces** (`ip netns` + `tc netem`) so latency is scripted and deterministic without any cloud spend. On Azure, the same orchestrator connects over CDP to Chromium agents running on VMs in 3 regions; a lab VM hosts the netem container rig, the TURN server, and the Flutter **native desktop** peer under xvfb. The Dart library participates three ways: Flutter-web tab (browser mapping), native Linux desktop peer (native `flutter_webrtc` transport + `dart:io` paths), and unit-level fakes (already done).

**Tech Stack:** Playwright (local + `chromium.connectOverCDP` for remote), Node 20 (`az` CLI for provisioning, `ssh`/`scp`), Linux network namespaces + `tc netem`, Docker + `tc` (lab VM), `coturn`, flutter Linux desktop + `xvfb`.

**Repo layout additions:**

```
src/e2e/                          # new package (Node 20, TypeScript, Playwright)
├── package.json                  # playwright dep, test script
├── playwright.config.ts          # CDP remote + local projects
├── src/
│   ├── orchestrator.ts           # topology definition, staggered joins, assertions
│   ├── cdp.ts                    # connectOverCDP helpers (remote browsers)
│   ├── netns.ts                  # local netns/netem rig (create/delete, wire peers)
│   ├── assertions.ts             # state polling via window.__meridian (topology, rings, leader)
│   └── topology.ts               # topology model: peers, regions, delays
├── tests/
│   ├── local-two-peer.spec.ts
│   ├── local-query-routing.spec.ts
│   ├── local-media.spec.ts
│   ├── local-failure.spec.ts
│   ├── dart-interop.spec.ts
│   ├── geo.spec.ts               # runs against Azure (tagged [geo])
│   ├── desktop-peer.spec.ts
│   └── turn.spec.ts              # tagged, needs coturn
├── scripts/
│   ├── serve.mjs                 # static server for JS demo + Dart web build (reuse pattern from src/js)
│   ├── netns-up.sh               # create namespaces + netem + run chromium per netns
│   ├── netns-down.sh
│   └── lab-netem.sh              # docker peers + tc on lab VM
└── azure/
    ├── provision.sh              # az CLI: VMs, NSG, cloud-init
    ├── cloud-init.yaml           # node20, chromium+playwright deps, xvfb, flutter, coturn
    ├── signaling.service         # systemd unit on lab VM
    ├── turnserver.conf           # coturn config
    └── teardown.sh               # delete resource group
```

**Demo/example seams required (small, example-only changes):**

- `src/js/examples/browser/main.js`: expose `window.__meridian = { peerId, connect, state, wireLog }` — read-only projections of existing node fields, **plus a `?wirelog=1` ring buffer** (bounded 2000 entries) recording every DataChannel send/receive `{dir, type, ts, peerId, payload}` and every signaling message; surfaced as `state.wireLog`.
- `src/dart/example/lib/main.dart`: same via `package:web`/`dart:js_interop` — `window.__meridianState()` returning a JSON string of the same fields (including the Dart-side wire log); plus `--platforms linux` scaffolding for the desktop peer.
- **Orchestrator-side collection:** per run, a `runs/<timestamp>/` artifact dir with one JSONL state-history file per peer (snapshots every 500ms + every event callback), page console logs, `getStats()` ICE candidate-pair snapshots every second during media, the signaling server's journald log (lab VM, `journalctl -u flash-signaling`), and the applied netns/netem config — all captured on teardown for post-run debugging.

---

### Task 1: e2e package scaffold + demo state hooks

**Files:**
- Create: `src/e2e/package.json`, `src/e2e/tsconfig.json`, `src/e2e/playwright.config.ts`
- Create: `src/e2e/src/orchestrator.ts`, `src/e2e/src/cdp.ts`, `src/e2e/src/assertions.ts`, `src/e2e/src/netns.ts` (types)
- Modify: `src/js/examples/browser/main.js` (add `window.__meridian` projection)
- Modify: `src/dart/example/lib/main.dart` (add `window.__meridianState()` via JS interop)

- [ ] **Step 1: Scaffold the e2e package**

```bash
mkdir -p src/e2e/{src,scripts,azure}
cd src/e2e && npm init -y
npm i -D playwright typescript @types/node
npx playwright install chromium
```

`package.json` essentials: `"type": "module"`, `"test": "playwright test"`, `"lint": "tsc --noEmit"`.

`src/e2e/playwright.config.ts`:

```typescript
import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  timeout: 120_000,
  use: {
    ignoreHTTPSErrors: true,
    launchOptions: {
      args: [
        '--use-fake-ui-for-media-stream',
        '--use-fake-device-for-media-stream',
        '--no-sandbox',
      ],
    },
  },
  // geo.spec.ts runs only when ORCH_URL / PEER_CDP_ENDPOINTS are set
  grep: !process.env.E2E_GEO ? '@local' : '@local|@geo',
});
```

- [ ] **Step 2: Add the JS demo state hook**

Append to `src/js/examples/browser/main.js` (after `node` exists):

```javascript
window.__meridian = {
  peerId: () => node.peerId,
  // Driving affordance for e2e: fills the URL input, then the Connect
  // button's code path (ids per examples/browser/index.html).
  connect: (url) => {
    document.getElementById('signal-url').value = url;
    document.getElementById('connect').click();
  },
  state: () => ({
    peerId: node.peerId,
    knownPeers: [...node.knownPeers.entries()].map(([id, p]) => ({
      id, rtt: p.rtt, status: p.status, ringIndex: p.ringIndex,
    })),
    rings: node.rings.map((r) => ({
      index: r.index,
      primary: r.primaryMembers.map((m) => m.peerId),
    })),
    isSupernode: node.isSupernode,
    clusterLeader: node.clusterLeader,
    activeStreams: [...node.activeStreams.keys()],
  }),
};
```

- [ ] **Step 3: Add the Dart example state hook**

In `src/dart/example/lib/main.dart`, add `package:web`/`dart:js_interop` export of the same shape (a `stateJson()` string — simplest for Playwright to read via `page.evaluate('window.__meridianState()')`). Wire it in `_MeridianExampleState.initState` after `node.initialize`, updating after each event callback. Requires `web: ^1.0.0` dev/dep in `example/pubspec.yaml` and `uses-material-design` unchanged.

- [ ] **Step 4: Verify the hooks**

```bash
cd src/js && npm test && npx tsc --noEmit
```

Expected: 60 JS tests still pass; e2e package compiles (`npx tsc --noEmit` from `src/e2e`).

- [ ] **Step 5: Commit**

```bash
git add src/e2e src/js/examples/browser/main.js src/dart/example
git commit -m "e2e: Playwright scaffold + read-only state hooks in both demos"
```

---

### Task 2: Local two-peer discovery e2e (JS ↔ JS)

**Files:**
- Create: `src/e2e/src/orchestrator.ts` (full), `src/e2e/test/local-two-peer.spec.ts`
- Create: `src/e2e/scripts/serve.mjs` (static server for the JS demo — reuse the pattern from `src/js/examples/browser/serve.mjs`, serving from `src/js/` so `/src/index.js` resolves)

- [ ] **Step 1: Orchestrator core**

```typescript
// src/e2e/src/orchestrator.ts
import { spawn, type ChildProcess } from 'node:child_process';
import type { Page } from '@playwright/test';

export function startSignaling(port = 8080): ChildProcess {
  return spawn('node', ['src/signaling-server/server.js'], {
    cwd: new URL('../../', import.meta.url).pathname,
    env: { ...process.env, PORT: String(port) },
    stdio: 'ignore',
  });
}

/** Polls a boolean/object predicate in the page until truthy or deadline. */
export async function waitFor<T>(
  page: Page,
  predicateJs: string,
  timeoutMs = 15_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = (await page.evaluate(`(${predicateJs})()`)) as T | null;
    if (value != null) return value;
    if (Date.now() > deadline) throw new Error(`waitFor timed out: ${predicateJs}`);
    await page.waitForTimeout(250);
  }
}

export const openPeer = (page: Page, demoUrl: string) => page.goto(demoUrl);
export const connect = (page: Page, signalingUrl: string) =>
  page.evaluate((url) => window.__meridian.connect(url), signalingUrl);

export interface PeerState {
  peerId: string;
  knownPeers: { id: string; rtt: number; status: string; ringIndex: number | null }[];
  rings: { index: number; primary: string[] }[];
  isSupernode: boolean;
  clusterLeader: string | null;
  activeStreams: string[];
}

export const state = (page: Page) =>
  page.evaluate(() => window.__meridian.state()) as Promise<PeerState>;
```

Note: `connect` implies the demo page exposes a connect affordance through `window.__meridian`; add it in `src/js/examples/browser/main.js` alongside the state hook (`connect(url)` calling the same initialize flow the Connect button uses).

- [ ] **Step 2: Write the failing test**

- [ ] **Step 2: Write the failing test**

```typescript
import { test, expect } from '@playwright/test';
// two pages → signaling → assert both discover each other + gossip populates rings
```

Assert: within `2 * gossipPeriod` both pages list each other in `state().knownPeers` with `status: 'connected'`, and `findClosestNode(otherPeerId)` resolves with `closestPeerId === otherPeerId` from each side.

- [ ] **Step 3: Run** `npx playwright test test/local-two-peer.spec.ts` → PASS (kill signaling in `afterAll`).

- [ ] **Step 4: Commit** — `git commit -m "e2e: local two-peer JS discovery"`

---

### Task 3: Latency rig — netns launcher + latency-ordered query e2e

**Files:**
- Create: `src/e2e/scripts/netns-up.sh`, `src/e2e/scripts/netns-down.sh`
- Create: `src/e2e/test/local-query-routing.spec.ts`

**netns-up.sh** (deterministic local geo simulation): creates `mrd-eu`, `mrd-us`, `mrd-asia` namespaces. Each gets a veth pair to the host bridge `mrd-br0` (10.200.<i>.1/24 host, peer 10.200.<i>.2), NAT via the host, and **`tc qdisc add dev <veth-host> root netem delay <region delay>`** applied per-namespace egress (30ms EU, 80ms US↔EU via matching both directions, etc.). Each namespace runs its own Chromium (`--remote-debugging-port=922x`, dedicated `--user-data-dir`) with the demo loaded against the signaling server on the host.

Scripted matrix: EU↔US 80ms, EU↔Asia 160ms, US↔Asia 180ms (netem per-namespace egress delay composes: 80 = 40+40).

- [ ] **Step 1:** Write `netns-up.sh` (veth pairs, `tc qdisc` per namespace, chromium launch, CDP port export: 9223/9224/9225).
- [ ] **Step 2:** Verify: `ip netns list` shows 3 namespaces; each CDP endpoint serves `/json/version`.
- [ ] **Step 3:** Test `local-query-routing.spec.ts` (`@local`): from the EU peer, `findClosestNode('us-peer')` resolves `closestPeerId === 'us-peer'`; from EU, `findClosestNode('asia-peer')` resolves asia; assert `state().rings` places the US peer in EU's ring ≥ 5 (80ms ⇒ ring 7) and the EU peer in US's ring ≥ 5. Also assert `closestRtt` is within ±30% of the scripted delay (this is the **ring-placement truth test**).
- [ ] **Step 4:** Leader election test (`@local`, needs ≥ 3 peers): `electSupernode`/`supernode_elected` — assert the elected peer minimizes avg RTT to the scripted target set.
- [ ] **Step 5:** Commit.

---

### Task 4: Media e2e (looping file fixture, local)

**Files:**
- Create: `src/e2e/test/local-media.spec.ts`
- Create: `src/e2e/fixtures/penguin.mp4` (copy of `/home/victor/Videos/penguin.mp4`, 3.7MB — committed; served statically by the e2e helper at `/fixtures/penguin.mp4`)
- Modify: `src/js/examples/browser/main.js` + `index.html` — support `?mediaSrc=<url>`: when present, create a hidden `<video autoplay muted loop playsinline src>` and use `video.captureStream()` as the uplink instead of `getUserMedia` (test affordance only; the Connect flow accepts it via the `__meridian.connect` seam — pass the resolved stream into the same initialize path)
- Modify: `src/dart/example/lib/main.dart` — same `?mediaSrc=` affordance on web via interop (`HTMLVideoElement.createShadowRoot` not needed — use `package:web` to build the video element and `captureStream()`)

- [ ] **Step 1:** Test: both peers open the demo with `?mediaSrc=/fixtures/penguin.mp4` (looping). Peer A `establishMediaStream(peerB)` → peer B's `state().activeStreams` contains A within 10s; B's `onRemoteStreamAdded` fired.
- [ ] **Step 2:** Assert frames actually flow (not just track events): on peer B, evaluate a `requestVideoFrameCallback` counter on the rendered remote `<video>` — expect > 3 callbacks in 1s while the media connection is live.
- [ ] **Step 3:** Supernode SFU: three tabs, elect the middle peer as supernode (scripted RTT so the center wins), stream A→B asserting C receives `forwarded_stream`/track and its frame counter advances.
- [ ] **Step 4:** Close → `media_close`/stream removal + frame counter stops advancing.
- [ ] **Step 5:** Run + commit.

Keep Chromium's `--use-fake-device-for-media-stream` flags as the fallback when no `mediaSrc` is given (fake green/tone) — the file fixture is the primary media source for the media suite.

---

### Task 5: Failure + recovery e2e (local)

**Files:**
- Create: `src/e2e/test/local-failure.spec.ts`

- [ ] **Step 1:** Tests: (a) close a tab → remaining peer sees `onPeerDisconnected`, rings re-filled from secondaries; (b) kill the elected supernode's tab → `supernode_elected` fires for a new peer within `1 gossip period + query budget` (assert on wall clock); (c) `pruneStalePeers` path via short `gossipPeriodMs` config override in the demo (`?gossipMs=` URL param — add to both demos reading config overrides from the query string, example-only change).

---

### Task 6: Dart web interop e2e (local)

**Files:**
- Create: `src/e2e/test/dart-interop.spec.ts`

- [ ] **Step 1:** Build + serve the Dart example web bundle (`flutter build web --release` output copied to a fixture dir, or `flutter run -d web-server --web-port` for dev). Drive it via the same Playwright helpers reading `window.__meridianState()` (JSON string).
- [ ] **Step 2:** Test: JS peer + Dart peer in the same room → each appears in the other's `knownPeers`; JS peer's `findClosestNode` to the Dart peer resolves (probe/pong across languages); **media**: JS → Dart `establishMediaStream` completes (Dart side receives `onRemoteStreamAdded`).
- [ ] **Step 3:** Commit.

---

### Task 7: Flutter native desktop peer (xvfb, lab VM; also runnable locally)

**Files:**
- Modify: `src/dart/example/` (add linux platform: `flutter create . --platforms linux`; keep `main.dart` shared, add a `--headless-status` mode: writes one JSON status line per second to stdout and accepts the signaling URL via `--dart-define`)
- Create: `azure/` cloud-init bits installing Flutter SDK + `sudo apt-get install clang cmake ninja-build pkg-config libgtk-3-dev` + xvfb

- [ ] **Step 1:** Desktop build works locally: `flutter build linux --release`, run under `xvfb-run`, verify the status JSON lines contain the peer id and later a non-empty `knownPeers`.
- [ ] **Step 2:** e2e test `desktop-peer.spec.ts` (local variant): desktop peer joins the local rig; assert a Chromium peer discovers it, `findClosestNode` to it works, and (media) the desktop peer receives a remote track. This is the **native-transport coverage**: flutter_webrtc's libwebrtc ↔ Chromium ICE.
- [ ] **Step 3:** Commit.

---

### Task 8: Azure provisioning

**Files:**
- Create: `src/e2e/azure/provision.sh`, `cloud-init.yaml`, `teardown.sh`, `signaling.service`, `turnserver.conf`

- [ ] **Step 1:** `provision.sh` (az CLI, parameterized `AZ_LOCATION_LIST="eastus westeurope southeastasia"`): creates resource group `flash-e2e-rg`, one B2s VM per region + lab VM; NSG allowing: TCP 22 (orchestrator IP only), TCP 80 (demo/static), TCP 8080 (signaling ws), TCP 9222 (CDP, orchestrator IP only), TCP+UDP 3478 + UDP 49152-65535 (ICE/TURN relay range); cloud-init installs Node 20, Chromium (playwright deps: `npx playwright install --with-deps chromium`), xvfb + Flutter SDK deps, copies the built demo bundles + agent via `scp` after boot.
- [ ] **Step 2:** `signaling.service` systemd unit on the lab VM (`ExecStart=/usr/bin/node /opt/flash/signaling-server/server.js`, `Environment=PORT=8080`).
- [ ] **Step 3:** Verify: from orchestrator, `curl http://<each-ip>/` returns the demo; `wscat -c ws://<lab-ip>:8080` registers.

---

### Task 9: Remote Chromium agents + geo suite

**Files:**
- Create: `src/e2e/src/remote.ts` (launch remote headless chromium via SSH: `ssh azureuser@<ip> 'chromium --headless --remote-debugging-port=9222 --user-data-dir=/tmp/p1 <flags>'`), then `playwright.chromium.connectOverCDP('http://<ip>:9222')`.

- [ ] **Step 1:** Remote open helper + topology definition: `E2E_GEO=1` env activates `geo.spec.ts` with the region topology (delays = real; assertions must tolerate jitter — assert ordering/inequalities, not exact values).
- [ ] **Step 2:** `geo.spec.ts`: full-mesh discovery within 2 gossip periods; ring placement monotonic with geography (US peer sits in EU's outer ring, EU-neighbor in inner rings); `findClosestNode` from **every** origin returns the geographically-closest peer; leader = min avg RTT; media EU→US establishes; **supernode-kill**: close the leader's CDP connection → re-election completes < 1 gossip period + election timeout; TURN path (Task 10) forced-relay check.

---

### Task 10: Native desktop peer on Azure + TURN (coturn)

**Files:**
- Create: `turnserver.conf` (static-auth-secret, realm `flash.test`, relay port range 49152-65535), `lab-netem.sh`

- [ ] **Step 1:** coturn on the lab VM (apt, NSG: 3478 tcp+udp, relay UDP range), wire `turnServers` into both demos via URL params (`?turn=url,username,credential` read at startup).
- [ ] **Step 2:** TURN e2e test: lab container with **outbound UDP blocked** (iptables in the container) → ICE must complete with `relay` candidates; assert via `getStats()` in the page (`candidate-pair` with relay local candidate) or the node's ICE candidate-type classification.
- [ ] **Step 3:** Flutter desktop peer on the lab VM (xvfb + systemd or tmux), joining the geo topology; assert Chromium peers discover the native peer and `findClosestNode` can route to it.
- [ ] **Step 4:** netem lab: `lab-netem.sh` — Docker containers per scripted peer + `tc netem` inside (delay matrix), running **Node peers** via a `node-data-webRTC` shim through the library's `rtcFactory` seam; assertions: ring placement matches the scripted matrix, hypervolume replacement fires (secondaries promoted), multi-hop `findClosestNode` hop counts ≤ expected.
- [ ] **Step 5:** `teardown.sh` deletes `flash-e2e-rg`.

---

### Task 11: Runbook + final parity sweep

**Files:**
- Create: `src/e2e/README.md` (how to run local + geo suites, Azure costs/teardown, what each test proves)
- [ ] **Step 1:** Run every suite; record results in `src/e2e/README.md` (what's automated vs manual).
- [ ] **Step 2:** Re-run all unit suites (`src/js` 60, `src/dart` 57, signaling 14) — must stay green.
- [ ] **Step 3:** Commit + push.

---

### Task 12: Run artifacts + central observability

**Files:**
- Create: `src/e2e/src/artifacts.ts` (run-dir + JSONL collectors)
- Modify: `src/signaling-server/server.js` + `package.json` — optional file logging: `LOG=flash-signaling` env enables `logs/signaling.jsonl` (every relayed message + peer connect/disconnect with timestamps), path overridable via `LOG_FILE`
- Modify: both demos — `?wirelog=1` enables the per-peer wire ring buffer surfaced as `wireLog` (Task 1 seam, JS + Dart)

- [ ] **Step 1: Orchestrator collector** — every run creates `src/e2e/runs/<UTC timestamp>/`:
  - `peers/<peerId>.jsonl` — state snapshots every 500ms plus one line per event callback (`supernode_elected`, `peer_disconnected`, `remote_stream_added/removed`, `wire` messages when wirelog enabled) with peer-local sequence numbers and ms timestamps
  - `console/<peerId>.log` — page console + pageerror streams (Playwright `page.on('console')`)
  - `ice-stats/<peerId>.jsonl` — `getStats()` snapshots every second during live media: selected candidate pair, local/remote candidate types (`host`/`srflx`/`relay`), per-pair RTT
  - `signaling.jsonl` — journald dump of `flash-signaling` (lab VM, via SSH on teardown)
  - `topology.json` — the scripted netns/netem matrix actually applied
  - `result.json` — test name, pass/fail, failure message, wall-clock start/end
- [ ] **Step 2: Signaling server visibility** — add `LOG` env-gated JSONL file logging to `src/signaling-server/server.js` (one line per relayed message + register/disconnect). Extend its existing vitest suite: a test asserting a relayed `connect_offer` produces the expected JSONL line.
- [ ] **Step 3: Failure playbook** — document in `src/e2e/README.md` which artifact answers which failure class: discovery never converged → `peers/*.jsonl` `knownPeers` growth + signaling.jsonl `peers_list`; ICE fails → `ice-stats` candidate types; query times out → wire log `probe_request`/`probe_result` correlation by `queryId`; ring placement wrong → `peers/*.jsonl` `ringIndex` timeline vs `topology.json`.
- [ ] **Step 4:** Verify — a run of `local-media.spec.ts` produces the run-dir with all five artifact kinds populated; then `flutter analyze`/`npm test`/`eslint` all green.
- [ ] **Step 5:** Commit.

---

## Known risks / notes

- Chromium's `--unsafely-treat-insecure-origin-as-secure` + `ws://` from a treated-secure origin must be smoke-tested on the first Azure VM **early** (Task 8 acceptance) — if mixed-content rules still block `ws://`, fall back to self-signed TLS + `wss://` + `ignoreHTTPSErrors` (cert generated in cloud-init).
- Azure NSG must allow inbound UDP broadly on peer VMs for ICE (host candidates use ephemeral ports); Chromium can't restrict its ICE port range.
- Flutter Linux desktop peer needs the example rebuilt after `flutter create . --platforms linux` — commit the linux/ platform dir.
- The `supernode_elected`-driven re-election e2e asserts on *which* peer wins under the scripted delay matrix — keep assertions inequality-based (winner's avg RTT ≤ runner-up's), never exact-peer-based, to tolerate jitter.
- `stream_metadata`/`topology_change` committed-log cases are optional hooks today — the e2e suite asserts topology via observed peer behavior, not via those hooks.