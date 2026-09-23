# JS examples

Browser demos for [`flash-webrtc`](https://www.npmjs.com/package/flash-webrtc).

## Run

```bash
# 1. Signaling server (the demo's default rendezvous, ws://localhost:8080)
node src/signaling-server/server.js

# 2. The demo (http://127.0.0.1:9000)
node src/js/examples/browser/serve.mjs
```

Open two tabs and press Connect in both; the peers discover each other
through the signaling server, sort into latency-ordered rings, and can
exchange media. The page exposes the full `MERIDIAN_CONFIG` in the
collapsible "Overlay configuration" panel (prefilled from the URL params,
applied on Connect — reload the page to change values after connecting),
a peer inspector fed from the node's `knownPeers` table, and an event log.

## Overlay configuration

Every knob of `MERIDIAN_CONFIG` (`src/js/src/config.js`), with defaults.
Set per tab via the panel or the URL affordances below.

| Name | Default | What it does | When to change it |
| --- | --- | --- | --- |
| `ringsPerNode` | 9 | How many latency-ordered rings each peer maintains. | More rings = finer routing granularity, more open connections. |
| `nodesPerRing` | 8 | Primary members kept per ring. | Larger overlays need fuller rings; smaller values cut connection count. |
| `secondaryCandidates` | 4 | Backup members stored per ring. | Raise for more failover headroom in churny overlays. |
| `innermostRingRadius` | 1 (ms) | RTT radius of ring 0, the closest-latency band. | Raise if you want ring 0 to tolerate more jitter. |
| `ringMultiplicativeFactor` | 2 | Each outer ring's radius = previous ring's × r. | Lower for denser outer rings, higher for wider spread. |
| `routeAcceptanceThreshold` | 0.5 (β) | Fraction of queried rings that must answer before a routed query's result is accepted. | Raise for stricter (more reliable, slower) query results. |
| `probeTimeoutFactor` | 2 (ε) | An RTT probe times out at ε × the peer's last RTT. | Raise on lossy links where probes drop spuriously. |
| `gossipPeriodMs` | 30000 | Interval between gossip exchanges (discovery + ring maintenance input). | Lower to converge faster in small/test overlays. |
| `ringReplacementPeriodMs` | 60000 | Interval between sweeps that replace dead/slow ring members. | Lower in churny environments. |
| `maxEphemeralConnections` | 10 | Cap on short-lived probe connections opened for RTT measurement. | Raise to probe more candidates concurrently. |
| `stunServers` | `['stun:stun.l.google.com:19302']` | STUN servers for public-candidate discovery. | Point at your own STUN in restricted networks. |
| `turnServers` | `[]` | TURN relays (array of `{urls, username, credential}`). | Add one when NATs/symmetric firewalls block direct paths. |
| `maxHops` | 32 | Hop cap for routed closest-node / central-leader queries. | Raise for very large overlays. |
| `ephemeralProbeTimeoutMs` | 5000 | Timeout for one ephemeral probe connection. | Raise on high-latency paths. |
| `queryTimeoutMs` | 30000 | Overall timeout for closest-node / central-leader queries. | Raise for deep overlays, lower to fail fast. |

## URL affordances

| Param | Effect |
| --- | --- |
| `?wirelog=1` | Records every DataChannel send/receive and signaling message into `__meridian.state().wireLog` (bounded ring buffer, off by default). |
| `?gossipMs=<n>` | Overrides `gossipPeriodMs` (n > 0). The panel is prefilled with it. |
| `?stun=a,b` | Overrides `stunServers` (comma-separated). |
| `?turn=url,username,cred` | Adds one long-term-credential TURN entry (TURN URLs contain no commas). |
| `?mediaSrc=<url>` | Replaces the `getUserMedia` uplink with a looping `<video>` playing the file (`captureStream()`) — deterministic media for tests. |

The e2e suite also reads `window.__meridian` (`peerId()`, `connect(url)`,
`state()`, `elect()`, `closeStream()`); see `src/e2e`.

## Use the published package

The demo imports the library **relatively** (`../../src/index.js`) so
`serve.mjs` can serve it with no build step. In your own app, install the
published package and import from it instead:

```bash
npm install flash-webrtc
```

```diff
- import { MeridianNode, MERIDIAN_CONFIG } from '../../src/index.js';
+ import { MeridianNode, MERIDIAN_CONFIG } from 'flash-webrtc';
```