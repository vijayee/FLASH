<p align="center">
  <img src="FLASH.svg" alt="FLASH" width="420">
</p>

**FLASH** is a peer-to-peer video and audio streaming overlay that runs entirely between browsers and Flutter apps — no media servers, no SFU clusters, no bandwidth bill that scales with your audience. It implements the [Meridian](docs/Meridian.pdf) overlay network on top of standard WebRTC: multi-resolution rings for organizing peers by real-world latency, gossip-based discovery, and query routing driven by direct latency measurements rather than coordinate guesses.

## The problem it solves

Traditional WebRTC streaming at scale needs a media relay (SFU/MCU) in the middle — expensive to run, a single point of failure, and it re-introduces a server hop that streaming was supposed to avoid. And when peers *can* connect directly, nothing tells a peer which of the hundreds of potential peers is actually the **closest** one for low-latency streaming, or which peer sits in the middle of a group to act as its relay.

FLASH answers both questions with the network itself:

- **Peers find peers.** A minimal WebSocket signaling server relays SDP offers and ICE candidates, then gets out of the way. From there, every peer maintains connections to its closest peers and exchanges gossip to discover the rest of the network.
- **Latency is measured, not estimated.** Peers ping each other over DataChannels (and probe unknown peers over ephemeral WebRTC connections) to build a real latency map. Ring membership, query routing, and leadership are all decisions made from these direct measurements.
- **Closest-peer discovery in O(log N) hops.** `findClosestNode(target)` routes a query through the overlay using each hop's ring neighbors, converging on the peer with the lowest measured RTT to the target.
- **The crowd funds the infrastructure.** Any peer — a browser tab, a phone — can be elected a *supernode*: the node minimizing average RTT to a group. Supernodes relay media for peers who need it (SFU forwarding), and the role is confirmed by a Raft consensus running over the same DataChannels.
- **It heals.** DataChannel close events and a last-seen heartbeat prune failed peers, secondary ring candidates are promoted automatically, and a dead supernode triggers a new election within one gossip period.

Everything runs in the browser (JavaScript) or natively in a Flutter app (Dart) — Android, iOS, web, macOS, Windows, and Linux from the same Dart codebase. The two implementations speak the **same wire protocol**, so a Dart peer and a JavaScript peer can discover each other, route queries, and stream media to each other through one signaling server.

## Packages

| Path | What it is |
|---|---|
| [`src/js`](src/js) | **`meridian-webrtc`** — the browser library. Plain ES modules, no build step. |
| [`src/dart`](src/dart) | **`meridian_webrtc`** — the Flutter/Dart library on [`flutter_webrtc`](https://pub.dev/packages/flutter_webrtc). Includes a minimal example app. |
| [`src/signaling-server`](src/signaling-server) | The minimal WebSocket relay: peer registration, peer list bootstrap, and SDP/ICE forwarding. It never touches media or overlay traffic. |

## How the overlay works

Every node organizes the peers it knows into **9 rings** of exponentially increasing radius (1ms, 2ms, 4ms, …): ring *i* holds peers measured within `2^(i-1)` ms. Rings are kept full via **hypervolume optimization** — the most *latency-diverse* peers are selected as primary members, so queries can always make progress toward any target.

```
Discovery:  gossip pushes a random peer sample from each ring to a random
            peer in each ring  →  unknown peers are dialed and measured

Queries:    findClosestNode(target)      → greedily forwards toward the target,
            findCentralLeader(peers)       accepting a hop only if measured
            findNodesSatisfyingConstraints RTT improves past a 0.5× threshold

Streaming:  media_offer / media_answer are exchanged over the control
            DataChannel; a supernode forwards streams (SFU) to its cluster

Consensus:  supernode groups confirm control-plane state (membership,
            stream metadata) with Raft over the DataChannels
```

Failure of a peer, or of the supernode, is detected by DataChannel close events, probe timeouts, and stale heartbeats — recovery ranges from promoting secondary ring members to re-running the central-leader election.

## Quick start

```bash
# 1. Start the signaling server (defaults to ws://localhost:8080)
node src/signaling-server/server.js

# 2a. JavaScript peers — open the browser demo in two tabs
npx serve src/js/examples/browser

# 2b. Flutter peer — run the example app
cd src/dart/example && flutter pub get && flutter run -d chrome
```

Using the library directly:

```javascript
import { MeridianNode, MERIDIAN_CONFIG } from 'meridian-webrtc';

const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
const node = new MeridianNode(crypto.randomUUID(), null, MERIDIAN_CONFIG);
await node.initialize('ws://localhost:8080', stream);

const result = await node.findClosestNode('some-peer-id', 'peer');
if (result.closestPeerId) await node.establishMediaStream(result.closestPeerId);
```

```dart
import 'package:meridian_webrtc/meridian_webrtc.dart';

final node = MeridianNode(peerId: uuidV4());
await node.initialize('ws://localhost:8080', mediaStream: stream);

final result = await node.findClosestNode('some-peer-id');
if (result.closestPeerId != null) {
  await node.establishMediaStream(result.closestPeerId!);
}
```

## Properties

| Property | Guarantee | How |
|---|---|---|
| Closest-node accuracy | Median error ~2ms | Direct measurement, no coordinate embeddings |
| Query latency | ~300ms, constant with system size | Logarithmic hop count over exponentially-spaced rings |
| Scalability | O(log N) hops | Exponentially increasing ring radii |
| Load balance | In-degree ratio < 2 for 90% of nodes | Stochastic ring independence + hypervolume optimization |
| Failure recovery | < 1 gossip period (~30s) | Channel-close detection + secondary promotion + re-election |
| Supernode election | Minimizes average latency to the group | Meridian central-leader election |
| Platforms | Web, Android, iOS, macOS, Windows, Linux | Single Flutter codebase over WebRTC |

## Status

Both libraries implement the full specification (overlay, query routing, ring optimization, supernode election + Raft consensus, streaming, failure recovery) and are covered by behavioral test suites — 60 vitest tests for the JS library, 57 Flutter tests for the Dart package, 14 for the signaling server. The two implementations are wire-compatible: peers in either language interoperate through the same signaling server. What remains untested by automation is a live multi-peer session in real browsers (see the [end-to-end testing notes](docs/FLASH%20Javascript.md#12-implementation-order-recommended)); TURN credentials are not configured, so only STUN traversal is exercised.

## Documentation

- [`docs/FLASH Javascript.md`](docs/FLASH%20Javascript.md) — the JS implementation specification
- [`docs/FLASH Dart.md`](docs/FLASH%20Dart.md) — the Dart/Flutter implementation specification
- [`docs/Meridian.pdf`](docs/Meridian.pdf) — the Meridian overlay paper the architecture derives from

## License

[MIT](LICENSE)