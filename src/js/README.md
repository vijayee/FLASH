# meridian-webrtc

Browser library for **FLASH** — peer-to-peer video/audio streaming built on the [Meridian](https://github.com/vijayee/FLASH) overlay over standard WebRTC. No media servers: peers organize into latency-ordered multi-resolution rings, discover each other by gossip, and route closest-peer / central-leader / multi-constraint queries by direct latency measurement.

Part of the [FLASH monorepo](https://github.com/vijayee/FLASH) — a Dart/Flutter twin (`meridian_webrtc`) speaks the same wire protocol, so JS and Flutter peers interoperate.

## Install

```bash
npm install meridian-webrtc
```

## Usage

```javascript
import { MeridianNode, MERIDIAN_CONFIG } from 'meridian-webrtc';

const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
const node = new MeridianNode(crypto.randomUUID(), null, MERIDIAN_CONFIG);
await node.initialize('wss://your-signaling-server', stream);

// Closest peer to a target, in O(log N) hops
const result = await node.findClosestNode('some-peer-id', 'peer');
if (result.closestPeerId) await node.establishMediaStream(result.closestPeerId);

// Central leader for a group (minimizes average latency)
const leader = await node.findCentralLeader(['peer-a', 'peer-b', 'peer-c']);

// Nodes satisfying multiple latency constraints
const nodes = await node.findNodesSatisfyingConstraints([
  { target: 'edge-1', maxLatencyMs: 50 },
]);
```

A signaling server (peer list bootstrap + SDP/ICE relay only — never media) is required; a minimal one ships in the monorepo's [`src/signaling-server`](https://github.com/vijayee/FLASH/tree/main/src/signaling-server).

## API surface

- `MeridianNode` — the overlay node: `initialize(url, stream?)`, `findClosestNode`, `findCentralLeader`, `findNodesSatisfyingConstraints`, `establishMediaStream`, `closeStream`, plus `handlers` callbacks (`onPeerDisconnected`, `onSupernodeElected`, `onStreamOffer`, `onStreamRequest`)
- `ConnectionPool`, `calculateRingIndex`, `getRingBounds`, `MERIDIAN_CONFIG` (all tuning knobs: ring count/radii, gossip period, acceptance threshold, STUN/TURN servers)
- Optional `rtcFactory` injection point for environments with a non-global `RTCPeerConnection`

## Demo

`examples/browser/` in the repository is a two-tab demo (discovery, ring status, closest-node queries, live video) — run the signaling server and open it in two tabs.

## License

MIT — see the repository root.