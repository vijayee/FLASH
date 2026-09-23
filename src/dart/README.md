# meridian_webrtc

Flutter library for **FLASH** — peer-to-peer video/audio streaming built on the [Meridian](https://github.com/vijayee/FLASH) overlay over WebRTC. No media servers: peers organize into latency-ordered multi-resolution rings, discover each other by gossip, and route closest-peer / central-leader / multi-constraint queries by direct latency measurement. Runs on Android, iOS, web, macOS, Windows, and Linux from one codebase, via [`flutter_webrtc`](https://pub.dev/packages/flutter_webrtc).

A JavaScript twin (`meridian-webrtc` on npm) speaks the same wire protocol, so Flutter and browser peers interoperate through one signaling server.

## Install

```yaml
dependencies:
  meridian_webrtc:
```

## Usage

```dart
import 'package:meridian_webrtc/meridian_webrtc.dart';

final node = MeridianNode(peerId: uuidV4());
await node.initialize('wss://your-signaling-server', mediaStream: stream);

// Closest peer to a target, in O(log N) hops
final result = await node.findClosestNode('some-peer-id');
if (result.closestPeerId != null) {
  await node.establishMediaStream(result.closestPeerId!);
}

// Central leader for a group (minimizes average latency)
final leader = await node.findCentralLeader(['peer-a', 'peer-b']);

// Nodes satisfying multiple latency constraints
final nodes = await node.findNodesSatisfyingConstraints([
  const Constraint(target: 'edge-1', maxLatencyMs: 50),
]);
```

A signaling server (peer list bootstrap + SDP/ICE relay only — never media) is required; a minimal one ships in the monorepo's [`src/signaling-server`](https://github.com/vijayee/FLASH/tree/main/src/signaling-server).

`example/` in the repository is a minimal Flutter app wiring up camera capture, discovery, closest-node queries, and remote video rendering.

## License

MIT — see the repository root.