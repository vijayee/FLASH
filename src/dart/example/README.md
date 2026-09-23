# meridian_example

Minimal demo of the [`flash_webrtc`](https://pub.dev/packages/flash_webrtc)
overlay (spec §11) — the Dart twin of the JS browser demo
(`src/js/examples/browser`).

## Run

1. From the repo root, start the signaling server:
   `node src/signaling-server/server.js` (listens on ws://localhost:8080).
2. In `src/dart/example`, run `flutter pub get` then `flutter run -d chrome`.
3. Open a second tab/instance (or run the JS demo alongside); the peers
   discover each other through the signaling server and join the Meridian
   rings. The app shows the status line, closest-node / stream actions with
   in-app guidance text, and an event log.

## Overlay configuration

The app builds its `MeridianConfig` from URL params **before** the
`MeridianNode` is constructed (web builds only; native keeps defaults).
Parsed on `http(s)` bases:

| Param | Effect |
| --- | --- |
| `?signaling=<url>` | Bootstrap signaling URL. |
| `?stun=a,b` | Overrides `MeridianConfig.stunServers` (comma-separated). |
| `?gossipMs=<n>` | Overrides `gossipPeriod` (n > 0). |
| `?turn=url,username,cred` | Adds one long-term-credential TURN relay (same shape as the JS demo's affordance). |
| `?mediaSrc=<url>` | Swaps the `getUserMedia` uplink for a looping `<video>` playing the file (`captureStream()`). |
| `?config=<json>` | JSON blob of `MERIDIAN_CONFIG` overrides keyed by the published JS package's names, e.g. `?config={"gossipPeriodMs":2000,"ringsPerNode":5}`. Accepted keys: `ringsPerNode`, `nodesPerRing`, `secondaryCandidates`, `innermostRingRadius(Ms)`, `ringMultiplicativeFactor`, `routeAcceptanceThreshold`, `probeTimeoutFactor`, `gossipPeriodMs`, `ringReplacementPeriodMs`, `maxEphemeralConnections`, `stunServers`, `turnServers` (`{"url","username","credential"}` maps or `url,username,credential` strings), `maxHops`, `ephemeralProbeTimeoutMs`, `queryTimeoutMs`. Malformed/missing keys keep the library defaults; the explicit params (`stun`/`gossipMs`/`turn`) win over the blob. |

Durations are milliseconds in the blob (the JS `MERIDIAN_CONFIG` names), so
one JSON blob configures both demo flavors.

## Headless desktop peer

The native (Linux desktop) build has no browser seams; it is driven by env
defines instead:

```bash
flutter run -d linux \
  --dart-define=MRD_SIGNALING=ws://localhost:8080 \
  --dart-define=MRD_STATUS_FILE=/tmp/peer-status.jsonl
```

It auto-connects to `MRD_SIGNALING` and appends one JSON status line per
second (the same fields as the web `window.__meridianState()` hook) to
`MRD_STATUS_FILE`, so an agent/test reads the native peer's state without
any display. Without the defines it behaves like the interactive demo.

## Use the published package

`lib/main.dart` imports `package:flash_webrtc/flash_webrtc.dart` — the
published pub.dev package. In `example/` the dependency points at the
monorepo path for development; in your own app add the registry version:

```yaml
dependencies:
  flash_webrtc: ^0.1.0
```