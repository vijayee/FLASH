# meridian_example

Minimal demo of the `meridian_webrtc` overlay (spec §11).

1. From the repo root, start the signaling server: `node src/signaling-server/server.js`
   (listens on ws://localhost:8080).
2. In `src/dart/example`, run `flutter pub get` then `flutter run -d chrome`.
3. Open a second tab/instance; the two peers discover each other through the
   signaling server and join the Meridian rings.