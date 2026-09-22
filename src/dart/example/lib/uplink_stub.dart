import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

/// No-op on non-web platforms: the `?mediaSrc=` file uplink is a browser
/// e2e affordance (Task 6 — a hidden looping `<video>` captured via
/// `captureStream()`); native targets get a stub. Selected by the
/// conditional import in main.dart. Unreachable in practice: the URL-param
/// parsing only reads query parameters on http(s) bases.
Future<rtc.MediaStream?> fileUplink(String src) async {
  throw UnsupportedError('mediaSrc uplink is web-only');
}
