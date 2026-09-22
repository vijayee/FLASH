import 'dart:async';
import 'dart:js_interop';

import 'package:dart_webrtc/dart_webrtc.dart' show MediaStreamWeb;
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:web/web.dart' as web;

/// `?mediaSrc=<url>` uplink (Task 6, example-only): a looping, muted
/// `<video>` playing the file becomes the node's uplink via
/// `captureStream()` — the Dart twin of the JS demo's startFileUplink
/// (examples/browser/main.js), giving e2e a deterministic media source
/// instead of the fake device. The element must be RENDERED for headless
/// Chromium to decode it (a display:none video never starts), so it is
/// appended as a 1px, transparent, off-flow box. The caller falls back to
/// getUserMedia if this throws (never plays / decode error / autoplay
/// blocked).
Future<rtc.MediaStream?> fileUplink(String src) async {
  final video = web.HTMLVideoElement()
    ..src = src
    ..loop = true
    ..muted = true
    ..autoplay = true
    ..playsInline = true;
  video.style
    ..position = 'fixed'
    ..top = '0'
    ..left = '0'
    ..width = '1px'
    ..height = '1px'
    ..opacity = '0'
    ..pointerEvents = 'none';
  final body = web.document.body;
  if (body == null) {
    throw StateError('mediaSrc uplink: no document body to attach the video');
  }

  final playing = Completer<void>();
  video.addEventListener(
      'error',
      ((web.Event _) {
        final error = video.error;
        if (!playing.isCompleted) {
          playing.completeError(
            StateError('mediaSrc video failed (code ${error?.code ?? '?'})'),
          );
        }
      }).toJS);
  video.addEventListener(
      'playing',
      ((web.Event _) {
        if (!playing.isCompleted) playing.complete();
      }).toJS);

  body.appendChild(video);
  try {
    await video.play().toDart;
    // play() resolving is not enough: the file must actually be decoding
    // (an error event or a stalled load rejects here — the JS demo waits
    // for the same `playing` event with the same budget).
    await playing.future.timeout(const Duration(seconds: 20));
  } catch (err) {
    video.remove();
    rethrow;
  }
  return MediaStreamWeb(video.captureStream(), 'local');
}
