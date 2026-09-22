import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import '../config/meridian_config.dart';

/// ICE-server list from config: STUN collapsed into one entry, TURN appended
/// (mirrors the JS implementation's rtc-utils.js).
List<Map<String, dynamic>> buildIceServers(MeridianConfig config) {
  final iceServers = <Map<String, dynamic>>[];
  if (config.stunServers.isNotEmpty) {
    iceServers.add({'urls': config.stunServers});
  }
  for (final turn in config.turnServers) {
    iceServers.add({
      'urls': turn.url,
      'username': turn.username,
      'credential': turn.credential,
    });
  }
  return iceServers;
}

/// Serializes a local session description into the signaling wire format
/// ({type, sdp}) — the shape a browser RTCSessionDescription serializes to,
/// so JS and Dart peers interop.
Map<String, dynamic>? sessionDescriptionToMap(
  rtc.RTCSessionDescription? description,
) {
  if (description == null) return null;
  return {'type': description.type, 'sdp': description.sdp};
}

/// Validates a relayed SDP payload ({type, sdp} string fields).
bool isWellFormedSdp(Map<dynamic, dynamic> sdp) =>
    sdp['type'] is String && sdp['sdp'] is String;

/// Parses a trickled ICE candidate relayed as JSON into an
/// [rtc.RTCIceCandidate]; returns null when the payload is not a usable
/// candidate (flutter_webrtc uses `addCandidate`).
rtc.RTCIceCandidate? candidateFromMap(dynamic raw) {
  if (raw is! Map) return null;
  final candidate = raw['candidate'];
  if (candidate is! String || candidate.isEmpty) return null;
  return rtc.RTCIceCandidate(
    candidate,
    raw['sdpMid'] as String?,
    (raw['sdpMLineIndex'] as num?)?.toInt(),
  );
}
