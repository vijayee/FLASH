import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

/// Liveness of a peer known to this node.
enum PeerStatus { discovered, connecting, connected, failed }

/// A peer this node has discovered (includes ring members and extras).
class KnownPeer {
  final String peerId;
  rtc.RTCDataChannel? dataChannel;
  double? rttMs;
  DateTime lastSeen;
  bool isSupernode;
  int? ringIndex;
  PeerStatus status;

  KnownPeer({
    required this.peerId,
    this.dataChannel,
    this.rttMs,
    DateTime? lastSeen,
    this.isSupernode = false,
    this.ringIndex,
    this.status = PeerStatus.discovered,
  }) : lastSeen = lastSeen ?? DateTime.now();
}
