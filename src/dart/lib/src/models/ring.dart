import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

/// One resolution ring of the Meridian overlay (spec §4.2). Member lists are
/// mutable; [RingMember] entries are re-placed by the ring manager.
class Ring {
  final int index;
  final double innerRadiusMs;
  final double outerRadiusMs;
  final List<RingMember> primaryMembers;
  final List<RingMember> secondaryMembers;

  Ring({
    required this.index,
    required this.innerRadiusMs,
    required this.outerRadiusMs,
    List<RingMember>? primaryMembers,
    List<RingMember>? secondaryMembers,
  })  : primaryMembers = primaryMembers ?? [],
        secondaryMembers = secondaryMembers ?? [];
}

/// A peer enrolled in one ring, with its live DataChannel and last measured
/// RTT.
class RingMember {
  final String peerId;
  rtc.RTCDataChannel? dataChannel;
  double rttMs;
  DateTime lastProbed;
  String iceCandidateType;
  String natType;
  bool isSupernode;
  bool isFirewalled;
  DateTime joinedAt;

  RingMember({
    required this.peerId,
    this.dataChannel,
    required this.rttMs,
    DateTime? lastProbed,
    this.iceCandidateType = 'host',
    this.natType = 'unknown',
    this.isSupernode = false,
    this.isFirewalled = false,
    DateTime? joinedAt,
  })  : lastProbed = lastProbed ?? DateTime.now(),
        joinedAt = joinedAt ?? DateTime.now();
}
