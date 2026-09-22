import 'dart:async';
import 'dart:math' as math;

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import '../config/meridian_config.dart';
import '../models/peer_state.dart';
import '../models/ring.dart';
import 'meridian_node.dart';

/// The (inner, outer) radius bounds, in ms, of one ring.
class RingBounds {
  final double inner;
  final double outer;

  const RingBounds({required this.inner, required this.outer});
}

/// Determines which ring a peer belongs to based on measured RTT. Rings have
/// exponentially increasing radii; the result is clamped to the outermost
/// ring (spec §3.1).
int calculateRingIndex(double rttMs, MeridianConfig config) =>
    _calculateRingIndex(rttMs, config);

/// Ring bounds for [index]. Ring 0 spans (0, innermostRingRadiusMs] so its
/// inner radius is 0 (not 1); the outermost ring is unbounded above.
RingBounds getRingBounds(int index, MeridianConfig config) =>
    _getRingBounds(index, config);

// Private implementations: the [RingManager] delegates expose same-named
// instance methods, which would shadow the public top-level functions inside
// the class.

int _calculateRingIndex(double rttMs, MeridianConfig config) {
  if (rttMs <= config.innermostRingRadiusMs) return 0;

  final index = (math.log(rttMs / config.innermostRingRadiusMs) /
          math.log(config.ringMultiplicativeFactor))
      .ceil();

  return math.min(index, config.ringsPerNode - 1);
}

RingBounds _getRingBounds(int index, MeridianConfig config) {
  final factor = config.ringMultiplicativeFactor;
  return RingBounds(
    inner: index == 0 ? 0.0 : math.pow(factor, index - 1).toDouble(),
    outer: index < config.ringsPerNode - 1
        ? math.pow(factor, index).toDouble()
        : double.infinity,
  );
}

/// Ring membership operations (spec §3.4): enrollment by RTT,
/// hypervolume-based optimization, and periodic re-measurement.
class RingManager {
  final MeridianNode node;

  // Skip overlapping refresh cycles: they would double-probe members racing
  // with ring moves.
  bool _refreshing = false;

  RingManager(this.node);

  /// See [calculateRingIndex].
  int calculateRingIndex(double rttMs) =>
      _calculateRingIndex(rttMs, node.config);

  /// See [getRingBounds].
  RingBounds getRingBounds(int index) => _getRingBounds(index, node.config);

  /// Adds a peer to the appropriate ring based on a measured RTT. Any stale
  /// same-peer membership is removed first so a changed RTT cleanly
  /// relocates it. When the ring is full the peer becomes a secondary
  /// candidate and hypervolume optimization decides promotions.
  Future<RingMember> addPeerToRing(
    String peerId,
    rtc.RTCDataChannel dataChannel,
    double rttMs,
  ) async {
    for (final ring in node.rings) {
      ring.primaryMembers.removeWhere((m) => m.peerId == peerId);
      ring.secondaryMembers.removeWhere((m) => m.peerId == peerId);
    }

    final ringIndex = calculateRingIndex(rttMs);
    final ring = node.rings[ringIndex];

    final member = RingMember(
      peerId: peerId,
      dataChannel: dataChannel,
      rttMs: rttMs,
    );

    if (ring.primaryMembers.length < node.config.nodesPerRing) {
      ring.primaryMembers.add(member);
    } else {
      // Secondary pool, FIFO with a cap.
      ring.secondaryMembers.add(member);
      if (ring.secondaryMembers.length > node.config.secondaryCandidates) {
        ring.secondaryMembers.removeAt(0);
      }
      optimizeRing(ringIndex);
    }

    final existing = node.knownPeers[peerId];
    if (existing != null) {
      existing.dataChannel = dataChannel;
      existing.rttMs = rttMs;
      existing.ringIndex = ringIndex;
      existing.lastSeen = DateTime.now();
      existing.status = PeerStatus.connected;
    } else {
      node.knownPeers[peerId] = KnownPeer(
        peerId: peerId,
        dataChannel: dataChannel,
        rttMs: rttMs,
        ringIndex: ringIndex,
        status: PeerStatus.connected,
      );
    }

    node.dcHandler.setupHandlers(dataChannel, peerId);
    return member;
  }

  /// Hypervolume-based ring optimization (spec §3.4): builds a local
  /// coordinate space from RTTs, then greedily drops the candidate whose
  /// removal reduces the hypervolume (diversity proxy) the least until the
  /// ring fits [MeridianConfig.nodesPerRing].
  void optimizeRing(int ringIndex) {
    final ring = node.rings[ringIndex];
    final allCandidates = [
      ...ring.primaryMembers,
      ...ring.secondaryMembers,
    ];

    if (allCandidates.length <= node.config.nodesPerRing) return;

    final coordinates = <String, List<double>>{};
    for (final a in allCandidates) {
      coordinates[a.peerId] = allCandidates
          .map((b) => a.peerId == b.peerId ? 0.0 : b.rttMs)
          .toList();
    }

    var selected = allCandidates.map((c) => c.peerId).toList();

    while (selected.length > node.config.nodesPerRing) {
      final volumeWith = computeHypervolume(selected, coordinates);
      String? worstPeer;
      var smallestReduction = double.infinity;

      for (final peerId in selected) {
        final without = selected.where((id) => id != peerId).toList();
        final reduction = volumeWith - computeHypervolume(without, coordinates);
        if (reduction < smallestReduction) {
          smallestReduction = reduction;
          worstPeer = peerId;
        }
      }

      if (worstPeer == null) break;
      selected = selected.where((id) => id != worstPeer).toList();
    }

    final newPrimary = <RingMember>[];
    final newSecondary = <RingMember>[];
    for (final candidate in allCandidates) {
      if (selected.contains(candidate.peerId)) {
        newPrimary.add(candidate);
      } else {
        newSecondary.add(candidate);
      }
    }

    ring.primaryMembers
      ..clear()
      ..addAll(newPrimary);
    ring.secondaryMembers
      ..clear()
      ..addAll(newSecondary.take(node.config.secondaryCandidates));
  }

  /// Product of per-dimension coordinate ranges, +1 per dimension to avoid
  /// zero-volume degenerate sets.
  double computeHypervolume(
    List<String> peerIds,
    Map<String, List<double>> coordinates,
  ) {
    if (peerIds.isEmpty) return 0;
    if (peerIds.length == 1) return 1;

    var volume = 1.0;
    final dims = coordinates[peerIds.first]!.length;

    for (var d = 0; d < dims; d++) {
      var minVal = double.infinity;
      var maxVal = double.negativeInfinity;

      for (final peerId in peerIds) {
        final val = coordinates[peerId]![d];
        if (val < minVal) minVal = val;
        if (val > maxVal) maxVal = val;
      }

      volume *= maxVal - minVal + 1;
    }

    return volume;
  }

  /// Periodic maintenance (spec §3.4): re-measures RTTs to ring members and
  /// moves members whose ring assignment changed. Probe failures are treated
  /// as peer failures.
  Future<void> refreshRings() async {
    if (_refreshing) return;
    _refreshing = true;
    try {
      for (final ring in node.rings) {
        for (final member in [...ring.primaryMembers]) {
          final dc = member.dataChannel;
          if (dc == null) continue;
          try {
            final newRtt = await node.rttMeasurement.measureOverDataChannel(dc);
            member.rttMs = newRtt;
            member.lastProbed = DateTime.now();

            final correctRing = calculateRingIndex(newRtt);
            if (correctRing != ring.index) {
              moveMember(member, ring.index, correctRing);
            }
          } catch (_) {
            node.handlePeerFailure(member.peerId);
          }
        }

        optimizeRing(ring.index);
      }
    } finally {
      _refreshing = false;
    }
  }

  /// Relocates a member between rings, promoting a secondary into the source
  /// ring when possible.
  void moveMember(RingMember member, int fromRingIndex, int toRingIndex) {
    final fromRing = node.rings[fromRingIndex];
    final toRing = node.rings[toRingIndex];

    fromRing.primaryMembers.removeWhere((m) => m.peerId == member.peerId);

    if (fromRing.secondaryMembers.isNotEmpty) {
      fromRing.primaryMembers.add(fromRing.secondaryMembers.removeAt(0));
    }

    if (toRing.primaryMembers.length < node.config.nodesPerRing) {
      toRing.primaryMembers.add(member);
    } else {
      toRing.secondaryMembers.add(member);
      optimizeRing(toRingIndex);
    }

    final known = node.knownPeers[member.peerId];
    if (known != null) {
      known.ringIndex = toRingIndex;
    }
  }
}
