import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import '../message_types.dart';
import '../models/peer_state.dart';
import 'meridian_node.dart';

/// Anti-entropy push gossip (spec §3.5): for each ring, send one random peer
/// sample per ring to one random primary member of that ring.
class GossipProtocol {
  final MeridianNode node;
  final Random _random;

  GossipProtocol(this.node, {Random? random}) : _random = random ?? Random();

  /// Sends one gossip payload per populated ring.
  Future<void> runGossipCycle() async {
    final now = DateTime.now().millisecondsSinceEpoch;

    for (final ring in node.rings) {
      if (ring.primaryMembers.isEmpty) continue;

      final target =
          ring.primaryMembers[_random.nextInt(ring.primaryMembers.length)];

      // Build the payload: one random peer sample from each ring.
      final ringSamples = <String, String>{};
      for (final r in node.rings) {
        if (r.primaryMembers.isNotEmpty) {
          final sample =
              r.primaryMembers[_random.nextInt(r.primaryMembers.length)];
          ringSamples['${r.index}'] = sample.peerId;
        }
      }

      final dc = target.dataChannel;
      if (dc == null) {
        node.handlePeerFailure(target.peerId);
        continue;
      }
      try {
        await dc.send(rtc.RTCDataChannelMessage(jsonEncode({
          'type': MeridianMessageTypes.gossip,
          'senderId': node.peerId,
          'timestamp': now,
          'ringSamples': ringSamples,
        })));
      } catch (_) {
        // A failing gossip send is treated as a peer failure; the channel
        // may be dead.
        node.handlePeerFailure(target.peerId);
      }
    }

    node.lastGossipTime = now;
  }

  /// Handles an incoming gossip message: re-measures the RTT to the sender,
  /// refreshes lastSeen of sampled peers we know, and establishes connections
  /// to peers we do not yet know.
  Future<void> handleGossip(Map<String, dynamic> msg) async {
    final senderId = msg['senderId'];
    if (senderId is! String) return;
    if (senderId == node.peerId) return;

    final sender = node.knownPeers[senderId];
    if (sender != null && sender.dataChannel != null) {
      try {
        final rtt = await node.rttMeasurement
            .measureOverDataChannel(sender.dataChannel!);
        sender.rttMs = rtt;
        sender.lastSeen = DateTime.now();
      } catch (_) {
        // Handled by ring refresh.
      }
    }

    final samples = msg['ringSamples'];
    if (samples is! Map) return;

    for (final peerId in samples.values) {
      if (peerId is! String) continue;
      if (peerId == node.peerId) continue;

      final known = node.knownPeers[peerId];
      if (known != null) {
        known.lastSeen = DateTime.now();
        // Attempt a reconnect for peers that lost their connection.
        if (known.status != PeerStatus.connected &&
            !node.pendingConnections.contains(peerId)) {
          _connectTo(peerId);
        }
        continue;
      }

      if (node.pendingConnections.contains(peerId)) continue;
      _connectTo(peerId);
    }
  }

  void _connectTo(String peerId) {
    node.pendingConnections.add(peerId);
    unawaited(
      node.pcManager
          .establishConnection(peerId)
          .catchError((_) {})
          .whenComplete(
            () => node.pendingConnections.remove(peerId),
          ),
    );
  }
}
