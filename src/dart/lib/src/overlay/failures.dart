import 'dart:async';

import 'package:logging/logging.dart';

import '../message_types.dart';
import '../models/peer_state.dart';
import '../streaming/sfu_forwarder.dart';
import 'meridian_node.dart';

// Spec §8 recovery layered on top of MeridianNode.handlePeerFailure's
// core cleanup: supernode re-election, streaming-partner replacement and
// Raft cluster-membership removal. Runs without awaiting from the
// synchronous failure path so the app-facing onPeerDisconnected
// notification is never delayed by a 30s election query.

final Logger _logger = Logger('meridian_webrtc.failures');

/// Failure recovery entry point (spec §8). Never awaited by the caller.
Future<void> handleFailureRecovery(MeridianNode node, String peerId) async {
  if (node.shuttingDown || peerId.isEmpty || peerId == node.peerId) return;

  final recovery = <Future<void>>[];

  if (node.clusterLeader == peerId && !node.isSupernode) {
    recovery.add(
      triggerSupernodeReelection(node, peerId).catchError((Object _) {}),
    );
  }
  if (node.activeStreams.containsKey(peerId)) {
    recovery.add(
      replaceStreamingPartner(node, peerId).catchError((Object _) {}),
    );
  }
  removeRaftClusterMember(node, peerId);

  await Future.wait(recovery);
}

/// Triggers a new supernode election when the current one failed (spec
/// §8): central-leader election over the remaining cluster members (or
/// up to 20 known peers), excluding the failed one.
Future<void> triggerSupernodeReelection(
  MeridianNode node,
  String failedPeerId,
) async {
  if (node.clusterLeader != failedPeerId || node.isSupernode) return;

  final source =
      node.supernodeCluster?.members ?? node.knownPeers.keys.take(20).toList();
  final candidates = [
    for (final id in source)
      if (id != failedPeerId && id != node.peerId) id,
  ];

  if (candidates.isEmpty) {
    // Nobody left to elect over; forget the dead leader until gossip
    // repopulates the peer set.
    node.clusterLeader = null;
    return;
  }

  final result = await node.queryRouting.findCentralLeader(candidates);

  if (result.leaderId == node.peerId) {
    node.isSupernode = true;
    node.clusterLeader = node.peerId;
    node.raftConsensus.initRaftState(candidates);
    SfuForwarder.setupMediaForwarding(node);
    // The spec's recovery path announces the new leader like §5.1 does,
    // so surviving peers converge instead of each electing privately.
    node.raftConsensus.broadcastToCluster({
      'type': MeridianMessageTypes.supernodeElected,
      'supernodeId': node.peerId,
      'clusterPeers': candidates,
    });
    node.onSupernodeElected?.call(node.peerId);
  } else {
    node.clusterLeader = result.leaderId;
  }
}

/// Replaces a failed streaming partner with the closest other peer (spec
/// §8): find the closest node to the failed one, stream from there, and
/// stop the dead partner's tracks.
Future<void> replaceStreamingPartner(
  MeridianNode node,
  String failedPeerId,
) async {
  if (!node.activeStreams.containsKey(failedPeerId)) return;

  // The failed partner's stream is dead regardless of what replaces it.
  final oldStream = node.activeStreams.remove(failedPeerId);
  if (oldStream != null) {
    for (final track in oldStream.getTracks()) {
      try {
        unawaited(track.stop());
      } catch (_) {
        // Tracks may already be stopped.
      }
    }
    node.onRemoteStreamRemoved?.call(failedPeerId);
  }

  final result =
      await node.queryRouting.findClosestNode(failedPeerId, targetType: 'peer');
  final replacement = result.closestPeerId;
  if (replacement != null &&
      replacement != failedPeerId &&
      replacement != node.peerId) {
    await node.streamManager.establishMediaStream(replacement);
  }
}

/// Supernode-side Raft bookkeeping when a cluster member fails (spec
/// §8): drop it locally and replicate the leave through Raft.
void removeRaftClusterMember(MeridianNode node, String peerId) {
  final cluster = node.supernodeCluster;
  if (!node.isSupernode || cluster == null) return;
  if (!cluster.members.contains(peerId)) return;

  cluster.members = cluster.members.where((id) => id != peerId).toList();
  cluster.nextIndex.remove(peerId);
  cluster.matchIndex.remove(peerId);

  node.raftConsensus.replicateCommand({
    'type': 'cluster_membership',
    'action': 'leave',
    'peerId': peerId,
  });
}

/// Periodic lastSeen pruning (spec §8): a peer silent for three gossip
/// periods is treated as failed. Wired into the gossip interval cycle.
void pruneStalePeers(MeridianNode node) {
  if (node.shuttingDown) return;
  final cutoff = node.config.gossipPeriod * 3;
  final now = DateTime.now();

  final stale = [
    for (final entry in node.knownPeers.entries)
      if (entry.value.status == PeerStatus.connected &&
          entry.value.dataChannel != null &&
          now.difference(entry.value.lastSeen) > cutoff)
        entry.key,
  ];

  for (final peerId in stale) {
    _logger.fine('pruning stale peer $peerId');
    node.handlePeerFailure(peerId);
  }
}
