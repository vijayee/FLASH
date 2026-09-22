import { findCentralLeader, findClosestNode } from './query-routing.js';
import { initRaftState, replicateCommand } from './raft.js';
import { establishMediaStream, setupMediaForwarding } from './media.js';
import { MESSAGE_TYPES } from './message-types.js';

/**
 * Failure recovery layered on top of node.js's core `_handlePeerFailure`
 * (spec §8): supernode re-election, streaming-partner replacement, and
 * Raft cluster-membership removal. Runs without awaiting from the
 * synchronous failure path so the app-facing onPeerDisconnected
 * notification is never delayed by a 30s election query.
 */
export function handleFailureRecovery(node, peerId) {
  if (node._shuttingDown || !peerId || peerId === node.peerId) return;

  const recovery = [];

  if (node.clusterLeader === peerId && !node.isSupernode) {
    recovery.push(triggerSupernodeReelection(node, peerId).catch(() => {}));
  }
  if (node.activeStreams.has(peerId)) {
    recovery.push(replaceStreamingPartner(node, peerId).catch(() => {}));
  }
  removeRaftClusterMember(node, peerId);

  return Promise.all(recovery);
}

/**
 * Triggers a new supernode election when the current one failed
 * (spec §8): central-leader election over the remaining cluster members
 * (or up to 20 known peers), excluding the failed one.
 */
export async function triggerSupernodeReelection(node, failedPeerId) {
  if (node.clusterLeader !== failedPeerId || node.isSupernode) return;

  const source = node.supernodeCluster
    ? node.supernodeCluster.members
    : Array.from(node.knownPeers.keys()).slice(0, 20);
  const candidates = source.filter(
    (id) => id !== failedPeerId && id !== node.peerId
  );

  if (candidates.length === 0) {
    // Nobody left to elect over; forget the dead leader until gossip
    // repopulates the peer set.
    node.clusterLeader = null;
    return;
  }

  const result = await findCentralLeader(node, candidates);

  if (result.leaderId === node.peerId) {
    node.isSupernode = true;
    node.clusterLeader = node.peerId;
    initRaftState(node, candidates);
    setupMediaForwarding(node);
    // The spec's recovery path announces the new leader like §5.1 does,
    // so surviving peers converge instead of each electing privately.
    broadcastToCluster(node, {
      type: MESSAGE_TYPES.SUPERNODE_ELECTED,
      supernodeId: node.peerId,
      clusterPeers: candidates
    });
    if (node.handlers.onSupernodeElected) {
      node.handlers.onSupernodeElected(node.peerId);
    }
  } else {
    node.clusterLeader = result.leaderId;
  }
}

/**
 * Replaces a failed streaming partner with the closest other peer
 * (spec §8): find the closest node to the failed one, stream from there,
 * and stop the dead partner's tracks.
 */
export async function replaceStreamingPartner(node, failedPeerId) {
  if (!node.activeStreams.has(failedPeerId)) return;

  // The failed partner's stream is dead regardless of what replaces it.
  const oldStream = node.activeStreams.get(failedPeerId);
  if (oldStream) {
    try {
      oldStream.getTracks().forEach((track) => track.stop());
    } catch {
      // Tracks may already be stopped.
    }
    node.activeStreams.delete(failedPeerId);
  }

  const result = await findClosestNode(node, failedPeerId, 'peer');
  const replacement = result && result.closestPeerId;
  if (
    replacement &&
    replacement !== failedPeerId &&
    replacement !== node.peerId
  ) {
    await establishMediaStream(node, replacement);
  }
}

/**
 * Supernode-side Raft bookkeeping when a cluster member fails
 * (spec §8): drop it locally and replicate the leave through Raft.
 */
export function removeRaftClusterMember(node, peerId) {
  const cluster = node.supernodeCluster;
  if (!node.isSupernode || !cluster) return;
  if (!cluster.members.includes(peerId)) return;

  cluster.members = cluster.members.filter((id) => id !== peerId);
  cluster.nextIndex.delete(peerId);
  cluster.matchIndex.delete(peerId);

  replicateCommand(node, {
    type: 'cluster_membership',
    action: 'leave',
    peerId
  });
}

/**
 * Periodic lastSeen pruning (spec §8): a peer silent for three gossip
 * periods is treated as failed. Wired into the gossip interval cycle.
 */
export function pruneStalePeers(node) {
  if (node._shuttingDown) return;
  const cutoff = node.config.gossipPeriodMs * 3;
  const now = Date.now();

  for (const [peerId, known] of node.knownPeers) {
    if (
      known.status === 'connected' &&
      known.dataChannel &&
      now - known.lastSeen > cutoff
    ) {
      node._handlePeerFailure(peerId);
    }
  }
}

function broadcastToCluster(node, message) {
  for (const ring of node.rings) {
    for (const member of ring.primaryMembers) {
      try {
        member.dataChannel.send(JSON.stringify(message));
      } catch {
        // Channel may be dead.
      }
    }
  }
}