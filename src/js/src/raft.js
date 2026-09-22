import { MESSAGE_TYPES } from './message-types.js';
import { findCentralLeader } from './query-routing.js';
import { setupMediaForwarding } from './media.js';

/**
 * Initializes Raft state for a supernode cluster (spec §2.7, §5.2). The
 * electing supernode starts as leader; peers joining an existing cluster
 * under a known leader start as followers.
 *
 * Completeness fix (plan): the leader role is not confirmed at this point,
 * so a real election timer is armed here in every state — if no Raft
 * activity confirms leadership, a re-election can actually occur instead
 * of the node staying "leader" forever.
 */
export function initRaftState(node, clusterMembers, options = {}) {
  shutdownRaft(node);

  // Cluster membership always includes ourselves: quorum math
  // (floor(n / 2) + 1) only works over the full member list.
  const members = [
    ...new Set([...(clusterMembers || []), node.peerId])
  ];

  const cluster = {
    clusterId: crypto.randomUUID(),
    members,
    leaderId: options.leaderId || node.peerId,

    // Persistent state.
    currentTerm: 0,
    votedFor: null,
    log: [],

    // Volatile state. Log entries are 0-indexed array positions, so
    // -1 means "nothing committed/applied yet" (spec §2.7 inits these to
    // 0, which would permanently pin the first entry out of reach).
    commitIndex: -1,
    lastApplied: -1,

    // Leader-only volatile state.
    nextIndex: new Map(),
    matchIndex: new Map(),

    // Timers (spec §2.7).
    electionTimeoutMs: 150 + Math.floor(Math.random() * 150),
    heartbeatIntervalMs: 50,

    raftState: options.initialState === 'follower' ? 'follower' : 'leader',
    electionTimer: null,
    heartbeatTimer: null,

    // Live election round (term + votes granted), for response correlation.
    _election: null
  };

  for (const member of members) {
    if (member === node.peerId) continue;
    cluster.nextIndex.set(member, 0);
    cluster.matchIndex.set(member, -1);
  }

  node.supernodeCluster = cluster;

  if (cluster.raftState === 'leader') {
    // Leadership is unconfirmed until followers answer heartbeats; still,
    // the elected node sends heartbeats from the start (spec §5.2) and the
    // election timer (armed below) forces a real election if none are.
    startHeartbeats(node);
  }
  resetElectionTimeout(node);

  return cluster;
}

/**
 * Re-arms the election timer. Every confirmed Raft activity (valid
 * AppendEntries as follower, acknowledged AppendEntries as leader) feeds
 * this; when it fires, a new election round starts.
 */
export function resetElectionTimeout(node) {
  const cluster = node.supernodeCluster;
  if (!cluster) return;

  if (cluster.electionTimer) clearTimeout(cluster.electionTimer);
  cluster.electionTimer = null;

  // A single-member cluster needs no consensus; leadership is trivial.
  if (cluster.members.length <= 1 || node._shuttingDown) return;

  cluster.electionTimer = setTimeout(() => {
    if (node._shuttingDown || node.supernodeCluster !== cluster) return;
    startElection(node);
  }, cluster.electionTimeoutMs);
}

/**
 * Starts an election round (spec §5.2): increment the term, vote for self,
 * and request votes from every member over its DataChannel.
 */
export function startElection(node) {
  const cluster = node.supernodeCluster;
  if (!cluster || cluster.members.length <= 1 || node._shuttingDown) return;

  if (cluster.raftState !== 'candidate') stopHeartbeats(node);
  cluster.raftState = 'candidate';
  cluster.currentTerm++;
  cluster.votedFor = node.peerId;

  const term = cluster.currentTerm;
  // Election-round guard: responses from any other term are ignored.
  cluster._election = { term, votesGranted: new Set([node.peerId]) };

  const lastLogIndex = cluster.log.length - 1;
  const lastLogTerm = lastLogIndex >= 0 ? cluster.log[lastLogIndex].term : 0;

  for (const member of cluster.members) {
    if (member === node.peerId) continue;
    const known = node.knownPeers.get(member);
    if (!known || !known.dataChannel) continue;
    sendRaftMessage(node, known.dataChannel, {
      type: MESSAGE_TYPES.RAFT_REQUEST_VOTE,
      term,
      candidateId: node.peerId,
      lastLogIndex,
      lastLogTerm
    });
  }

  // A round that gathers no majority is retried when the timer re-fires.
  resetElectionTimeout(node);
}

/**
 * Heartbeat / log-replication loop (spec §5.2): one AppendEntries RPC per
 * follower per tick, carrying everything from nextIndex onward.
 */
export function startHeartbeats(node) {
  const cluster = node.supernodeCluster;
  if (!cluster) return;
  stopHeartbeats(node);

  cluster.heartbeatTimer = setInterval(() => {
    if (
      node.supernodeCluster !== cluster ||
      cluster.raftState !== 'leader'
    ) {
      stopHeartbeats(node);
      return;
    }
    for (const member of cluster.members) {
      if (member === node.peerId) continue;
      sendAppendEntriesToMember(node, member);
    }
  }, cluster.heartbeatIntervalMs);
}

export function stopHeartbeats(node) {
  const cluster = node.supernodeCluster;
  if (cluster && cluster.heartbeatTimer) {
    clearInterval(cluster.heartbeatTimer);
    cluster.heartbeatTimer = null;
  }
}

function sendAppendEntriesToMember(node, member) {
  const cluster = node.supernodeCluster;
  if (!cluster || cluster.raftState !== 'leader') return;

  const known = node.knownPeers.get(member);
  if (!known || !known.dataChannel) return;

  const nextIdx = Math.min(
    Math.max(cluster.nextIndex.get(member) || 0, 0),
    cluster.log.length
  );

  sendRaftMessage(node, known.dataChannel, {
    type: MESSAGE_TYPES.RAFT_APPEND_ENTRIES,
    term: cluster.currentTerm,
    leaderId: node.peerId,
    prevLogIndex: nextIdx - 1,
    prevLogTerm: nextIdx > 0 ? cluster.log[nextIdx - 1].term : 0,
    entries: cluster.log.slice(nextIdx),
    leaderCommit: cluster.commitIndex
  });
}

/**
 * Handles an incoming AppendEntries RPC (spec §5.2, follower side).
 */
export function handleAppendEntries(node, msg, dataChannel) {
  const cluster = node.supernodeCluster;
  if (!cluster || typeof msg.term !== 'number') return;

  if (msg.term > cluster.currentTerm) {
    // Completeness fix: a higher term always deposes us.
    stepDown(node, msg.term, msg.leaderId || null);
  }
  if (msg.term < cluster.currentTerm) {
    sendRaftMessage(node, dataChannel, {
      type: MESSAGE_TYPES.RAFT_APPEND_ENTRIES_RESPONSE,
      term: cluster.currentTerm,
      success: false,
      lastLogIndex: cluster.log.length - 1
    });
    return;
  }

  cluster.leaderId = msg.leaderId || cluster.leaderId;
  if (cluster.raftState !== 'follower') {
    stopHeartbeats(node);
    cluster.raftState = 'follower';
  }
  // Valid leader activity defers our own election.
  resetElectionTimeout(node);

  if (msg.prevLogIndex >= 0) {
    if (
      msg.prevLogIndex >= cluster.log.length ||
      cluster.log[msg.prevLogIndex].term !== msg.prevLogTerm
    ) {
      // Drop the conflicting suffix so the next retry (decremented
      // nextIndex on the leader) can succeed.
      cluster.log = cluster.log.slice(
        0,
        Math.min(msg.prevLogIndex, cluster.log.length)
      );
      sendRaftMessage(node, dataChannel, {
        type: MESSAGE_TYPES.RAFT_APPEND_ENTRIES_RESPONSE,
        term: cluster.currentTerm,
        success: false,
        lastLogIndex: cluster.log.length - 1
      });
      return;
    }
  }

  for (const entry of msg.entries || []) {
    if (
      typeof entry.index !== 'number' ||
      typeof entry.term !== 'number' ||
      entry.index < 0
    ) {
      continue;
    }
    if (entry.index < cluster.log.length) {
      if (cluster.log[entry.index].term !== entry.term) {
        cluster.log = cluster.log.slice(0, entry.index);
        cluster.log.push(entry);
      }
    } else if (entry.index === cluster.log.length) {
      cluster.log.push(entry);
    }
  }

  if (msg.leaderCommit > cluster.commitIndex && cluster.log.length > 0) {
    cluster.commitIndex = Math.max(
      cluster.commitIndex,
      Math.min(msg.leaderCommit, cluster.log.length - 1)
    );
  }
  applyCommittedEntries(node);

  sendRaftMessage(node, dataChannel, {
    type: MESSAGE_TYPES.RAFT_APPEND_ENTRIES_RESPONSE,
    term: cluster.currentTerm,
    success: true,
    lastLogIndex: cluster.log.length - 1
  });
}

/**
 * Handles an AppendEntries response (leader side, spec §5.2): update
 * nextIndex/matchIndex on success, back up on failure, and recompute the
 * commit index by majority matchIndex (currentTerm entries only).
 */
export function handleAppendEntriesResponse(node, msg, fromPeerId) {
  const cluster = node.supernodeCluster;
  if (!cluster || !fromPeerId || fromPeerId === node.peerId) return;
  if (typeof msg.term !== 'number') return;

  if (msg.term > cluster.currentTerm) {
    stepDown(node, msg.term);
    return;
  }
  if (msg.term < cluster.currentTerm || cluster.raftState !== 'leader') return;
  if (!cluster.nextIndex.has(fromPeerId)) return;

  // Quorum still answering: our leadership is confirmed.
  resetElectionTimeout(node);

  if (msg.success) {
    const lastLogIndex =
      typeof msg.lastLogIndex === 'number'
        ? Math.max(0, Math.min(msg.lastLogIndex, cluster.log.length - 1))
        : cluster.log.length - 1;
    cluster.matchIndex.set(
      fromPeerId,
      Math.max(cluster.matchIndex.get(fromPeerId) ?? -1, lastLogIndex)
    );
    cluster.nextIndex.set(
      fromPeerId,
      Math.min(lastLogIndex + 1, cluster.log.length)
    );
    advanceCommitIndex(node);
  } else {
    // Back up one entry per NACK; the next heartbeat retries.
    cluster.nextIndex.set(
      fromPeerId,
      Math.max(0, (cluster.nextIndex.get(fromPeerId) || 1) - 1)
    );
  }
}

/**
 * Handles an incoming RequestVote RPC (spec §5.2, follower/candidate side).
 */
export function handleRequestVote(node, msg, dataChannel) {
  const cluster = node.supernodeCluster;
  if (!cluster || typeof msg.term !== 'number') return;

  if (msg.term > cluster.currentTerm) {
    stepDown(node, msg.term);
  }
  if (msg.term < cluster.currentTerm) {
    sendRaftMessage(node, dataChannel, {
      type: MESSAGE_TYPES.RAFT_REQUEST_VOTE_RESPONSE,
      term: cluster.currentTerm,
      voteGranted: false
    });
    return;
  }

  let voteGranted = false;
  const myLastIndex = cluster.log.length - 1;
  const myLastTerm = myLastIndex >= 0 ? cluster.log[myLastIndex].term : 0;
  const logUpToDate =
    msg.lastLogTerm > myLastTerm ||
    (msg.lastLogTerm === myLastTerm && msg.lastLogIndex >= myLastIndex);

  if (
    (cluster.votedFor === null || cluster.votedFor === msg.candidateId) &&
    logUpToDate
  ) {
    voteGranted = true;
    cluster.votedFor = msg.candidateId;
    // Granting a vote is live Raft activity; a denial must not defer
    // our own election.
    resetElectionTimeout(node);
  }

  sendRaftMessage(node, dataChannel, {
    type: MESSAGE_TYPES.RAFT_REQUEST_VOTE_RESPONSE,
    term: cluster.currentTerm,
    voteGranted
  });
}

/**
 * Collects a vote response for the current election round (completeness
 * fix: votes are actually gathered here, driven by the DataChannel
 * dispatch). Late responses from an older round are ignored.
 */
export function handleRequestVoteResponse(node, msg, fromPeerId) {
  const cluster = node.supernodeCluster;
  if (!cluster || !fromPeerId || fromPeerId === node.peerId) return;
  if (typeof msg.term !== 'number') return;

  if (msg.term > cluster.currentTerm) {
    stepDown(node, msg.term);
    return;
  }
  if (cluster.raftState !== 'candidate' || !cluster._election) return;
  const election = cluster._election;
  if (msg.term !== election.term) return; // stale round
  if (!msg.voteGranted) return;

  election.votesGranted.add(fromPeerId);
  const majority = Math.floor(cluster.members.length / 2) + 1;
  if (election.votesGranted.size < majority) return;

  // We won the round.
  cluster.raftState = 'leader';
  cluster.leaderId = node.peerId;
  cluster._election = null;

  for (const member of cluster.members) {
    if (member === node.peerId) continue;
    cluster.nextIndex.set(member, cluster.log.length);
    cluster.matchIndex.set(member, -1);
  }
  stopHeartbeats(node);
  startHeartbeats(node);
  resetElectionTimeout(node);

  if (node.handlers.onSupernodeElected) {
    node.handlers.onSupernodeElected(node.peerId);
  }
}

/**
 * Leader-side log append + immediate replication (spec §5.2). Used for
 * cluster membership changes; the commit advances as responses arrive.
 */
export function replicateCommand(node, command) {
  const cluster = node.supernodeCluster;
  if (!cluster || cluster.raftState !== 'leader') return;

  cluster.log.push({
    term: cluster.currentTerm,
    index: cluster.log.length,
    command
  });

  for (const member of cluster.members) {
    if (member === node.peerId) continue;
    sendAppendEntriesToMember(node, member);
  }
}

/**
 * Applies committed log entries to the state machine (spec §5.2).
 */
export function applyCommittedEntries(node) {
  const cluster = node.supernodeCluster;
  if (!cluster) return;

  while (cluster.lastApplied < cluster.commitIndex) {
    cluster.lastApplied++;
    const entry = cluster.log[cluster.lastApplied];
    if (!entry) break;

    const command = entry.command || {};
    if (command.type === 'cluster_membership') {
      if (command.action === 'join') {
        if (!cluster.members.includes(command.peerId)) {
          cluster.members.push(command.peerId);
          cluster.nextIndex.set(command.peerId, cluster.log.length);
          cluster.matchIndex.set(command.peerId, -1);
        }
      } else if (command.action === 'leave') {
        cluster.members = cluster.members.filter(
          (id) => id !== command.peerId
        );
        cluster.nextIndex.delete(command.peerId);
        cluster.matchIndex.delete(command.peerId);
      }
    } else if (command.type === 'stream_metadata') {
      node._updateStreamMetadata?.(command.streamId, command.metadata);
    } else if (command.type === 'topology_change') {
      node._handleTopologyChange?.(command.change);
    }
  }
}

/**
 * Steps down to follower on seeing a newer term (completeness fix):
 * clears leader timers, forgets the stale election round, and re-arms the
 * election timer for follower duty.
 */
export function stepDown(node, term, leaderId = null) {
  const cluster = node.supernodeCluster;
  if (!cluster) return;

  if (typeof term === 'number' && term > cluster.currentTerm) {
    cluster.currentTerm = term;
    cluster.votedFor = null;
  }
  cluster._election = null;

  if (cluster.raftState !== 'follower') {
    cluster.raftState = 'follower';
    stopHeartbeats(node);
  }
  if (leaderId) cluster.leaderId = leaderId;

  resetElectionTimeout(node);
}

/**
 * Runs supernode election (spec §5.1): Meridian central-leader election
 * over the candidate peers; the winner self-initializes the Raft cluster
 * and announces it.
 */
export async function electSupernode(node, clusterPeers) {
  const result = await findCentralLeader(node, clusterPeers);

  if (result.leaderId === node.peerId) {
    node.isSupernode = true;
    node.clusterLeader = node.peerId;
    initRaftState(node, clusterPeers);
    setupMediaForwarding(node);
    broadcastToCluster(node, {
      type: MESSAGE_TYPES.SUPERNODE_ELECTED,
      supernodeId: node.peerId,
      clusterPeers
    });
    if (node.handlers.onSupernodeElected) {
      node.handlers.onSupernodeElected(node.peerId);
    }
  } else {
    node.isSupernode = false;
    node.clusterLeader = result.leaderId;
    // The elected supernode announces the cluster; its announcement (via
    // handleSupernodeElected) enrolls us as a Raft follower.
  }

  return result;
}

/**
 * Handles an incoming `supernode_elected` (spec §5.1): acknowledge the
 * leader; join as a Raft follower so the leader's heartbeats keep our
 * election timer fed (and its death triggers a real re-election).
 */
export function handleSupernodeElected(node, msg) {
  if (!msg || typeof msg.supernodeId !== 'string') return;
  node.clusterLeader = msg.supernodeId;

  if (msg.supernodeId === node.peerId) {
    // Our own election echoed back by a peer that also elected us: keep
    // any live Raft state, never re-initialize over it.
    if (!node.supernodeCluster) {
      node.isSupernode = true;
      initRaftState(node, msg.clusterPeers || []);
      setupMediaForwarding(node);
    }
  } else if (!node.supernodeCluster) {
    node.isSupernode = false;
    initRaftState(node, [...(msg.clusterPeers || []), msg.supernodeId], {
      initialState: 'follower',
      leaderId: msg.supernodeId
    });
  }

  if (node.handlers.onSupernodeElected) {
    node.handlers.onSupernodeElected(msg.supernodeId);
  }
}

/**
 * Broadcasts a message to all ring primaries (spec §5.1).
 */
export function broadcastToCluster(node, message) {
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

export function shutdownRaft(node) {
  const cluster = node.supernodeCluster;
  if (!cluster) return;
  stopHeartbeats(node);
  if (cluster.electionTimer) {
    clearTimeout(cluster.electionTimer);
    cluster.electionTimer = null;
  }
  cluster._election = null;
}

function sendRaftMessage(node, dataChannel, payload) {
  if (!dataChannel || dataChannel.readyState === 'closed') return;
  try {
    dataChannel.send(JSON.stringify(payload));
  } catch {
    // Channel may be dead; the sender's own timeout/retry covers it.
  }
}

function advanceCommitIndex(node) {
  const cluster = node.supernodeCluster;
  if (!cluster || cluster.raftState !== 'leader' || cluster.log.length === 0) {
    return;
  }

  const confirmed = [cluster.log.length - 1];
  for (const member of cluster.members) {
    if (member === node.peerId) continue;
    const match = cluster.matchIndex.get(member);
    if (typeof match === 'number') confirmed.push(match);
  }
  confirmed.sort((a, b) => b - a);

  const quorumIndex = confirmed[Math.floor(cluster.members.length / 2)];
  const entry = cluster.log[quorumIndex];
  // Only entries from our own term are commitable (Raft §5.4.2).
  if (entry && entry.term === cluster.currentTerm && quorumIndex > cluster.commitIndex) {
    cluster.commitIndex = quorumIndex;
    applyCommittedEntries(node);
  }
}