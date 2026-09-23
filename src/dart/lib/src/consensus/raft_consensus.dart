import 'dart:async';
import 'dart:math' as math;

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:logging/logging.dart';

import '../data_channel_handler.dart';
import '../message_types.dart';
import '../models/query_types.dart';
import '../models/raft_state.dart';
import '../overlay/meridian_node.dart';
import '../streaming/sfu_forwarder.dart';
import '../utils/crypto_utils.dart';

/// Raft consensus for the supernode cluster (spec §2.7, §4.6, §5.2), per
/// the JS implementation's raft.js. Log/array indices are 0-based and
/// "nothing yet" is -1 ([SupernodeCluster.commitIndex] /
/// [SupernodeCluster.lastApplied] / every matchIndex start at -1), while
/// every nextIndex starts at 0 — the start of the (empty) log.
///
/// Completeness fixes carried over from the JS twin: the leader role is
/// never confirmed at init, so an election timer is armed in EVERY state
/// (single-member clusters exempt) — if no Raft activity confirms
/// leadership, a re-election actually occurs.
class RaftConsensus {
  static final Logger _logger = Logger('flash_webrtc.raft');

  final MeridianNode node;

  // Timers and the live election round belong to the currently installed
  // cluster; every callback re-checks the identity of
  // [MeridianNode.supernodeCluster] so a replaced cluster's stale timers
  // are no-ops.
  Timer? _heartbeatTimer;
  Timer? _electionTimer;
  _ElectionRound? _election;

  /// Test seam: whether an election timeout is currently armed (armed in
  /// every non-single-member cluster state — leadership is unconfirmed
  /// until followers answer).
  bool get isElectionTimerArmed => _electionTimer != null;

  RaftConsensus(this.node);

  // --- Lifecycle ---

  /// Initializes Raft state for a supernode cluster (spec §2.7, §5.2).
  /// The electing supernode starts as leader; peers joining an existing
  /// cluster under a known leader start as followers.
  SupernodeCluster initRaftState(
    List<String> clusterMembers, {
    RaftState? initialState,
    String? leaderId,
  }) {
    shutdown();

    // Cluster membership always includes ourselves: quorum math
    // (floor(n / 2) + 1) only works over the full member list.
    final members = <String>{...clusterMembers, node.peerId}.toList();

    final cluster = SupernodeCluster(
      clusterId: uuidV4(),
      members: members,
      leaderId: leaderId ?? node.peerId,
      raftState: initialState ?? RaftState.leader,
    );

    for (final member in members) {
      if (member == node.peerId) continue;
      cluster.nextIndex[member] = 0;
      cluster.matchIndex[member] = -1;
    }

    node.supernodeCluster = cluster;

    if (cluster.raftState == RaftState.leader) {
      // Leadership is unconfirmed until followers answer heartbeats;
      // still, the elected node sends heartbeats from the start (spec
      // §5.2) and the election timer (armed below) forces a real
      // election if none are.
      startHeartbeats();
    }
    resetElectionTimeout();

    return cluster;
  }

  /// Re-arms the election timer. Every confirmed Raft activity (valid
  /// AppendEntries as follower, acknowledged AppendEntries as leader,
  /// GRANTED votes) feeds this; when it fires, a new election round
  /// starts.
  void resetElectionTimeout() {
    final cluster = node.supernodeCluster;
    if (cluster == null) return;

    _electionTimer?.cancel();
    _electionTimer = null;

    // A single-member cluster needs no consensus; leadership is trivial.
    if (cluster.members.length <= 1 || node.shuttingDown) return;

    _electionTimer = Timer(cluster.electionTimeout, () {
      if (node.shuttingDown || node.supernodeCluster != cluster) return;
      startElection();
    });
  }

  /// Starts an election round (spec §5.2): increment the term, vote for
  /// self, and request votes from every member over its DataChannel.
  void startElection() {
    final cluster = node.supernodeCluster;
    if (cluster == null || cluster.members.length <= 1 || node.shuttingDown) {
      return;
    }

    if (cluster.raftState != RaftState.candidate) stopHeartbeats();
    cluster.raftState = RaftState.candidate;
    cluster.currentTerm++;
    cluster.votedFor = node.peerId;

    final term = cluster.currentTerm;
    // Election-round guard: responses from any other term are ignored.
    _election = _ElectionRound(term, node.peerId);

    final lastLogIndex = cluster.log.length - 1;
    final lastLogTerm = lastLogIndex >= 0 ? cluster.log[lastLogIndex].term : 0;

    for (final member in cluster.members) {
      if (member == node.peerId) continue;
      final channel = node.knownPeers[member]?.dataChannel;
      if (channel == null) continue;
      _sendRaftMessage(channel, {
        'type': MeridianMessageTypes.raftRequestVote,
        'term': term,
        'candidateId': node.peerId,
        'lastLogIndex': lastLogIndex,
        'lastLogTerm': lastLogTerm,
      });
    }

    // A round that gathers no majority is retried when the timer
    // re-fires.
    resetElectionTimeout();
  }

  /// Heartbeat / log-replication loop (spec §5.2): one AppendEntries RPC
  /// per follower per tick, carrying everything from nextIndex onward.
  void startHeartbeats() {
    final cluster = node.supernodeCluster;
    if (cluster == null) return;
    stopHeartbeats();

    _heartbeatTimer = Timer.periodic(cluster.heartbeatInterval, (_) {
      if (node.supernodeCluster != cluster ||
          cluster.raftState != RaftState.leader) {
        stopHeartbeats();
        return;
      }
      for (final member in cluster.members) {
        if (member == node.peerId) continue;
        _sendAppendEntriesToMember(member);
      }
    });
  }

  void stopHeartbeats() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = null;
  }

  void _sendAppendEntriesToMember(String member) {
    final cluster = node.supernodeCluster;
    if (cluster == null || cluster.raftState != RaftState.leader) return;

    final channel = node.knownPeers[member]?.dataChannel;
    if (channel == null) return;

    final nextIdx = math.min(
      math.max(cluster.nextIndex[member] ?? 0, 0),
      cluster.log.length,
    );

    _sendRaftMessage(channel, {
      'type': MeridianMessageTypes.raftAppendEntries,
      'term': cluster.currentTerm,
      'leaderId': node.peerId,
      'prevLogIndex': nextIdx - 1,
      'prevLogTerm': nextIdx > 0 ? cluster.log[nextIdx - 1].term : 0,
      'entries': [
        for (final entry in cluster.log.sublist(nextIdx)) _entryToWire(entry),
      ],
      'leaderCommit': cluster.commitIndex,
    });
  }

  /// Leader-side log append + immediate replication (spec §5.2). Used for
  /// cluster membership changes; the commit advances as responses arrive.
  void replicateCommand(Map<String, dynamic> command) {
    final cluster = node.supernodeCluster;
    if (cluster == null || cluster.raftState != RaftState.leader) return;

    cluster.log.add(RaftLogEntry(
      term: cluster.currentTerm,
      index: cluster.log.length,
      command: command,
    ));

    for (final member in cluster.members) {
      if (member == node.peerId) continue;
      _sendAppendEntriesToMember(member);
    }
  }

  // --- RPC handlers ---

  /// Handles an incoming AppendEntries RPC (spec §5.2, follower side).
  void handleAppendEntries(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dataChannel,
  ) {
    final cluster = node.supernodeCluster;
    if (cluster == null) return;
    final term = _termOf(msg);
    if (term == null) return;

    if (term > cluster.currentTerm) {
      // A higher term always deposes us.
      stepDown(
          term, msg['leaderId'] is String ? msg['leaderId'] as String : null);
    }
    if (term < cluster.currentTerm) {
      _sendRaftMessage(dataChannel, {
        'type': MeridianMessageTypes.raftAppendEntriesResponse,
        'term': cluster.currentTerm,
        'success': false,
        'lastLogIndex': cluster.log.length - 1,
      });
      return;
    }

    final leaderId = msg['leaderId'];
    cluster.leaderId = leaderId is String ? leaderId : cluster.leaderId;
    if (cluster.raftState != RaftState.follower) {
      stopHeartbeats();
      cluster.raftState = RaftState.follower;
    }
    // Valid leader activity defers our own election.
    resetElectionTimeout();

    final prevLogIndex = (msg['prevLogIndex'] as num?)?.toInt();
    if (prevLogIndex != null && prevLogIndex >= 0) {
      final prevLogTerm = (msg['prevLogTerm'] as num?)?.toInt() ?? -1;
      if (prevLogIndex >= cluster.log.length ||
          cluster.log[prevLogIndex].term != prevLogTerm) {
        // Drop the conflicting suffix so the next retry (decremented
        // nextIndex on the leader) can succeed.
        cluster.log =
            cluster.log.sublist(0, math.min(prevLogIndex, cluster.log.length));
        _sendRaftMessage(dataChannel, {
          'type': MeridianMessageTypes.raftAppendEntriesResponse,
          'term': cluster.currentTerm,
          'success': false,
          'lastLogIndex': cluster.log.length - 1,
        });
        return;
      }
    }

    for (final raw in msg['entries'] as List? ?? const <dynamic>[]) {
      if (raw is! Map) continue;
      final index = (raw['index'] as num?)?.toInt();
      final entryTerm = (raw['term'] as num?)?.toInt();
      if (index == null || entryTerm == null || index < 0) continue;
      final entry = RaftLogEntry(
        term: entryTerm,
        index: index,
        command: raw['command'] is Map
            ? Map<String, dynamic>.from(raw['command'] as Map)
            : <String, dynamic>{},
      );
      if (index < cluster.log.length) {
        if (cluster.log[index].term != entry.term) {
          cluster.log = cluster.log.sublist(0, index);
          cluster.log.add(entry);
        }
      } else if (index == cluster.log.length) {
        cluster.log.add(entry);
      }
    }

    final leaderCommit = (msg['leaderCommit'] as num?)?.toInt();
    if (leaderCommit != null &&
        leaderCommit > cluster.commitIndex &&
        cluster.log.isNotEmpty) {
      cluster.commitIndex = math.max(
        cluster.commitIndex,
        math.min(leaderCommit, cluster.log.length - 1),
      );
    }
    applyCommittedEntries();

    _sendRaftMessage(dataChannel, {
      'type': MeridianMessageTypes.raftAppendEntriesResponse,
      'term': cluster.currentTerm,
      'success': true,
      'lastLogIndex': cluster.log.length - 1,
    });
  }

  /// Handles an AppendEntries response (leader side, spec §5.2): update
  /// nextIndex/matchIndex on success, back up on failure, and recompute
  /// the commit index by majority matchIndex (currentTerm entries only).
  void handleAppendEntriesResponse(
    Map<String, dynamic> msg,
    String fromPeerId,
  ) {
    final cluster = node.supernodeCluster;
    if (cluster == null || fromPeerId.isEmpty || fromPeerId == node.peerId) {
      return;
    }
    final term = _termOf(msg);
    if (term == null) return;

    if (term > cluster.currentTerm) {
      stepDown(term);
      return;
    }
    if (term < cluster.currentTerm || cluster.raftState != RaftState.leader) {
      return;
    }
    if (!cluster.nextIndex.containsKey(fromPeerId)) return;

    // Quorum still answering: our leadership is confirmed.
    resetElectionTimeout();

    if (msg['success'] == true) {
      final rawLastLogIndex = (msg['lastLogIndex'] as num?)?.toInt();
      final lastLogIndex = rawLastLogIndex != null
          ? math.max(0, math.min(rawLastLogIndex, cluster.log.length - 1))
          : cluster.log.length - 1;
      cluster.matchIndex[fromPeerId] = math.max(
        cluster.matchIndex[fromPeerId] ?? -1,
        lastLogIndex,
      );
      cluster.nextIndex[fromPeerId] =
          math.min(lastLogIndex + 1, cluster.log.length);
      advanceCommitIndex();
    } else {
      // Back up one entry per NACK; the next heartbeat retries.
      cluster.nextIndex[fromPeerId] =
          math.max(0, (cluster.nextIndex[fromPeerId] ?? 1) - 1);
    }
  }

  /// Handles an incoming RequestVote RPC (spec §5.2, follower side).
  void handleRequestVote(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dataChannel,
  ) {
    final cluster = node.supernodeCluster;
    if (cluster == null) return;
    final term = _termOf(msg);
    if (term == null) return;

    if (term > cluster.currentTerm) {
      stepDown(term);
    }
    if (term < cluster.currentTerm) {
      _sendRaftMessage(dataChannel, {
        'type': MeridianMessageTypes.raftRequestVoteResponse,
        'term': cluster.currentTerm,
        'voteGranted': false,
      });
      return;
    }

    var voteGranted = false;
    final myLastIndex = cluster.log.length - 1;
    final myLastTerm = myLastIndex >= 0 ? cluster.log[myLastIndex].term : 0;
    final msgLastLogIndex = (msg['lastLogIndex'] as num?)?.toInt() ?? -1;
    final msgLastLogTerm = (msg['lastLogTerm'] as num?)?.toInt() ?? -1;
    final logUpToDate = msgLastLogTerm > myLastTerm ||
        (msgLastLogTerm == myLastTerm && msgLastLogIndex >= myLastIndex);

    final candidateId = msg['candidateId'];
    if (candidateId is String &&
        (cluster.votedFor == null || cluster.votedFor == candidateId) &&
        logUpToDate) {
      voteGranted = true;
      cluster.votedFor = candidateId;
      // Granting a vote is live Raft activity; a denial must not defer
      // our own election.
      resetElectionTimeout();
    }

    _sendRaftMessage(dataChannel, {
      'type': MeridianMessageTypes.raftRequestVoteResponse,
      'term': cluster.currentTerm,
      'voteGranted': voteGranted,
    });
  }

  /// Collects a vote response for the current election round. Late
  /// responses from an older round are ignored.
  void handleRequestVoteResponse(Map<String, dynamic> msg, String fromPeerId) {
    final cluster = node.supernodeCluster;
    if (cluster == null || fromPeerId.isEmpty || fromPeerId == node.peerId) {
      return;
    }
    final term = _termOf(msg);
    if (term == null) return;

    if (term > cluster.currentTerm) {
      stepDown(term);
      return;
    }
    final election = _election;
    if (cluster.raftState != RaftState.candidate || election == null) return;
    if (term != election.term) return; // Stale round.
    if (msg['voteGranted'] != true) return;

    election.votesGranted.add(fromPeerId);
    final majority = cluster.members.length ~/ 2 + 1;
    if (election.votesGranted.length < majority) return;

    // We won the round.
    cluster.raftState = RaftState.leader;
    cluster.leaderId = node.peerId;
    _election = null;

    for (final member in cluster.members) {
      if (member == node.peerId) continue;
      cluster.nextIndex[member] = cluster.log.length;
      cluster.matchIndex[member] = -1;
    }
    stopHeartbeats();
    startHeartbeats();
    resetElectionTimeout();

    node.onSupernodeElected?.call(node.peerId);
  }

  /// Applies committed log entries to the state machine (spec §5.2).
  void applyCommittedEntries() {
    final cluster = node.supernodeCluster;
    if (cluster == null) return;

    while (cluster.lastApplied < cluster.commitIndex) {
      cluster.lastApplied++;
      if (cluster.lastApplied >= cluster.log.length) break;
      final entry = cluster.log[cluster.lastApplied];

      final command = entry.command;
      if (command['type'] == 'cluster_membership') {
        final peerId = command['peerId'];
        if (peerId is! String) continue;
        if (command['action'] == 'join') {
          if (!cluster.members.contains(peerId)) {
            cluster.members.add(peerId);
            cluster.nextIndex[peerId] = cluster.log.length;
            cluster.matchIndex[peerId] = -1;
          }
        } else if (command['action'] == 'leave') {
          cluster.members =
              cluster.members.where((id) => id != peerId).toList();
          cluster.nextIndex.remove(peerId);
          cluster.matchIndex.remove(peerId);
        }
      } else if (command['type'] == 'stream_metadata') {
        final streamId = command['streamId'];
        if (streamId is! String) continue;
        node.updateStreamMetadata?.call(
          streamId,
          command['metadata'] is Map
              ? Map<String, dynamic>.from(command['metadata'] as Map)
              : const <String, dynamic>{},
        );
      } else if (command['type'] == 'topology_change') {
        final change = command['change'];
        if (change is Map) {
          node.handleTopologyChange?.call(Map<String, dynamic>.from(change));
        }
      }
    }
  }

  /// Steps down to follower on seeing a newer term (completeness fix):
  /// clears leader timers, forgets the stale election round, and re-arms
  /// the election timer for follower duty.
  void stepDown(int term, [String? leaderId]) {
    final cluster = node.supernodeCluster;
    if (cluster == null) return;

    if (term > cluster.currentTerm) {
      cluster.currentTerm = term;
      cluster.votedFor = null;
    }
    _election = null;

    if (cluster.raftState != RaftState.follower) {
      _logger.fine('stepping down to follower at term $term');
      cluster.raftState = RaftState.follower;
      stopHeartbeats();
    }
    if (leaderId != null) cluster.leaderId = leaderId;

    resetElectionTimeout();
  }

  /// Runs supernode election (spec §5.1): Meridian central-leader
  /// election over the candidate peers; the winner self-initializes the
  /// Raft cluster and announces it.
  Future<QueryResult> electSupernode(List<String> clusterPeers) async {
    final result = await node.queryRouting.findCentralLeader(clusterPeers);

    if (result.leaderId == node.peerId) {
      node.isSupernode = true;
      node.clusterLeader = node.peerId;
      initRaftState(clusterPeers);
      SfuForwarder.setupMediaForwarding(node);
      broadcastToCluster({
        'type': MeridianMessageTypes.supernodeElected,
        'supernodeId': node.peerId,
        'clusterPeers': clusterPeers,
      });
      node.onSupernodeElected?.call(node.peerId);
    } else {
      node.isSupernode = false;
      node.clusterLeader = result.leaderId;
      // The elected supernode announces the cluster; its announcement
      // (via handleSupernodeElected) enrolls us as a Raft follower.
    }

    return result;
  }

  /// Handles an incoming `supernode_elected` (spec §5.1): acknowledge the
  /// leader; join as a Raft follower so the leader's heartbeats keep our
  /// election timer fed (and its death triggers a real re-election).
  void handleSupernodeElected(Map<String, dynamic> msg) {
    final supernodeId = msg['supernodeId'];
    if (supernodeId is! String) return;
    node.clusterLeader = supernodeId;

    if (supernodeId == node.peerId) {
      // Our own election echoed back by a peer that also elected us: keep
      // any live Raft state, never re-initialize over it.
      if (node.supernodeCluster == null) {
        node.isSupernode = true;
        initRaftState(
          (msg['clusterPeers'] as List?)?.cast<String>().toList() ??
              const <String>[],
        );
        SfuForwarder.setupMediaForwarding(node);
      }
    } else if (node.supernodeCluster == null) {
      node.isSupernode = false;
      initRaftState(
        [
          ...((msg['clusterPeers'] as List?)?.cast<String>() ??
              const <String>[]),
          supernodeId,
        ],
        initialState: RaftState.follower,
        leaderId: supernodeId,
      );
    }

    node.onSupernodeElected?.call(supernodeId);
  }

  /// Broadcasts [message] to all ring primaries (spec §5.1).
  void broadcastToCluster(Map<String, dynamic> message) {
    for (final ring in node.rings) {
      for (final member in ring.primaryMembers) {
        final channel = member.dataChannel;
        if (channel == null) continue;
        sendChannelMessage(channel, message);
      }
    }
  }

  /// Stops the heartbeat + election timers and forgets any live election
  /// round; called from [MeridianNode.dispose] and before re-init.
  void shutdown() {
    stopHeartbeats();
    _electionTimer?.cancel();
    _electionTimer = null;
    _election = null;
  }

  // --- Internals ---

  int? _termOf(Map<String, dynamic> msg) =>
      (msg['term'] is num) ? (msg['term'] as num).toInt() : null;

  void _sendRaftMessage(
      rtc.RTCDataChannel dataChannel, Map<String, dynamic> payload) {
    if (dataChannel.state == rtc.RTCDataChannelState.RTCDataChannelClosed) {
      return;
    }
    sendChannelMessage(dataChannel, payload);
  }

  Map<String, dynamic> _entryToWire(RaftLogEntry entry) => {
        'term': entry.term,
        'index': entry.index,
        'command': entry.command,
      };

  /// Leader-side commit advancement: quorum math over matchIndex
  /// INCLUDING self at log.length - 1; only entries from our own term are
  /// commitable (Raft §5.4.2).
  void advanceCommitIndex() {
    final cluster = node.supernodeCluster;
    if (cluster == null ||
        cluster.raftState != RaftState.leader ||
        cluster.log.isEmpty) {
      return;
    }

    final confirmed = <int>[cluster.log.length - 1];
    for (final member in cluster.members) {
      if (member == node.peerId) continue;
      final match = cluster.matchIndex[member];
      if (match != null) confirmed.add(match);
    }
    confirmed.sort((a, b) => b - a);

    final quorumIndex = confirmed[cluster.members.length ~/ 2];
    if (quorumIndex < 0 || quorumIndex >= cluster.log.length) return;
    final entry = cluster.log[quorumIndex];
    if (entry.term == cluster.currentTerm &&
        quorumIndex > cluster.commitIndex) {
      cluster.commitIndex = quorumIndex;
      applyCommittedEntries();
    }
  }
}

/// A live election round (term + votes granted), for response correlation.
class _ElectionRound {
  final int term;
  final Set<String> votesGranted;

  _ElectionRound(this.term, String selfVote) : votesGranted = {selfVote};
}
