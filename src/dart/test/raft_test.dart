import 'package:flutter_test/flutter_test.dart';
import 'package:flash_webrtc/flash_webrtc.dart';

import 'fakes.dart';

void main() {
  test('AppendEntries appplies entries and commits through leaderCommit',
      () async {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(
      ['leader'],
      initialState: RaftState.follower,
    );
    addTearDown(node.raftConsensus.shutdown);
    cluster.electionTimeout = const Duration(hours: 1);
    node.raftConsensus.resetElectionTimeout();

    final spyMetadata = <Map<String, dynamic>>[];
    node.updateStreamMetadata = (streamId, metadata) {
      spyMetadata.add({'streamId': streamId, 'metadata': metadata});
    };

    final dc = FakeRTCDataChannel();
    node.dcHandler.setupHandlers(dc, 'leader');

    node.raftConsensus.handleAppendEntries({
      'term': 1,
      'leaderId': 'leader',
      'prevLogIndex': -1,
      'prevLogTerm': 0,
      'entries': [
        {
          'term': 1,
          'index': 0,
          'command': {
            'type': 'stream_metadata',
            'streamId': 's1',
            'metadata': {'k': 'v'},
          },
        },
      ],
      'leaderCommit': 0,
    }, dc);

    // The entry is applied and the commit index clamps to the log.
    expect(cluster.commitIndex, 0);
    expect(cluster.lastApplied, 0);
    expect(spyMetadata, hasLength(1));
    expect(spyMetadata.single['streamId'], 's1');
    expect(spyMetadata.single['metadata'], {'k': 'v'});

    final responses = [
      for (final message in dc.sent) decoded(message),
    ].where((m) => m['type'] == 'raft_append_entries_response').toList();
    expect(responses, hasLength(1));
    expect(responses.single['term'], 1);
    expect(responses.single['success'], isTrue);
    expect(responses.single['lastLogIndex'], 0);
  });

  test('AppendEntries with an older term is rejected', () {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(
      ['leader'],
      initialState: RaftState.follower,
    );
    addTearDown(node.raftConsensus.shutdown);
    cluster.currentTerm = 5;

    final dc = FakeRTCDataChannel();
    node.dcHandler.setupHandlers(dc, 'leader');
    node.raftConsensus.handleAppendEntries({
      'term': 4,
      'leaderId': 'leader',
      'prevLogIndex': -1,
      'entries': [],
      'leaderCommit': -1,
    }, dc);

    final responses = [
      for (final message in dc.sent) decoded(message),
    ];
    expect(responses.single['success'], isFalse);
    expect(responses.single['term'], 5);
    // No step-down on a stale term.
    expect(cluster.raftState, RaftState.follower);
    expect(cluster.currentTerm, 5);
  });

  test('a higher term in AppendEntries steps us down', () async {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(['a', 'b']);
    addTearDown(node.raftConsensus.shutdown);
    cluster.currentTerm = 5;
    cluster.votedFor = 'f';
    expect(cluster.raftState, RaftState.leader);

    node.raftConsensus.handleAppendEntries({
      'term': 6,
      'leaderId': 'new-leader',
      'prevLogIndex': -1,
      'entries': [],
      'leaderCommit': -1,
    }, FakeRTCDataChannel());

    expect(cluster.raftState, RaftState.follower);
    expect(cluster.currentTerm, 6);
    expect(cluster.votedFor, isNull);
    expect(cluster.leaderId, 'new-leader');
  });

  test('log conflicts at prevLogIndex truncate the suffix and NACK', () async {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(
      ['leader'],
      initialState: RaftState.follower,
    );
    addTearDown(node.raftConsensus.shutdown);
    cluster.electionTimeout = const Duration(hours: 1);
    node.raftConsensus.resetElectionTimeout();
    cluster.log.addAll([
      const RaftLogEntry(term: 1, index: 0, command: {}),
      const RaftLogEntry(term: 1, index: 1, command: {}),
    ]);

    final dc = FakeRTCDataChannel();
    node.dcHandler.setupHandlers(dc, 'leader');
    node.raftConsensus.handleAppendEntries({
      'term': 1,
      'leaderId': 'leader',
      'prevLogIndex': 1,
      'prevLogTerm': 2,
      'entries': [],
      'leaderCommit': -1,
    }, dc);

    // The conflicting suffix is dropped so the leader's next retry
    // (with a decremented nextIndex) can succeed.
    expect(cluster.log, hasLength(1));
    final responses = [
      for (final message in dc.sent) decoded(message),
    ].where((m) => m['type'] == 'raft_append_entries_response').toList();
    expect(responses.single['success'], isFalse);
    expect(responses.single['lastLogIndex'], 0);
  });

  test('leaderCommit is clamped to our log length', () {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(
      ['leader'],
      initialState: RaftState.follower,
    );
    addTearDown(node.raftConsensus.shutdown);
    cluster.currentTerm = 1;

    node.raftConsensus.handleAppendEntries({
      'term': 1,
      'leaderId': 'leader',
      'prevLogIndex': -1,
      'entries': [
        {
          'term': 1,
          'index': 0,
          'command': {'action': 'noop'}
        },
      ],
      'leaderCommit': 10,
    }, FakeRTCDataChannel());

    expect(cluster.log, hasLength(1));
    expect(cluster.commitIndex, 0, reason: 'min(leaderCommit, log.length - 1)');
  });

  test('RequestVote rules: stale term denies, fresh log grants', () {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(
      ['a', 'b'],
      initialState: RaftState.follower,
    );
    addTearDown(node.raftConsensus.shutdown);
    cluster.currentTerm = 3;
    cluster.votedFor = 'someone-else';
    cluster.log.add(const RaftLogEntry(term: 3, index: 0, command: {}));

    final dc = FakeRTCDataChannel();
    node.dcHandler.setupHandlers(dc, 'candidate');

    // Stale term: denied, no state change.
    node.raftConsensus.handleRequestVote({
      'term': 2,
      'candidateId': 'c',
      'lastLogIndex': 5,
      'lastLogTerm': 3,
    }, dc);
    var responses = [
      for (final message in dc.sent) decoded(message),
    ];
    expect(responses.single['voteGranted'], isFalse);
    expect(responses.single['term'], 3);
    expect(cluster.votedFor, 'someone-else');
    expect(cluster.currentTerm, 3);

    // Same term, already voted for another: denied.
    node.raftConsensus.handleRequestVote({
      'term': 3,
      'candidateId': 'c',
      'lastLogIndex': 5,
      'lastLogTerm': 3,
    }, dc);
    responses = [for (final message in dc.sent) decoded(message)];
    expect(responses.last['voteGranted'], isFalse);

    // Log freshness: our last entry is more up to date.
    cluster.votedFor = null;
    node.raftConsensus.handleRequestVote({
      'term': 3,
      'candidateId': 'c',
      'lastLogIndex': 0,
      'lastLogTerm': 1,
    }, dc);
    responses = [for (final message in dc.sent) decoded(message)];
    expect(responses.last['voteGranted'], isFalse);

    // Up-to-date candidate at the same term: granted — and a grant is
    // live Raft activity, so our own election timer re-arms.
    node.raftConsensus.shutdown();
    expect(node.raftConsensus.isElectionTimerArmed, isFalse);
    node.raftConsensus.handleRequestVote({
      'term': 3,
      'candidateId': 'c',
      'lastLogIndex': 0,
      'lastLogTerm': 3,
    }, dc);
    expect(cluster.votedFor, 'c');
    expect(node.raftConsensus.isElectionTimerArmed, isTrue,
        reason: 'only a GRANTED vote re-arms the election timeout');
  });

  test('a vote denial does not arm the election timeout', () {
    final node = MeridianNode(peerId: 'f');
    final cluster = node.raftConsensus.initRaftState(
      ['a', 'b'],
      initialState: RaftState.follower,
    );
    addTearDown(node.raftConsensus.shutdown);
    cluster.electionTimeout = const Duration(hours: 1);
    cluster.currentTerm = 3;
    cluster.votedFor = 'other';
    node.raftConsensus.shutdown();
    expect(node.raftConsensus.isElectionTimerArmed, isFalse);

    final dc = FakeRTCDataChannel();
    node.dcHandler.setupHandlers(dc, 'candidate');
    node.raftConsensus.handleRequestVote({
      'term': 3,
      'candidateId': 'candidate',
      'lastLogIndex': 0,
      'lastLogTerm': 0,
    }, dc);

    expect(node.raftConsensus.isElectionTimerArmed, isFalse,
        reason: 'a denial must not defer our own election');
  });

  test('an election round gathers votes to a majority', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12);

    final clusterA = a.raftConsensus.initRaftState(['b']);
    final clusterB = b.raftConsensus.initRaftState(['a']);
    addTearDown(a.raftConsensus.shutdown);
    addTearDown(b.raftConsensus.shutdown);
    // Keep the (real) election timers from re-firing mid-test.
    clusterA.electionTimeout = const Duration(hours: 1);
    a.raftConsensus.resetElectionTimeout();
    clusterB.electionTimeout = const Duration(hours: 1);
    b.raftConsensus.resetElectionTimeout();

    var elected = <String>[];
    a.onSupernodeElected = (id) => elected.add(id);

    a.raftConsensus.startElection();
    await Future<void>.delayed(const Duration(milliseconds: 150));

    // Majority of 2 is 2: our own vote plus b's.
    expect(clusterA.raftState, RaftState.leader);
    expect(clusterA.leaderId, 'a');
    expect(clusterA.currentTerm, 1);
    expect(clusterB.raftState, RaftState.follower);
    expect(elected, ['a']);

    // The win starts heartbeats: b receives an AppendEntries within a
    // couple of ticks.
    await Future<void>.delayed(const Duration(milliseconds: 400));
    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final heartbeats = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'raft_append_entries') decoded(message),
    ];
    expect(heartbeats, isNotEmpty);
    expect(heartbeats.first['term'], 1);
    expect(heartbeats.first['leaderId'], 'a');
    expect(heartbeats.first['prevLogIndex'], -1);
    expect(heartbeats.first['leaderCommit'], -1);
  });

  test('AppendEntries responses advance the commit index by quorum math', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b', 'c']);
    addTearDown(node.raftConsensus.shutdown);
    cluster.currentTerm = 1;
    cluster.log.add(const RaftLogEntry(term: 1, index: 0, command: {}));
    expect(cluster.raftState, RaftState.leader);

    final spyMetadata = <String>[];
    node.updateStreamMetadata = (streamId, _) => spyMetadata.add(streamId);

    // c has replicated nothing; b confirms index 0. Quorum over
    // [0 (self), 0, -1] is index 0.
    node.raftConsensus.handleAppendEntriesResponse({
      'term': 1,
      'success': true,
      'lastLogIndex': 0,
    }, 'b');

    expect(cluster.matchIndex['b'], 0);
    expect(cluster.nextIndex['b'], 1);
    expect(cluster.commitIndex, 0);
    expect(cluster.lastApplied, 0);
    expect(spyMetadata, isEmpty);

    // A stale-term response is ignored outright.
    node.raftConsensus.handleAppendEntriesResponse(const {
      'term': 0,
      'success': true,
      'lastLogIndex': 2,
    }, 'c');
    expect(cluster.matchIndex['c'], -1);

    // A NACK backs nextIndex up one, floored at 0.
    node.raftConsensus.handleAppendEntriesResponse(const {
      'term': 1,
      'success': false,
    }, 'b');
    expect(cluster.nextIndex['b'], 0);
  });

  test('commit only advances for entries of the current term', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b', 'c']);
    addTearDown(node.raftConsensus.shutdown);
    cluster.currentTerm = 2;
    cluster.log.add(const RaftLogEntry(term: 1, index: 0, command: {}));

    // b confirms the old-term entry; commitIndex must not advance
    // (Raft §5.4.2: only currentTerm entries are commitable).
    node.raftConsensus.handleAppendEntriesResponse(const {
      'term': 2,
      'success': true,
      'lastLogIndex': 0,
    }, 'b');
    expect(cluster.commitIndex, -1);
    expect(cluster.lastApplied, -1);
  });

  test('replicateCommand appends at the next 0-based index and replicates', () {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12);
    final cluster = a.raftConsensus.initRaftState(['b']);
    addTearDown(a.raftConsensus.shutdown);
    expect(cluster.raftState, RaftState.leader);

    // Pretend b already replicated our two existing entries: the next
    // AppendEntries carries only the new one.
    cluster.log.addAll([
      const RaftLogEntry(term: 3, index: 0, command: {'x': 1}),
      const RaftLogEntry(term: 3, index: 1, command: {'y': 2}),
    ]);
    cluster.currentTerm = 3;
    cluster.commitIndex = 1;
    cluster.nextIndex['b'] = 2;

    final command = {'type': 'cluster_membership', 'action': 'join'};
    a.raftConsensus.replicateCommand(command);

    expect(cluster.log, hasLength(3));
    expect(cluster.log.last.term, 3);
    expect(cluster.log.last.index, 2);
    expect(cluster.log.last.command, same(command));

    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final appends = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'raft_append_entries') decoded(message),
    ];
    expect(appends, hasLength(1));
    final wire = appends.single;
    expect(wire['type'], 'raft_append_entries');
    expect(wire['term'], 3);
    expect(wire['leaderId'], 'a');
    expect(wire['prevLogIndex'], 1);
    expect(wire['prevLogTerm'], 3);
    expect(wire['leaderCommit'], 1);
    expect(wire['entries'], hasLength(1));
    expect(wire['entries'].first['term'], 3);
    expect(wire['entries'].first['index'], 2);
    expect(wire['entries'].first['command'], command);
  });

  test('applyCommittedEntries replays cluster_membership changes', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b']);
    addTearDown(node.raftConsensus.shutdown);

    cluster.log.addAll([
      const RaftLogEntry(
        term: 1,
        index: 0,
        command: {
          'type': 'cluster_membership',
          'action': 'join',
          'peerId': 'z'
        },
      ),
      const RaftLogEntry(
        term: 1,
        index: 1,
        command: {
          'type': 'cluster_membership',
          'action': 'leave',
          'peerId': 'b'
        },
      ),
    ]);
    cluster.commitIndex = 1;

    node.raftConsensus.applyCommittedEntries();

    expect(cluster.members, equals(['a', 'z']));
    expect(cluster.nextIndex['z'], 2);
    expect(cluster.matchIndex['z'], -1);
    expect(cluster.nextIndex.containsKey('b'), isFalse);
    expect(cluster.matchIndex.containsKey('b'), isFalse);
  });

  test('applyCommittedEntries drives the topology_change hook', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b']);
    addTearDown(node.raftConsensus.shutdown);

    final changes = <Map<String, dynamic>>[];
    node.handleTopologyChange = (change) => changes.add(change);

    cluster.log.add(const RaftLogEntry(
      term: 1,
      index: 0,
      command: {
        'type': 'topology_change',
        'change': {'ringIndex': 2},
      },
    ));
    cluster.commitIndex = 0;

    node.raftConsensus.applyCommittedEntries();
    expect(changes, [
      {'ringIndex': 2},
    ]);
  });

  test('stepDown forgets the live election round and re-arms the timer', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b', 'c']);
    addTearDown(node.raftConsensus.shutdown);
    expect(cluster.raftState, RaftState.leader);

    node.raftConsensus.stepDown(4, 'new-leader');
    expect(cluster.raftState, RaftState.follower);
    expect(cluster.currentTerm, 4);
    expect(cluster.votedFor, isNull);
    expect(node.raftConsensus.isElectionTimerArmed, isTrue,
        reason: 'a follower still needs its own election timer armed');
  });

  test('supernode_elected from another peer enrolls us as a follower', () {
    final node = MeridianNode(peerId: 'f');
    node.raftConsensus.handleSupernodeElected({
      'supernodeId': 'leader',
      'clusterPeers': ['other'],
    });

    expect(node.clusterLeader, 'leader');
    expect(node.isSupernode, isFalse);
    final cluster = node.supernodeCluster;
    expect(cluster, isNotNull);
    expect(cluster!.raftState, RaftState.follower);
    expect(cluster.leaderId, 'leader');
    // The known leader is part of the member set (quorum math).
    expect(cluster.members, containsAll(<String>['f', 'leader', 'other']));
    expect(cluster.nextIndex['leader'], 0);
    expect(cluster.matchIndex['leader'], -1);
    // Leadership is unconfirmed: our own election timer is armed.
    expect(node.raftConsensus.isElectionTimerArmed, isTrue);
  });

  test('an echoed self election never re-initializes live Raft state', () {
    final node = MeridianNode(peerId: 'a');
    node.isSupernode = true;
    final cluster = node.raftConsensus.initRaftState(['b']);

    node.raftConsensus.handleSupernodeElected({
      'supernodeId': 'a',
      'clusterPeers': ['b'],
    });

    expect(identical(node.supernodeCluster, cluster), isTrue,
        reason: 'self-echo keeps any live Raft state');
    expect(node.isSupernode, isTrue);
    expect(cluster.currentTerm, 0);
  });

  test('initRaftState arms the election timer in every state', () {
    final node = MeridianNode(peerId: 'a');
    node.raftConsensus.initRaftState(['b', 'c']);
    expect(node.raftConsensus.isElectionTimerArmed, isTrue);

    node.raftConsensus.initRaftState(
      ['b', 'c'],
      initialState: RaftState.follower,
      leaderId: 'leader',
    );
    expect(node.raftConsensus.isElectionTimerArmed, isTrue,
        reason: 'leadership is unconfirmed until followers answer');

    addTearDown(node.raftConsensus.shutdown);
  });

  test('shutdownRaft cancels both timers', () {
    final node = MeridianNode(peerId: 'a');
    node.raftConsensus.initRaftState(['b', 'c']);
    expect(node.raftConsensus.isElectionTimerArmed, isTrue);

    node.raftConsensus.shutdown();
    expect(node.raftConsensus.isElectionTimerArmed, isFalse);
  });

  test('RequestVote carries the JS wire field names', () {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12);
    final cluster = a.raftConsensus.initRaftState(['b']);
    addTearDown(a.raftConsensus.shutdown);
    cluster.electionTimeout = const Duration(hours: 1);
    a.raftConsensus.resetElectionTimeout();
    cluster.log.addAll([
      const RaftLogEntry(term: 3, index: 0, command: {'x': 1}),
      const RaftLogEntry(term: 4, index: 1, command: {'y': 2}),
    ]);

    a.raftConsensus.startElection();

    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final voteRequests = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'raft_request_vote') decoded(message),
    ];
    expect(voteRequests, hasLength(1));
    final wire = voteRequests.single;
    expect(wire['term'], 1);
    expect(wire['candidateId'], 'a');
    expect(wire['lastLogIndex'], 1);
    expect(wire['lastLogTerm'], 4);
  });
}
