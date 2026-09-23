import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:flash_webrtc/flash_webrtc.dart';

import 'fakes.dart';

void main() {
  test('constructs a node with the documented ring layout', () {
    final node = MeridianNode(peerId: 'self');
    expect(node.peerId, 'self');
    expect(node.rings, hasLength(9));
    expect(node.knownPeers, isEmpty);
    expect(node.pendingConnections, isEmpty);
    expect(node.pendingQueries, isEmpty);
    expect(node.isSupernode, isFalse);

    // Ring 0 spans (0, innermost]; inner radius is 0, not 1.
    expect(node.rings.first.index, 0);
    expect(node.rings.first.innerRadiusMs, 0.0);
    expect(node.rings.first.outerRadiusMs, 1.0);
    expect(node.rings[1].innerRadiusMs, 1.0);
    expect(node.rings[1].outerRadiusMs, 2.0);
    expect(node.rings[8].innerRadiusMs, 128.0);
    // The outermost ring is unbounded above.
    expect(node.rings.last.outerRadiusMs, double.infinity);
  });

  test('calculateRingIndex places rtt and clamps to the outermost ring', () {
    final node = MeridianNode(peerId: 'self');
    final manager = node.ringManager;
    expect(manager.calculateRingIndex(1), 0);
    expect(manager.calculateRingIndex(2), 1);
    expect(manager.calculateRingIndex(4), 2);
    expect(manager.calculateRingIndex(1000), 8);
  });

  test('getRingBounds matches the ring radii', () {
    const config = MeridianConfig();
    expect(getRingBounds(0, config).inner, 0.0);
    expect(getRingBounds(0, config).outer, 1.0);
    expect(getRingBounds(1, config).inner, 1.0);
    expect(getRingBounds(3, config).outer, 8.0);
    expect(getRingBounds(8, config).outer, double.infinity);
  });

  test('message-type vocabulary matches the JS library byte-for-byte', () {
    expect(MeridianMessageTypes.all, hasLength(24));
    expect(MeridianMessageTypes.ping, 'ping');
    expect(MeridianMessageTypes.pong, 'pong');
    expect(MeridianMessageTypes.gossip, 'gossip');
    expect(MeridianMessageTypes.queryForward, 'query_forward');
    expect(MeridianMessageTypes.leaderQueryForward, 'leader_query_forward');
    expect(MeridianMessageTypes.constraintQueryForward,
        'constraint_query_forward');
    expect(MeridianMessageTypes.probeRequest, 'probe_request');
    expect(MeridianMessageTypes.probeResult, 'probe_result');
    expect(MeridianMessageTypes.probeRequestAvg, 'probe_request_avg');
    expect(MeridianMessageTypes.probeResultAvg, 'probe_result_avg');
    expect(MeridianMessageTypes.probeRequestConstraints,
        'probe_request_constraints');
    expect(MeridianMessageTypes.probeResultConstraints,
        'probe_result_constraints');
    expect(MeridianMessageTypes.queryResult, 'query_result');
    expect(MeridianMessageTypes.mediaOffer, 'media_offer');
    expect(MeridianMessageTypes.mediaAnswer, 'media_answer');
    expect(MeridianMessageTypes.forwardedStream, 'forwarded_stream');
    expect(MeridianMessageTypes.mediaClose, 'media_close');
    expect(MeridianMessageTypes.supernodeElected, 'supernode_elected');
    expect(MeridianMessageTypes.raftAppendEntries, 'raft_append_entries');
    expect(MeridianMessageTypes.raftAppendEntriesResponse,
        'raft_append_entries_response');
    expect(MeridianMessageTypes.raftRequestVote, 'raft_request_vote');
    expect(MeridianMessageTypes.raftRequestVoteResponse,
        'raft_request_vote_response');
    expect(MeridianMessageTypes.peerLeaving, 'peer_leaving');
    expect(MeridianMessageTypes.peerStatus, 'peer_status');
  });

  test('a dispatched pong correlates the pending RTT measurement', () async {
    final node = MeridianNode(peerId: 'self');
    final dc = FakeRTCDataChannel();
    final sent = <Map<String, dynamic>>[];
    dc.onSend = (message) async {
      sent.add(jsonDecode(message.text) as Map<String, dynamic>);
    };
    node.dcHandler.setupHandlers(dc, 'peer');

    // Measure: a ping goes out on the channel, unanswered.
    final measure = node.rttMeasurement.measureOverDataChannel(dc);
    expect(sent.single['type'], 'ping');
    expect(sent.single['id'], isA<String>());
    expect(sent.single['t'], isA<num>());

    // The pong arrives through the same dispatch (single onMessage) —
    // regression guard: an unwired pong case left every measure timing out.
    final ping = sent.single;
    node.dcHandler.dispatch(
      {'type': 'pong', 'id': ping['id'], 't': ping['t']},
      dc,
      'peer',
    );

    expect(await measure, isA<num>());
  });

  test('raft indices start before the first log entry', () {
    final cluster = SupernodeCluster(clusterId: 'c', members: ['a', 'b']);
    // "Nothing yet" is -1, not 0: 0 would pin the first entry out of reach.
    expect(cluster.commitIndex, -1);
    expect(cluster.lastApplied, -1);
    expect(cluster.log, isEmpty);
    expect(cluster.nextIndex['b'], 0);
    expect(cluster.matchIndex['b'], -1);
    expect(cluster.raftState, RaftState.follower);
  });

  test('connection pool evicts the least-recently-used entry at maxSize', () {
    final pool = ConnectionPool(maxSize: 2);
    final a = ConnectionPoolEntry(
      targetId: 'a',
      pc: FakeRTCPeerConnection(),
      dc: FakeRTCDataChannel(),
    );
    final b = ConnectionPoolEntry(
      targetId: 'b',
      pc: FakeRTCPeerConnection(),
      dc: FakeRTCDataChannel(),
    );
    pool.add(a);
    pool.add(b);
    expect(pool.find('a'), same(a));
    expect(pool.find('b'), same(b));

    // Touch a so b becomes the LRU entry, then overflow: b is evicted.
    a.lastUsed = DateTime.now();
    pool.add(ConnectionPoolEntry(
      targetId: 'c',
      pc: FakeRTCPeerConnection(),
      dc: FakeRTCDataChannel(),
    ));
    expect(pool.find('a'), isNotNull);
    expect(pool.find('b'), isNull);
    expect(pool.find('c'), isNotNull);
  });

  test('connection pool cleanup drops entries older than maxAge', () {
    final pool = ConnectionPool(maxSize: 5);
    final old = ConnectionPoolEntry(
      targetId: 'old',
      pc: FakeRTCPeerConnection(),
      dc: FakeRTCDataChannel(),
      lastUsed: DateTime.now().subtract(const Duration(hours: 1)),
    );
    pool.add(old);
    pool.cleanup();
    expect(pool.find('old'), isNull);
  });

  test('config defaults match the JS MERIDIAN_CONFIG', () {
    const config = MeridianConfig();
    expect(config.ringsPerNode, 9);
    expect(config.nodesPerRing, 8);
    expect(config.secondaryCandidates, 4);
    expect(config.innermostRingRadiusMs, 1.0);
    expect(config.ringMultiplicativeFactor, 2.0);
    expect(config.gossipPeriod, const Duration(seconds: 30));
    expect(config.ringReplacementPeriod, const Duration(seconds: 60));
    expect(config.maxEphemeralConnections, 10);
    expect(config.stunServers, ['stun:stun.l.google.com:19302']);
    expect(config.maxHops, 32);
    expect(config.ephemeralProbeTimeout, const Duration(seconds: 5));
    expect(config.queryTimeout, const Duration(seconds: 30));
  });

  test('ICE-server assembly collapses STUN and appends TURN', () {
    final servers = buildIceServers(const MeridianConfig(
      stunServers: ['stun:one', 'stun:two'],
      turnServers: [
        TurnServerConfig(url: 'turn:x', username: 'u', credential: 'c'),
      ],
    ));
    expect(servers, hasLength(2));
    expect(servers[0], {
      'urls': ['stun:one', 'stun:two']
    });
    expect(servers[1], {'urls': 'turn:x', 'username': 'u', 'credential': 'c'});
  });
}
