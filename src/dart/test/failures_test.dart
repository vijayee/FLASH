import 'package:flutter_test/flutter_test.dart';
import 'package:meridian_webrtc/meridian_webrtc.dart';
import 'package:meridian_webrtc/src/overlay/failures.dart';

import 'fakes.dart';

void main() {
  test(
      'pruneStalePeers keeps a peer silent for exactly three gossip '
      'periods', () {
    final node = MeridianNode(peerId: 'a');
    final config = node.config;
    node.knownPeers['p'] = KnownPeer(
      peerId: 'p',
      dataChannel: FakeRTCDataChannel(),
      lastSeen: DateTime.now()
          .subtract(config.gossipPeriod * 3)
          .add(const Duration(seconds: 1)),
      status: PeerStatus.connected,
    );

    final disconnected = <String>[];
    node.onPeerDisconnected = (peerId) => disconnected.add(peerId);

    pruneStalePeers(node);

    expect(disconnected, isEmpty,
        reason: 'the cutoff is strictly greater-than');
    expect(node.knownPeers['p']!.status, PeerStatus.connected);
  });

  test(
      'pruneStalePeers treats a peer silent past three gossip periods '
      'as failed', () {
    final node = MeridianNode(peerId: 'a');
    final config = node.config;
    node.knownPeers['p'] = KnownPeer(
      peerId: 'p',
      dataChannel: FakeRTCDataChannel(),
      lastSeen: DateTime.now()
          .subtract(config.gossipPeriod * 3)
          .subtract(const Duration(milliseconds: 1)),
      status: PeerStatus.connected,
    );

    final disconnected = <String>[];
    node.onPeerDisconnected = (peerId) => disconnected.add(peerId);

    pruneStalePeers(node);

    expect(disconnected, ['p']);
    expect(node.knownPeers['p']!.status, PeerStatus.failed);
    expect(node.knownPeers['p']!.dataChannel, isNull);
  });

  test(
      'pruneStalePeers ignores disconnected peers regardless of '
      'lastSeen', () {
    final node = MeridianNode(peerId: 'a');
    node.knownPeers['p'] = KnownPeer(
      peerId: 'p',
      dataChannel: null,
      lastSeen: DateTime.now().subtract(const Duration(days: 1)),
      status: PeerStatus.connected,
    );

    final disconnected = <String>[];
    node.onPeerDisconnected = (peerId) => disconnected.add(peerId);

    pruneStalePeers(node);
    expect(disconnected, isEmpty,
        reason: 'nothing is wired to a channel we could declare dead');
  });

  test(
      'a failed supernode triggers a re-election the losing node '
      'acknowledges', () async {
    // Deterministic timing recipe (as in query_routing_test.dart): a
    // shared FakeClock scripts every ping->pong measurement at exactly
    // 6ms, so a's live self-side measurement lands exactly on b's
    // enrolled 6ms — inside the [rtt/2, rtt*2] candidate window no
    // matter how loaded the test run is — and b is always probed.
    final clock = FakeClock(6);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    connectPeers(a, b, rttMs: 6);

    // The failed peer was our cluster leader; we are not a supernode.
    a.clusterLeader = 'dead-leader';
    a.knownPeers['dead-leader'] = KnownPeer(peerId: 'dead-leader');

    a.handlePeerFailure('dead-leader');

    // b probes itself (0ms average), so the central-leader query
    // forwards and b wins: we acknowledge the new leader.
    final elected = <String>[];
    a.onSupernodeElected = (id) => elected.add(id);

    var checked = false;
    final deadline = DateTime.now().add(const Duration(seconds: 3));
    while (DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
      if (a.clusterLeader != null && a.clusterLeader != 'dead-leader') {
        checked = true;
        break;
      }
    }
    expect(checked, isTrue, reason: 'the recovery query resolves fast');
    expect(a.clusterLeader, 'b');
    expect(a.isSupernode, isFalse);
    expect(elected, isEmpty,
        reason: 'only the winner fires onSupernodeElected for itself');
  });

  test('a failed supernode re-election can be won by this node', () async {
    // The only other known peer sits far outside a's candidate window
    // (its enrolled rtt of 100ms lands in ring 7 while a measures ~24ms
    // and checks rings 4-6): nobody is probed, so a answers as the
    // central leader itself - deterministically.
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 1000, delay: const Duration(milliseconds: 2));

    a.clusterLeader = 'dead-leader';
    a.knownPeers['dead-leader'] = KnownPeer(peerId: 'dead-leader');

    var elected = <String>[];
    a.onSupernodeElected = (id) => elected.add(id);

    a.handlePeerFailure('dead-leader');

    final deadline = DateTime.now().add(const Duration(seconds: 3));
    while (DateTime.now().isBefore(deadline) && !a.isSupernode) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }

    expect(a.isSupernode, isTrue);
    expect(a.clusterLeader, 'a');
    expect(a.mediaForwarding, isTrue,
        reason: 'every win path activates SFU forwarding');
    expect(elected, ['a']);

    // The winner announces itself like §5.1 does, so peers converge.
    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final announcements = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'supernode_elected') decoded(message),
    ];
    expect(announcements, hasLength(1));
    expect(announcements.single['supernodeId'], 'a');
    expect(announcements.single['clusterPeers'], ['b']);

    // The re-election winner self-initialized the Raft cluster.
    final cluster = a.supernodeCluster;
    expect(cluster, isNotNull);
    expect(cluster!.raftState, RaftState.leader);
    expect(cluster.members, containsAll(['a', 'b']));
  });

  test('a failed leader with no remaining candidates is forgotten', () async {
    final a = MeridianNode(peerId: 'a');
    a.clusterLeader = 'dead-leader';
    // Only the failed peer is known.

    a.handlePeerFailure('dead-leader');
    // Give the (unawaited) recovery a moment; nothing may throw.
    await Future<void>.delayed(const Duration(milliseconds: 10));

    expect(a.clusterLeader, isNull, reason: 'nobody left to elect over');
    expect(a.isSupernode, isFalse);
    expect(a.supernodeCluster, isNull);
  });

  test('removeRaftClusterMember drops the peer and replicates the leave', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b', 'c']);
    addTearDown(node.raftConsensus.shutdown);
    node.isSupernode = true;

    removeRaftClusterMember(node, 'b');

    expect(cluster.members, unorderedEquals(['a', 'c']));
    expect(cluster.nextIndex.containsKey('b'), isFalse);
    expect(cluster.matchIndex.containsKey('b'), isFalse);
    expect(cluster.log, hasLength(1));
    expect(cluster.log.single.command, {
      'type': 'cluster_membership',
      'action': 'leave',
      'peerId': 'b',
    });
  });

  test('removeRaftClusterMember is inert for non-supernodes', () {
    final node = MeridianNode(peerId: 'a');
    final cluster = node.raftConsensus.initRaftState(['b', 'c']);
    addTearDown(node.raftConsensus.shutdown);
    node.isSupernode = false;

    removeRaftClusterMember(node, 'b');

    expect(cluster.members, containsAll(['b', 'c']));
    expect(cluster.log, isEmpty);
  });

  test(
      'a failed streaming partner is torn down locally before the '
      'closest-peer query runs', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 6, delay: const Duration(milliseconds: 2));

    final deadTrack = FakeMediaStreamTrack();
    final deadStream = FakeMediaStream('from-dead')..tracks.add(deadTrack);
    a.activeStreams['dead'] = deadStream;
    // The failed peer keeps its (piped) channel here: the spec's
    // replacement queries the closest node to the failed id, which —
    // while the channel lives — is the failed id itself (it probes
    // itself at 0ms). The replacement guard (replacement != failedPeerId)
    // therefore skips the re-establishment, but the dead partner's
    // tracks are stopped regardless.
    var factoryUsed = false;
    a.mediaConnectionFactory = () async {
      factoryUsed = true;
      return FakeMediaPeerConnection();
    };

    await replaceStreamingPartner(a, 'dead');

    expect((deadStream.tracks.single as FakeMediaStreamTrack).stopped, isTrue,
        reason: 'the failed partner stream is dead regardless');
    expect(a.activeStreams, isEmpty);
    expect(factoryUsed, isFalse,
        reason: 'closest(dead) == dead, so nothing replaces it yet');
  });

  test('handlePeerFailure never delays the app notification', () {
    final a = MeridianNode(peerId: 'a');
    // A leader failure would start a 30s central-leader query; the
    // notification must still fire synchronously.
    a.clusterLeader = 'p';

    var notified = false;
    a.onPeerDisconnected = (peerId) => notified = true;

    a.handlePeerFailure('p');
    expect(notified, isTrue, reason: 'recovery runs without awaiting');
  });
}
