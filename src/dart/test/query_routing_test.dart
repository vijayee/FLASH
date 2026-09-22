import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:meridian_webrtc/meridian_webrtc.dart';

import 'fakes.dart';

// Timing recipe for these tests: the piped channel is zero-delay, while
// each node reads a FakeClock whose per-read step scripts the RTT a
// ping->pong exchange measures (exactly [stepMs]). Live measurements and
// enrolled RTTs are therefore both deterministic under the [rtt/2, rtt*2]
// candidate window, no matter how loaded the `flutter test` run is.

void main() {
  test('connectPeers enrolls both sides into ring 0 at 0.5ms', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b);

    expect(a.knownPeers['b']!.status, PeerStatus.connected);
    final member = a.ringMember('b');
    expect(member, isNotNull);
    expect(member!.rttMs, 0.5);
    expect(a.ringManager.calculateRingIndex(0.5), 0);
    expect(b.knownPeers['a']!.dataChannel, isNotNull);
  });

  test('probe_request carries the scripted wire shape', () async {
    final clock = FakeClock(0.75);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    connectPeers(a, b);

    final resultFuture = a.findClosestNode('b');
    // Everything is zero-delay, so the exchange settles within the first
    // event-loop turns; give it a beat before reading the wire.
    await Future<void>.delayed(const Duration(milliseconds: 120));

    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final probes = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'probe_request') decoded(message),
    ];
    expect(probes, hasLength(1));
    final probe = probes.single;
    expect(probe['probeId'], isA<String>());
    expect(probe['target'], 'b');
    expect(probe['targetType'], 'peer');
    expect(probe['queryId'], isA<String>());

    // The query_forward that follows the (self-0) probe result must carry
    // the FULL query - and never the local requesterDc channel.
    await expectLater(resultFuture, completes);
    final forwards = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'query_forward') decoded(message),
    ];
    expect(forwards, hasLength(1));
    final query = forwards.single['query'] as Map<String, dynamic>;
    expect(query['queryId'], isA<String>());
    expect(query['type'], 'closest_node');
    expect(query['target'], 'b');
    expect(query['targetType'], 'peer');
    expect(query['hopCount'], 1);
    expect(query['originator'], 'a');
    expect(query['satisfyingPeers'], isEmpty);
    expect(query.containsKey('requesterDc'), isFalse,
        reason: 'a live channel cannot survive JSON round-trips');
  });

  test('originator resolves a forwarded closest-node query', () async {
    final clock = FakeClock(0.75);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    connectPeers(a, b);

    // b probes itself (target == its own id -> 0ms), so a forwards and b
    // answers as the closest node; the result travels back over the
    // channel the forward arrived on.
    final result = await a.findClosestNode('b');

    expect(result.queryId, isA<String>());
    expect(result.closestPeerId, 'b');
    expect(result.closestRttMs, 0);
    expect(result.hopCount, 1);
    expect(result.error, isNull);
  });

  test('max-hops responses travel back over the arriving channel', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b);

    final bDc = b.ringMember('a')!.dataChannel! as FakeRTCDataChannel;
    b.dcHandler.dispatch({
      'type': 'query_forward',
      'query': {
        'queryId': 'q1',
        'type': 'closest_node',
        'target': 'a',
        'targetType': 'peer',
        'hopCount': 33,
        'originator': 'c',
      },
    }, bDc, 'a');

    final replies = [
      for (final message in bDc.sent) decoded(message),
    ].where((m) => m['type'] == 'query_result').toList();
    expect(replies, hasLength(1));
    expect(replies.single['queryId'], 'q1');
    expect(replies.single['error'], 'Max hops exceeded');
  });

  test('query timeout rejects and drains the pending entry', () async {
    final a = MeridianNode(
        peerId: 'a',
        config: const MeridianConfig(
          queryTimeout: Duration(milliseconds: 100),
        ));
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b);
    // Black-hole b's replies: a's ping/pong and b's probe_result never
    // arrive, so the (much shorter) query timeout fires.
    for (final ring in b.rings) {
      for (final member in [
        ...ring.primaryMembers,
        ...ring.secondaryMembers,
      ]) {
        (member.dataChannel as FakeRTCDataChannel).onSend = null;
      }
    }

    await expectLater(
      a.findClosestNode('b'),
      throwsA(isA<TimeoutException>()),
    );
    expect(a.pendingQueries, isEmpty);
    // Never hang past the (short) configured timeout.
  }, timeout: const Timeout(Duration(seconds: 5)));

  test('probe_result correlates by probeId, falling back to senderId',
      () async {
    final clock = FakeClock(5);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    // Enroll b mid-window relative to a's scripted 5ms own measurement:
    // a measures b at exactly 5ms, so 8ms sits inside [rtt/2, rtt*2] and
    // is probed (a 0.5ms enrolled RTT would fall outside).
    connectPeers(a, b, rttMs: 8);
    // Black-hole ONLY b's real probe_result (probeId echoed): the
    // ping/pong for a's own measurement still pipes through, but its
    // probeId-less probe_result a dispatches below must correlate via
    // the senderId fallback.
    (b.ringMember('a')!.dataChannel! as FakeRTCDataChannel).onSend =
        (message) async {
      if (decoded(message)['type'] == 'probe_result') return;
      await deliver(a.ringMember('b')!.dataChannel! as FakeRTCDataChannel,
          message, Duration.zero);
    };

    final resultFuture = a.findClosestNode('b');
    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    // Wait until a's probe_request is out (polling: the scripted
    // measurement settles in the first event-loop turns), then answer
    // without echoing the probeId: the asker falls back to the sender's
    // pending probe.
    Map<String, dynamic>? probe;
    final deadline = DateTime.now().add(const Duration(seconds: 3));
    while (probe == null && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
      for (final message in aDc.sent) {
        final wire = decoded(message);
        if (wire['type'] == 'probe_request') {
          probe = wire;
          break;
        }
      }
    }
    final sentTypes = [
      for (final m in aDc.sent) decoded(m)['type'] as String? ?? '?'
    ];
    expect(probe, isNotNull, reason: 'wire sent: $sentTypes');
    a.dcHandler.dispatch({
      'type': 'probe_result',
      'rttMs': 0.5,
      'queryId': probe!['queryId'],
    }, aDc, 'b');

    // The fallback-correlated outcome (0.5ms) beats our own scripted 5ms
    // measurement, so the query forwards to b and resolves with it.
    final result = await resultFuture;
    expect(result.closestPeerId, 'b');
    expect(result.hopCount, 1);
  });

  test('leader query routes by average RTT over probe_request_avg', () async {
    final clock = FakeClock(0.75);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    connectPeers(a, b);

    final result = await a.findCentralLeader(['b']);

    // b probes itself (0ms average), so a forwards and b wins.
    expect(result.leaderId, 'b');
    expect(result.avgRttMs, 0);
    expect(result.hopCount, 1);

    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final requests = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'probe_request_avg') decoded(message),
    ];
    expect(requests, hasLength(1));
    expect(requests.single['probeId'], isA<String>());
    expect(requests.single['targets'], ['b']);
    expect(requests.single['queryId'], isA<String>());
  });

  test('constraint queries accumulate satisfying peers', () async {
    final clock = FakeClock(0.75);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    // The scripted 0.75ms against the 100ms budget satisfies all
    // constraints; b (enrolled at 80ms, ring 7) is inside the
    // [maxLatency/2, maxLatency*2] candidate window and its probe outcome
    // is also satisfying - both accumulate.
    connectPeers(a, b, rttMs: 80);

    final result = await a.findNodesSatisfyingConstraints([
      const Constraint(target: 'b', maxLatencyMs: 100),
    ]);

    expect(result.satisfyingPeers, ['a', 'b']);
    expect(result.hopCount, 0);
    expect(result.error, isNull);
  });

  test(
      'probe_request_constraints responders report satisfiesAll and '
      'totalDistance', () async {
    final clock = FakeClock(5);
    final a = MeridianNode(peerId: 'a', clock: clock.read);
    final b = MeridianNode(peerId: 'b', clock: clock.read);
    connectPeers(a, b);

    final bDc = b.ringMember('a')!.dataChannel! as FakeRTCDataChannel;
    b.dcHandler.dispatch({
      'type': 'probe_request_constraints',
      'probeId': 'p1',
      'constraints': [
        {'target': 'a', 'maxLatencyMs': 1},
        {'target': 'b', 'maxLatencyMs': 100},
      ],
      'queryId': 'q1',
    }, bDc, 'a');
    await Future<void>.delayed(const Duration(milliseconds: 120));

    final results = [
      for (final message in bDc.sent)
        if (decoded(message)['type'] == 'probe_result_constraints')
          decoded(message),
    ];
    expect(results, hasLength(1));
    expect(results.single['probeId'], 'p1');
    expect(results.single['satisfiesAll'], isFalse,
        reason: 'the 1ms budget cannot absorb the scripted 5ms RTT');
    final totalDistance = (results.single['totalDistance'] as num).toDouble();
    expect(totalDistance, greaterThan(2));
    expect(totalDistance, lessThan(9));
    expect(results.single['queryId'], 'q1');
  });
}
