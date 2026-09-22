import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'package:collection/collection.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:logging/logging.dart';

import '../data_channel_handler.dart';
import '../message_types.dart';
import '../models/query_types.dart';
import '../models/ring.dart';
import '../utils/crypto_utils.dart';
import 'meridian_node.dart';

// Spec §3.8: respond as soon as this many satisfying peers are known.
const int _earlyResponsePeerCount = 5;

// Bound on hop-by-hop query_result return routes kept for in-flight queries
// whose originator is upstream of us (entries are deleted when the result
// passes through; the cap stops unbounded growth for queries that never do).
const int _maxQueryBackRoutes = 200;

/// One locally originated query awaiting its result: the Completer lives in
/// [MeridianNode.pendingQueries], its timeout here (same queryId key).
class _PendingQuery {
  final Completer<QueryResult> completer;
  final Timer timer;
  _PendingQuery(this.completer, this.timer);
}

/// A one-shot probe-result listener (spec §7.2). flutter_webrtc
/// DataChannels expose a single assignable `onMessage`, so the per-call
/// listeners of the JS implementation ride the channel's installed
/// dispatch instead: pending entries live in [QueryRouting._pendingProbes]
/// and correlate the result by `probeId`. Cleanup means dropping the entry
/// and cancelling its timer.
class _PendingProbe {
  final String probeId;
  final String peerId;
  final String resultType;
  final Completer<_ProbeOutcome> completer;
  late final Timer timer;
  bool settled = false;

  _PendingProbe({
    required this.probeId,
    required this.peerId,
    required this.resultType,
    required this.completer,
  });

  void settle(_ProbeOutcome outcome) {
    if (settled) return;
    settled = true;
    timer.cancel();
    if (!completer.isCompleted) completer.complete(outcome);
  }

  void fail(Object error) {
    if (settled) return;
    settled = true;
    timer.cancel();
    if (!completer.isCompleted) completer.completeError(error);
  }
}

/// The outcome of one probe exchange with a ring member.
class _ProbeOutcome {
  final String peerId;
  final double? rttMs;
  final double? avgRttMs;
  final bool? satisfiesAll;
  final double? totalDistance;

  const _ProbeOutcome({
    required this.peerId,
    this.rttMs,
    this.avgRttMs,
    this.satisfiesAll,
    this.totalDistance,
  });
}

/// A single (target, rtt, maxLatency) measurement of one constraint.
typedef _ConstraintMeasurement = ({
  String target,
  double rtt,
  double maxLatency,
});

/// Query routing over the overlay rings (spec §3.6-§3.8): closest-node,
/// central-leader and multi-constraint families, routed hop-by-hop with
/// `probe_request` exchanges against ring primaries.
class QueryRouting {
  static final Logger _logger = Logger('meridian_webrtc.query');

  final MeridianNode node;

  // queryId -> timeout of a locally originated query.
  final Map<String, Timer> _queryTimers = {};

  // probeId -> pending one-shot probe result listener.
  final Map<String, _PendingProbe> _pendingProbes = {};

  QueryRouting(this.node);

  // --- Entry points (spec §3.6-§3.8) ---

  /// Entry point (spec §3.6): find the peer closest to [target] by
  /// starting a multi-hop query rooted at this node. The pending promise
  /// is correlated by `queryId` through [MeridianNode.pendingQueries].
  Future<QueryResult> findClosestNode(String target, {String? targetType}) {
    final queryId = uuidV4();
    final completer = Completer<QueryResult>();
    final pending = _PendingQuery(
        completer,
        _queryTimeoutTimer(
          queryId,
          completer,
          'Query timeout',
        ));
    _queryTimers[queryId] = pending.timer;
    node.pendingQueries[queryId] = completer;

    unawaited(
      routeQuery(Query(
        queryId: queryId,
        type: QueryType.closestNode,
        target: target,
        targetType: targetType ?? 'peer',
        hopCount: 0,
        originator: node.peerId,
      )).catchError((_) {}),
    );

    return completer.future;
  }

  /// Entry point (spec §3.7): find the peer minimizing AVERAGE latency to
  /// [peerIds].
  Future<QueryResult> findCentralLeader(List<String> peerIds) {
    final queryId = uuidV4();
    final completer = Completer<QueryResult>();
    _queryTimers[queryId] = _queryTimeoutTimer(
      queryId,
      completer,
      'Leader election timeout',
    );
    node.pendingQueries[queryId] = completer;

    unawaited(
      routeLeaderQuery(Query(
        queryId: queryId,
        type: QueryType.leaderElection,
        targets: [...peerIds],
        hopCount: 0,
        originator: node.peerId,
      )).catchError((_) {}),
    );

    return completer.future;
  }

  /// Entry point (spec §3.8): find nodes satisfying all [constraints].
  Future<QueryResult> findNodesSatisfyingConstraints(
    List<Constraint> constraints,
  ) {
    final queryId = uuidV4();
    final completer = Completer<QueryResult>();
    _queryTimers[queryId] = _queryTimeoutTimer(
      queryId,
      completer,
      'Multi-constraint query timeout',
    );
    node.pendingQueries[queryId] = completer;

    unawaited(
      routeConstraintQuery(Query(
        queryId: queryId,
        type: QueryType.multiConstraint,
        constraints: [...constraints],
        hopCount: 0,
        originator: node.peerId,
      )).catchError((_) {}),
    );

    return completer.future;
  }

  /// Registers the query timeout for a locally originated query: on fire,
  /// the pending entry is dropped and the caller's future rejects.
  Timer _queryTimeoutTimer(
    String queryId,
    Completer<QueryResult> completer,
    String message,
  ) {
    return Timer(node.config.queryTimeout, () {
      node.pendingQueries.remove(queryId);
      _queryTimers.remove(queryId);
      if (!completer.isCompleted) {
        completer.completeError(TimeoutException(message));
      }
    });
  }

  // --- Routing (one hop each) ---

  /// Routes one hop of a closest-node query (spec §3.6).
  Future<void> routeQuery(Query query) async {
    try {
      if (query.hopCount > node.config.maxHops) {
        _respondToQuery(query, _errorResult(query, 'Max hops exceeded'));
        return;
      }

      final target = query.target;
      if (target is! String) return;
      final myRtt =
          await _measureRttToTarget(target, query.targetType ?? 'peer');

      final peersToQuery = _collectCandidates(
        myRtt,
        node.ringManager.calculateRingIndex(myRtt),
      );

      if (peersToQuery.isEmpty) {
        // No peers worth querying — we are the closest known node.
        _respondToQuery(
          query,
          QueryResult(
            queryId: query.queryId,
            closestPeerId: node.peerId,
            closestRttMs: myRtt,
            hopCount: query.hopCount,
          ),
        );
        return;
      }

      final outcomes = await _allSettled([
        for (final member in peersToQuery)
          _askPeer(
            member,
            {
              'type': MeridianMessageTypes.probeRequest,
              'target': query.target,
              'targetType': query.targetType,
              'queryId': query.queryId,
            },
            MeridianMessageTypes.probeResult,
            myRtt,
          ),
      ]);

      _ProbeOutcome? closestPeer;
      var closestRtt = myRtt;
      for (final outcome in outcomes) {
        final rtt = outcome?.rttMs;
        if (rtt != null && rtt < closestRtt) {
          closestRtt = rtt;
          closestPeer = outcome;
        }
      }

      if (closestPeer != null &&
          closestRtt < myRtt * node.config.routeAcceptanceThreshold) {
        final forwarded = _forwardQuery(
          query,
          node.ringMember(closestPeer.peerId),
          MeridianMessageTypes.queryForward,
        );
        if (forwarded) return;
      }

      _respondToQuery(
        query,
        QueryResult(
          queryId: query.queryId,
          closestPeerId: closestPeer?.peerId ?? node.peerId,
          closestRttMs: closestRtt,
          hopCount: query.hopCount,
        ),
      );
    } catch (error) {
      _logger.fine('query routing failed: $error');
      _respondToQuery(query, _errorResult(query, 'Query routing failed'));
    }
  }

  /// Routes one hop of a leader-election query; the metric is the average
  /// RTT to all targets (spec §3.7).
  Future<void> routeLeaderQuery(Query query) async {
    try {
      if (query.hopCount > node.config.maxHops) {
        _respondToQuery(query, _errorResult(query, 'Max hops exceeded'));
        return;
      }

      double myAvgRtt;
      final targets = query.targets ?? const <String>[];
      try {
        final rtts = await Future.wait([
          for (final target in targets) _measureRttToTarget(target, 'peer'),
        ]);
        myAvgRtt =
            rtts.isEmpty ? 0.0 : rtts.reduce((a, b) => a + b) / rtts.length;
      } catch (_) {
        _respondToQuery(query, _errorResult(query, 'Cannot measure targets'));
        return;
      }

      final peersToQuery = _collectCandidates(
        myAvgRtt,
        node.ringManager.calculateRingIndex(myAvgRtt),
      );

      if (peersToQuery.isEmpty) {
        _respondToQuery(
          query,
          QueryResult(
            queryId: query.queryId,
            leaderId: node.peerId,
            avgRttMs: myAvgRtt,
            hopCount: query.hopCount,
          ),
        );
        return;
      }

      final outcomes = await _allSettled([
        for (final member in peersToQuery)
          _askPeer(
            member,
            {
              'type': MeridianMessageTypes.probeRequestAvg,
              'targets': query.targets,
              'queryId': query.queryId,
            },
            MeridianMessageTypes.probeResultAvg,
            myAvgRtt,
          ),
      ]);

      _ProbeOutcome? closestPeer;
      var closestAvgRtt = myAvgRtt;
      for (final outcome in outcomes) {
        final avgRtt = outcome?.avgRttMs;
        if (avgRtt != null && avgRtt.isFinite && avgRtt < closestAvgRtt) {
          closestAvgRtt = avgRtt;
          closestPeer = outcome;
        }
      }

      if (closestPeer != null &&
          closestAvgRtt < myAvgRtt * node.config.routeAcceptanceThreshold) {
        final forwarded = _forwardQuery(
          query,
          node.ringMember(closestPeer.peerId),
          MeridianMessageTypes.leaderQueryForward,
        );
        if (forwarded) return;
      }

      _respondToQuery(
        query,
        QueryResult(
          queryId: query.queryId,
          leaderId: closestPeer?.peerId ?? node.peerId,
          avgRttMs: closestAvgRtt,
          hopCount: query.hopCount,
        ),
      );
    } catch (error) {
      _logger.fine('leader query routing failed: $error');
      _respondToQuery(
          query, _errorResult(query, 'Leader query routing failed'));
    }
  }

  /// Routes one hop of a multi-constraint query (spec §3.8). Distance to
  /// the solution space is max(0, measured_rtt - maxLatencyMs) per
  /// constraint.
  Future<void> routeConstraintQuery(Query query) async {
    try {
      if (query.hopCount > node.config.maxHops) {
        _respondToQuery(query, _errorResult(query, 'Max hops exceeded'));
        return;
      }

      final measurements = <_ConstraintMeasurement>[];
      for (final constraint in query.constraints ?? const <Constraint>[]) {
        var rtt = double.infinity;
        try {
          rtt = await _measureRttToTarget(constraint.target, 'peer');
        } catch (_) {
          // Unmeasurable constraint: Infinity keeps it unsatisfied.
        }
        measurements.add((
          target: constraint.target,
          rtt: rtt,
          maxLatency: constraint.maxLatencyMs,
        ));
      }

      var totalDistance = 0.0;
      var satisfiesAll = true;
      for (final m in measurements) {
        final distance = math.max(0.0, m.rtt - m.maxLatency);
        totalDistance += distance;
        if (distance > 0) satisfiesAll = false;
      }

      if (satisfiesAll) {
        if (!query.satisfyingPeers.contains(node.peerId)) {
          query.satisfyingPeers.add(node.peerId);
        }
        if (query.satisfyingPeers.length >= _earlyResponsePeerCount) {
          _respondToQuery(
            query,
            QueryResult(
              queryId: query.queryId,
              satisfyingPeers: [...query.satisfyingPeers],
              hopCount: query.hopCount,
            ),
          );
          return;
        }
      }

      // Candidate selection: any primary member (across all rings) whose
      // distance from us falls within [maxLatency / 2, maxLatency * 2] of
      // at least one constraint.
      final peersToQuery = <RingMember>[];
      final seen = <String>{};
      for (final ring in node.rings) {
        for (final member in ring.primaryMembers) {
          if (seen.contains(member.peerId)) continue;
          for (final m in measurements) {
            if (member.rttMs >= m.maxLatency / 2 &&
                member.rttMs <= m.maxLatency * 2) {
              peersToQuery.add(member);
              seen.add(member.peerId);
              break;
            }
          }
        }
      }

      if (peersToQuery.isEmpty) {
        _respondToQuery(
          query,
          QueryResult(
            queryId: query.queryId,
            satisfyingPeers: [...query.satisfyingPeers],
            hopCount: query.hopCount,
          ),
        );
        return;
      }

      final outcomes = await _allSettled([
        for (final member in peersToQuery)
          _askPeer(
            member,
            {
              'type': MeridianMessageTypes.probeRequestConstraints,
              'constraints': _constraintsToWire(query.constraints),
              'queryId': query.queryId,
            },
            MeridianMessageTypes.probeResultConstraints,
            0,
          ),
      ]);

      for (final outcome in outcomes) {
        if (outcome?.satisfiesAll ?? false) {
          if (!query.satisfyingPeers.contains(outcome!.peerId)) {
            query.satisfyingPeers.add(outcome.peerId);
          }
        }
      }

      _ProbeOutcome? bestPeer;
      var bestDistance = totalDistance;
      for (final outcome in outcomes) {
        final distance = outcome?.totalDistance;
        if (distance != null && distance.isFinite && distance < bestDistance) {
          bestDistance = distance;
          bestPeer = outcome;
        }
      }

      if (bestPeer != null &&
          bestDistance < totalDistance * node.config.routeAcceptanceThreshold) {
        final forwarded = _forwardQuery(
          query,
          node.ringMember(bestPeer.peerId),
          MeridianMessageTypes.constraintQueryForward,
        );
        if (forwarded) return;
      }

      _respondToQuery(
        query,
        QueryResult(
          queryId: query.queryId,
          satisfyingPeers: [...query.satisfyingPeers],
          hopCount: query.hopCount,
        ),
      );
    } catch (error) {
      _logger.fine('constraint query routing failed: $error');
      _respondToQuery(
        query,
        _errorResult(query, 'Constraint query routing failed'),
      );
    }
  }

  // --- Dispatch handlers (query_forward family + probes + results) ---

  /// A query_forward hop: the channel it arrived on becomes the back
  /// route for our response; the carried query continues routing.
  void handleQueryForward(Map<String, dynamic> msg, rtc.RTCDataChannel dc) {
    _handleForward(msg, dc, MeridianMessageTypes.queryForward);
  }

  /// A leader_query_forward hop.
  void handleLeaderQueryForward(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dc,
  ) {
    _handleForward(msg, dc, MeridianMessageTypes.leaderQueryForward);
  }

  /// A constraint_query_forward hop.
  void handleConstraintQueryForward(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dc,
  ) {
    _handleForward(msg, dc, MeridianMessageTypes.constraintQueryForward);
  }

  void _handleForward(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dc,
    String expectedType,
  ) {
    if (msg['type'] != expectedType) return;
    final query = msg['query'];
    if (query is! Map) return;
    if (query['queryId'] is! String) return;
    final parsed = _queryFromWire(query.cast<String, dynamic>(), dc);
    if (parsed == null) return;
    unawaited(_routeByType(parsed).catchError((_) {}));
  }

  /// Responder side of a probe (spec §7.2): measure our own RTT to the
  /// target and report it back over the channel the request arrived on.
  Future<void> handleProbeRequest(
    rtc.RTCDataChannel dc,
    Map<String, dynamic> msg,
  ) async {
    final probeId = msg['probeId'];
    final target = msg['target'];
    if (probeId is! String || target is! String) return;
    try {
      final rtt = await _measureRttToTarget(
        target,
        (msg['targetType'] as String?) ?? 'peer',
      );
      sendChannelMessage(dc, {
        'type': MeridianMessageTypes.probeResult,
        'probeId': probeId,
        'rttMs': rtt,
        'queryId': msg['queryId'],
      });
    } catch (_) {
      // Cannot measure: send nothing; the asker's probe timeout discards us.
    }
  }

  /// Responder side of an average-RTT probe (spec §3.7).
  Future<void> handleProbeRequestAvg(
    rtc.RTCDataChannel dc,
    Map<String, dynamic> msg,
  ) async {
    final probeId = msg['probeId'];
    final targets = msg['targets'];
    if (probeId is! String || targets is! List) return;
    try {
      final rtts = await Future.wait([
        for (final target in targets)
          if (target is String) _measureRttToTarget(target, 'peer'),
      ]);
      final avgRtt =
          rtts.isEmpty ? 0.0 : rtts.reduce((a, b) => a + b) / rtts.length;
      sendChannelMessage(dc, {
        'type': MeridianMessageTypes.probeResultAvg,
        'probeId': probeId,
        'avgRttMs': avgRtt,
        'queryId': msg['queryId'],
      });
    } catch (_) {
      // Cannot measure every target: no result; the asker times out.
    }
  }

  /// Responder side of a constraints probe (spec §3.8): evaluates the
  /// constraints locally and replies with our own satisfiesAll /
  /// totalDistance.
  Future<void> handleProbeRequestConstraints(
    rtc.RTCDataChannel dc,
    Map<String, dynamic> msg,
  ) async {
    final probeId = msg['probeId'];
    final constraints = msg['constraints'];
    if (probeId is! String || constraints is! List) return;

    var totalDistance = 0.0;
    var satisfiesAll = true;
    for (final raw in constraints) {
      if (raw is! Map) {
        // Malformed constraint counts as unsatisfiable.
        satisfiesAll = false;
        continue;
      }
      final target = raw['target'];
      final maxLatency = (raw['maxLatencyMs'] as num?)?.toDouble();
      if (target is! String || maxLatency == null) {
        satisfiesAll = false;
        continue;
      }
      var rtt = double.infinity;
      try {
        rtt = await _measureRttToTarget(target, 'peer');
      } catch (_) {
        // Counts as unsatisfiable.
      }
      final distance = math.max(0.0, rtt - maxLatency);
      totalDistance += distance;
      if (distance > 0) satisfiesAll = false;
    }

    sendChannelMessage(dc, {
      'type': MeridianMessageTypes.probeResultConstraints,
      'probeId': probeId,
      'satisfiesAll': satisfiesAll,
      'totalDistance': totalDistance,
      'queryId': msg['queryId'],
    });
  }

  /// Correlates an incoming probe_result family message with its pending
  /// one-shot listener. Tolerant when the echoed `probeId` is absent:
  /// peers that drop the field fall back to the sender's single pending
  /// probe of that result type.
  void handleProbeResult(Map<String, dynamic> msg, String senderId) {
    final type = msg['type'];
    if (type is! String) return;

    final echoed = msg['probeId'];
    final _PendingProbe? pending;
    if (echoed is String) {
      final found = _pendingProbes[echoed];
      if (found == null || found.resultType != type || found.settled) return;
      pending = found;
    } else {
      pending = _pendingProbes.values.firstWhereOrNull(
        (entry) =>
            entry.peerId == senderId &&
            entry.resultType == type &&
            !entry.settled,
      );
      if (pending == null) return;
    }

    _pendingProbes.remove(pending.probeId);
    pending.settle(_outcomeFromWire(pending.peerId, type, msg));
  }

  // --- Result handling ---

  /// A query_result hop: either we originated the query, or we forward
  /// the result back along the channel we received the query on.
  void handleQueryResult(Map<String, dynamic> msg) {
    final queryId = msg['queryId'];
    if (queryId is! String) return;

    final pending = node.pendingQueries[queryId];
    if (pending != null) {
      node.pendingQueries.remove(queryId);
      _queryTimers.remove(queryId)?.cancel();
      if (!pending.isCompleted) {
        pending.complete(_resultFromWire(msg));
      }
      return;
    }

    final backRoute = node.queryBackRoutes.remove(queryId);
    if (backRoute != null) {
      sendChannelMessage(backRoute, {
        ...msg,
        'type': MeridianMessageTypes.queryResult,
      });
    }
  }

  // --- Internals ---

  Future<void> _routeByType(Query query) {
    switch (query.type) {
      case QueryType.closestNode:
        return routeQuery(query);
      case QueryType.leaderElection:
        return routeLeaderQuery(query);
      case QueryType.multiConstraint:
        return routeConstraintQuery(query);
    }
  }

  /// Runs a probe exchange with one ring member; the outcome resolves
  /// once its correlated probe_result arrives, or errors on the
  /// probe-timeout floor of ε × [referenceRtt] (5s minimum).
  Future<_ProbeOutcome> _askPeer(
    RingMember member,
    Map<String, dynamic> request,
    String resultType,
    double referenceRtt,
  ) {
    final channel = member.dataChannel;
    if (channel == null) {
      return Future.error(
        StateError('Probe target ${member.peerId} not connected'),
      );
    }

    final probeId = uuidV4();
    final completer = Completer<_ProbeOutcome>();
    final pending = _PendingProbe(
      probeId: probeId,
      peerId: member.peerId,
      resultType: resultType,
      completer: completer,
    );
    pending.timer = Timer(_probeTimeout(referenceRtt), () {
      if (_pendingProbes.remove(probeId) != null) {
        pending.fail(TimeoutException('Probe timeout'));
      }
    });
    _pendingProbes[probeId] = pending;

    try {
      channel
          .send(rtc.RTCDataChannelMessage(
        jsonEncode({...request, 'probeId': probeId}),
      ))
          .then((_) {}, onError: (Object error) {
        if (_pendingProbes.remove(probeId) != null) {
          pending.fail(error);
        }
      });
    } catch (error) {
      _pendingProbes.remove(probeId);
      pending.fail(error);
    }

    return completer.future;
  }

  /// AllSettled equivalent: errors surface as null outcomes.
  Future<List<_ProbeOutcome?>> _allSettled(
    List<Future<_ProbeOutcome>> futures,
  ) {
    return Future.wait([
      for (final future in futures)
        future.then<_ProbeOutcome?>(
          (value) => value,
          onError: (Object error) => null,
        ),
    ]);
  }

  _ProbeOutcome _outcomeFromWire(
    String peerId,
    String resultType,
    Map<String, dynamic> msg,
  ) {
    return _ProbeOutcome(
      peerId: peerId,
      rttMs: resultType == MeridianMessageTypes.probeResult
          ? (msg['rttMs'] as num?)?.toDouble()
          : null,
      avgRttMs: resultType == MeridianMessageTypes.probeResultAvg
          ? (msg['avgRttMs'] as num?)?.toDouble()
          : null,
      satisfiesAll: resultType == MeridianMessageTypes.probeResultConstraints
          ? msg['satisfiesAll'] == true
          : null,
      totalDistance: resultType == MeridianMessageTypes.probeResultConstraints
          ? (msg['totalDistance'] as num?)?.toDouble()
          : null,
    );
  }

  /// ε × referenceRtt, floored at 5s: a peer probing an unknown target
  /// over an ephemeral connection always gets at least the
  /// connection-establishment window.
  Duration _probeTimeout(double referenceRtt) {
    final reference =
        referenceRtt.isFinite && referenceRtt > 0 ? referenceRtt : 0.0;
    final scaled = node.config.probeTimeoutFactor * reference;
    final ms = scaled.isFinite ? scaled : 5000.0;
    return Duration(milliseconds: math.max(5000, ms.ceil()));
  }

  /// Candidate primaries from rings i-1, i, i+1 whose distance from us
  /// lies in [myRtt / 2, myRtt * 2] — only they could be meaningfully
  /// closer to the target (spec §3.6).
  List<RingMember> _collectCandidates(double myRtt, int ringIndex) {
    final ringsToCheck = <int>[ringIndex];
    if (ringIndex > 0) ringsToCheck.add(ringIndex - 1);
    if (ringIndex < node.config.ringsPerNode - 1) {
      ringsToCheck.add(ringIndex + 1);
    }

    final candidates = <RingMember>[];
    final seen = <String>{};
    for (final ri in ringsToCheck) {
      for (final member in node.rings[ri].primaryMembers) {
        if (seen.contains(member.peerId)) continue;
        if (member.rttMs >= myRtt / 2 && member.rttMs <= myRtt * 2) {
          candidates.add(member);
          seen.add(member.peerId);
        }
      }
    }
    return candidates;
  }

  /// Measures RTT to an arbitrary target, dispatching by target type
  /// (spec §3.6). Self-targets measure 0; peers we hold no channel to are
  /// probed over an ephemeral connection.
  Future<double> _measureRttToTarget(String target, String targetType) {
    if (target == node.peerId) return Future.value(0);
    return node.rttMeasurement.measureToTarget(target, targetType);
  }

  /// Delivers a query result to the originator: over the channel the
  /// query arrived from when we are an intermediate hop, otherwise by
  /// resolving our own pending query promise.
  void _respondToQuery(Query query, QueryResult result) {
    final requesterDc = query.requesterDc;
    if (requesterDc != null) {
      sendChannelMessage(requesterDc, {
        'type': MeridianMessageTypes.queryResult,
        ..._resultToWire(result),
      });
      return;
    }

    final pending = node.pendingQueries[query.queryId];
    if (pending == null) return;
    node.pendingQueries.remove(query.queryId);
    _queryTimers.remove(query.queryId)?.cancel();
    if (!pending.isCompleted) {
      pending.complete(result);
    }
  }

  /// Forwards a query one hop, stamping the channel it is being
  /// forwarded on as the answer route so `query_result` travels back
  /// hop-by-hop to the originator. The channel the query ARRIVED on is
  /// remembered locally for that return trip — a live channel cannot
  /// survive JSON round-trips, so `requesterDc` is never serialized.
  bool _forwardQuery(Query query, RingMember? member, String messageType) {
    final dc = member?.dataChannel;
    if (dc == null) return false;

    final backRoute = query.requesterDc;
    if (backRoute != null) {
      if (node.queryBackRoutes.length >= _maxQueryBackRoutes) {
        node.queryBackRoutes.remove(node.queryBackRoutes.keys.first);
      }
      node.queryBackRoutes[query.queryId] = backRoute;
    }

    query.hopCount++;
    query.requesterDc = dc;
    try {
      unawaited(
        dc
            .send(rtc.RTCDataChannelMessage(
          jsonEncode({'type': messageType, 'query': _queryToWire(query)}),
        ))
            .then((_) {}, onError: (Object _) {
          // Late send failure: the query dies at this hop; the
          // originator's timeout recovers.
        }),
      );
      return true;
    } catch (_) {
      if (backRoute != null) node.queryBackRoutes.remove(query.queryId);
      query.requesterDc = backRoute;
      return false;
    }
  }

  // --- Wire serialization ---

  /// The wire shape of a query. `requesterDc` is intentionally absent: the
  /// return channel is stamped on every *_forward receipt and rewritten
  /// hop-by-hop; it never travels the wire.
  Map<String, dynamic> _queryToWire(Query q) => {
        'queryId': q.queryId,
        'type': _queryTypeToWire(q.type),
        if (q.target != null) 'target': q.target,
        if (q.targetType != null) 'targetType': q.targetType,
        if (q.targets != null) 'targets': q.targets,
        if (q.constraints != null)
          'constraints': _constraintsToWire(q.constraints),
        'hopCount': q.hopCount,
        'originator': q.originator,
        'timestamp': q.timestamp.millisecondsSinceEpoch,
        'satisfyingPeers': q.satisfyingPeers,
      };

  List<Map<String, dynamic>>? _constraintsToWire(
    List<Constraint>? constraints,
  ) {
    if (constraints == null) return null;
    return [
      for (final c in constraints)
        {'target': c.target, 'maxLatencyMs': c.maxLatencyMs},
    ];
  }

  Query? _queryFromWire(
    Map<String, dynamic> wire,
    rtc.RTCDataChannel requesterDc,
  ) {
    final queryId = wire['queryId'];
    final type = wire['type'];
    if (queryId is! String || type is! String) return null;
    final wireType = _queryTypeFromWire(type);
    if (wireType == null) return null;

    return Query(
      queryId: queryId,
      type: wireType,
      target: wire['target'] as String?,
      targetType: wire['targetType'] as String?,
      targets: (wire['targets'] as List?)?.cast<String>().toList(),
      constraints: _constraintsFromWire(wire['constraints']),
      hopCount: (wire['hopCount'] as num?)?.toInt() ?? 0,
      originator: wire['originator'] as String? ?? '',
      requesterDc: requesterDc,
      satisfyingPeers:
          (wire['satisfyingPeers'] as List?)?.cast<String>().toList() ?? [],
    );
  }

  List<Constraint>? _constraintsFromWire(dynamic raw) {
    if (raw is! List) return null;
    final constraints = <Constraint>[];
    for (final entry in raw) {
      if (entry is! Map) continue;
      final target = entry['target'];
      final maxLatencyMs = (entry['maxLatencyMs'] as num?)?.toDouble();
      if (target is! String || maxLatencyMs == null) continue;
      constraints.add(Constraint(target: target, maxLatencyMs: maxLatencyMs));
    }
    return constraints;
  }

  QueryResult _resultFromWire(Map<String, dynamic> msg) => QueryResult(
        queryId: msg['queryId'] as String,
        closestPeerId: msg['closestPeerId'] as String?,
        closestRttMs: (msg['closestRtt'] as num?)?.toDouble(),
        leaderId: msg['leaderId'] as String?,
        avgRttMs: (msg['avgRtt'] as num?)?.toDouble(),
        satisfyingPeers:
            (msg['satisfyingPeers'] as List?)?.cast<String>().toList(),
        hopCount: (msg['hopCount'] as num?)?.toInt() ?? 0,
        error: msg['error'] as String?,
      );

  Map<String, dynamic> _resultToWire(QueryResult r) => {
        'queryId': r.queryId,
        if (r.closestPeerId != null) 'closestPeerId': r.closestPeerId,
        if (r.closestRttMs != null) 'closestRtt': r.closestRttMs,
        if (r.leaderId != null) 'leaderId': r.leaderId,
        if (r.avgRttMs != null) 'avgRtt': r.avgRttMs,
        if (r.satisfyingPeers != null) 'satisfyingPeers': r.satisfyingPeers,
        'hopCount': r.hopCount,
        if (r.error != null) 'error': r.error,
      };

  /// Cancels every pending query/probe timer and fails their completers;
  /// called from [MeridianNode.dispose] so callers never hang.
  void shutdown() {
    for (final timer in _queryTimers.values) {
      timer.cancel();
    }
    _queryTimers.clear();
    for (final pending in _pendingProbes.values) {
      pending.fail(StateError('Node shutting down'));
    }
    _pendingProbes.clear();
  }

  static QueryResult _errorResult(Query query, String error) => QueryResult(
        queryId: query.queryId,
        hopCount: query.hopCount,
        error: error,
      );

  static String _queryTypeToWire(QueryType type) {
    switch (type) {
      case QueryType.closestNode:
        return 'closest_node';
      case QueryType.leaderElection:
        return 'leader_election';
      case QueryType.multiConstraint:
        return 'multi_constraint';
    }
  }

  static QueryType? _queryTypeFromWire(String type) {
    switch (type) {
      case 'closest_node':
        return QueryType.closestNode;
      case 'leader_election':
        return QueryType.leaderElection;
      case 'multi_constraint':
        return QueryType.multiConstraint;
      default:
        return null;
    }
  }
}
