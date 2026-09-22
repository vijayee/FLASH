import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

/// The three Meridian query families (spec §3.6-§3.8).
enum QueryType { closestNode, leaderElection, multiConstraint }

/// A query being routed hop-by-hop through the overlay. `requesterDc` is
/// local-only (a live channel cannot survive JSON round-trips): it is
/// stamped on every `*_forward` receipt and never serialized.
class Query {
  final String queryId;
  final QueryType type;
  final String? target;
  final String? targetType;
  final List<String>? targets;
  final List<Constraint>? constraints;
  int hopCount;
  final String originator;
  rtc.RTCDataChannel? requesterDc;
  final DateTime timestamp;

  /// Peers found satisfying all constraints so far (multi-constraint
  /// queries); accumulates hop-by-hop inside the forwarded query.
  final List<String> satisfyingPeers;

  Query({
    required this.queryId,
    required this.type,
    this.target,
    this.targetType,
    this.targets,
    this.constraints,
    this.hopCount = 0,
    required this.originator,
    this.requesterDc,
    List<String>? satisfyingPeers,
    DateTime? timestamp,
  })  : satisfyingPeers = satisfyingPeers ?? [],
        timestamp = timestamp ?? DateTime.now();
}

/// A single (target, maxLatency) pair of a multi-constraint query.
class Constraint {
  final String target;
  final double maxLatencyMs;

  const Constraint({required this.target, required this.maxLatencyMs});
}

/// The outcome of a query, returned to the originator.
class QueryResult {
  final String queryId;
  final String? closestPeerId;
  final double? closestRttMs;
  final String? leaderId;
  final double? avgRttMs;
  final List<String>? satisfyingPeers;
  final int hopCount;
  final String? error;

  QueryResult({
    required this.queryId,
    this.closestPeerId,
    this.closestRttMs,
    this.leaderId,
    this.avgRttMs,
    this.satisfyingPeers,
    this.hopCount = 0,
    this.error,
  });
}
