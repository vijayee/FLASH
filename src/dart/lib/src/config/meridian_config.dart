/// Handshake teardown deadline for offers we sent that never complete
/// (matching the JS implementation's hard-coded 10s).
const Duration connectionEstablishmentTimeout = Duration(seconds: 10);

/// Interval between connection-pool age sweeps.
const Duration poolCleanupInterval = Duration(minutes: 1);

/// Delay after bootstrap before the supernode eligibility check runs.
const Duration electionCheckDelay = Duration(seconds: 10);

/// Minimum known peers before the supernode eligibility check runs.
const int electionMinKnownPeers = 5;

/// System-wide constants (spec §4.1). Apps may build a [MeridianConfig] with
/// overrides: `const MeridianConfig(ringsPerNode: 5)`.
class MeridianConfig {
  final int ringsPerNode;
  final int nodesPerRing;
  final int secondaryCandidates;
  final double innermostRingRadiusMs;
  final double ringMultiplicativeFactor;
  final double routeAcceptanceThreshold;
  final double probeTimeoutFactor;
  final Duration gossipPeriod;
  final Duration ringReplacementPeriod;
  final int maxEphemeralConnections;
  final List<String> stunServers;
  final List<TurnServerConfig> turnServers;
  final int maxHops;
  final Duration ephemeralProbeTimeout;
  final Duration queryTimeout;

  const MeridianConfig({
    this.ringsPerNode = 9,
    this.nodesPerRing = 8,
    this.secondaryCandidates = 4,
    this.innermostRingRadiusMs = 1.0,
    this.ringMultiplicativeFactor = 2.0,
    this.routeAcceptanceThreshold = 0.5,
    this.probeTimeoutFactor = 2.0,
    this.gossipPeriod = const Duration(seconds: 30),
    this.ringReplacementPeriod = const Duration(seconds: 60),
    this.maxEphemeralConnections = 10,
    this.stunServers = const ['stun:stun.l.google.com:19302'],
    this.turnServers = const [],
    this.maxHops = 32,
    this.ephemeralProbeTimeout = const Duration(seconds: 5),
    this.queryTimeout = const Duration(seconds: 30),
  });
}

/// A single TURN relay server entry.
class TurnServerConfig {
  final String url;
  final String username;
  final String credential;

  const TurnServerConfig({
    required this.url,
    required this.username,
    required this.credential,
  });
}
