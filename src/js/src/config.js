// System-wide constants per spec §2.1. Callers may spread-override:
// `{ ...MERIDIAN_CONFIG, ringsPerNode: 5 }`.
export const MERIDIAN_CONFIG = {
  ringsPerNode: 9,
  nodesPerRing: 8,
  secondaryCandidates: 4,
  innermostRingRadius: 1, // ms
  ringMultiplicativeFactor: 2, // r
  routeAcceptanceThreshold: 0.5, // β
  probeTimeoutFactor: 2, // ε
  gossipPeriodMs: 30000, // 30 seconds
  ringReplacementPeriodMs: 60000, // 60 seconds
  maxEphemeralConnections: 10,
  stunServers: ['stun:stun.l.google.com:19302'],
  turnServers: [],
  maxHops: 32,
  ephemeralProbeTimeoutMs: 5000,
  queryTimeoutMs: 30000
};