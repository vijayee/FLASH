import type { PeerState } from './orchestrator.js';

/**
 * Asserts `peerId` is known to `state`'s peer and connected. Throwing
 * (rather than Playwright's expect) keeps these usable from plain helpers
 * and from inside `waitFor` predicates.
 */
export function expectKnownPeer(state: PeerState, peerId: string): void {
  const known = state.knownPeers.find((p) => p.id === peerId);
  if (!known) {
    throw new Error(
      `peer ${peerId} not in knownPeers of ${state.peerId} ` +
        `(known: ${state.knownPeers.map((p) => p.id).join(', ') || 'none'})`,
    );
  }
  if (known.status !== 'connected') {
    throw new Error(
      `peer ${peerId} is '${known.status}', expected 'connected'`,
    );
  }
}

/**
 * Asserts ring placement is monotonic with latency: a peer whose RTT should
 * put it at least `minRing` rings out must not sit closer. Inner rings are
 * lower-index (ring 0 is the closest); an unplaced peer (null) fails.
 */
export function expectRingMonotonic(
  state: PeerState,
  peerId: string,
  minRing: number,
): void {
  const known = state.knownPeers.find((p) => p.id === peerId);
  const ringIndex = known?.ringIndex ?? null;
  if (ringIndex === null) {
    throw new Error(
      `peer ${peerId} has no ring placement yet (ringIndex null)`,
    );
  }
  if (ringIndex < minRing) {
    throw new Error(
      `peer ${peerId} sits in ring ${ringIndex}, expected >= ${minRing} ` +
        `(observed rtt ${known?.rtt ?? '?'}ms from ${state.peerId})`,
    );
  }
}