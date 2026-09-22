/**
 * Determines which ring a peer belongs to based on measured RTT.
 * Rings have exponentially increasing radii; the result is clamped to the
 * outermost ring.
 */
export function calculateRingIndex(rtt, config) {
  if (rtt <= config.innermostRingRadius) return 0;

  const ringIndex = Math.ceil(
    Math.log(rtt / config.innermostRingRadius) /
      Math.log(config.ringMultiplicativeFactor)
  );

  return Math.min(ringIndex, config.ringsPerNode - 1);
}

/**
 * Returns the ring bounds for a given ring index.
 * The outermost ring has an unbounded outer radius.
 */
export function getRingBounds(index, config) {
  const r = config.ringMultiplicativeFactor;
  return {
    // Ring 0 spans (0, innermostRadius].
    inner: index === 0 ? 0 : Math.pow(r, index - 1),
    outer: index < config.ringsPerNode - 1 ? Math.pow(r, index) : Infinity
  };
}