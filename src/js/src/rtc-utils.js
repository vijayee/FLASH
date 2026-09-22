/**
 * Shared PeerConnection assembly, used by both the node's handshakes
 * (node.js) and the ephemeral-probe path (rtt.js).
 */

/**
 * ICE-server list from config: STUN collapsed into one entry, TURN appended.
 */
export function buildIceServers(config) {
  const iceServers = [];
  if (config.stunServers && config.stunServers.length > 0) {
    iceServers.push({ urls: config.stunServers });
  }
  iceServers.push(...(config.turnServers || []));
  return iceServers;
}

/**
 * Resolves the RTCPeerConnection factory: an injected factory (test seam)
 * wins over the global browser constructor.
 */
export function createPeerConnection(config, factoryOverride = null) {
  const iceServers = buildIceServers(config);
  const factory = factoryOverride || config.rtcFactory;
  if (factory) {
    return factory.createPeerConnection(iceServers);
  }
  return new RTCPeerConnection({ iceServers });
}