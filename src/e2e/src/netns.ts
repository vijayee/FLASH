/**
 * Topology model for the local netns/netem rig (Task 3): each scripted peer
 * runs in its own Linux network namespace with a per-region `tc netem`
 * egress delay, so latency is deterministic without cloud spend.
 */

export type NetnsRegion = 'eu' | 'us' | 'asia';

export interface NetnsPeer {
  /** Namespace (and peer) name, e.g. `mrd-eu`. */
  name: string;
  region: NetnsRegion;
  /** Chromium `--remote-debugging-port` inside the namespace. */
  cdpPort: number;
  /** Host-side veth the namespace's netem qdisc is applied to. */
  vethHost: string;
  /** Peer-side address inside the namespace (10.200.<i>.2). */
  address: string;
}

/** Scripted one-way pair delays (ms); netem composes them per-egress. */
export type NetnsDelayMatrix = Record<
  'eu-us' | 'eu-asia' | 'us-asia',
  number
>;

export interface NetnsTopology {
  /** Host bridge the namespace veths attach to (e.g. `mrd-br0`). */
  bridge: string;
  peers: NetnsPeer[];
  delaysMs: NetnsDelayMatrix;
}

/**
 * Parses + validates a topology JSON (as written into the `topology.json`
 * run artifact). Throws with the offending field on any mismatch, so a
 * rig/script drift fails loudly instead of silently testing against the
 * wrong delay matrix.
 */
export function loadTopology(json: unknown): NetnsTopology {
  if (typeof json !== 'object' || json === null) {
    throw new Error('topology must be a JSON object');
  }
  const { bridge, peers, delaysMs } = json as Record<string, unknown>;
  if (typeof bridge !== 'string' || bridge.length === 0) {
    throw new Error('topology.bridge must be a non-empty string');
  }
  if (!Array.isArray(peers) || peers.length === 0) {
    throw new Error('topology.peers must be a non-empty array');
  }
  const seenRegions = new Set<NetnsRegion>();
  const parsed = peers.map((peer) => {
    const typed = assertNetnsPeer(peer);
    if (seenRegions.has(typed.region)) {
      throw new Error(`duplicate region ${typed.region} in topology.peers`);
    }
    seenRegions.add(typed.region);
    return typed;
  });
  if (typeof delaysMs !== 'object' || delaysMs === null) {
    throw new Error('topology.delaysMs must be an object');
  }
  const delays = delaysMs as Record<string, unknown>;
  for (const key of ['eu-us', 'eu-asia', 'us-asia'] as const) {
    const value = delays[key];
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
      throw new Error(`topology.delaysMs.${key} must be a non-negative ms`);
    }
  }
  return {
    bridge,
    peers: parsed,
    delaysMs: delaysMs as NetnsDelayMatrix,
  };
}

function assertNetnsPeer(value: unknown): NetnsPeer {
  if (typeof value !== 'object' || value === null) {
    throw new Error('topology.peers entries must be objects');
  }
  const peer = value as Record<string, unknown>;
  const nameValue = peer.name;
  if (typeof nameValue !== 'string' || nameValue.length === 0) {
    throw new Error('topology.peers[].name must be a non-empty string');
  }
  const name = nameValue;
  const region = peer.region;
  if (region !== 'eu' && region !== 'us' && region !== 'asia') {
    throw new Error(`topology.peers[${name}].region must be eu|us|asia`);
  }
  const cdpPort = peer.cdpPort;
  if (typeof cdpPort !== 'number' || !Number.isInteger(cdpPort) || cdpPort <= 0) {
    throw new Error(`topology.peers[${name}].cdpPort must be a port number`);
  }
  return {
    name,
    region,
    cdpPort,
    vethHost: assertString(peer.vethHost, 'vethHost', name),
    address: assertString(peer.address, 'address', name),
  };
}

function assertString(
  value: unknown,
  field: string,
  peerName: string,
): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`topology.peers[${peerName}].${field} must be a string`);
  }
  return value;
}