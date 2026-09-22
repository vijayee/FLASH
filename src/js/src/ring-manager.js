import { calculateRingIndex } from './ring.js';
import { measureRttOverDataChannel } from './rtt.js';

/**
 * Adds a peer to the appropriate ring based on a measured RTT (spec §3.4).
 * If the ring is full, the peer becomes a secondary candidate and a
 * hypervolume-based optimization decides promotions.
 */
export async function addPeerToRing(node, peerId, dataChannel, rtt) {
  // A peer may be re-added after a reconnect; remove any stale membership
  // first so a changed RTT cleanly relocates it.
  for (const ring of node.rings) {
    ring.primaryMembers = ring.primaryMembers.filter((m) => m.peerId !== peerId);
    ring.secondaryMembers = ring.secondaryMembers.filter((m) => m.peerId !== peerId);
  }

  const ringIndex = calculateRingIndex(rtt, node.config);
  const ring = node.rings[ringIndex];

  const member = {
    peerId,
    dataChannel,
    rtt,
    lastProbed: Date.now(),
    iceCandidateType: 'host', // Updated from ICE stats later.
    natType: 'unknown',
    isSupernode: false,
    isFirewalled: false,
    joinedAt: Date.now()
  };

  if (ring.primaryMembers.length < node.config.nodesPerRing) {
    ring.primaryMembers.push(member);
  } else {
    // Secondary pool, FIFO with a cap.
    ring.secondaryMembers.push(member);
    if (ring.secondaryMembers.length > node.config.secondaryCandidates) {
      ring.secondaryMembers.shift();
    }
    node._optimizeRing(ringIndex);
  }

  const existing = node.knownPeers.get(peerId);
  if (existing) {
    existing.dataChannel = dataChannel;
    existing.rtt = rtt;
    existing.ringIndex = ringIndex;
    existing.lastSeen = Date.now();
    existing.status = 'connected';
  } else {
    node.knownPeers.set(peerId, {
      peerId,
      dataChannel,
      rtt,
      lastSeen: Date.now(),
      isSupernode: false,
      ringIndex,
      status: 'connected'
    });
  }

  node._setupDataChannelHandlers(dataChannel, peerId);
  return member;
}

/**
 * Hypervolume-based ring optimization (spec §3.4). Builds a local coordinate
 * space from RTTs, then greedily drops the candidate whose removal reduces
 * the hypervolume (diversity proxy) the least until the ring fits.
 */
export function optimizeRing(ringIndex, rings, config) {
  const ring = rings[ringIndex];
  const allCandidates = [...ring.primaryMembers, ...ring.secondaryMembers];

  if (allCandidates.length <= config.nodesPerRing) {
    return;
  }

  const coordinates = new Map();
  for (const a of allCandidates) {
    coordinates.set(
      a.peerId,
      allCandidates.map((b) => (a.peerId === b.peerId ? 0 : b.rtt))
    );
  }

  let selected = allCandidates.map((c) => c.peerId);

  while (selected.length > config.nodesPerRing) {
    const volumeWith = computeHypervolume(selected, coordinates);
    let worstPeer = null;
    let smallestReduction = Infinity;

    for (const peerId of selected) {
      const without = selected.filter((id) => id !== peerId);
      const reduction = volumeWith - computeHypervolume(without, coordinates);
      if (reduction < smallestReduction) {
        smallestReduction = reduction;
        worstPeer = peerId;
      }
    }

    if (worstPeer === null) break;
    selected = selected.filter((id) => id !== worstPeer);
  }

  const newPrimary = [];
  const newSecondary = [];
  for (const candidate of allCandidates) {
    if (selected.includes(candidate.peerId)) {
      newPrimary.push(candidate);
    } else {
      newSecondary.push(candidate);
    }
  }

  ring.primaryMembers = newPrimary;
  ring.secondaryMembers = newSecondary.slice(0, config.secondaryCandidates);
}

/**
 * Product of per-dimension coordinate ranges (+1 to avoid zero volume).
 */
export function computeHypervolume(peerIds, coordinates) {
  if (peerIds.length === 0) return 0;
  if (peerIds.length === 1) return 1;

  let volume = 1;
  const dims = coordinates.get(peerIds[0]).length;

  for (let d = 0; d < dims; d++) {
    let minVal = Infinity;
    let maxVal = -Infinity;

    for (const peerId of peerIds) {
      const val = coordinates.get(peerId)[d];
      if (val < minVal) minVal = val;
      if (val > maxVal) maxVal = val;
    }

    volume *= maxVal - minVal + 1;
  }

  return volume;
}

/**
 * Periodic maintenance (spec §3.4): re-measure RTTs to ring members and move
 * members whose ring assignment changed. Probe failures are treated as peer
 * failures.
 */
export async function refreshRings(node) {
  // Skip while a cycle is still running: overlapping cycles would
  // double-probe members racing with ring moves.
  if (node._refreshingRings) return;
  node._refreshingRings = true;
  try {
    for (const ring of node.rings) {
      for (const member of [...ring.primaryMembers]) {
        try {
          const newRtt = await measureRttOverDataChannel(member.dataChannel);
          member.rtt = newRtt;
          member.lastProbed = Date.now();

          const correctRing = calculateRingIndex(newRtt, node.config);
          if (correctRing !== ring.index) {
            moveMember(node, member, ring.index, correctRing);
          }
        } catch {
          node._handlePeerFailure(member.peerId);
        }
      }

      node._optimizeRing(ring.index);
    }
  } finally {
    node._refreshingRings = false;
  }
}

/**
 * Relocates a member between rings, promoting a secondary into the source
 * ring when possible.
 */
export function moveMember(node, member, fromRingIndex, toRingIndex) {
  const fromRing = node.rings[fromRingIndex];
  const toRing = node.rings[toRingIndex];

  fromRing.primaryMembers = fromRing.primaryMembers.filter(
    (m) => m.peerId !== member.peerId
  );

  if (fromRing.secondaryMembers.length > 0) {
    const promoted = fromRing.secondaryMembers.shift();
    fromRing.primaryMembers.push(promoted);
  }

  if (toRing.primaryMembers.length < node.config.nodesPerRing) {
    toRing.primaryMembers.push(member);
  } else {
    toRing.secondaryMembers.push(member);
    node._optimizeRing(toRingIndex);
  }

  const known = node.knownPeers.get(member.peerId);
  if (known) {
    known.ringIndex = toRingIndex;
  }
}