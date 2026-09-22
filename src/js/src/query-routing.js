import { MESSAGE_TYPES } from './message-types.js';
import { calculateRingIndex } from './ring.js';
import {
  measureRttOverDataChannel,
  probePeerViaEphemeralConnection,
  probeHttpTarget
} from './rtt.js';

// Spec §3.8: respond as soon as this many satisfying peers are known.
const EARLY_RESPONSE_PEER_COUNT = 5;

// Bound on hop-by-hop query_result return routes kept for in-flight queries
// whose originator is upstream of us (entries are deleted when the result
// passes through; the cap stops unbounded growth for queries that never do).
const MAX_QUERY_BACK_ROUTES = 200;

/**
 * Entry point (spec §3.6): find the peer closest to a target by starting a
 * multi-hop query rooted at this node. The pending promise is correlated by
 * queryId through node.pendingProbes.
 */
export function findClosestNode(node, target, targetType) {
  const queryId = crypto.randomUUID();

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      node.pendingProbes.delete(queryId);
      reject(new Error('Query timeout'));
    }, node.config.queryTimeoutMs);

    node.pendingProbes.set(queryId, { resolve, reject, timer });

    routeQuery(node, {
      queryId,
      type: 'closest_node',
      target,
      targetType: targetType || 'peer',
      hopCount: 0,
      originator: node.peerId,
      requesterDc: null,
      timestamp: Date.now()
    }).catch(() => {});
  });
}

/**
 * Routes one hop of a closest-node query (spec §3.6).
 */
export async function routeQuery(node, query) {
  try {
    if (query.hopCount > node.config.maxHops) {
      respondToQuery(node, query, { error: 'Max hops exceeded' });
      return;
    }

    let myRtt;
    try {
      myRtt = await measureRttToTarget(node, query.target, query.targetType);
    } catch {
      respondToQuery(node, query, { error: 'Cannot measure target' });
      return;
    }

    const peersToQuery = collectCandidates(
      node,
      myRtt,
      calculateRingIndex(myRtt, node.config)
    );

    if (peersToQuery.length === 0) {
      // No peers worth querying — we are the closest known node.
      respondToQuery(node, query, {
        closestPeerId: node.peerId,
        closestRtt: myRtt,
        hopCount: query.hopCount
      });
      return;
    }

    const probeResults = await Promise.allSettled(
      peersToQuery.map((member) => askPeerToProbe(node, member, query, myRtt))
    );

    let closestPeer = null;
    let closestRtt = myRtt;
    for (const result of probeResults) {
      if (result.status === 'fulfilled' && result.value.rtt < closestRtt) {
        closestRtt = result.value.rtt;
        closestPeer = result.value;
      }
    }

    if (
      closestPeer &&
      closestRtt < myRtt * node.config.routeAcceptanceThreshold
    ) {
      const forwarded = forwardQuery(
        node,
        query,
        closestPeer,
        MESSAGE_TYPES.QUERY_FORWARD
      );
      if (forwarded) return;
    }

    respondToQuery(node, query, {
      closestPeerId: closestPeer ? closestPeer.peerId : node.peerId,
      closestRtt,
      hopCount: query.hopCount
    });
  } catch {
    respondToQuery(node, query, { error: 'Query routing failed' });
  }
}

/**
 * Entry point (spec §3.7): find the peer minimizing AVERAGE latency to a
 * set of peers.
 */
export function findCentralLeader(node, peerIds) {
  const queryId = crypto.randomUUID();

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      node.pendingProbes.delete(queryId);
      reject(new Error('Leader election timeout'));
    }, node.config.queryTimeoutMs);

    node.pendingProbes.set(queryId, { resolve, reject, timer });

    routeLeaderQuery(node, {
      queryId,
      type: 'leader_election',
      targets: [...peerIds],
      hopCount: 0,
      originator: node.peerId,
      requesterDc: null,
      timestamp: Date.now()
    }).catch(() => {});
  });
}

/**
 * Routes one hop of a leader-election query. The metric is the average RTT
 * to all targets (spec §3.7).
 */
export async function routeLeaderQuery(node, query) {
  try {
    if (query.hopCount > node.config.maxHops) {
      respondToQuery(node, query, { error: 'Max hops exceeded' });
      return;
    }

    let myAvgRtt;
    try {
      const rtts = await Promise.all(
        query.targets.map((target) => measureRttToTarget(node, target, 'peer'))
      );
      myAvgRtt = rtts.reduce((a, b) => a + b, 0) / rtts.length;
    } catch {
      respondToQuery(node, query, { error: 'Cannot measure targets' });
      return;
    }

    const peersToQuery = collectCandidates(
      node,
      myAvgRtt,
      calculateRingIndex(myAvgRtt, node.config)
    );

    if (peersToQuery.length === 0) {
      respondToQuery(node, query, {
        leaderId: node.peerId,
        avgRtt: myAvgRtt,
        hopCount: query.hopCount
      });
      return;
    }

    const probeResults = await Promise.allSettled(
      peersToQuery.map((member) =>
        askPeerToProbeAverage(node, member, query, myAvgRtt)
      )
    );

    let closestPeer = null;
    let closestAvgRtt = myAvgRtt;
    for (const result of probeResults) {
      if (
        result.status === 'fulfilled' &&
        Number.isFinite(result.value.avgRtt) &&
        result.value.avgRtt < closestAvgRtt
      ) {
        closestAvgRtt = result.value.avgRtt;
        closestPeer = result.value;
      }
    }

    if (
      closestPeer &&
      closestAvgRtt < myAvgRtt * node.config.routeAcceptanceThreshold
    ) {
      const forwarded = forwardQuery(
        node,
        query,
        closestPeer,
        MESSAGE_TYPES.LEADER_QUERY_FORWARD
      );
      if (forwarded) return;
    }

    respondToQuery(node, query, {
      leaderId: closestPeer ? closestPeer.peerId : node.peerId,
      avgRtt: closestAvgRtt,
      hopCount: query.hopCount
    });
  } catch {
    respondToQuery(node, query, { error: 'Leader query routing failed' });
  }
}

/**
 * Entry point (spec §3.8): find nodes satisfying all latency constraints.
 */
export function findNodesSatisfyingConstraints(node, constraints) {
  const queryId = crypto.randomUUID();

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      node.pendingProbes.delete(queryId);
      reject(new Error('Multi-constraint query timeout'));
    }, node.config.queryTimeoutMs);

    node.pendingProbes.set(queryId, { resolve, reject, timer });

    routeConstraintQuery(node, {
      queryId,
      type: 'multi_constraint',
      constraints: [...constraints],
      hopCount: 0,
      originator: node.peerId,
      requesterDc: null,
      timestamp: Date.now(),
      satisfyingPeers: []
    }).catch(() => {});
  });
}

/**
 * Routes one hop of a multi-constraint query (spec §3.8). Distance to the
 * solution space is max(0, measured_rtt - maxLatencyMs) per constraint.
 */
export async function routeConstraintQuery(node, query) {
  try {
    if (query.hopCount > node.config.maxHops) {
      respondToQuery(node, query, { error: 'Max hops exceeded' });
      return;
    }

    const measurements = [];
    for (const constraint of query.constraints || []) {
      let rtt = Infinity;
      try {
        rtt = await measureRttToTarget(node, constraint.target, 'peer');
      } catch {
        // Unmeasurable constraint: Infinity keeps it unsatisfied.
      }
      measurements.push({
        target: constraint.target,
        rtt,
        maxLatency: constraint.maxLatencyMs
      });
    }

    let totalDistance = 0;
    let satisfiesAll = true;
    for (const m of measurements) {
      const distance = Math.max(0, m.rtt - m.maxLatency);
      totalDistance += distance;
      if (distance > 0) satisfiesAll = false;
    }

    if (satisfiesAll) {
      if (!query.satisfyingPeers.includes(node.peerId)) {
        query.satisfyingPeers.push(node.peerId);
      }
      if (query.satisfyingPeers.length >= EARLY_RESPONSE_PEER_COUNT) {
        respondToQuery(node, query, {
          satisfyingPeers: query.satisfyingPeers,
          hopCount: query.hopCount
        });
        return;
      }
    }

    // Candidate selection: any primary member whose distance from us falls
    // within [maxLatency / 2, maxLatency * 2] of at least one constraint.
    const peersToQuery = [];
    const seen = new Set();
    for (const ring of node.rings) {
      for (const member of ring.primaryMembers) {
        if (seen.has(member.peerId)) continue;
        for (const m of measurements) {
          if (member.rtt >= m.maxLatency / 2 && member.rtt <= m.maxLatency * 2) {
            peersToQuery.push(member);
            seen.add(member.peerId);
            break;
          }
        }
      }
    }

    if (peersToQuery.length === 0) {
      respondToQuery(node, query, {
        satisfyingPeers: query.satisfyingPeers,
        hopCount: query.hopCount
      });
      return;
    }

    const probeResults = await Promise.allSettled(
      peersToQuery.map((member) => askPeerToCheckConstraints(node, member, query))
    );

    for (const result of probeResults) {
      if (result.status === 'fulfilled' && result.value.satisfiesAll) {
        if (!query.satisfyingPeers.includes(result.value.peerId)) {
          query.satisfyingPeers.push(result.value.peerId);
        }
      }
    }

    let bestPeer = null;
    let bestDistance = totalDistance;
    for (const result of probeResults) {
      if (
        result.status === 'fulfilled' &&
        Number.isFinite(result.value.totalDistance) &&
        result.value.totalDistance < bestDistance
      ) {
        bestDistance = result.value.totalDistance;
        bestPeer = result.value;
      }
    }

    if (
      bestPeer &&
      bestDistance < totalDistance * node.config.routeAcceptanceThreshold
    ) {
      const forwarded = forwardQuery(
        node,
        query,
        bestPeer,
        MESSAGE_TYPES.CONSTRAINT_QUERY_FORWARD
      );
      if (forwarded) return;
    }

    respondToQuery(node, query, {
      satisfyingPeers: query.satisfyingPeers,
      hopCount: query.hopCount
    });
  } catch {
    respondToQuery(node, query, { error: 'Constraint query routing failed' });
  }
}

/**
 * Asks a ring member to measure its RTT to the query target (spec §3.6).
 * The probe is discarded after ε * myRtt (floored at 5s): a peer slower
 * than that cannot be meaningfully closer. The one-shot result listener is
 * removed on every exit path.
 */
export function askPeerToProbe(node, member, query, myRtt) {
  const probeId = crypto.randomUUID();
  const channel = member.dataChannel;

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;

    const cleanup = () => {
      clearTimeout(timeout);
      channel.removeEventListener('message', handler);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };
    const succeed = (value) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    };

    const handler = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      if (msg.type === MESSAGE_TYPES.PROBE_RESULT && msg.probeId === probeId) {
        succeed({
          peerId: member.peerId,
          rtt: msg.rttMs,
          dataChannel: channel
        });
      }
    };

    timeout = setTimeout(
      () => fail(new Error('Probe timeout')),
      probeTimeoutMs(node, myRtt)
    );
    channel.addEventListener('message', handler);
    try {
      channel.send(
        JSON.stringify({
          type: MESSAGE_TYPES.PROBE_REQUEST,
          probeId,
          target: query.target,
          targetType: query.targetType,
          queryId: query.queryId
        })
      );
    } catch (err) {
      fail(err);
    }
  });
}

/**
 * Asks a ring member to measure its AVERAGE RTT to a set of targets
 * (spec §3.7).
 */
export function askPeerToProbeAverage(node, member, query, myAvgRtt) {
  const probeId = crypto.randomUUID();
  const channel = member.dataChannel;

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;

    const cleanup = () => {
      clearTimeout(timeout);
      channel.removeEventListener('message', handler);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };
    const succeed = (value) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    };

    const handler = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      if (
        msg.type === MESSAGE_TYPES.PROBE_RESULT_AVG &&
        msg.probeId === probeId
      ) {
        succeed({
          peerId: member.peerId,
          avgRtt: msg.avgRttMs,
          dataChannel: channel
        });
      }
    };

    timeout = setTimeout(
      () => fail(new Error('Probe timeout')),
      probeTimeoutMs(node, myAvgRtt)
    );
    channel.addEventListener('message', handler);
    try {
      channel.send(
        JSON.stringify({
          type: MESSAGE_TYPES.PROBE_REQUEST_AVG,
          probeId,
          targets: query.targets,
          queryId: query.queryId
        })
      );
    } catch (err) {
      fail(err);
    }
  });
}

/**
 * Asks a ring member to evaluate the query's constraints locally
 * (spec §3.8). The reply carries the peer's own satisfiesAll/totalDistance.
 */
export function askPeerToCheckConstraints(node, member, query, myDistance = 0) {
  const probeId = crypto.randomUUID();
  const channel = member.dataChannel;

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;

    const cleanup = () => {
      clearTimeout(timeout);
      channel.removeEventListener('message', handler);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };
    const succeed = (value) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    };

    const handler = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      if (
        msg.type === MESSAGE_TYPES.PROBE_RESULT_CONSTRAINTS &&
        msg.probeId === probeId
      ) {
        succeed({
          peerId: member.peerId,
          satisfiesAll: !!msg.satisfiesAll,
          totalDistance: msg.totalDistance,
          dataChannel: channel
        });
      }
    };

    timeout = setTimeout(
      () => fail(new Error('Probe timeout')),
      probeTimeoutMs(node, myDistance)
    );
    channel.addEventListener('message', handler);
    try {
      channel.send(
        JSON.stringify({
          type: MESSAGE_TYPES.PROBE_REQUEST_CONSTRAINTS,
          probeId,
          constraints: query.constraints,
          queryId: query.queryId
        })
      );
    } catch (err) {
      fail(err);
    }
  });
}

/**
 * Responder side of a probe (spec §7.2): measure our own RTT to the target
 * and report it back over the channel the request arrived on.
 */
export async function handleProbeRequest(node, msg, dataChannel) {
  if (!msg || typeof msg.probeId !== 'string') return;
  try {
    const rtt = await measureRttToTarget(
      node,
      msg.target,
      msg.targetType || 'peer'
    );
    dataChannel.send(
      JSON.stringify({
        type: MESSAGE_TYPES.PROBE_RESULT,
        probeId: msg.probeId,
        rttMs: rtt,
        queryId: msg.queryId
      })
    );
  } catch {
    // Cannot measure: send nothing; the asker's probe timeout discards us.
  }
}

export async function handleProbeRequestAvg(node, msg, dataChannel) {
  if (!msg || typeof msg.probeId !== 'string' || !Array.isArray(msg.targets)) {
    return;
  }
  try {
    const rtts = await Promise.all(
      msg.targets.map((target) => measureRttToTarget(node, target, 'peer'))
    );
    const avgRtt = rtts.length
      ? rtts.reduce((a, b) => a + b, 0) / rtts.length
      : 0;
    dataChannel.send(
      JSON.stringify({
        type: MESSAGE_TYPES.PROBE_RESULT_AVG,
        probeId: msg.probeId,
        avgRttMs: avgRtt,
        queryId: msg.queryId
      })
    );
  } catch {
    // Cannot measure every target: no result; the asker times out.
  }
}

export async function handleProbeRequestConstraints(node, msg, dataChannel) {
  if (
    !msg ||
    typeof msg.probeId !== 'string' ||
    !Array.isArray(msg.constraints)
  ) {
    return;
  }

  let totalDistance = 0;
  let satisfiesAll = true;
  for (const constraint of msg.constraints) {
    let rtt = Infinity;
    try {
      rtt = await measureRttToTarget(node, constraint.target, 'peer');
    } catch {
      // Counts as unsatisfiable.
    }
    const distance = Math.max(0, rtt - constraint.maxLatencyMs);
    totalDistance += distance;
    if (distance > 0) satisfiesAll = false;
  }

  try {
    dataChannel.send(
      JSON.stringify({
        type: MESSAGE_TYPES.PROBE_RESULT_CONSTRAINTS,
        probeId: msg.probeId,
        satisfiesAll,
        totalDistance,
        queryId: msg.queryId
      })
    );
  } catch {
    // Channel may have just closed; the asker times out.
  }
}

/**
 * Delivers a query result to the originator (spec §3.6): over the channel
 * the query arrived from when we are an intermediate hop, otherwise by
 * resolving our own pending query promise.
 */
export function respondToQuery(node, query, result) {
  if (!result) return;
  result.queryId = query.queryId;

  if (query.requesterDc) {
    try {
      query.requesterDc.send(
        JSON.stringify({ type: MESSAGE_TYPES.QUERY_RESULT, ...result })
      );
    } catch {
      // Channel may have just closed; the originator's timeout fires.
    }
    return;
  }

  const pending = node.pendingProbes.get(query.queryId);
  if (pending) {
    clearTimeout(pending.timer);
    node.pendingProbes.delete(query.queryId);
    pending.resolve(result);
  }
}

/**
 * A query_result hop: either we originated the query, or we forward the
 * result back along the channel we received the query on.
 */
export function handleQueryResult(node, msg) {
  if (!msg || typeof msg.queryId !== 'string') return;

  const pending = node.pendingProbes.get(msg.queryId);
  if (pending) {
    clearTimeout(pending.timer);
    node.pendingProbes.delete(msg.queryId);
    pending.resolve(msg);
    return;
  }

  const backRoute = node._queryBackRoutes.get(msg.queryId);
  if (backRoute) {
    node._queryBackRoutes.delete(msg.queryId);
    try {
      backRoute.send(
        JSON.stringify({ type: MESSAGE_TYPES.QUERY_RESULT, ...msg })
      );
    } catch {
      // Channel may have just closed; the originator's timeout fires.
    }
  }
}

/**
 * Measures RTT to an arbitrary target, dispatching by target type
 * (spec §3.6). Peers we hold no channel to are probed over an ephemeral
 * connection.
 */
export async function measureRttToTarget(node, target, targetType) {
  if (target === node.peerId) return 0;

  switch (targetType) {
    case 'peer': {
      const known = node.knownPeers.get(target);
      if (
        known &&
        known.dataChannel &&
        known.dataChannel.readyState !== 'closed'
      ) {
        return measureRttOverDataChannel(known.dataChannel);
      }
      return probePeerViaEphemeralConnection(
        target,
        node.signalChannel,
        node.config,
        node.connectionPool,
        node.peerId
      );
    }
    case 'http':
    case 'https':
      return probeHttpTarget(target, node.config.ephemeralProbeTimeoutMs);
    default:
      throw new Error('Unknown target type: ' + targetType);
  }
}

/**
 * Forwards a query one hop, stamping the channel it is being forwarded on
 * as the answer route so `query_result` travels back hop-by-hop to the
 * originator. The channel the query ARRIVED on is remembered locally for
 * that return trip.
 */
function forwardQuery(node, query, member, messageType) {
  const backRoute = query.requesterDc || null;
  if (backRoute) {
    if (node._queryBackRoutes.size >= MAX_QUERY_BACK_ROUTES) {
      const oldest = node._queryBackRoutes.keys().next().value;
      if (oldest !== undefined) node._queryBackRoutes.delete(oldest);
    }
    node._queryBackRoutes.set(query.queryId, backRoute);
  }

  query.hopCount++;
  query.requesterDc = member.dataChannel;
  try {
    member.dataChannel.send(JSON.stringify({ type: messageType, query }));
    return true;
  } catch {
    if (backRoute) node._queryBackRoutes.delete(query.queryId);
    query.requesterDc = backRoute;
    return false;
  }
}

/**
 * ε * myRtt, floored at 5s: a peer probing an unknown target over an
 * ephemeral connection always gets at least the connection-establishment
 * window.
 */
function probeTimeoutMs(node, referenceRtt) {
  const scaled = node.config.probeTimeoutFactor * (referenceRtt || 0);
  return Math.max(5000, Number.isFinite(scaled) ? scaled : 5000);
}
/**
 * Candidate primaries from rings i-1, i, i+1 whose distance from us lies in
 * [rtt / 2, rtt * 2] — only they could be meaningfully closer to the target
 * (spec §3.6).
 */
function collectCandidates(node, myRtt, ringIndex) {
  const ringsToCheck = [ringIndex];
  if (ringIndex > 0) ringsToCheck.push(ringIndex - 1);
  if (ringIndex < node.config.ringsPerNode - 1) {
    ringsToCheck.push(ringIndex + 1);
  }

  const candidates = [];
  const seen = new Set();
  for (const ri of ringsToCheck) {
    for (const member of node.rings[ri].primaryMembers) {
      if (seen.has(member.peerId)) continue;
      if (member.rtt >= myRtt / 2 && member.rtt <= myRtt * 2) {
        candidates.push(member);
        seen.add(member.peerId);
      }
    }
  }
  return candidates;
}
