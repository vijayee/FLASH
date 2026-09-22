import { describe, expect, it, vi } from 'vitest';
import { MESSAGE_TYPES, MeridianNode } from '../src/index.js';
import {
  routeConstraintQuery,
  handleProbeRequestConstraints
} from '../src/query-routing.js';
import { electSupernode } from '../src/raft.js';
import { forwardStreamToCluster } from '../src/media.js';
import {
  FakeDataChannel,
  latencyChannel,
  wiredPair,
  wire,
  feed,
  lastSent,
  sentOfType,
  tick,
  dispose
} from './helpers.js';

/**
 * Seeds `targetId` into the node's knownPeers over a channel that answers
 * PING after `delayMs` — the same seam measureRttToTarget reads — so
 * queries measure a controllable RTT.
 */
function knowTarget(node, targetId, delayMs) {
  node.knownPeers.set(targetId, {
    peerId: targetId,
    dataChannel: latencyChannel(delayMs),
    rtt: delayMs,
    lastSeen: Date.now(),
    isSupernode: false,
    ringIndex: 0,
    status: 'connected'
  });
}

describe('query routing over mock DataChannels', () => {
  it('rejects with "Query timeout" and drains the pending query', async () => {
    const node = new MeridianNode('O', null, { queryTimeoutMs: 100 });
    const chO = new FakeDataChannel();
    const chRaw = new FakeDataChannel();
    chO.peer = chRaw;
    chRaw.peer = chO;
    // M is enrolled (so it is probed) but its side is a bare channel with
    // no dispatch wired: the probe is never answered.
    wire(node, chO, 'M');
    await node.addPeerToRing('M', chO, 12);
    knowTarget(node, 'T', 10);

    await expect(node.findClosestNode('T', 'peer')).rejects.toThrow(
      'Query timeout'
    );
    expect(node.pendingProbes.size).toBe(0);
    // The probe went out over M's enrolled channel; the raw far side
    // recorded nothing because it has no dispatch wired.
    expect(sentOfType(chO, MESSAGE_TYPES.PROBE_REQUEST)).toHaveLength(1);
    expect(chRaw.sent).toHaveLength(0);
    dispose(node);
  });

  it('answers max-hops queries with an error on the arriving channel', () => {
    const node = new MeridianNode('O', null);
    const channel = new FakeDataChannel();
    wire(node, channel, 'P');

    for (const type of [
      MESSAGE_TYPES.QUERY_FORWARD,
      MESSAGE_TYPES.LEADER_QUERY_FORWARD,
      MESSAGE_TYPES.CONSTRAINT_QUERY_FORWARD
    ]) {
      feed(channel, {
        type,
        query: { queryId: 'q-' + type, hopCount: 33, requesterDc: null }
      });
      const reply = lastSent(channel);
      expect(reply.type).toBe(MESSAGE_TYPES.QUERY_RESULT);
      expect(reply.error).toBe('Max hops exceeded');
      expect(reply.queryId).toBe('q-' + type);
    }
    dispose(node);
  });
});

describe('closest-node query: two-hop scripted flow', () => {
  it('forwards past the beta threshold and resolves the originator via the back-route', async () => {
    const origin = new MeridianNode('O', null);
    const mid = new MeridianNode('M1', null);
    const edge = new MeridianNode('M2', null);

    const [chO, chM1] = wiredPair('O', 'M1', origin, mid);
    const [chM1b, chM2] = wiredPair('M1', 'M2', mid, edge);

    // Originator: T is 20ms away; M1 enrolled with rtt 12 (ring 4, probed
    // inside [rtt/2, rtt*2] of the measured ~20ms).
    knowTarget(origin, 'T', 20);
    await origin.addPeerToRing('M1', chO, 12);

    // M1: T is 4ms away for it too; M2 enrolled (rtt 4, ring 2).
    knowTarget(mid, 'T', 4);
    await mid.addPeerToRing('M2', chM1b, 4);

    // M2: knows T best (1ms), holds no ring members to ask.
    knowTarget(edge, 'T', 1);

    const result = await origin.findClosestNode('T', 'peer');

    expect(result.closestPeerId).toBe('M2');
    expect(result.closestRtt).toBeLessThan(40);
    expect(result.hopCount).toBe(2);
    expect(origin.pendingProbes.size).toBe(0);

    // Wire shapes along the chain.
    const probe = sentOfType(chO, MESSAGE_TYPES.PROBE_REQUEST)[0];
    expect(probe.target).toBe('T');
    expect(probe.targetType).toBe('peer');
    expect(probe.queryId).toBe(result.queryId);
    expect(typeof probe.probeId).toBe('string');

    const probeResult = sentOfType(chM1, MESSAGE_TYPES.PROBE_RESULT)[0];
    expect(probeResult.probeId).toBe(probe.probeId);
    expect(probeResult.rttMs).toBeGreaterThan(2);
    expect(probeResult.rttMs).toBeLessThan(40);

    // beta: M1's rtt (~4) < ours (~20) * 0.5, so the full query is forwarded.
    const forward = sentOfType(chO, MESSAGE_TYPES.QUERY_FORWARD)[0];
    expect(forward.query.queryId).toBe(result.queryId);
    expect(forward.query.target).toBe('T');
    expect(forward.query.hopCount).toBe(1);

    const forward2 = sentOfType(chM1b, MESSAGE_TYPES.QUERY_FORWARD)[0];
    expect(forward2.query.queryId).toBe(result.queryId);
    expect(forward2.query.hopCount).toBe(2);

    // The result travelled back hop-by-hop over the recorded return routes.
    expect(sentOfType(chM2, MESSAGE_TYPES.QUERY_RESULT)).toHaveLength(1);
    expect(sentOfType(chM1, MESSAGE_TYPES.QUERY_RESULT)).toHaveLength(1);
    expect(sentOfType(chO, MESSAGE_TYPES.QUERY_RESULT)).toHaveLength(0);
    expect(mid._queryBackRoutes.has(result.queryId)).toBe(false);

    dispose(origin, mid, edge);
  });
});

describe('leader election query (avg-RTT metric)', () => {
  it('probes with probe_request_avg, answers probe_result_avg, forwards past beta', async () => {
    const origin = new MeridianNode('O', null);
    const mid = new MeridianNode('M', null);
    const [chO, chM] = wiredPair('O', 'M', origin, mid);

    await origin.addPeerToRing('M', chO, 12);
    knowTarget(origin, 'T1', 10);
    knowTarget(origin, 'T2', 30);

    knowTarget(mid, 'T1', 2);
    knowTarget(mid, 'T2', 2);

    const result = await origin.findCentralLeader(['T1', 'T2']);
    expect(result.leaderId).toBe('M');
    expect(result.avgRtt).toBeGreaterThan(0);
    expect(result.avgRtt).toBeLessThan(40);

    const probe = sentOfType(chO, MESSAGE_TYPES.PROBE_REQUEST_AVG)[0];
    expect(probe.targets).toEqual(['T1', 'T2']);
    expect(typeof probe.probeId).toBe('string');
    expect(probe.queryId).toBe(result.queryId);

    const probeResult = sentOfType(chM, MESSAGE_TYPES.PROBE_RESULT_AVG)[0];
    expect(probeResult.probeId).toBe(probe.probeId);
    expect(probeResult.queryId).toBe(result.queryId);
    expect(probeResult.avgRttMs).toBeGreaterThan(0);
    expect(probeResult.avgRttMs).toBeLessThan(40);

    expect(sentOfType(chO, MESSAGE_TYPES.LEADER_QUERY_FORWARD)).toHaveLength(1);
    expect(origin.pendingProbes.size).toBe(0);
    dispose(origin, mid);
  });

  it('elects the avg-RTT winner and announces supernode_elected with clusterPeers', async () => {
    const winner = new MeridianNode('W', null);
    const peer = new MeridianNode('M', null);
    const [chW] = wiredPair('W', 'M', winner, peer);

    await winner.addPeerToRing('M', chW, 8);
    knowTarget(winner, 'x', 5);
    knowTarget(winner, 'y', 5);

    knowTarget(peer, 'x', 8);
    knowTarget(peer, 'y', 8);

    const elected = vi.fn();
    winner.handlers.onSupernodeElected = elected;

    const result = await electSupernode(winner, ['x', 'y']);

    // Our own avg (~5ms) is not beaten by beta * 5 = 2.5ms, so W wins.
    expect(result.leaderId).toBe('W');
    expect(result.avgRtt).toBeGreaterThan(3);
    expect(result.avgRtt).toBeLessThan(40);
    expect(winner.isSupernode).toBe(true);
    expect(winner.supernodeCluster.raftState).toBe('leader');
    expect(elected).toHaveBeenCalledWith('W');

    // SFU forwarding is live: a stream received from x is signalled to the
    // rest of the cluster (gated on isSupernode + forwarding being on).
    // The cluster is W + the probed x/y: y is signalled, x and ourselves
    // are skipped.
    forwardStreamToCluster(winner, 'x', { id: 's-x' });
    const relayed = sentOfType(
      winner.knownPeers.get('y').dataChannel,
      MESSAGE_TYPES.FORWARDED_STREAM
    );
    expect(relayed).toHaveLength(1);
    expect(relayed[0].sourcePeerId).toBe('x');
    expect(sentOfType(chW, MESSAGE_TYPES.FORWARDED_STREAM)).toHaveLength(0);

    const announcement = sentOfType(chW, MESSAGE_TYPES.SUPERNODE_ELECTED)[0];
    expect(announcement.type).toBe(MESSAGE_TYPES.SUPERNODE_ELECTED);
    expect(announcement.supernodeId).toBe('W');
    expect(announcement.clusterPeers).toEqual(['x', 'y']);

    // The peer enrolls under the announced leader through its dispatch.
    expect(peer.clusterLeader).toBe('W');
    expect(peer.supernodeCluster.raftState).toBe('follower');
    expect([...peer.supernodeCluster.members].sort()).toEqual([
      'M',
      'W',
      'x',
      'y'
    ]);
    dispose(winner, peer);
  });
});

describe('multi-constraint query', () => {
  it('answers a probe with distance max(0, rtt - maxLatencyMs)', async () => {
    const node = new MeridianNode('O', null);
    knowTarget(node, 'T', 10);
    const channel = new FakeDataChannel();

    handleProbeRequestConstraints(
      node,
      {
        probeId: 'p1',
        queryId: 'qc',
        constraints: [{ target: 'T', maxLatencyMs: 5 }]
      },
      channel
    );
    await tick(20);
    const result = lastSent(channel);
    expect(result.type).toBe(MESSAGE_TYPES.PROBE_RESULT_CONSTRAINTS);
    expect(result.probeId).toBe('p1');
    expect(result.queryId).toBe('qc');
    expect(result.satisfiesAll).toBe(false);
    expect(result.totalDistance).toBeGreaterThanOrEqual(4);
    expect(result.totalDistance).toBeLessThan(40);

    const satisfying = new FakeDataChannel();
    handleProbeRequestConstraints(
      node,
      {
        probeId: 'p2',
        queryId: 'qc2',
        constraints: [{ target: 'T', maxLatencyMs: 50 }]
      },
      satisfying
    );
    await tick(20);
    const ok = lastSent(satisfying);
    expect(ok.satisfiesAll).toBe(true);
    expect(ok.totalDistance).toBe(0);
    dispose(node);
  });

  it('accumulates satisfyingPeers from probed ring members and resolves', async () => {
    const origin = new MeridianNode('O', null);
    const member = new MeridianNode('M', null);
    const [chO, chM] = wiredPair('O', 'M', origin, member);

    // rtt 30 (ring 5) sits inside the [25, 100] candidate window of the
    // 50ms constraint, so M is probed; M satisfies all constraints too.
    await origin.addPeerToRing('M', chO, 30);
    knowTarget(origin, 'T1', 10);
    knowTarget(member, 'T1', 10);

    const result = await origin.findNodesSatisfyingConstraints([
      { target: 'T1', maxLatencyMs: 50 }
    ]);

    expect(result.satisfyingPeers.sort()).toEqual(['M', 'O']);
    expect(origin.pendingProbes.size).toBe(0);
    expect(sentOfType(chM, MESSAGE_TYPES.CONSTRAINT_QUERY_FORWARD)).toHaveLength(0);
    dispose(origin, member);
  });

  it('forwards constraint_query_forward to a strictly closer peer', async () => {
    const origin = new MeridianNode('O', null);
    const member = new MeridianNode('M', null);
    const [chO] = wiredPair('O', 'M', origin, member);

    await origin.addPeerToRing('M', chO, 5);
    knowTarget(origin, 'T1', 10); // our distance is 5: unsatisfied

    knowTarget(member, 'T1', 1);

    const result = await origin.findNodesSatisfyingConstraints([
      { target: 'T1', maxLatencyMs: 5 }
    ]);

    const forward = sentOfType(chO, MESSAGE_TYPES.CONSTRAINT_QUERY_FORWARD);
    expect(forward).toHaveLength(1);
    expect(forward[0].query.hopCount).toBe(1);
    expect(forward[0].query.constraints).toEqual([
      { target: 'T1', maxLatencyMs: 5 }
    ]);
    expect(result.satisfyingPeers).toEqual(['M']);
    expect(origin.pendingProbes.size).toBe(0);
    dispose(origin, member);
  });

  it('responds early at >= 5 satisfying peers without probing anyone', async () => {
    const node = new MeridianNode('O', null);
    const channel = new FakeDataChannel();
    const pending = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {}, 60000);
      // A refactor clearing the timer must not hang the worker process.
      timer.unref?.();
      node.pendingProbes.set('qc-early', {
        resolve,
        reject,
        timer
      });
    });

    const query = {
      queryId: 'qc-early',
      type: 'multi_constraint',
      constraints: [{ target: node.peerId, maxLatencyMs: 50 }],
      hopCount: 0,
      originator: node.peerId,
      requesterDc: null,
      timestamp: Date.now(),
      satisfyingPeers: ['p1', 'p2', 'p3', 'p4']
    };
    await routeConstraintQuery(node, query);

    const result = await pending;
    expect(result.satisfyingPeers).toHaveLength(5);
    expect(result.satisfyingPeers).toContain(node.peerId);
    expect(channel.sent).toHaveLength(0);
    expect(node.pendingProbes.size).toBe(0);
    dispose(node);
  });
});

describe('originator-side query_result handling', () => {
  it('resolves an originator query when query_result arrives', async () => {
    const node = new MeridianNode('self', null);
    const channel = new FakeDataChannel();
    wire(node, channel, 'peer');
    const pending = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {}, 60000);
      // A refactor clearing the timer must not hang the worker process.
      timer.unref?.();
      node.pendingProbes.set('q1', {
        resolve,
        reject,
        timer
      });
    });

    feed(channel, {
      type: MESSAGE_TYPES.QUERY_RESULT,
      queryId: 'q1',
      closestPeerId: 'peer-close'
    });

    await expect(pending).resolves.toMatchObject({
      closestPeerId: 'peer-close'
    });
    expect(node.pendingProbes.has('q1')).toBe(false);
    node.shutdown();
  });
});