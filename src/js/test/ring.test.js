import { describe, expect, it, vi } from 'vitest';
import {
  MERIDIAN_CONFIG,
  MeridianNode,
  ConnectionPool
} from '../src/index.js';
import { calculateRingIndex, getRingBounds } from '../src/ring.js';
import { optimizeRing, moveMember } from '../src/ring-manager.js';
import { measureRttOverDataChannel } from '../src/rtt.js';
import {
  FakeDataChannel,
  latencyChannel,
  makeMember,
  tick
} from './helpers.js';

const CFG = MERIDIAN_CONFIG;

describe('ring index boundaries', () => {
  it('puts the innermost radius and anything below it in ring 0', () => {
    expect(calculateRingIndex(0.5, CFG)).toBe(0);
    expect(calculateRingIndex(1, CFG)).toBe(0);
  });

  it('grows exponentially with r=2: 2->1, 4->2, 8->3', () => {
    expect(calculateRingIndex(2, CFG)).toBe(1);
    expect(calculateRingIndex(4, CFG)).toBe(2);
    expect(calculateRingIndex(8, CFG)).toBe(3);
  });

  it('clamps huge rtt to the outermost ring (ringsPerNode - 1 = 8)', () => {
    expect(calculateRingIndex(1e9, CFG)).toBe(8);
    expect(calculateRingIndex(1e12, CFG)).toBe(8);
  });

  it('keeps rtt exactly on a ring radius inside that ring', () => {
    // Ring 1 spans (1, 2]: an rtt of exactly 2 stays in ring 1.
    expect(calculateRingIndex(2, CFG)).toBe(1);
    expect(calculateRingIndex(2.0001, CFG)).toBe(2);
    expect(calculateRingIndex(4, CFG)).toBe(2);
    expect(calculateRingIndex(4.0001, CFG)).toBe(3);
  });

  it('returns bounds with the outermost ring unbounded', () => {
    expect(getRingBounds(0, CFG)).toEqual({ inner: 0, outer: 1 });
    expect(getRingBounds(1, CFG)).toEqual({ inner: 1, outer: 2 });
    expect(getRingBounds(CFG.ringsPerNode - 1, CFG).outer).toBe(Infinity);
  });
});

describe('optimizeRing (hypervolume greedy selection)', () => {
  it('leaves the ring untouched when candidates fit the primary cap', () => {
    const node = new MeridianNodeShim();
    const ring = node.rings[0];
    const primaries = [makeMember('a', null, 1), makeMember('b', null, 2)];
    const secondaries = [makeMember('c', null, 3)];
    ring.primaryMembers = primaries;
    ring.secondaryMembers = secondaries;

    optimizeRing(0, node.rings, CFG);
    expect(ring.primaryMembers).toBe(primaries);
    expect(ring.secondaryMembers).toBe(secondaries);
  });

  it('caps primaries at nodesPerRing and demotes the least-diverse duplicate', () => {
    const node = new MeridianNodeShim();
    const ring = node.rings[0];
    // 10 candidates: p9 duplicates p1's rtt, p10 duplicates p8's rtt.
    const rtts = { p1: 1, p2: 2, p3: 3, p4: 4, p5: 5, p6: 6, p7: 7, p8: 8, p9: 1, p10: 8 };
    const members = Object.entries(rtts).map(([id, rtt]) => makeMember(id, null, rtt));
    ring.primaryMembers = members.slice(0, 8);
    ring.secondaryMembers = members.slice(8);

    optimizeRing(0, node.rings, CFG);

    expect(ring.primaryMembers).toHaveLength(CFG.nodesPerRing);
    expect(ring.secondaryMembers).toHaveLength(2);

    const primaryIds = ring.primaryMembers.map((m) => m.peerId);
    const secondaryIds = ring.secondaryMembers.map((m) => m.peerId);
    // Greedy hypervolume selection is deterministic. With candidates
    // 1..8 plus a duplicate of each extreme, the redundant low-rtt peers
    // (p1 and its duplicate p9) lose their marginal volume and are
    // demoted; the wider spread plus the high-rtt duplicate is kept.
    expect(primaryIds).toEqual(['p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p10']);
    expect(secondaryIds).toEqual(['p1', 'p9']);
  });
});

describe('moveMember promotes a secondary in the source ring', () => {
  it('backfills the vacated primary slot from secondaries', () => {
    const node = new MeridianNodeShim();
    const m = makeMember('moving', null, 1);
    const s = makeMember('secondary', null, 1);
    node.rings[0].primaryMembers = [m];
    node.rings[0].secondaryMembers = [s];
    node.knownPeers.set('moving', {
      peerId: 'moving',
      dataChannel: null,
      rtt: 1,
      lastSeen: Date.now(),
      isSupernode: false,
      ringIndex: 0,
      status: 'connected'
    });

    node._moveMember(m, 0, 1);

    expect(node.rings[0].primaryMembers.map((x) => x.peerId)).toEqual([
      'secondary'
    ]);
    expect(node.rings[0].secondaryMembers).toEqual([]);
    expect(node.rings[1].primaryMembers).toContain(m);
    expect(node.knownPeers.get('moving').ringIndex).toBe(1);
  });
});

describe('addPeerToRing relocation and caps', () => {
  it('re-enrolling with a new rtt moves the peer between rings', async () => {
    const node = new MeridianNode('self', null);
    await node.addPeerToRing('X', new FakeDataChannel(), 1);
    expect(node.rings[0].primaryMembers.map((m) => m.peerId)).toEqual(['X']);

    await node.addPeerToRing('X', new FakeDataChannel(), 100);
    expect(node.rings[0].primaryMembers).toEqual([]);
    expect(node.rings[7].primaryMembers.map((m) => m.peerId)).toEqual(['X']);
    expect(node.knownPeers.get('X').ringIndex).toBe(7);
  });

  it('caps primaries and secondaries when the ring is overfull', async () => {
    const node = new MeridianNode('self', null);
    for (let i = 0; i < CFG.nodesPerRing; i++) {
      await node.addPeerToRing('p' + i, new FakeDataChannel(), 1);
    }
    for (let i = 0; i < CFG.secondaryCandidates + 2; i++) {
      await node.addPeerToRing('s' + i, new FakeDataChannel(), 1);
    }

    const ring = node.rings[0];
    expect(ring.primaryMembers).toHaveLength(CFG.nodesPerRing);
    expect(ring.secondaryMembers).toHaveLength(CFG.secondaryCandidates);
    // The most recently added candidate survived as a primary (the greedy
    // pass runs once per over-cap enrollment and re-selects the set).
    expect(ring.primaryMembers.map((m) => m.peerId)).toContain('s5');
  });
});

describe('refreshRings re-measures and relocates members', () => {
  it('moves a member whose measured rtt now belongs in a farther ring', async () => {
    const node = new MeridianNode('self', null);
    // Enrolled in ring 0 with a stale rtt of 1; the channel really measures ~30ms.
    await node.addPeerToRing('M', latencyChannel(30), 1);
    expect(node.rings[0].primaryMembers).toHaveLength(1);

    await node.refreshRings();

    const m = node.rings
      .map((ring) => ring.primaryMembers.find((x) => x.peerId === 'M'))
      .find(Boolean);
    expect(m.rtt).toBeGreaterThanOrEqual(25);
    expect(m.rtt).toBeLessThan(300);
    expect(node.rings[0].primaryMembers).toHaveLength(0);
    // A loaded CI may inflate the 30ms timer, relocating M to a farther
    // ring than 5: assert the bookkeeping landed it consistently.
    const relocatedRing = node.rings.findIndex((ring) =>
      ring.primaryMembers.includes(m)
    );
    expect(relocatedRing).toBeGreaterThanOrEqual(5);
    expect(node.knownPeers.get('M').ringIndex).toBe(relocatedRing);
  });
});

describe('ConnectionPool', () => {
  it('evicts the least-recently-used entry and closes its pc', () => {
    const pool = new ConnectionPool(2);
    const pc1 = { close: vi.fn() };
    const pc2 = { close: vi.fn() };
    const pc3 = { close: vi.fn() };
    pool.add({ targetId: 'a', pc: pc1, lastUsed: 100 });
    pool.add({ targetId: 'b', pc: pc2, lastUsed: 200 });
    pool.add({ targetId: 'c', pc: pc3, lastUsed: 300 });

    expect(pc1.close).toHaveBeenCalledTimes(1);
    expect(pc2.close).not.toHaveBeenCalled();
    expect(pool.find('a')).toBeNull();
    expect(pool.find('b').targetId).toBe('b');
    expect(pool.find('c').targetId).toBe('c');

    // LRU, not FIFO: touching 'b' makes 'c' the victim of the next eviction.
    pool.find('b').lastUsed = 400;
    const pc4 = { close: vi.fn() };
    pool.add({ targetId: 'd', pc: pc4, lastUsed: 500 });
    expect(pc3.close).toHaveBeenCalledTimes(1);
    expect(pool.find('c')).toBeNull();
    expect(pool.find('b')).not.toBeNull();
  });

  it('cleanup(maxAge) removes only stale entries and closes them', () => {
    const pool = new ConnectionPool();
    const stalePc = { close: vi.fn() };
    const freshPc = { close: vi.fn() };
    pool.add({ targetId: 'stale', pc: stalePc, lastUsed: Date.now() - 500000 });
    pool.add({ targetId: 'fresh', pc: freshPc, lastUsed: Date.now() });

    pool.cleanup(120000);

    expect(stalePc.close).toHaveBeenCalledTimes(1);
    expect(freshPc.close).not.toHaveBeenCalled();
    expect(pool.find('stale')).toBeNull();
    expect(pool.find('fresh')).not.toBeNull();
  });
});

describe('measureRttOverDataChannel', () => {
  it('sends a ping shaped {type, id, t} and resolves on the correlated pong', async () => {
    const channel = latencyChannel(2);
    const rtt = await measureRttOverDataChannel(channel);
    expect(rtt).toBeGreaterThanOrEqual(0);

    const ping = JSON.parse(channel.sent[0]);
    expect(ping.type).toBe('ping');
    expect(typeof ping.id).toBe('string');
    expect(ping.id).toHaveLength(36);
    expect(typeof ping.t).toBe('number');
  });

  it('ignores a pong with a mismatched id, then resolves on the right one', async () => {
    const channel = new FakeDataChannel();
    const pending = measureRttOverDataChannel(channel);
    const ping = JSON.parse(channel.sent[0]);

    let settled = false;
    pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      }
    );

    channel._deliver(JSON.stringify({ type: 'pong', id: 'other', t: 0 }));
    await tick(5);
    expect(settled).toBe(false);

    channel._deliver(JSON.stringify({ type: 'pong', id: ping.id, t: 0 }));
    const rtt = await pending;
    expect(rtt).toBeGreaterThanOrEqual(0);
  });

  it('times out, rejects, and removes its message listener', async () => {
    vi.useFakeTimers();
    try {
      const channel = new FakeDataChannel();
      const removals = [];
      const originalRemove = channel.removeEventListener.bind(channel);
      channel.removeEventListener = (type, fn) => {
        removals.push([type, fn]);
        originalRemove(type, fn);
      };
      const pending = measureRttOverDataChannel(channel);
      const rejection = expect(pending).rejects.toThrow('RTT probe timeout');

      await vi.advanceTimersByTimeAsync(10000);
      await rejection;
      // The probe's own message listener is the only thing it registered,
      // so exactly one removal, of the 'message' listener it added.
      expect(removals).toHaveLength(1);
      expect(removals[0][0]).toBe('message');
    } finally {
      vi.useRealTimers();
    }
  });

  it('rejects immediately when the channel is closed', async () => {
    const channel = new FakeDataChannel();
    channel.close();
    await expect(measureRttOverDataChannel(channel)).rejects.toThrow(
      'DataChannel is closed'
    );
  });
});

/** Minimal node double for pure ring-array functions (no channels needed). */
class MeridianNodeShim {
  constructor() {
    this.config = CFG;
    this.rings = [];
    for (let i = 0; i < CFG.ringsPerNode; i++) {
      const bounds = getRingBounds(i, CFG);
      this.rings.push({
        index: i,
        innerRadius: bounds.inner,
        outerRadius: bounds.outer,
        primaryMembers: [],
        secondaryMembers: []
      });
    }
    this.knownPeers = new Map();
  }

  _moveMember(member, from, to) {
    moveMember(this, member, from, to);
  }
}