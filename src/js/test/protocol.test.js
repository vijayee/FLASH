import { describe, expect, it, vi } from 'vitest';
import { MESSAGE_TYPES, MeridianNode } from '../src/index.js';
import { measureRttOverDataChannel } from '../src/rtt.js';
import {
  FakeDataChannel,
  pairChannels,
  wiredPair,
  makeMember,
  wire,
  feed,
  lastSent,
  tick,
  dispose
} from './helpers.js';

function makePair(aId, bId) {
  const a = new MeridianNode(aId, null);
  const b = new MeridianNode(bId, null);
  const [chA, chB] = wiredPair(aId, bId, a, b);
  return { a, b, chA, chB };
}

describe('two-peer DataChannel protocol', () => {
  it('answers PING over channels wired before ring enrollment (deadlock regression)', async () => {
    const { a, b, chA, chB } = makePair('A', 'B');
    // Neither peer is enrolled anywhere yet: the wired responder must already
    // answer, or the very first RTT measurement of a handshake would deadlock.
    const rtt = await measureRttOverDataChannel(chA);
    expect(rtt).toBeGreaterThanOrEqual(0);

    const pong = JSON.parse(chB.sent[0]);
    const ping = JSON.parse(chA.sent[0]);
    expect(pong.type).toBe(MESSAGE_TYPES.PONG);
    expect(pong.id).toBe(ping.id);
    expect(pong.t).toBe(ping.t);

    // Symmetry: B can measure A over the same pipes.
    const rttBack = await measureRttOverDataChannel(chB);
    expect(rttBack).toBeGreaterThanOrEqual(0);
    dispose(a, b);
  });

  it('gossips {type, senderId, timestamp, ringSamples} over the wire', async () => {
    const { a, b, chA } = makePair('A', 'B');
    await a.addPeerToRing('B', chA, 5);

    await a.runGossipCycle();

    // The gossip A sent travels over A's side of the pipe.
    expect(chA.sent).toHaveLength(1);
    const gossip = lastSent(chA);
    expect(gossip.type).toBe(MESSAGE_TYPES.GOSSIP);
    expect(gossip.senderId).toBe('A');
    expect(typeof gossip.timestamp).toBe('number');
    // B was enrolled with rtt 5, i.e. into ring 3: the sample reflects
    // that ring's index.
    expect(gossip.ringSamples).toEqual({ 3: 'B' });
    expect(a.lastGossipTime).toBe(gossip.timestamp);
    dispose(a, b);
  });

  it('handles gossip: re-measures the sender and refreshes lastSeen of sampled peers', async () => {
    const { a, b, chA, chB } = makePair('A', 'B');
    await a.addPeerToRing('B', chA, 5);
    await b.addPeerToRing('A', chB, 500);

    const before = b.knownPeers.get('A');
    // Backdate so the dispatch's lastSeen refresh is observable even when
    // enrollment and gossip land in the same millisecond.
    const oldLastSeen = Date.now() - 1000;
    before.lastSeen = oldLastSeen;
    feed(chB, {
      type: MESSAGE_TYPES.GOSSIP,
      senderId: 'A',
      timestamp: Date.now(),
      ringSamples: { 0: 'B' }
    });
    await tick(10);

    // B re-measured A over its channel (A's responder answers PING), so the
    // stale enrolled rtt of 500 was replaced by the measured one.
    const known = b.knownPeers.get('A');
    expect(known.rtt).toBeLessThan(50);
    expect(known.lastSeen).toBeGreaterThan(oldLastSeen);
    // The only sample is B itself: no connection attempts were started.
    expect(b.pendingConnections.size).toBe(0);
    dispose(a, b);
  });

  it('handles peer_leaving: strips the peer from rings, promotes a secondary, fires onPeerDisconnected', async () => {
    const b = new MeridianNode('B', null);
    const [, chB] = pairChannels();
    wire(b, chB, 'A');
    await b.addPeerToRing('A', chB, 5);

    // Fill ring 0 to the primary cap and add one secondary candidate.
    for (let i = 0; i < 7; i++) {
      b.rings[0].primaryMembers.push(
        makeMember('filler' + i, new FakeDataChannel(), 5)
      );
    }
    b.rings[0].secondaryMembers.push(
      makeMember('candidate', new FakeDataChannel(), 5)
    );
    const onDisconnected = vi.fn();
    b.handlers.onPeerDisconnected = onDisconnected;

    feed(chB, { type: MESSAGE_TYPES.PEER_LEAVING, senderId: 'A' });
    await tick(5);

    const primaries = b.rings[0].primaryMembers.map((m) => m.peerId);
    expect(primaries).not.toContain('A');
    expect(primaries).toContain('candidate');
    expect(primaries).toHaveLength(8);
    expect(b.rings[0].secondaryMembers.map((m) => m.peerId)).toEqual([]);
    expect(b.knownPeers.get('A').status).toBe('failed');
    expect(onDisconnected).toHaveBeenCalledWith('A');
    dispose(b);
  });

  it('treats a failing gossip send as a peer failure', async () => {
    const a = new MeridianNode('A', null);
    const channel = new FakeDataChannel();
    channel.send = () => {
      throw new Error('channel dead');
    };
    await a.addPeerToRing('B', channel, 5);
    const onDisconnected = vi.fn();
    a.handlers.onPeerDisconnected = onDisconnected;

    await a.runGossipCycle();

    expect(onDisconnected).toHaveBeenCalledWith('B');
    expect(a.rings[0].primaryMembers).toHaveLength(0);
    dispose(a);
  });
});