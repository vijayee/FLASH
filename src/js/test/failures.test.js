import { describe, expect, it, vi } from 'vitest';
import { MESSAGE_TYPES, MERIDIAN_CONFIG, MeridianNode } from '../src/index.js';
import { pruneStalePeers } from '../src/failures.js';
import {
  FakeDataChannel,
  pairChannels,
  makeMember,
  wire,
  sentOfType,
  tick,
  dispose
} from './helpers.js';

const GOSSIP_PERIOD = MERIDIAN_CONFIG.gossipPeriodMs;

function makeKnown(node, peerId, dataChannel, lastSeen, status = 'connected') {
  node.knownPeers.set(peerId, {
    peerId,
    dataChannel,
    rtt: 5,
    lastSeen,
    isSupernode: false,
    ringIndex: 0,
    status
  });
}

describe('_handlePeerFailure core cleanup', () => {
  it('removes the peer from ALL rings, promotes secondaries up to nodesPerRing, closes the pc', async () => {
    const node = new MeridianNode('self', null);
    const pc = { close: vi.fn() };
    node._peerConnections.set('X', pc);
    const channelX = new FakeDataChannel();
    await node.addPeerToRing('X', channelX, 5);

    // Pad ring 0 to the primary cap and park two secondary candidates.
    for (let i = 0; i < 7; i++) {
      node.rings[0].primaryMembers.push(
        makeMember('pad' + i, new FakeDataChannel())
      );
    }
    node.rings[0].secondaryMembers.push(
      makeMember('s1', new FakeDataChannel())
    );
    node.rings[0].secondaryMembers.push(
      makeMember('s2', new FakeDataChannel())
    );
    // X also sits as a secondary in ring 3: failure must strip BOTH places.
    node.rings[3].secondaryMembers.push(makeMember('X', new FakeDataChannel()));
    makeKnown(node, 'X', channelX, Date.now());

    const onDisconnected = vi.fn();
    node.handlers.onPeerDisconnected = onDisconnected;

    node._handlePeerFailure('X');
    await tick(10);

    const primaries = node.rings[0].primaryMembers.map((m) => m.peerId);
    expect(primaries).not.toContain('X');
    expect(primaries).toHaveLength(8); // refilled up to nodesPerRing
    expect(primaries).toContain('s1');
    expect(node.rings[0].secondaryMembers.map((m) => m.peerId)).toEqual([
      's2'
    ]);
    expect(node.rings[3].secondaryMembers.map((m) => m.peerId)).toEqual([]);

    const known = node.knownPeers.get('X');
    expect(known.status).toBe('failed');
    expect(known.dataChannel).toBeNull();

    expect(pc.close).toHaveBeenCalledTimes(1);
    expect(node._peerConnections.has('X')).toBe(false);
    expect(onDisconnected).toHaveBeenCalledWith('X');
    dispose(node);
  });

  it('ignores failures for unknown peers and for ourselves', async () => {
    const node = new MeridianNode('self', null);
    const onDisconnected = vi.fn();
    node.handlers.onPeerDisconnected = onDisconnected;

    node._handlePeerFailure('');
    node._handlePeerFailure(node.peerId);

    expect(onDisconnected).not.toHaveBeenCalled();
    dispose(node);
  });
});

describe('supernode re-election after a leader fails', () => {
  it('runs a re-election only when the failed peer was the cluster leader', async () => {
    const node = new MeridianNode('self', null);
    const peerNode = new MeridianNode('M', null);
    const [chSelf, chPeer] = pairChannels();
    wire(node, chSelf, 'M');
    wire(peerNode, chPeer, 'self');
    // Enrolled far away (ring 6, rtt 50): measured avg to the candidates
    // (~0-1ms) cannot reach into ring 6's candidate window, so our own
    // measured avg wins the central-leader election.
    await node.addPeerToRing('M', chSelf, 50);
    node.clusterLeader = 'L';
    makeKnown(node, 'L', new FakeDataChannel(), Date.now());

    const elected = vi.fn();
    node.handlers.onSupernodeElected = elected;

    node._handlePeerFailure('L');
    await tick(20);

    expect(node.isSupernode).toBe(true);
    expect(node.clusterLeader).toBe('self');
    expect(node.supernodeCluster.raftState).toBe('leader');
    // The new leader announced itself on the wire with the cluster peers.
    const announcement = sentOfType(chSelf, MESSAGE_TYPES.SUPERNODE_ELECTED)[0];
    expect(announcement.supernodeId).toBe('self');
    expect(announcement.clusterPeers).toEqual(['M']);
    expect(elected).toHaveBeenCalledWith('self');
    dispose(node);
  });

  it('does nothing when the failed peer was not the cluster leader', async () => {
    const node = new MeridianNode('self', null);
    const channel = new FakeDataChannel();
    await node.addPeerToRing('X', channel, 50);
    makeKnown(node, 'X', channel, Date.now());
    node.clusterLeader = 'someone-else';

    node._handlePeerFailure('X');
    await tick(20);

    expect(node.isSupernode).toBe(false);
    expect(node.supernodeCluster).toBeNull();
    expect(node.clusterLeader).toBe('someone-else');
    expect(node.pendingProbes.size).toBe(0);
    dispose(node);
  });

  it('forgets a dead leader when nobody is left to elect over', async () => {
    const node = new MeridianNode('self', null);
    node.clusterLeader = 'L';

    node._handlePeerFailure('L');
    await tick(10);

    expect(node.clusterLeader).toBeNull();
    expect(node.isSupernode).toBe(false);
    dispose(node);
  });
});

describe('removeRaftClusterMember (raft bookkeeping on member failure)', () => {
  it('only a supernode drops the member and replicates a cluster_membership leave', async () => {
    const node = new MeridianNode('self', null);
    node.isSupernode = true;
    node._initRaftState(['X', 'Y']);
    const [chX, chY] = pairChannels();
    wire(node, chX, 'X');
    wire(node, chY, 'Y');
    await node.addPeerToRing('X', chX, 5);
    await node.addPeerToRing('Y', chY, 5);

    node._handlePeerFailure('X');
    await tick(5);

    const cluster = node.supernodeCluster;
    expect(cluster.members).not.toContain('X');
    expect(cluster.nextIndex.has('X')).toBe(false);
    const entry = cluster.log[0];
    expect(entry.command).toEqual({
      type: 'cluster_membership',
      action: 'leave',
      peerId: 'X'
    });
    // The leave is replicated to the surviving member over the wire.
    const ae = sentOfType(chY, MESSAGE_TYPES.RAFT_APPEND_ENTRIES)[0];
    expect(ae.entries).toEqual([
      { term: 0, index: 0, command: entry.command }
    ]);
    dispose(node);
  });

  it('a non-supernode keeps its cluster intact on peer failure', async () => {
    const node = new MeridianNode('self', null);
    node._initRaftState(['X', 'Y']); // leader raft state but NOT isSupernode
    const [chX] = pairChannels();
    wire(node, chX, 'X');
    await node.addPeerToRing('X', chX, 5);
    makeKnown(node, 'X', chX, Date.now());

    node._handlePeerFailure('X');
    await tick(5);

    expect(node.supernodeCluster.members).toContain('X');
    expect(node.supernodeCluster.log).toHaveLength(0);
    dispose(node);
  });
});

describe('pruneStalePeers', () => {
  it('fails peers silent for over three gossip periods; fresh peers are untouched', () => {
    const node = new MeridianNode('self', null);
    const onDisconnected = vi.fn();
    node.handlers.onPeerDisconnected = onDisconnected;

    makeKnown(node, 'stale', new FakeDataChannel(), Date.now() - (GOSSIP_PERIOD * 3 + 5000));
    makeKnown(node, 'boundary', new FakeDataChannel(), Date.now() - (GOSSIP_PERIOD * 3 - 10000));
    makeKnown(node, 'fresh', new FakeDataChannel(), Date.now());
    makeKnown(
      node,
      'already-disconnected',
      new FakeDataChannel(),
      Date.now() - (GOSSIP_PERIOD * 5),
      'failed'
    );
    makeKnown(node, 'no-channel', null, Date.now() - (GOSSIP_PERIOD * 5));

    pruneStalePeers(node);

    expect(onDisconnected).toHaveBeenCalledTimes(1);
    expect(onDisconnected).toHaveBeenCalledWith('stale');
    expect(node.knownPeers.get('stale').status).toBe('failed');
    expect(node.knownPeers.get('boundary').status).toBe('connected');
    expect(node.knownPeers.get('fresh').status).toBe('connected');
    expect(node.knownPeers.get('already-disconnected').status).toBe('failed');
    expect(node.knownPeers.get('no-channel').status).toBe('connected');
  });
});