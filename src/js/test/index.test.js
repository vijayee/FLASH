import { describe, expect, it, vi } from 'vitest';
import {
  MeridianNode,
  MERIDIAN_CONFIG,
  MESSAGE_TYPES,
  calculateRingIndex,
  ConnectionPool
} from '../src/index.js';
import { FakeDataChannel } from './helpers.js';

// Smoke file: construction, export shape, and one boundary case per API.
// The behavioral suites live in protocol/ring/query/raft/failures/glare.

describe('meridian-webrtc module graph (smoke)', () => {
  it('exposes the public API', () => {
    expect(typeof MeridianNode).toBe('function');
    expect(typeof ConnectionPool).toBe('function');
    expect(typeof calculateRingIndex).toBe('function');
    expect(MESSAGE_TYPES.PING).toBe('ping');
    expect(MESSAGE_TYPES.PEER_LEAVING).toBe('peer_leaving');
  });

  it('provides the default config per spec §2.1', () => {
    expect(MERIDIAN_CONFIG.ringsPerNode).toBe(9);
    expect(MERIDIAN_CONFIG.nodesPerRing).toBe(8);
    expect(MERIDIAN_CONFIG.secondaryCandidates).toBe(4);
    expect(MERIDIAN_CONFIG.innermostRingRadius).toBe(1);
    expect(MERIDIAN_CONFIG.ringMultiplicativeFactor).toBe(2);
    expect(MERIDIAN_CONFIG.routeAcceptanceThreshold).toBe(0.5);
    expect(MERIDIAN_CONFIG.probeTimeoutFactor).toBe(2);
    expect(MERIDIAN_CONFIG.gossipPeriodMs).toBe(30000);
    expect(MERIDIAN_CONFIG.ringReplacementPeriodMs).toBe(60000);
    expect(MERIDIAN_CONFIG.maxEphemeralConnections).toBe(10);
    expect(MERIDIAN_CONFIG.stunServers).toEqual(['stun:stun.l.google.com:19302']);
    expect(MERIDIAN_CONFIG.turnServers).toEqual([]);
    expect(MERIDIAN_CONFIG.maxHops).toBe(32);
    expect(MERIDIAN_CONFIG.ephemeralProbeTimeoutMs).toBe(5000);
    expect(MERIDIAN_CONFIG.queryTimeoutMs).toBe(30000);
  });

  it('clamps huge rtt into the outermost ring (boundary sweep in ring.test.js)', () => {
    expect(calculateRingIndex(1e9, MERIDIAN_CONFIG)).toBe(
      MERIDIAN_CONFIG.ringsPerNode - 1
    );
  });

  it('exposes the query-routing, media, and raft message types', () => {
    expect(MESSAGE_TYPES.QUERY_FORWARD).toBe('query_forward');
    expect(MESSAGE_TYPES.LEADER_QUERY_FORWARD).toBe('leader_query_forward');
    expect(MESSAGE_TYPES.CONSTRAINT_QUERY_FORWARD).toBe(
      'constraint_query_forward'
    );
    expect(MESSAGE_TYPES.PROBE_REQUEST).toBe('probe_request');
    expect(MESSAGE_TYPES.PROBE_RESULT).toBe('probe_result');
    expect(MESSAGE_TYPES.PROBE_REQUEST_AVG).toBe('probe_request_avg');
    expect(MESSAGE_TYPES.PROBE_RESULT_AVG).toBe('probe_result_avg');
    expect(MESSAGE_TYPES.PROBE_REQUEST_CONSTRAINTS).toBe(
      'probe_request_constraints'
    );
    expect(MESSAGE_TYPES.PROBE_RESULT_CONSTRAINTS).toBe(
      'probe_result_constraints'
    );
    expect(MESSAGE_TYPES.QUERY_RESULT).toBe('query_result');
    expect(MESSAGE_TYPES.MEDIA_OFFER).toBe('media_offer');
    expect(MESSAGE_TYPES.MEDIA_ANSWER).toBe('media_answer');
    expect(MESSAGE_TYPES.FORWARDED_STREAM).toBe('forwarded_stream');
    expect(MESSAGE_TYPES.MEDIA_CLOSE).toBe('media_close');
    expect(MESSAGE_TYPES.SUPERNODE_ELECTED).toBe('supernode_elected');
    expect(MESSAGE_TYPES.RAFT_APPEND_ENTRIES).toBe('raft_append_entries');
    expect(MESSAGE_TYPES.RAFT_APPEND_ENTRIES_RESPONSE).toBe(
      'raft_append_entries_response'
    );
    expect(MESSAGE_TYPES.RAFT_REQUEST_VOTE).toBe('raft_request_vote');
    expect(MESSAGE_TYPES.RAFT_REQUEST_VOTE_RESPONSE).toBe(
      'raft_request_vote_response'
    );
  });

  it('wires the DataChannel protocol responder only once', () => {
    const node = new MeridianNode('self', null);
    const channel = new FakeDataChannel();
    const addSpy = vi.spyOn(channel, 'addEventListener');
    node._setupDataChannelHandlers(channel, 'peer');
    node._setupDataChannelHandlers(channel, 'peer');
    expect(
      addSpy.mock.calls.filter(([type]) => type === 'message')
    ).toHaveLength(1);
    expect(addSpy.mock.calls.filter(([type]) => type === 'close')).toHaveLength(
      1
    );
  });

  it('shuts down cleanly, draining in-flight connection attempts', () => {
    const node = new MeridianNode('self', null);
    node.pendingConnections.add('peer-a');
    node.shutdown();
    expect(node.pendingConnections.size).toBe(0);
  });

  it('constructs a node with the default ring layout', () => {
    const node = new MeridianNode('self', null);
    expect(node.peerId).toBe('self');
    expect(node.rings).toHaveLength(MERIDIAN_CONFIG.ringsPerNode);
    expect(node.rings[0].primaryMembers).toEqual([]);
    expect(node.knownPeers).toBeInstanceOf(Map);
    expect(node.pendingProbes).toBeInstanceOf(Map);
    expect(node.pendingConnections).toBeInstanceOf(Set);
    expect(node.connectionPool.maxSize).toBe(MERIDIAN_CONFIG.maxEphemeralConnections);
    expect(node.handlers.onPeerDisconnected).toBeNull();
  });

  it('initializes a Raft leader cluster and clears it on shutdown', () => {
    const node = new MeridianNode('self', null);
    node._initRaftState(['peer-a', 'peer-b']);
    const cluster = node.supernodeCluster;
    expect(cluster.raftState).toBe('leader');
    expect(cluster.currentTerm).toBe(0);
    expect(cluster.votedFor).toBeNull();
    expect(cluster.log).toEqual([]);
    // 0-indexed log positions: -1 means nothing committed/applied yet.
    expect(cluster.commitIndex).toBe(-1);
    expect(cluster.leaderId).toBe('self');
    // Membership always includes ourselves (quorum math).
    expect([...cluster.members].sort()).toEqual(['peer-a', 'peer-b', 'self']);
    expect(cluster.nextIndex.get('peer-a')).toBe(0);
    expect(cluster.matchIndex.get('peer-b')).toBe(-1);
    // Completeness: both raft timers are actually armed.
    expect(cluster.heartbeatTimer).toBeTruthy();
    expect(cluster.electionTimer).toBeTruthy();

    node.shutdown();
    expect(node.supernodeCluster.heartbeatTimer).toBeNull();
    expect(node.supernodeCluster.electionTimer).toBeNull();
  });
});