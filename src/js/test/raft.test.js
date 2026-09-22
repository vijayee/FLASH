import { describe, expect, it, vi, afterEach } from 'vitest';
import { MESSAGE_TYPES, MeridianNode } from '../src/index.js';
import { replicateCommand, startElection, stopHeartbeats } from '../src/raft.js';
import {
  FakeDataChannel,
  wire,
  feed,
  lastSent,
  sentOfType,
  dispose
} from './helpers.js';

const AE = MESSAGE_TYPES.RAFT_APPEND_ENTRIES;
const AER = MESSAGE_TYPES.RAFT_APPEND_ENTRIES_RESPONSE;
const RV = MESSAGE_TYPES.RAFT_REQUEST_VOTE;
const RVR = MESSAGE_TYPES.RAFT_REQUEST_VOTE_RESPONSE;

function followerWithChannel() {
  const node = new MeridianNode('self', null);
  node._initRaftState(['L'], { initialState: 'follower', leaderId: 'L' });
  const channel = new FakeDataChannel();
  wire(node, channel, 'L');
  return { node, channel };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('raft AppendEntries (follower side, via the real dispatch)', () => {
  it('applies entries, answers success, and wires the state-machine hooks', () => {
    vi.useFakeTimers();
    const { node, channel } = followerWithChannel();
    node._updateStreamMetadata = vi.fn();
    node._handleTopologyChange = vi.fn();

    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: -1,
      prevLogTerm: 0,
      entries: [
        {
          term: 1,
          index: 0,
          command: { type: 'cluster_membership', action: 'join', peerId: 'p1' }
        }
      ],
      leaderCommit: 0
    });

    // Higher term deposed us into followerhood; the join was applied.
    const cluster = node.supernodeCluster;
    expect(cluster.currentTerm).toBe(1);
    expect(cluster.raftState).toBe('follower');
    expect(cluster.members).toContain('p1');
    expect(cluster.commitIndex).toBe(0);
    expect(cluster.log).toHaveLength(1);
    expect(lastSent(channel)).toMatchObject({
      type: AER,
      term: 1,
      success: true,
      lastLogIndex: 0
    });

    // A duplicate of an already-held entry is not appended twice.
    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: -1,
      prevLogTerm: 0,
      entries: [
        {
          term: 1,
          index: 0,
          command: { type: 'cluster_membership', action: 'join', peerId: 'p1' }
        }
      ],
      leaderCommit: 0
    });
    expect(cluster.log).toHaveLength(1);

    // Two more entries, one optional-hook command of each kind, committed
    // immediately (leaderCommit 2 clamps to log.length - 1 = 2).
    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: 0,
      prevLogTerm: 1,
      entries: [
        {
          term: 1,
          index: 1,
          command: { type: 'stream_metadata', streamId: 's1', metadata: { v: 3 } }
        },
        {
          term: 1,
          index: 2,
          command: { type: 'topology_change', change: { removed: 'pX' } }
        }
      ],
      leaderCommit: 2
    });
    expect(cluster.log).toHaveLength(3);
    expect(cluster.commitIndex).toBe(2);
    expect(node._updateStreamMetadata).toHaveBeenCalledWith('s1', { v: 3 });
    expect(node._handleTopologyChange).toHaveBeenCalledWith({ removed: 'pX' });

    // A consistency-check mismatch truncates the conflicting suffix.
    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: 1,
      prevLogTerm: 9,
      entries: [],
      leaderCommit: 2
    });
    expect(cluster.log).toHaveLength(1);
    expect(lastSent(channel)).toMatchObject({
      type: AER,
      term: 1,
      success: false,
      lastLogIndex: 0
    });
    dispose(node);
  });

  it('clamps commitIndex to min(leaderCommit, log.length - 1) and applies leaves', () => {
    vi.useFakeTimers();
    const { node, channel } = followerWithChannel();

    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: -1,
      prevLogTerm: 0,
      entries: [
        {
          term: 1,
          index: 0,
          command: { type: 'cluster_membership', action: 'join', peerId: 'p1' }
        }
      ],
      leaderCommit: 0
    });
    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: 0,
      prevLogTerm: 1,
      entries: [
        {
          term: 1,
          index: 1,
          command: { type: 'cluster_membership', action: 'leave', peerId: 'p1' }
        }
      ],
      leaderCommit: 1
    });
    let cluster = node.supernodeCluster;
    expect(cluster.commitIndex).toBe(1);
    expect(cluster.members).not.toContain('p1');
    expect(cluster.nextIndex.has('p1')).toBe(false);

    // A leaderCommit beyond our own log clamps to log.length - 1 (2 here,
    // never 50) — and everything held up to that clamp is applied.
    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: 1,
      prevLogTerm: 1,
      entries: [
        {
          term: 1,
          index: 2,
          command: { type: 'cluster_membership', action: 'join', peerId: 'p9' }
        }
      ],
      leaderCommit: 50
    });
    cluster = node.supernodeCluster;
    expect(cluster.log).toHaveLength(3);
    expect(cluster.commitIndex).toBe(2);
    expect(cluster.members).toContain('p9');
    dispose(node);
  });
});

describe('raft leader log replication', () => {
  it('replicates commands and commits on a majority of matching responses', () => {
    vi.useFakeTimers();
    const node = new MeridianNode('self', null);
    node._updateStreamMetadata = vi.fn();
    node._initRaftState(['a', 'b']);
    const channelA = new FakeDataChannel();
    const channelB = new FakeDataChannel();
    wire(node, channelA, 'a');
    wire(node, channelB, 'b');
    node.knownPeers.set('a', { peerId: 'a', dataChannel: channelA });
    node.knownPeers.set('b', { peerId: 'b', dataChannel: channelB });

    replicateCommand(node, {
      type: 'stream_metadata',
      streamId: 's1',
      metadata: { v: 1 }
    });

    const cluster = node.supernodeCluster;
    expect(cluster.log).toHaveLength(1);
    expect(cluster.log[0].term).toBe(0);
    expect(cluster.log[0].index).toBe(0);

    const ae = sentOfType(channelA, AE)[0];
    expect(ae.term).toBe(0);
    expect(ae.leaderId).toBe('self');
    expect(ae.prevLogIndex).toBe(-1);
    expect(ae.entries).toEqual([
      { term: 0, index: 0, command: { type: 'stream_metadata', streamId: 's1', metadata: { v: 1 } } }
    ]);
    expect(ae.leaderCommit).toBe(-1);
    expect(sentOfType(channelB, AE)).toHaveLength(1);

    // The leader's own index (log.length - 1 = 0) plus one matched follower
    // is already a majority of 3: commit advances and the entry applies.
    feed(channelA, { type: AER, term: 0, success: true, lastLogIndex: 0 });
    expect(cluster.commitIndex).toBe(0);
    expect(cluster.nextIndex.get('a')).toBe(1);
    expect(cluster.matchIndex.get('a')).toBe(0);
    expect(node._updateStreamMetadata).toHaveBeenCalledTimes(1);
    expect(node._updateStreamMetadata).toHaveBeenCalledWith('s1', { v: 1 });

    // The second match changes nothing further.
    feed(channelB, { type: AER, term: 0, success: true, lastLogIndex: 0 });
    expect(cluster.commitIndex).toBe(0);
    expect(node._updateStreamMetadata).toHaveBeenCalledTimes(1);

    // A NACK backs the follower's nextIndex up for the retry heartbeat.
    feed(channelA, { type: AER, term: 0, success: false });
    expect(cluster.nextIndex.get('a')).toBe(0);
    dispose(node);
  });
});

describe('raft RequestVote rules', () => {
  it('denies stale terms and stale logs, grants fresh ones, records votedFor', () => {
    vi.useFakeTimers();
    const { node, channel } = followerWithChannel();
    // Give ourselves a committed entry at term 1 so log freshness bites.
    feed(channel, {
      type: AE,
      term: 1,
      leaderId: 'L',
      prevLogIndex: -1,
      prevLogTerm: 0,
      entries: [{ term: 1, index: 0, command: {} }],
      leaderCommit: 0
    });

    // Stale term: denied, stamped with our current term.
    feed(channel, {
      type: RV,
      term: 0,
      candidateId: 'c0',
      lastLogIndex: 9,
      lastLogTerm: 9
    });
    expect(lastSent(channel)).toMatchObject({
      type: RVR,
      term: 1,
      voteGranted: false
    });

    // Fresher-term candidate but a stale log: denied.
    feed(channel, {
      type: RV,
      term: 1,
      candidateId: 'c1',
      lastLogIndex: 9,
      lastLogTerm: 0
    });
    expect(lastSent(channel)).toMatchObject({ term: 1, voteGranted: false });
    expect(node.supernodeCluster.votedFor).toBeNull();

    // Equal term, equal-or-fresher log: granted and recorded.
    feed(channel, {
      type: RV,
      term: 1,
      candidateId: 'c1',
      lastLogIndex: 0,
      lastLogTerm: 1
    });
    expect(lastSent(channel)).toMatchObject({ term: 1, voteGranted: true });
    expect(node.supernodeCluster.votedFor).toBe('c1');

    // Already voted for someone else this term: denied.
    feed(channel, {
      type: RV,
      term: 1,
      candidateId: 'c2',
      lastLogIndex: 0,
      lastLogTerm: 1
    });
    expect(lastSent(channel)).toMatchObject({ term: 1, voteGranted: false });
    dispose(node);
  });
});

describe('raft leader election', () => {
  it('collects real votes to a majority; stale-term responses are ignored', () => {
    vi.useFakeTimers();
    const node = new MeridianNode('self', null);
    const elected = vi.fn();
    node.handlers.onSupernodeElected = elected;
    node._initRaftState(['a', 'b', 'c', 'd']); // 5 members, majority 3
    const channels = {};
    for (const peerId of ['a', 'b', 'c', 'd']) {
      const channel = new FakeDataChannel();
      wire(node, channel, peerId);
      node.knownPeers.set(peerId, { peerId, dataChannel: channel });
      channels[peerId] = channel;
    }

    stopHeartbeats(node); // silence the initial leader heartbeat loop
    startElection(node);

    const cluster = node.supernodeCluster;
    expect(cluster.raftState).toBe('candidate');
    expect(cluster.currentTerm).toBe(1);
    expect(cluster.votedFor).toBe('self');

    const rv = sentOfType(channels.a, RV)[0];
    expect(rv).toMatchObject({
      term: 1,
      candidateId: 'self',
      lastLogIndex: -1,
      lastLogTerm: 0
    });
    expect(sentOfType(channels.b, RV)).toHaveLength(1);

    // A response from an older term does not count towards this round: with
    // only self + a's stale grant counted, b's fresh grant must NOT yet be
    // a majority (a stale vote that counted would make 3 and flip leader).
    feed(channels.a, { type: RVR, term: 0, voteGranted: true });
    feed(channels.b, { type: RVR, term: 1, voteGranted: true });
    expect(cluster.raftState).toBe('candidate');

    // Real grants through the dispatch: self + 2 = 3 reaches the majority.
    feed(channels.b, { type: RVR, term: 1, voteGranted: true });
    expect(cluster.raftState).toBe('candidate');
    feed(channels.c, { type: RVR, term: 1, voteGranted: true });
    expect(cluster.raftState).toBe('leader');
    expect(cluster.leaderId).toBe('self');
    expect(cluster.heartbeatTimer).toBeTruthy();
    expect(elected).toHaveBeenCalledWith('self');

    // A late duplicate grant after winning changes nothing: no re-solicited
    // RequestVote, no timer re-arm, no term bump.
    const heartbeatAfterWin = cluster.heartbeatTimer;
    feed(channels.d, { type: RVR, term: 1, voteGranted: true });
    expect(cluster.raftState).toBe('leader');
    expect(cluster.currentTerm).toBe(1);
    expect(cluster.heartbeatTimer).toBe(heartbeatAfterWin);
    expect(sentOfType(channels.d, RV)).toHaveLength(1);
    dispose(node);
  });

  it('reaches a 3-member majority with the self vote plus one grant', () => {
    vi.useFakeTimers();
    const node = new MeridianNode('self', null);
    node._initRaftState(['a', 'b']);
    const channel = new FakeDataChannel();
    wire(node, channel, 'a');
    node.knownPeers.set('a', { peerId: 'a', dataChannel: channel });

    stopHeartbeats(node);
    startElection(node);
    feed(channel, { type: RVR, term: 1, voteGranted: true });

    expect(node.supernodeCluster.raftState).toBe('leader');
    expect(node.supernodeCluster.currentTerm).toBe(1);
    dispose(node);
  });
});

describe('raft step-down on a higher term', () => {
  it('demotes the leader, clears timers, forgets the vote, re-arms the election timer', () => {
    vi.useFakeTimers();
    const node = new MeridianNode('self', null);
    node._initRaftState(['a']);
    expect(node.supernodeCluster.raftState).toBe('leader');
    expect(node.supernodeCluster.heartbeatTimer).toBeTruthy();

    const channel = new FakeDataChannel();
    wire(node, channel, 'a');

    feed(channel, {
      type: AE,
      term: 5,
      leaderId: 'L',
      prevLogIndex: -1,
      prevLogTerm: 0,
      entries: [],
      leaderCommit: -1
    });

    const cluster = node.supernodeCluster;
    expect(cluster.currentTerm).toBe(5);
    expect(cluster.raftState).toBe('follower');
    expect(cluster.leaderId).toBe('L');
    expect(cluster.heartbeatTimer).toBeNull();
    expect(cluster.votedFor).toBeNull();
    expect(cluster.electionTimer).toBeTruthy();

    // An AppendEntries from an older term is rejected with our newer term.
    feed(channel, {
      type: AE,
      term: 4,
      leaderId: 'L',
      prevLogIndex: -1,
      prevLogTerm: 0,
      entries: [],
      leaderCommit: -1
    });
    expect(lastSent(channel)).toMatchObject({
      type: AER,
      term: 5,
      success: false
    });
    expect(cluster.raftState).toBe('follower');
    dispose(node);
  });
});
describe('supernode_elected routing (peers follow a leader; self wins init raft)', () => {
  it('joins as a follower when another peer won', () => {
    const follower = new MeridianNode('self', null);
    const followerChannel = new FakeDataChannel();
    wire(follower, followerChannel, 'peer-x');
    feed(followerChannel, {
      type: MESSAGE_TYPES.SUPERNODE_ELECTED,
      supernodeId: 'leader-x',
      clusterPeers: ['peer-a']
    });
    expect(follower.clusterLeader).toBe('leader-x');
    expect(follower.isSupernode).toBe(false);
    expect(follower.supernodeCluster.raftState).toBe('follower');
    expect(follower.supernodeCluster.leaderId).toBe('leader-x');
    // Membership always includes ourselves (quorum math).
    expect([...follower.supernodeCluster.members].sort()).toEqual([
      'leader-x',
      'peer-a',
      'self'
    ]);
    follower.shutdown();
  });

  it('initializes leader raft when our own announcement is echoed back', () => {
    const winner = new MeridianNode('self', null);
    const winnerChannel = new FakeDataChannel();
    wire(winner, winnerChannel, 'peer-y');
    feed(winnerChannel, {
      type: MESSAGE_TYPES.SUPERNODE_ELECTED,
      supernodeId: 'self',
      clusterPeers: ['peer-a', 'peer-b']
    });
    expect(winner.isSupernode).toBe(true);
    expect(winner.clusterLeader).toBe('self');
    expect(winner.supernodeCluster.raftState).toBe('leader');
    winner.shutdown();
  });
});
