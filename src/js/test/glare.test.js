import { describe, expect, it, vi, afterEach } from 'vitest';
import { MeridianNode } from '../src/index.js';
import {
  FakePeerConnection,
  latencyChannel,
  sentOfType,
  dispose
} from './helpers.js';

const OFFER = { type: 'offer', sdp: 'offer-sdp' };

// The answering path constructs RTCSessionDescription; Node has no browser
// global for it, so stub the shape _answerOffer reads.
class RTCSessionDescriptionStub {
  constructor(init) {
    this.type = init?.type;
    this.sdp = init?.sdp;
  }
}
vi.stubGlobal('RTCSessionDescription', RTCSessionDescriptionStub);

afterEach(() => {
  vi.useRealTimers();
});

/** Fake signalChannel capturing every _sendSignaling payload, parsed. */
function signalingChannel() {
  const sent = [];
  return { sent, send: (payload) => sent.push(payload) };
}

function nodeWithFactory(peerId, pcs) {
  const node = new MeridianNode(peerId, signalingChannel());
  node.rtcFactory = {
    createPeerConnection: () => {
      const pc = new FakePeerConnection();
      pcs.push(pc);
      return pc;
    }
  };
  return node;
}

describe('signaling-driven handshakes (glare)', () => {
  it('refuses to dial a peer with a pending or open connection', async () => {
    const pcs = [];
    const node = new MeridianNode('self', signalingChannel());
    node.rtcFactory = {
      createPeerConnection: () => {
        const pc = new FakePeerConnection({ channel: latencyChannel(1) });
        pcs.push(pc);
        return pc;
      }
    };

    // Full handshake to B: offer sent, RTT measured over the opened
    // channel, peer enrolled.
    await node._establishConnectionToPeer('B');
    expect(node.knownPeers.has('B')).toBe(true);
    expect(sentOfType(node.signalChannel, 'connect_offer')).toHaveLength(1);

    // In-flight to C: a concurrent redial is refused (no second pc, no
    // second offer) and the original handshake still completes.
    const first = node._establishConnectionToPeer('C');
    await node._establishConnectionToPeer('C');
    expect(pcs).toHaveLength(2);
    await first;
    expect(pcs).toHaveLength(2);
    expect(sentOfType(node.signalChannel, 'connect_offer')).toHaveLength(2);

    // Already-open connection to B: refused too.
    await node._establishConnectionToPeer('B');
    expect(pcs).toHaveLength(2);
    expect(sentOfType(node.signalChannel, 'connect_offer')).toHaveLength(2);
    dispose(node);
  });

  it('glare: the lexicographically greater peer ignores the incoming offer', async () => {
    vi.useFakeTimers();
    const pcs = [];
    const greater = nodeWithFactory('B', pcs);

    // Our own offer to A is in flight (its channel never opens).
    const inflight = greater._establishConnectionToPeer('A');
    inflight.catch(() => {});
    await vi.advanceTimersByTimeAsync(0);
    expect(sentOfType(greater.signalChannel, 'connect_offer')).toHaveLength(1);
    expect(greater._pendingPeerConnections.get('A')).toBeTruthy();

    await greater.handleIncomingConnection({ senderId: 'A', sdp: OFFER });

    // Impolite peer: no second pc was created and no answer was sent; the
    // in-flight connection we started ourselves is untouched.
    expect(pcs).toHaveLength(1);
    expect(sentOfType(greater.signalChannel, 'connect_answer')).toHaveLength(0);
    expect(pcs[0].connectionState).toBe('new');
    dispose(greater);
  });

  it('glare: the lesser peer rolls back its own offer and answers', async () => {
    vi.useFakeTimers();
    const pcs = [];
    const lesser = nodeWithFactory('A', pcs);

    // Our own offer to B is in flight (polite peer: B outranks us).
    const inflight = lesser._establishConnectionToPeer('B');
    inflight.catch(() => {});
    await vi.advanceTimersByTimeAsync(0);
    expect(sentOfType(lesser.signalChannel, 'connect_offer')).toHaveLength(1);
    const stalePc = pcs[0];

    await lesser.handleIncomingConnection({ senderId: 'B', sdp: OFFER });

    // The in-flight offer was torn down via _failConnection and replaced by
    // the answering connection; exactly one answer went out.
    expect(stalePc.connectionState).toBe('closed');
    expect(pcs).toHaveLength(2);
    expect(lesser._pendingPeerConnections.get('B').pc).toBe(pcs[1]);
    expect(pcs[1].remoteDescription?.sdp).toBe('offer-sdp');

    const answers = sentOfType(lesser.signalChannel, 'connect_answer');
    expect(answers).toHaveLength(1);
    expect(answers[0].target).toBe('B');
    expect(answers[0].sdp.type).toBe('answer');
    expect(sentOfType(lesser.signalChannel, 'connect_offer')).toHaveLength(1);
    dispose(lesser);
  });
});