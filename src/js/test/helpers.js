import { MESSAGE_TYPES } from '../src/message-types.js';

/**
 * Browser-like DataChannel double. All own properties are non-enumerable so
 * the channel stringifies like a real RTCDataChannel ({} when embedded in a
 * forwarded query payload).
 */
export class FakeDataChannel {
  constructor() {
    Object.defineProperties(this, {
      readyState: { value: 'open', writable: true },
      sent: { value: [], writable: true },
      peer: { value: null, writable: true },
      _listeners: { value: new Map() }
    });
  }

  addEventListener(type, fn) {
    const list = this._listeners.get(type) || [];
    list.push(fn);
    this._listeners.set(type, list);
  }

  removeEventListener(type, fn) {
    const list = this._listeners.get(type);
    if (list) this._listeners.set(type, list.filter((f) => f !== fn));
  }

  send(payload) {
    this.sent.push(payload);
    if (this.peer) this.peer._deliver(payload);
  }

  _deliver(payload) {
    for (const fn of [...(this._listeners.get('message') || [])]) {
      fn({ data: payload });
    }
  }

  close() {
    this.readyState = 'closed';
    for (const fn of [...(this._listeners.get('close') || [])]) fn();
  }
}

/** Ring-member record shaped the way ring-manager enrollment produces it. */
export function makeMember(peerId, dataChannel, rtt = 5) {
  return {
    peerId,
    dataChannel,
    rtt,
    lastProbed: Date.now(),
    iceCandidateType: 'host',
    natType: 'unknown',
    isSupernode: false,
    isFirewalled: false,
    joinedAt: Date.now()
  };
}

/** Pair of channels wired to each other: chA.send reaches chB listeners and vice versa. */
export function pairChannels() {
  const a = new FakeDataChannel();
  const b = new FakeDataChannel();
  a.peer = b;
  b.peer = a;
  return [a, b];
}

/** pairChannels plus both nodes wired to each other's side. */
export function wiredPair(aId, bId, aNode, bNode) {
  const [chA, chB] = pairChannels();
  wire(aNode, chA, bId);
  wire(bNode, chB, aId);
  return [chA, chB];
}

/**
 * Channel that answers PING with PONG after `delayMs` (measured RTT is
 * controllable without a second node). Listeners are fed locally.
 */
export function latencyChannel(delayMs) {
  const listeners = new Map();
  const sent = [];
  const deliver = (payload) => {
    for (const fn of [...(listeners.get('message') || [])]) fn({ data: payload });
  };
  const channel = {
    readyState: 'open',
    sent,
    addEventListener(type, fn) {
      const list = listeners.get(type) || [];
      list.push(fn);
      listeners.set(type, list);
    },
    removeEventListener(type, fn) {
      const list = listeners.get(type);
      if (list) listeners.set(type, list.filter((f) => f !== fn));
    },
    send(payload) {
      sent.push(payload);
      let msg;
      try {
        msg = JSON.parse(payload);
      } catch {
        return;
      }
      if (msg.type !== MESSAGE_TYPES.PING) return;
      setTimeout(() => {
        deliver(
          JSON.stringify({ type: MESSAGE_TYPES.PONG, id: msg.id, t: msg.t })
        );
      }, delayMs);
    },
    close() {
      channel.readyState = 'closed';
    }
  };
  return channel;
}

/** Wires the node's protocol dispatch onto a channel. */
export function wire(node, channel, peerId) {
  node._setupDataChannelHandlers(channel, peerId);
  return channel;
}

/** Feeds one protocol message into the channel's dispatch listeners. */
export function feed(channel, message) {
  channel._deliver(JSON.stringify(message));
}

export function lastSent(channel) {
  return JSON.parse(channel.sent[channel.sent.length - 1]);
}

export function sentOfType(channel, type) {
  return channel.sent
    .map((payload) => JSON.parse(payload))
    .filter((msg) => msg.type === type);
}

export function tick(ms = 5) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Shuts every node down so raft/gossip/query timers cannot leak between tests. */
export function dispose(...nodes) {
  for (const node of nodes) {
    try {
      node.shutdown();
    } catch {
      // Already torn down.
    }
  }
}

/**
 * RTCPeerConnection double for the signaling-driven handshake paths
 * (_establishConnectionToPeer / _answerOffer). `channel` is what
 * createDataChannel returns; default is a channel that never opens, so a
 * handshake stays in flight until the test pumps it.
 */
export class FakePeerConnection {
  constructor({ channel } = {}) {
    this.connectionState = 'new';
    this.onconnectionstatechange = null;
    this.onicecandidate = null;
    this.ondatachannel = null;
    this.localDescription = null;
    this.remoteDescription = null;
    this._channel =
      channel || {
        readyState: 'connecting',
        sent: [],
        addEventListener() {},
        removeEventListener() {},
        send() {},
        close() {}
      };
  }

  createDataChannel(_label) {
    this.dataChannel = this._channel;
    return this.dataChannel;
  }

  createOffer() {
    return Promise.resolve({ type: 'offer', sdp: 'offer-sdp' });
  }

  createAnswer() {
    return Promise.resolve({ type: 'answer', sdp: 'answer-sdp' });
  }

  setLocalDescription(description) {
    this.localDescription = description;
    return Promise.resolve();
  }

  setRemoteDescription(description) {
    this.remoteDescription = description;
    return Promise.resolve();
  }

  addIceCandidate() {
    return Promise.resolve();
  }

  close() {
    if (this.connectionState === 'closed') return;
    this.connectionState = 'closed';
    this.onconnectionstatechange?.();
  }

  /** Test pump: ICE reaches a terminal failure state. */
  _fail() {
    if (this.connectionState === 'closed') return;
    this.connectionState = 'failed';
    this.onconnectionstatechange?.();
  }
}