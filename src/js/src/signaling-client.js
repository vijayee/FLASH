/**
 * WebSocket bootstrap per spec §4.2. Owns the signaling socket, performs
 * registration + initial peer discovery, and relays every signaling message
 * into the owning MeridianNode's handler methods.
 */
export class SignalingClient {
  constructor(peerId) {
    this.peerId = peerId;
    this.ws = null;
    this._node = null;
  }

  /**
   * Connects, registers, and requests the initial peer list. Resolves once
   * the first peers_list reply is processed.
   */
  connect(signalServerUrl, node) {
    this._node = node;
    const ws = new WebSocket(signalServerUrl);
    this.ws = ws;

    return new Promise((resolve, reject) => {
      let settled = false;
      const settle = (fn, value) => {
        if (settled) return;
        settled = true;
        fn(value);
      };

      ws.onopen = () => {
        // Publish the live socket before registering: peers_list processing
        // may trigger offers/ICE immediately, which must reach the server.
        node.signalChannel = ws;
        ws.send(JSON.stringify({ type: 'register', peerId: this.peerId }));
        ws.send(JSON.stringify({ type: 'get_peers', senderId: this.peerId }));
      };

      ws.onmessage = (event) => {
        let msg;
        try {
          msg = JSON.parse(event.data);
        } catch {
          return;
        }
        if (!msg || typeof msg.type !== 'string') return;

        switch (msg.type) {
          case 'peers_list':
            node.handlePeersList(msg);
            settle(resolve, msg);
            break;

          case 'connect_offer':
            if (msg.target === this.peerId) {
              node.handleIncomingConnection(msg).catch(() => {});
            }
            break;

          case 'connect_answer':
            if (msg.target === this.peerId) {
              node.handleConnectAnswer(msg);
            }
            break;

          case 'probe_offer':
            if (msg.target === this.peerId) {
              node.handleProbeOffer(msg).catch(() => {});
            }
            break;

          case 'probe_answer':
            if (msg.target === this.peerId) {
              node.handleProbeAnswer(msg);
            }
            break;

          case 'ice_candidate':
            if (msg.target === this.peerId) {
              node.handleIceCandidate(msg);
            }
            break;
        }
      };

      ws.onerror = () => {
        settle(reject, new Error('Signaling WebSocket error'));
      };
      ws.onclose = () => {
        settle(reject, new Error('Signaling connection closed before bootstrap completed'));
      };
    });
  }

  send(message) {
    if (this.ws && this.ws.readyState === this.ws.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }

  close() {
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        // Already closed.
      }
    }
  }
}