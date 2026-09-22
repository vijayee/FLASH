import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';
import WebSocket, { WebSocketServer } from 'ws';

const MAX_PEER_LIST = 20;

export class SignalingServer {
  constructor() {
    this.peers = new Map(); // peerId -> WebSocket
    this.httpServer = createServer((req, res) => {
      res.writeHead(426);
      res.end();
    });
    this.wss = new WebSocketServer({ server: this.httpServer, maxPayload: 1_000_000 });
    this.wss.on('connection', (ws) => {
      // A frame-level protocol violation emits 'error'; without a listener it
      // escapes as an uncaught exception and takes the process down.
      ws.on('error', () => ws.terminate());
      ws.on('message', (data) => this.handleMessage(ws, data.toString()));
      ws.on('close', () => this.handleDisconnect(ws));
    });
  }

  listen(port = 0) {
    return new Promise((resolve, reject) => {
      this.httpServer.once('error', reject);
      this.httpServer.listen(port, () => {
        this.httpServer.off('error', reject);
        resolve(this.httpServer.address().port);
      });
    });
  }

  async close() {
    // Upgraded sockets stay in the http server's connection set, so it hangs
    // on close() until every socket is destroyed outright.
    for (const client of this.wss.clients) client.terminate();
    this.wss.close();
    await new Promise((resolve) => this.httpServer.close(resolve));
  }

  handleMessage(ws, data) {
    let message;
    try {
      message = JSON.parse(data);
    } catch {
      return;
    }

    switch (message?.type) {
      case 'register':
        if (typeof message.peerId === 'string' && message.peerId.length > 0) {
          if (
            ws.peerId !== undefined &&
            ws.peerId !== message.peerId &&
            this.peers.get(ws.peerId) === ws
          ) {
            this.peers.delete(ws.peerId);
          }
          this.peers.set(message.peerId, ws);
          ws.peerId = message.peerId;
        }
        break;

      case 'connect_offer':
      case 'connect_answer':
      case 'probe_offer':
      case 'probe_answer':
      case 'ice_candidate':
        this.forward(message);
        break;

      case 'get_peers': {
        const peers = [...this.peers.keys()]
          .filter((id) => id !== message.senderId)
          .slice(0, MAX_PEER_LIST);
        ws.send(JSON.stringify({ type: 'peers_list', peers }));
        break;
      }

      case 'disconnect':
        if (typeof message.peerId === 'string' && this.peers.get(message.peerId) === ws) {
          this.peers.delete(message.peerId);
        }
        break;
    }
  }

  forward(message) {
    if (typeof message.target !== 'string') return;
    const targetWs = this.peers.get(message.target);
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      targetWs.send(JSON.stringify(message));
    }
  }

  handleDisconnect(ws) {
    // A closed socket may only evict its own registration; if the peerId was
    // already re-registered by a newer socket, leave that one alone.
    if (ws.peerId !== undefined && this.peers.get(ws.peerId) === ws) {
      this.peers.delete(ws.peerId);
    }
  }
}

const isMain =
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const server = new SignalingServer();
  const port = Number(process.env.PORT) || 8080;
  server
    .listen(port)
    .then(() => {
      console.log(`Signaling server listening on port ${port}`);
    })
    .catch((error) => {
      console.error(`Signaling server failed to listen on port ${port}:`, error);
      process.exit(1);
    });
}