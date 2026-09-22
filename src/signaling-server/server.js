import { createServer } from 'node:http';
import { createWriteStream, mkdirSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import WebSocket, { WebSocketServer } from 'ws';

const MAX_PEER_LIST = 20;

// Task 12: optional JSONL observability log. Enabled by `LOG=flash-signaling`
// (writes logs/signaling.jsonl under the cwd); path overridable via LOG_FILE.
// The SignalingServer class can also be given an explicit logFile option
// (used by the vitest suite and the e2e orchestrator).
const LOG_EVENT = 'flash-signaling';
const DEFAULT_LOG_FILE = 'logs/signaling.jsonl';

function logFileFromEnv() {
  if (process.env.LOG !== LOG_EVENT) return null;
  return process.env.LOG_FILE || DEFAULT_LOG_FILE;
}

export class SignalingServer {
  constructor({ logFile = logFileFromEnv() } = {}) {
    this.logFile = logFile ?? null;
    this.logStream = null;
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
    this.logStream?.end();
    this.logStream = null;
  }

  /**
   * Appends one JSONL line (with an ISO timestamp) to the log file. Lazily
   * creates the file's directory and stream; write errors are swallowed —
   * observability must never take the relay down.
   */
  log(event, fields = {}) {
    if (!this.logFile) return;
    if (!this.logStream) {
      mkdirSync(path.dirname(this.logFile), { recursive: true });
      this.logStream = createWriteStream(this.logFile, { flags: 'a' });
      this.logStream.on('error', () => {});
    }
    const line = JSON.stringify({ ts: new Date().toISOString(), event, ...fields });
    this.logStream.write(`${line}\n`);
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
          this.log('register', { peerId: message.peerId });
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
        this.log('peers_list', { to: message.senderId ?? null, count: peers.length });
        break;
      }

      case 'disconnect':
        if (typeof message.peerId === 'string' && this.peers.get(message.peerId) === ws) {
          this.peers.delete(message.peerId);
          this.log('disconnect', { peerId: message.peerId, reason: 'message' });
        }
        break;
    }
  }

  forward(message) {
    if (typeof message.target !== 'string') return;
    const targetWs = this.peers.get(message.target);
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      this.log('relay', {
        type: message.type,
        from: message.senderId ?? null,
        to: message.target,
        bytes: JSON.stringify(message).length,
      });
      targetWs.send(JSON.stringify(message));
    } else {
      this.log('relay_dropped', {
        type: message.type,
        from: message.senderId ?? null,
        to: message.target,
        reason: targetWs ? 'socket-not-open' : 'unknown-target',
      });
    }
  }

  handleDisconnect(ws) {
    // A closed socket may only evict its own registration; if the peerId was
    // already re-registered by a newer socket, leave that one alone.
    if (ws.peerId !== undefined && this.peers.get(ws.peerId) === ws) {
      this.peers.delete(ws.peerId);
      this.log('disconnect', { peerId: ws.peerId, reason: 'socket-closed' });
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
      if (server.logFile) console.log(`Signaling JSONL log: ${server.logFile}`);
    })
    .catch((error) => {
      console.error(`Signaling server failed to listen on port ${port}:`, error);
      process.exit(1);
    });
}