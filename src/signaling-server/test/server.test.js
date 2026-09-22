import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { SignalingServer } from '../server.js';

const servers = [];
const clients = [];

async function startServer() {
  const server = new SignalingServer();
  const port = await server.listen(0);
  servers.push(server);
  return { server, port };
}

function connect(port) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const received = [];
  const waiters = [];
  ws.on('message', (data) => {
    const message = JSON.parse(data.toString());
    if (waiters.length > 0) waiters.shift()(message);
    else received.push(message);
  });
  const open = new Promise((resolve, reject) => {
    ws.on('open', resolve);
    ws.on('error', reject);
  });
  const closed = new Promise((resolve) => ws.on('close', resolve));
  const next = () =>
    received.length > 0
      ? Promise.resolve(received.shift())
      : new Promise((resolve) => waiters.push(resolve));
  const send = (message) => ws.send(JSON.stringify(message));
  const client = { ws, open, closed, next, send };
  clients.push(client);
  return client;
}

async function register(client, peerId) {
  await client.open;
  client.send({ type: 'register', peerId });
  // Round-trip on the same socket: per-socket FIFO guarantees the register
  // was processed once the reply arrives, so later cross-socket relays
  // cannot race this registration.
  client.send({ type: 'get_peers', senderId: peerId });
  const reply = await client.next();
  if (reply.type !== 'peers_list') throw new Error(`register ack failed: ${reply.type}`);
}

async function getPeers(client, senderId) {
  client.send({ type: 'get_peers', senderId });
  const reply = await client.next();
  expect(reply.type).toBe('peers_list');
  return reply.peers;
}

// Server close handling is asynchronous, so poll until the peer list settles.
async function waitForPeers(client, senderId, predicate) {
  let peers = [];
  for (let i = 0; i < 100; i++) {
    peers = await getPeers(client, senderId);
    if (predicate(peers)) return peers;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return peers;
}

afterEach(async () => {
  while (servers.length > 0) await servers.pop().close();
  for (const client of clients.splice(0)) client.ws.terminate();
});

describe('signaling server', () => {
  it('registers peers and answers get_peers excluding the requester', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    const c = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');
    await register(c, 'peer-c');

    const peers = await getPeers(a, 'peer-a');
    expect([...peers].sort()).toEqual(['peer-b', 'peer-c']);
  });

  it('caps peers_list at 20 entries and still excludes the requester', async () => {
    const { port } = await startServer();
    await register(connect(port), 'self');
    for (let i = 0; i < 25; i++) {
      await register(connect(port), `peer-${i}`);
    }
    const me = connect(port);
    await register(me, 'me');

    const peers = await getPeers(me, 'me');
    expect(peers).toHaveLength(20);
    expect(peers).not.toContain('me');
  });

  it('relays connect_offer to the target only', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    const c = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');
    await register(c, 'peer-c');

    const offer = { type: 'connect_offer', target: 'peer-b', senderId: 'peer-a', sdp: { type: 'offer', sdp: 'v=0...' } };
    a.send(offer);

    expect(await b.next()).toEqual(offer);

    // C must not have received the offer; its next message is the peers_list reply.
    c.send({ type: 'get_peers', senderId: 'peer-c' });
    expect(await c.next()).toMatchObject({ type: 'peers_list' });
  });

  it('relays connect_answer back to the offer originator', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    const c = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');
    await register(c, 'peer-c');

    const answer = { type: 'connect_answer', target: 'peer-a', senderId: 'peer-b', sdp: { type: 'answer', sdp: 'v=0...' } };
    b.send(answer);

    expect(await a.next()).toEqual(answer);

    c.send({ type: 'get_peers', senderId: 'peer-c' });
    expect(await c.next()).toMatchObject({ type: 'peers_list' });
  });

  it('relays probe_offer and probe_answer', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');

    const offer = { type: 'probe_offer', target: 'peer-b', senderId: 'peer-a', sdp: { type: 'offer', sdp: 'v=0...' } };
    a.send(offer);
    expect(await b.next()).toEqual(offer);

    const answer = { type: 'probe_answer', target: 'peer-a', senderId: 'peer-b', sdp: { type: 'answer', sdp: 'v=0...' } };
    b.send(answer);
    expect(await a.next()).toEqual(answer);
  });

  it('relays ice candidates immediately, in order, unmodified', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');

    const candidate1 = { type: 'ice_candidate', target: 'peer-b', senderId: 'peer-a', candidate: { candidate: 'candidate:1', sdpMid: '0' } };
    const candidate2 = { type: 'ice_candidate', target: 'peer-b', senderId: 'peer-a', candidate: { candidate: 'candidate:2', sdpMid: '1' } };
    a.send(candidate1);
    a.send(candidate2);

    expect(await b.next()).toEqual(candidate1);
    expect(await b.next()).toEqual(candidate2);
  });

  it('silently ignores unknown or missing targets', async () => {
    const { port } = await startServer();
    const a = connect(port);
    await register(a, 'peer-a');

    a.send({ type: 'connect_offer', target: 'ghost', senderId: 'peer-a', sdp: {} });
    a.send({ type: 'ice_candidate', senderId: 'peer-a', candidate: {} });

    const peers = await getPeers(a, 'peer-a');
    expect(peers).toEqual([]);
  });

  it('removes a peer on a disconnect message', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');

    a.send({ type: 'disconnect', peerId: 'peer-a' });

    const peers = await waitForPeers(b, 'peer-b', (list) => list.length === 0);
    expect(peers).toEqual([]);
  });

  it('removes a peer when its socket closes without a disconnect message', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    const c = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');
    await register(c, 'peer-c');

    a.ws.close();
    await a.closed;

    const peers = await waitForPeers(b, 'peer-b', (list) => !list.includes('peer-a'));
    expect([...peers].sort()).toEqual(['peer-c']);
  });

  it('lets a newer socket take over a peerId and survive the old socket closing', async () => {
    const { port } = await startServer();
    const first = connect(port);
    const second = connect(port);
    const b = connect(port);
    await register(first, 'peer-a');
    await register(second, 'peer-a');
    await register(b, 'peer-b');

    first.ws.close();
    await first.closed;

    const peers = await waitForPeers(b, 'peer-b', (list) => list.includes('peer-a'));
    expect(peers).toContain('peer-a');

    const offer = { type: 'connect_offer', target: 'peer-a', senderId: 'peer-b', sdp: {} };
    b.send(offer);
    expect(await second.next()).toEqual(offer);
  });

  it('ignores malformed messages without crashing', async () => {
    const { port } = await startServer();
    const a = connect(port);
    await register(a, 'peer-a');

    a.ws.send('not json');
    a.ws.send(JSON.stringify({ type: 'register' }));

    const peers = await getPeers(a, 'peer-a');
    expect(peers).toEqual([]);
  });

  it('survives a socket that violates the WS protocol at the frame level', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');

    // Hand-roll a WebSocket upgrade, then follow it with a bogus frame
    // (reserved opcode 0x3) so the server-side receiver errors.
    const raw = net.connect(port, '127.0.0.1');
    await new Promise((resolve, reject) => {
      raw.once('error', reject);
      raw.once('connect', resolve);
    });
    raw.write(
      'GET / HTTP/1.1\r\n' +
        'Host: 127.0.0.1\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n' +
        'Sec-WebSocket-Version: 13\r\n' +
        '\r\n'
    );
    const handshake = await new Promise((resolve, reject) => {
      raw.once('error', reject);
      raw.once('data', resolve);
    });
    expect(handshake.toString().startsWith('HTTP/1.1 101')).toBe(true);
    raw.write(
      // FIN + reserved opcode 3, masked, empty payload.
      Buffer.from([0x83, 0x80, 0xde, 0xad, 0xbe, 0xef])
    );
    // The server replies with a close frame and waits up to 30s for our
    // closing reply, so just give it time to process the bad frame.
    await new Promise((resolve) => setTimeout(resolve, 50));
    raw.destroy();

    const offer = { type: 'connect_offer', target: 'peer-b', senderId: 'peer-a', sdp: {} };
    a.send(offer);
    expect(await b.next()).toEqual(offer);
  });

  it('ignores a disconnect message from a socket that no longer owns its peerId', async () => {
    const { port } = await startServer();
    const first = connect(port);
    const second = connect(port);
    const b = connect(port);
    await register(first, 'peer-a');
    await register(second, 'peer-a');
    await register(b, 'peer-b');

    first.send({ type: 'disconnect', peerId: 'peer-a' });

    const peers = await waitForPeers(b, 'peer-b', (list) => list.includes('peer-a'));
    expect(peers).toContain('peer-a');

    const offer = { type: 'connect_offer', target: 'peer-a', senderId: 'peer-b', sdp: {} };
    b.send(offer);
    expect(await second.next()).toEqual(offer);
  });

  it('evicts the previous registration when one socket re-registers under a new id', async () => {
    const { port } = await startServer();
    const a = connect(port);
    const b = connect(port);
    await register(a, 'ghost-a');
    await register(a, 'peer-a');
    await register(b, 'peer-b');

    expect(await getPeers(b, 'peer-b')).toEqual(['peer-a']);

    a.ws.close();
    await a.closed;

    const peers = await waitForPeers(b, 'peer-b', (list) => list.length === 0);
    expect(peers).toEqual([]);
  });

  it('writes a JSONL line for a relayed connect_offer when file logging is enabled', async () => {
    const dir = mkdtempSync(path.join(tmpdir(), 'signaling-log-'));
    const logFile = path.join(dir, 'signaling.jsonl');
    const server = new SignalingServer({ logFile });
    servers.push(server);
    const port = await server.listen(0);
    const a = connect(port);
    const b = connect(port);
    await register(a, 'peer-a');
    await register(b, 'peer-b');

    const offer = { type: 'connect_offer', target: 'peer-b', senderId: 'peer-a', sdp: { type: 'offer', sdp: 'v=0...' } };
    a.send(offer);
    expect(await b.next()).toEqual(offer);

    // The log stream writes asynchronously: poll the file for the relay line.
    const line = await pollForLine(logFile, (entry) => entry.event === 'relay' && entry.type === 'connect_offer');
    expect(line).toMatchObject({ event: 'relay', type: 'connect_offer', from: 'peer-a', to: 'peer-b' });
    expect(typeof line.ts).toBe('string');

    // register lines land too (one per registration).
    const lines = readLogLines(logFile);
    expect(lines.some((entry) => entry.event === 'register' && entry.peerId === 'peer-a')).toBe(true);
    expect(lines.some((entry) => entry.event === 'register' && entry.peerId === 'peer-b')).toBe(true);
  });
});

function readLogLines(file) {
  return readFileSync(file, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

// Stream writes are asynchronous; wait until the expected JSONL line lands.
async function pollForLine(file, predicate) {
  for (let i = 0; i < 200; i++) {
    if (existsSync(file)) {
      const hit = readLogLines(file).find(predicate);
      if (hit) return hit;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`expected JSONL line never appeared in ${file}`);
}
