/**
 * One-shot STUN binding probe used by the forced-relay TURN spec (Task 10).
 *
 * Sends a single RFC 5389 Binding Request over UDP to host:port and waits
 * up to `timeoutMs` (default 3000) for ANY UDP reply. Exit 0 = answered,
 * exit 1 = timed out (or no reply) — the two outcomes turn.spec.ts needs
 * for its netns firewall evidence (STUN blocked, TURN reachable):
 *
 *   node udp-probe.mjs <host> <port> [timeoutMs]   # exit 0 iff a reply came
 */
import dgram from 'node:dgram';

const [host, portArg, timeoutArg] = process.argv.slice(2);
const port = Number(portArg);
const timeoutMs = Number(timeoutArg) || 3000;
if (!host || !Number.isFinite(port)) {
  console.error('usage: udp-probe.mjs <host> <port> [timeoutMs]');
  process.exit(2);
}

const MAGIC = Buffer.from([0x21, 0x12, 0xa4, 0x42]);
const msg = Buffer.alloc(20);
msg.writeUInt16BE(0x0001, 0); // Binding Request
MAGIC.copy(msg, 4);
Buffer.from('0123456789abcdef012345', 'hex').copy(msg, 8);

const socket = dgram.createSocket('udp4');
const fail = (why) => {
  console.error(`udp-probe: no reply from ${host}:${port} (${why})`);
  try { socket.close(); } catch {}
  process.exit(1);
};
socket.on('message', () => {
  console.log(`udp-probe: reply from ${host}:${port}`);
  try { socket.close(); } catch {}
  process.exit(0);
});
socket.on('error', (err) => fail(err.message));
socket.on('close', () => {});
socket.bind(() => {
  socket.send(msg, port, host, (err) => {
    if (err) fail(err.message);
  });
});
setTimeout(() => fail(`timeout ${timeoutMs}ms`), timeoutMs);