// Minimal RFC 5389 STUN binding responder for the netns latency rig.
//
// slirp4netns gives every netns the SAME guest address (10.0.2.100), so
// host candidates are self-referential and real STUN (public IP) cannot
// hairpin on the same host. This server answers binding requests on the
// HOST loopback — which slirp maps to the netns gateway 10.0.2.2 — so each
// peer's srflx candidate becomes 127.0.0.1:<slirp-mapped-port>, an address
// every other netns can reach through its own slirp. Chromium needs
// --allow-loopback-in-peer-connection to pair those. netem on tap0 still
// shapes every packet, so measured RTTs stay honest.
//
// Usage: node mini-stun.mjs [port]   (default 3478, bound to 127.0.0.1)
// MAPPED_IP env (default 10.0.2.2) is the advertised mapped address: the
// slirp gateway. Peers advertise srflx <gateway>:<port>, and every other
// netns reaches that same gateway->host-loopback path. Advertising the
// loopback (127.0.0.1, as literally observed) would be self-referential —
// each netns has its OWN loopback.

import dgram from 'node:dgram';

const port = Number(process.argv[2]) || 3478;
const MAPPED_IP = process.env.MAPPED_IP || '10.0.2.2';
const MAGIC = Buffer.from([0x21, 0x12, 0xa4, 0x42]);

const socket = dgram.createSocket('udp4');

socket.on('message', (msg, rinfo) => {
  if (msg.length < 20 || msg.readUInt16BE(0) !== 0x0001) return; // Binding Request
  if (!msg.subarray(4, 8).equals(MAGIC)) return;

  const tid = msg.subarray(8, 20);
  // XOR-MAPPED-ADDRESS attribute (0x0020): port ^ magic[0:2], ip ^ magic.
  // Advertise the slirp gateway IP (MAPPED_IP), not the literal source —
  // see the header note.
  const xport = rinfo.port ^ (MAGIC[0] << 8 | MAGIC[1]);
  const octets = MAPPED_IP.split('.').map(Number);
  const xip = octets.map((o, i) => o ^ MAGIC[i]);
  const attrBody = Buffer.alloc(8);
  attrBody.writeUInt8(0, 0);
  attrBody.writeUInt8(0x01, 1); // IPv4
  attrBody.writeUInt16BE(xport, 2);
  for (let i = 0; i < 4; i++) attrBody.writeUInt8(xip[i], 4 + i);

  const attrLen = 4 + attrBody.length; // type + length + value
  const header = Buffer.alloc(20);
  header.writeUInt16BE(0x0101, 0); // Binding Response
  header.writeUInt16BE(attrLen, 2);
  MAGIC.copy(header, 4);
  tid.copy(header, 8);

  const attr = Buffer.alloc(4);
  attr.writeUInt16BE(0x0020, 0); // XOR-MAPPED-ADDRESS
  attr.writeUInt16BE(attrBody.length, 2);

  socket.send(Buffer.concat([header, attr, attrBody]), rinfo.port, rinfo.address);
});

socket.on('error', (err) => {
  console.error('mini-stun:', err.message);
  process.exit(1);
});

socket.bind(port, '127.0.0.1', () => {
  console.log(`mini-stun: listening on 127.0.0.1:${port}`);
});