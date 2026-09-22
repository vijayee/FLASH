// Verifies a slirp4netns API unix socket end-to-end: connects, sends
// add_hostfwd, prints the response, then TCP-probes the forwarded host
// port to confirm delivery into the netns.
import { connect as unixConnect } from 'node:net';

const sockPath = process.argv[2];
const hostPort = Number(process.argv[3]);
const guestAddr = process.argv[4] || '10.0.2.100';
const guestPort = Number(process.argv[5]) || 9223;

if (!sockPath || !hostPort) {
  console.error('usage: node slirp-api-check.mjs <apiSock> <hostPort> [guestAddr] [guestPort]');
  process.exit(2);
}

const req = JSON.stringify({
  execute: 'add_hostfwd',
  arguments: {
    proto: 'tcp',
    host_addr: '0.0.0.0',
    host_port: hostPort,
    guest_addr: guestAddr,
    guest_port: guestPort,
  },
});

await new Promise((resolve, reject) => {
  const sock = unixConnect({ path: sockPath });
  let buf = '';
  const done = (err, value) => {
    sock.destroy();
    if (err) reject(err);
    else resolve(value);
  };
  sock.on('data', (d) => (buf += d.toString()));
  sock.on('error', (e) => done(e));
  sock.on('close', () => {
    if (!buf.trim()) return done(new Error('empty reply from slirp api'));
    try {
      done(null, JSON.parse(buf));
    } catch {
      done(new Error(`unparseable reply: ${buf}`));
    }
  });
  sock.write(`${req}\n`);
});

console.log(`add_hostfwd: host ${hostPort} -> guest ${guestAddr}:${guestPort} registered`);

// Probe the forwarded host port for 15s (netem may add delay).
const deadline = Date.now() + 15_000;
let ok = false;
while (Date.now() < deadline && !ok) {
  try {
    const res = await fetch(`http://127.0.0.1:${hostPort}/json/version`, {
      signal: AbortSignal.timeout(3000),
    });
    const body = await res.text();
    if (res.ok && body.includes('Browser')) {
      console.log(`CDP through hostfwd: ${body.slice(0, 160)}`);
      ok = true;
      break;
    }
    console.log(`probe: http ${res.status}`);
  } catch (e) {
    console.log(`probe failed: ${e.message ?? e}`);
  }
  await new Promise((r) => setTimeout(r, 500));
}
if (!ok) {
  console.error('CDP endpoint never answered through hostfwd');
  process.exit(1);
}