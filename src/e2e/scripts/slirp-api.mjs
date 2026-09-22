// slirp4netns raw-JSON unix-socket API client (Task 3 rig).
//
// The API socket (--api-socket=PATH) is QMP-style, NOT HTTP: one JSON
// request per connection, no HTTP framing (curl would fail with
// "Received HTTP/0.9 when not allowed" — verified empirically, see
// netns-up.sh). Wire shape:
//
//   -> {"execute":"add_hostfwd","arguments":{"proto":"tcp",
//        "host_addr":"0.0.0.0","host_port":9223,
//        "guest_addr":"10.0.2.100","guest_port":9222}}
//   <- {"return":{"id":1}}
//
// Usage (CLI): node slirp-api.mjs <socketPath> add_hostfwd '<json args>'
// Importable: addHostfwd(sockPath, args) / request(sockPath, execute, args).

import { connect } from 'node:net';

/**
 * Sends one `execute` request over the API socket and resolves the parsed
 * response (`{"return":...}` or `{"error":{"desc":...}}`).
 */
export async function request(sockPath, execute, args = {}) {
  return new Promise((resolve, reject) => {
    const socket = connect({ path: sockPath });
    let buf = '';
    let settled = false;
    const done = (err, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      if (err) reject(err);
      else resolve(value);
    };
    const timer = setTimeout(() => {
      const line = buf.trim();
      if (!line) return done(new Error(`empty reply for ${execute}`));
      try {
        done(null, JSON.parse(line));
      } catch {
        done(new Error(`unparseable reply for ${execute}: ${buf}`));
      }
    }, 400);
    socket.on('data', (d) => (buf += d.toString()));
    socket.on('error', (e) => done(e));
    socket.write(JSON.stringify({ execute, arguments: args }) + '\n');
  });
}

/**
 * Forwards `hostPort` on the host (all addresses) to `guestAddr:guestPort`
 * inside the netns slirp4netns is attached to. This is how the
 * orchestrator reaches a netns Chromium's CDP endpoint (9222): rootlesskit's
 * builtin port driver is unusable on this machine (no newuidmap binary),
 * but the same forwarding exists in slirp4netns proper, driven
 * unprivileged through its API socket.
 */
export async function addHostfwd(
  sockPath,
  { proto = 'tcp', hostAddr = '0.0.0.0', hostPort, guestAddr, guestPort },
) {
  const reply = await request(sockPath, 'add_hostfwd', {
    proto,
    host_addr: hostAddr,
    host_port: hostPort,
    guest_addr: guestAddr,
    guest_port: guestPort,
  });
  if (reply.error) {
    throw new Error(`add_hostfwd rejected: ${JSON.stringify(reply.error)}`);
  }
  return reply.return;
}

// --- CLI: node slirp-api.mjs <socket> <execute> '<jsonArgs>' ---------------
if (process.argv[1] && process.argv[1].endsWith('slirp-api.mjs')) {
  const [sockPath, execute, rawArgs] = process.argv.slice(2);
  if (!sockPath || !execute) {
    console.error('usage: slirp-api.mjs <socketPath> <execute> [jsonArgs]');
    process.exit(2);
  }
  let args = {};
  if (rawArgs) {
    try {
      args = JSON.parse(rawArgs);
    } catch (err) {
      console.error(`bad jsonArgs: ${err.message}`);
      process.exit(2);
    }
  }
  request(sockPath, execute, args)
    .then((reply) => {
      console.log(JSON.stringify(reply));
      if (reply.error) process.exit(1);
    })
    .catch((err) => {
      console.error(String(err.message ?? err));
      process.exit(1);
    });
}