// Tiny TCP relay used INSIDE a netns rig (Task 3): Chromium's DevTools
// server can no longer be bound to a non-loopback address (new headless
// ignores --remote-debugging-address and always binds 127.0.0.1), so the
// slirp4netns host-forward — which injects packets addressed to the
// netns tap IP — needs this relay listening on 10.0.<i>.100:9223 and
// piping every connection through to 127.0.0.1:9222. Plain byte piping:
// carries both the /json/version HTTP and the DevTools WebSocket.
//
// Usage: node cdp-relay.mjs <listenIp> <listenPort> <upstreamPort> [parentPid]
//
// The parentPid guard makes the relay exit when the netns's main process
// (Chromium, the exec'd child) is gone, so the network namespace can die
// with the rig instead of leaking orphan relays.

import { createServer, connect as tcpConnect } from 'node:net';
import { existsSync } from 'node:fs';

const [listenIp, listenPort, upstreamPort, parentPid] = process.argv.slice(2);

if (!listenIp || !listenPort || !upstreamPort) {
  console.error(
    'usage: cdp-relay.mjs <listenIp> <listenPort> <upstreamPort> [parentPid]',
  );
  process.exit(2);
}

function pipe(client) {
  const upstream = tcpConnect(
    { host: '127.0.0.1', port: Number(upstreamPort) },
    () => {
      client.pipe(upstream);
      upstream.pipe(client);
    },
  );
  const tear = () => {
    client.destroy();
    upstream.destroy();
  };
  client.once('error', tear);
  upstream.once('error', tear);
  client.once('close', tear);
  upstream.once('close', tear);
}

const server = createServer(pipe);
server.on('error', (err) => {
  console.error(`cdp-relay: ${err.message}`);
  process.exit(1);
});
server.listen(Number(listenPort), listenIp, () => {
  console.log(
    `cdp-relay: ${listenIp}:${listenPort} -> 127.0.0.1:${upstreamPort}`,
  );
});

if (parentPid && Number.isFinite(Number(parentPid))) {
  const poll = setInterval(() => {
    // The parent is Chromium (this relay's parent process): when it dies
    // the netns's purpose is gone — exit so the namespace empties out.
    if (!existsSync(`/proc/${parentPid}`)) {
      clearInterval(poll);
      server.close();
      process.exit(0);
    }
  }, 1000);
  // Never keep the process alive just for the poll.
  poll.unref();
}