import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, test, type Page } from '@playwright/test';

import { expectKnownPeer, expectRingAtLeast } from '../src/assertions.js';
import {
  connect,
  openPeer,
  startSignaling,
  waitFor,
  waitForState,
  type PeerState,
  type SignalingProcess,
  type WireLogEntry,
} from '../src/orchestrator.js';

// Signaling: the plan's default port 8080; E2E_SIGNALING_PORT overrides it
// where the dev machine already has a foreign service on 8080. Peers are
// driven to connect with this exact URL via the demo's real connect flow.
const SIGNALING_PORT = Number(process.env.E2E_SIGNALING_PORT) || 8080;
const SIGNALING_URL = `ws://localhost:${SIGNALING_PORT}`;
// Served by src/e2e/scripts/serve.mjs (port 8090: distinct from the
// signaling 8080 and the src/js demo's own 9000).
const DEMO_ORIGIN = 'http://127.0.0.1:8090';
// MERIDIAN_CONFIG.gossipPeriodMs (src/js/src/config.js). Not imported so
// the e2e package never TS-resolves across into the JS library.
const GOSSIP_PERIOD_MS = 30_000;
// Plan Task 2: discovery converges within 2 x gossipPeriodMs. waitFor
// polls, so this is only an upper bound — it resolves in seconds.
const DISCOVERY_BUDGET_MS = 2 * GOSSIP_PERIOD_MS;

// --- page-side predicate sources (evaluated via orchestrator.waitFor) ----

/** PeerState -> truthy: the other peer is known and connected. */
const knownAndConnectedJs = (otherPeerId: string) =>
  `function knownAndConnected(state) {
    return state.knownPeers.some((p) =>
      p.id === ${JSON.stringify(otherPeerId)} && p.status === 'connected');
  }`;

/** WireLogEntry[] -> truthy: returns the log once `conditionSource` holds. */
const wireSeenJs = (conditionSource: string) =>
  `function wireSeen() {
    const hook = window.__meridian;
    const snapshot = hook && hook.state ? hook.state() : null;
    const log = snapshot && snapshot.wireLog ? snapshot.wireLog : null;
    if (!log || log.length === 0) return null;
    return (${conditionSource})(log) ? log : null;
  }`;

/** Signaling half: register, get_peers (sent), peers_list (received), and
 * sdp-bearing connect_offer / connect_answer, regardless of which side
 * offered (glare tie-break) or answered. */
const hasHandshakeWireJs = `function handshakeWire(log) {
  const has = (dir, type) =>
    log.some((e) => e.dir === dir && e.type === type);
  const withSdp = (type) =>
    log.some((e) => e.type === type && e.payload && e.payload.sdp &&
      typeof e.payload.sdp.sdp === 'string' && e.payload.sdp.sdp.length > 0);
  return has('send', 'register') &&
    has('send', 'get_peers') &&
    has('recv', 'peers_list') &&
    withSdp('connect_offer') &&
    withSdp('connect_answer');
}`;

/**
 * Deadlock-regression guard: our RTT ping was answered by a pong over the
 * channel wired before ring enrollment, and we answered the peer's ping
 * likewise. Both sides measure over the one DataChannel, so a responder
 * wired only at/after enrollment would deadlock BOTH RTT measures — the
 * handshake would fail and the 'connected' assertion above would catch it.
 */
const hasPingPongWireJs = `function pingPongWire(log) {
  const answered = (pingDir, pongDir) =>
    log.some((p) =>
      p.dir === pingDir && p.type === 'ping' &&
      p.payload && typeof p.payload.id === 'string' &&
      log.some((q) =>
        q.dir === pongDir && q.type === 'pong' && q.payload &&
        q.payload.id === p.payload.id));
  return answered('send', 'recv') && answered('recv', 'send');
}`;

/** Gossip exchange with the other peer (one cycle runs at
 * gossipPeriodMs; the receiver re-measures the sender's RTT on it). */
const hasGossipWireJs = (selfPeerId: string, otherPeerId: string) =>
  `function gossipWire(log) {
    const sent = log.some((e) =>
      e.dir === 'send' && e.type === 'gossip' && e.payload &&
      e.payload.senderId === ${JSON.stringify(selfPeerId)});
    const received = log.some((e) =>
      e.dir === 'recv' && e.type === 'gossip' && e.payload &&
      e.payload.senderId === ${JSON.stringify(otherPeerId)} &&
      e.payload.ringSamples &&
      Object.keys(e.payload.ringSamples).length > 0);
    return sent && received;
  }`;

/** Reads the newest "closest to <target>: {...}" line of the demo log. */
const readFindResultJs = (targetPeerId: string) =>
  `function readFindResult() {
    const logEl = document.getElementById('log');
    if (!logEl) return null;
    const marker = 'closest to ' + ${JSON.stringify(targetPeerId)} + ': ';
    for (const line of logEl.children) {
      const text = line.textContent || '';
      const idx = text.indexOf(marker);
      if (idx === -1) continue;
      const start = text.indexOf('{', idx);
      const end = text.indexOf('}', idx);
      if (start === -1 || end === -1 || end < start) continue;
      try {
        return JSON.parse(text.slice(start, end + 1));
      } catch {
        return null;
      }
    }
    return null;
  }`;

// --- spec-local helpers ---------------------------------------------------

interface ClosestResult {
  closestPeerId?: string;
  closestRtt?: number;
  error?: string;
}

const hasSdp = (entry: WireLogEntry): boolean => {
  const payload = entry.payload as { sdp?: { sdp?: unknown } } | null;
  return (
    !!payload?.sdp &&
    typeof payload.sdp.sdp === 'string' &&
    payload.sdp.sdp.length > 0
  );
};

/** Throws with a dir:type summary when `predicate` matches nothing. */
function expectWireEntry(
  log: WireLogEntry[],
  what: string,
  predicate: (entry: WireLogEntry) => boolean,
): void {
  if (!log.some(predicate)) {
    const summary =
      log.map((entry) => `${entry.dir}:${entry.type}`).join(', ') || 'none';
    throw new Error(`wireLog missing ${what}; observed: [${summary}]`);
  }
}

/** Direction-agnostic per-side handshake assertions (see plan Task 2). */
function assertHandshakeWire(
  log: WireLogEntry[],
  label: string,
  selfPeerId: string,
): void {
  expectWireEntry(
    log,
    `${label}: sent register(self)`,
    (e) =>
      e.dir === 'send' &&
      e.type === 'register' &&
      (e.payload as { peerId?: string } | null)?.peerId === selfPeerId,
  );
  expectWireEntry(
    log,
    `${label}: sent get_peers to signaling`,
    (e) => e.dir === 'send' && e.type === 'get_peers',
  );
  expectWireEntry(
    log,
    `${label}: received peers_list`,
    (e) =>
      e.dir === 'recv' &&
      e.type === 'peers_list' &&
      Array.isArray((e.payload as { peers?: unknown } | null)?.peers),
  );
  expectWireEntry(
    log,
    `${label}: connect_offer with sdp`,
    (e) => e.type === 'connect_offer' && hasSdp(e),
  );
  expectWireEntry(
    log,
    `${label}: connect_answer with sdp`,
    (e) => e.type === 'connect_answer' && hasSdp(e),
  );
}

/**
 * Drives the demo's real "Find closest node" flow (button + prompt
 * dialog) and parses the result line from the demo log. On a two-peer
 * overlay the answer is self or the target: routeQuery probes candidates
 * whose STORED ring rtt lies within [myRtt/2, myRtt*2] of the
 * originator's freshly measured rtt; the target's self-measure is 0ms and
 * always wins once it is probed, but the two loopback measures can drift
 * past that 2x window between enrollment / gossip refresh and query time,
 * in which case the node correctly answers itself (spec 3.6).
 */
async function findClosestNodeViaDemo(
  page: Page,
  targetPeerId: string,
): Promise<ClosestResult> {
  // prompt() is the demo's target-input seam; findClosestNode runs the
  // library's real query routing.
  page.once('dialog', (dialog) => {
    void dialog.accept(targetPeerId).catch(() => {});
  });
  await page.click('#find');
  return waitFor<ClosestResult>(page, readFindResultJs(targetPeerId));
}

// --- suite ----------------------------------------------------------------

let signaling: SignalingProcess;
let demoServer: ChildProcess;
let demoServerError: Error | null = null;
let peerA: Page;
let peerB: Page;

test.beforeAll(async ({ browser }) => {
  signaling = startSignaling(SIGNALING_PORT);
  await signaling.ready;

  demoServer = spawn(
    process.execPath,
    [fileURLToPath(new URL('../scripts/serve.mjs', import.meta.url))],
    { stdio: 'ignore' },
  );
  demoServer.once('exit', (code) => {
    demoServerError = new Error(
      `demo static server exited early (code ${code}); is port 8090 taken?`,
    );
  });
  await (async function waitForDemoServer(): Promise<void> {
    const deadline = Date.now() + 10_000;
    for (;;) {
      try {
        const res = await fetch('http://127.0.0.1:8090/');
        if (res.ok) return;
      } catch {
        // Not accepting yet.
      }
      if (Date.now() > deadline) {
        throw (
          demoServerError ??
          new Error('demo static server did not serve 127.0.0.1:8090 within 10s')
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();

  peerA = await browser.newPage();
  peerB = await browser.newPage();
  await openPeer(peerA, `${DEMO_ORIGIN}/?wirelog=1`);
  await openPeer(peerB, `${DEMO_ORIGIN}/?wirelog=1`);
  // Both dial the signaling server near-simultaneously: each sends register
  // + get_peers on open, so glare (both peers offering from the same
  // peers_list) is common; the library's deterministic tie-break resolves
  // it (the lexicographically greater peerId ignores the incoming offer).
  await Promise.all([
    connect(peerA, SIGNALING_URL),
    connect(peerB, SIGNALING_URL),
  ]);
});

test.afterAll(async () => {
  demoServer?.kill();
  try {
    signaling?.stop();
  } catch {
    // Already dead.
  }
  await Promise.allSettled(
    [peerA, peerB]
      .filter((page): page is Page => page !== undefined)
      .map((page) => page.close()),
  );
});

test('two peers discover each other, answer pre-enrollment pings, route a closest-node query @local', async () => {
  // 1. Both demo nodes exist (state() is null until then).
  const bootstrapped = 'function bootstrapped() { return true; }';
  const [snapshotA, snapshotB] = await Promise.all([
    waitForState(peerA, bootstrapped),
    waitForState(peerB, bootstrapped),
  ]);
  const peerIdA = snapshotA.peerId;
  const peerIdB = snapshotB.peerId;
  expect(peerIdA, 'peer A id').toBeTruthy();
  expect(peerIdB, 'peer B id').toBeTruthy();
  expect(peerIdA, 'distinct peer ids').not.toBe(peerIdB);

  // 2. Discovery: within 2 x gossipPeriodMs both list each other as
  // connected peers.
  const settledA: PeerState = await waitForState(
    peerA,
    knownAndConnectedJs(peerIdB),
    DISCOVERY_BUDGET_MS,
  );
  const settledB: PeerState = await waitForState(
    peerB,
    knownAndConnectedJs(peerIdA),
    DISCOVERY_BUDGET_MS,
  );
  expectKnownPeer(settledA, peerIdB);
  expectKnownPeer(settledB, peerIdA);
  // Both peers are ~0ms apart locally: assert ring MEMBERSHIP, never which
  // ring the enrollment RTT won (ring 0 vs 1 is loopback jitter).
  expectRingAtLeast(settledA, peerIdB, 0);
  expectRingAtLeast(settledB, peerIdA, 0);

  // 3. Signaling wire, per side.
  const wireA = await waitFor<WireLogEntry[]>(peerA, wireSeenJs(hasHandshakeWireJs));
  const wireB = await waitFor<WireLogEntry[]>(peerB, wireSeenJs(hasHandshakeWireJs));
  assertHandshakeWire(wireA, 'peer A', peerIdA);
  assertHandshakeWire(wireB, 'peer B', peerIdB);

  // 4. DataChannel wire: ping {type,id,t} -> pong {type,id,t}, in both
  // directions (each side measures RTT; each side's responder answered a
  // ping on the channel wired before its own ring enrollment).
  await waitFor<WireLogEntry[]>(peerA, wireSeenJs(hasPingPongWireJs));
  await waitFor<WireLogEntry[]>(peerB, wireSeenJs(hasPingPongWireJs));

  // 5. One gossip cycle (the first fires gossipPeriodMs after initialize):
  // each side sends ring samples to the other, and handleGossip re-measures
  // the sender's RTT on receipt — this refreshes the stored ring RTTs the
  // query router candidates against (loopback measures drift a few ms apart
  // and routeQuery's window is [myRtt/2, myRtt*2]). Also the plan's "gossip
  // populates rings" observation for the two-peer overlay.
  await waitFor<WireLogEntry[]>(
    peerA,
    wireSeenJs(hasGossipWireJs(peerIdA, peerIdB)),
    DISCOVERY_BUDGET_MS,
  );
  await waitFor<WireLogEntry[]>(
    peerB,
    wireSeenJs(hasGossipWireJs(peerIdB, peerIdA)),
    DISCOVERY_BUDGET_MS,
  );

  // 6. findClosestNode from each side resolves through the real query
  // routing (measured RTT to the target, multi-hop machinery, probe
  // request/result over the ring DataChannel). With exactly two peers the
  // answer can only be self or the target. Identity is NOT asserted:
  // routeQuery only probes candidates whose STORED ring rtt sits within
  // [myRtt/2, myRtt*2] of the originator's freshly measured rtt, and the
  // two loopback measures drift past that 2x window between enrollment /
  // gossip refresh and query time — in that regime the node correctly
  // answers itself (closestPeerId === self, closestRtt === its own fresh
  // measure), which the task anticipates ("either can win").
  const fromA = await findClosestNodeViaDemo(peerA, peerIdB);
  expect(fromA.error, 'closest from A error').toBeUndefined();
  expect(
    fromA.closestPeerId === peerIdA || fromA.closestPeerId === peerIdB,
    `closest from A resolves within the overlay, got ${String(fromA.closestPeerId)}`,
  ).toBe(true);
  expect(fromA.closestRtt, 'closest rtt from A').toBeGreaterThanOrEqual(0);
  const fromB = await findClosestNodeViaDemo(peerB, peerIdA);
  expect(fromB.error, 'closest from B error').toBeUndefined();
  expect(
    fromB.closestPeerId === peerIdB || fromB.closestPeerId === peerIdA,
    `closest from B resolves within the overlay, got ${String(fromB.closestPeerId)}`,
  ).toBe(true);
  expect(fromB.closestRtt, 'closest rtt from B').toBeGreaterThanOrEqual(0);
});