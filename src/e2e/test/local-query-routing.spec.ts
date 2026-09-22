import { expect, test, type Browser, type Page } from '@playwright/test';

import { expectKnownPeer, expectRingAtLeast } from '../src/assertions.js';
import { connectOverCDP } from '../src/cdp.js';
import {
  launchRegion,
  netnsDown,
  type LaunchedRegion,
  type NetnsRegion,
  type RegionSpec,
} from '../src/netns-launch.js';
import {
  connect,
  openPeer,
  startMiniStun,
  startServe,
  startSignaling,
  waitFor,
  waitForState,
  type DemoServer,
  type MiniStunServer,
  type PeerState,
  type SignalingProcess,
} from '../src/orchestrator.js';

// Task 3: three Chromium peers, each in its own UNPRIVILEGED Linux network
// namespace with a per-region `tc netem` egress delay (see
// src/netns-launch.ts for the rig and its RTT model), discovering each
// other through the real signaling server and answering latency-ordered
// closest-node queries through the real query routing.
//
//   RTT(a <-> b) = halfDelayMs_a + halfDelayMs_b  (+0-2ms slirp overhead)
//   eu<->us ~80ms (ring 7), eu<->asia ~160ms (ring 8),
//   us<->asia ~120ms (ring 7)
//
// Assertions are inequality/ring-threshold based, never exact-value: the
// slirp hops add a small nondeterministic overhead ON TOP of the scripted
// netem delays, never below them.

// E2E_SIGNALING_PORT mirrors the two-peer spec's override (this machine
// has a foreign service on 8080).
const SIGNALING_PORT = Number(process.env.E2E_SIGNALING_PORT) || 8080;
const DEMO_PORT = 8090;
// Loopback mini-STUN on the host: slirp gives every netns the same guest
// address, so only host candidates (self-referential) exist natively. The
// responder (scripts/mini-stun.mjs, MAPPED_IP=slirp gateway) makes each
// peer advertise srflx <gateway>:<port> — mutually reachable through every
// netns's gateway — and netem on tap0 still shapes every packet.
const STUN_PORT = Number(process.env.E2E_STUN_PORT) || 3478;
const STUN_URL = `stun:10.0.2.2:${STUN_PORT}`;
// MERIDIAN_CONFIG.gossipPeriodMs override (demo `?gossipMs=` affordance):
// discovery itself is signaling-driven (peers_list on register), but the
// short period makes the first gossip ring-sample refresh arrive in
// seconds instead of minutes.
const GOSSIP_MS_OVERRIDE = 2000;
// Upper bound only — waitFor polls and resolves as soon as it holds.
const DISCOVERY_BUDGET_MS = 60_000;

const REGIONS: NetnsRegion[] = ['eu', 'us', 'asia'];

// Ring index = ceil(log2(rtt / innermostRingRadius)) clamped to 8
// (src/js/src/ring.js with MERIDIAN_CONFIG: innermost 1ms, factor 2, 9
// rings; deliberately not imported so this e2e package never TS-resolves
// across into the JS library — the boundaries are pinned by src/js unit
// tests). Per pair, the MINIMUM ring the scripted RTT band can produce
// (keys are the alphabetically ordered region pair):
//   eu-us   80ms  -> (64, 128]  -> ring 7
//   asia-eu 160ms -> (128, 256] -> ring 8
//   asia-us 120ms -> (64, 128]  -> ring 7
const PAIRS = ['asia-eu', 'asia-us', 'eu-us'] as const;
type PairKey = (typeof PAIRS)[number];

const EXPECTED_MIN_RING: Record<PairKey, number> = {
  'asia-eu': 8,
  'asia-us': 7,
  'eu-us': 7,
};

/** Upper bound on the scripted RTT of a region pair (+ slirp slack). */
const RTT_SLACK_MS = 60;
const SCRIPTED_RTT: Record<PairKey, number> = {
  'asia-eu': 160,
  'asia-us': 120,
  'eu-us': 80,
};
const pairKey = (a: NetnsRegion, b: NetnsRegion): PairKey =>
  [a, b].sort().join('-') as PairKey;
const pairRttBound = (a: NetnsRegion, b: NetnsRegion): number =>
  (a === b ? 0 : SCRIPTED_RTT[pairKey(a, b)]) + RTT_SLACK_MS;

// --- page-side predicate sources (evaluated via waitForState) -------------

/** PeerState -> truthy: both other regions are known and connected. */
const othersConnectedJs = (otherIds: string[]) =>
  `function othersConnected(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let found = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected') found++;
    }
    return found === wanted.size;
  }`;

/**
 * PeerState -> truthy: both other regions are known, connected AND have a
 * ring placement (rtt is measured at enrollment; gossip refreshes it).
 */
const ringPlacedJs = (otherIds: string[]) =>
  `function ringsPlaced(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let placed = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected' &&
          p.ringIndex !== null && p.ringIndex !== undefined) placed++;
    }
    return placed === wanted.size;
  }`;

/**
 * Reads the newest "closest to <target>: {...}" line of the demo log —
 * the demo's real find-closest-node flow (its Find button + prompt()
 * dialog seam) runs the library's actual query routing.
 */
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

// --- suite-local helpers ---------------------------------------------------

interface ClosestResult {
  closestPeerId?: string;
  closestRtt?: number;
  hopCount?: number;
  error?: string;
}

/**
 * Drives the demo's real find-closest-node flow: answers the prompt()
 * dialog with the target, clicks the demo's own Find button, and polls
 * the demo log for the result line. Queries to different origins run
 * concurrently; queries from one origin run sequentially (one prompt()
 * dialog at a time per page).
 */
async function findClosestNodeViaDemo(
  page: Page,
  targetPeerId: string,
): Promise<ClosestResult> {
  page.once('dialog', (dialog) => {
    void dialog.accept(targetPeerId).catch(() => {});
  });
  await page.click('#find');
  return waitFor<ClosestResult>(
    page,
    readFindResultJs(targetPeerId),
    25_000,
  );
}

// --- rig lifecycle ---------------------------------------------------------

let signaling: SignalingProcess;
let demo: DemoServer;
let miniStun: MiniStunServer;
let launched: LaunchedRegion[];
const browsers: Browser[] = [];
const pages = {} as Record<NetnsRegion, Page>;
const peerIds = {} as Record<NetnsRegion, string>;

const rigFor = (region: NetnsRegion): LaunchedRegion =>
  launched.find((r) => r.region === region)!;

// The rig (3 netns + slirp + 3 Chromium boots) is driven by
// src/netns-launch.ts with a 180s readiness deadline; the setTimeout below
// lets the launcher's log-tail forensics surface before a generic hook
// timeout would.
test.beforeAll(async () => {
  test.setTimeout(180_000);
  signaling = startSignaling(SIGNALING_PORT);
  await signaling.ready;

  // HOST=0.0.0.0: the netns peers load the demo through their slirp
  // gateway (10.0.<i>.2 -> the host's loopback).
  demo = startServe({ host: '0.0.0.0', port: DEMO_PORT });
  await demo.ready;

  // Loopback STUN for the netns peers' srflx candidates (see STUN_URL).
  miniStun = startMiniStun({ port: STUN_PORT });
  await miniStun.ready;

  // Three region rigs in parallel (each: user+net namespace, tap0, netem,
  // slirp4netns attachment + CDP port forward, headless Chromium).
  launched = await Promise.all(REGIONS.map((r) => launchRegion(r)));

  // Attach Playwright to each netns Chromium over its forwarded CDP port.
  // CDP-attached browsers have no implicit-context creation: reuse the
  // browser's existing default context (about:blank).
  await Promise.all(
    REGIONS.map(async (region) => {
      const browser = await connectOverCDP(rigFor(region).endpoint);
      browsers.push(browser);
      const ctx = browser.contexts()[0] ?? (await browser.newContext());
      pages[region] = await ctx.newPage();
    }),
  );

  // Each demo page loads from its OWN region's gateway (the netns only
  // reaches the host through 10.0.<i>.2), then dials the signaling server
  // through the same gateway — all three near-simultaneously (glare is
  // resolved by the library's deterministic tie-break).
  await Promise.all(
    REGIONS.map(async (region) => {
      const spec: RegionSpec = rigFor(region).spec;
      await openPeer(
        pages[region],
        `http://${spec.gateway}:${DEMO_PORT}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}&stun=${STUN_URL}`,
      );
      await connect(pages[region], `ws://${spec.gateway}:${SIGNALING_PORT}`);
    }),
  );

  // Every node exists now (state() is null before that) and has an id.
  const bootstrapped = await Promise.all(
    REGIONS.map((region) =>
      waitForState(
        pages[region],
        `function bootstrapped(state) { return !!state.peerId; }`,
        DISCOVERY_BUDGET_MS,
      ),
    ),
  );
  REGIONS.forEach((region, i) => {
    peerIds[region] = bootstrapped[i].peerId;
    expect(peerIds[region], `${region} peer id`).toBeTruthy();
  });
});

test.afterAll(async () => {
  await Promise.allSettled(browsers.map((browser) => browser.close()));
  for (const region of REGIONS) {
    try {
      await netnsDown(region);
    } catch {
      // Best-effort: the down script is idempotent.
    }
  }
  try {
    demo?.stop();
  } catch {
    // Already dead.
  }
  try {
    miniStun?.stop();
  } catch {
    // Already dead.
  }
  try {
    signaling?.stop();
  } catch {
    // Already dead.
  }
});

// --- (a) full-mesh discovery ----------------------------------------------

test('full mesh: every region lists the other two as connected peers', async () => {
  const [eu, us, asia] = await Promise.all(
    REGIONS.map((region) => {
      const others = REGIONS.filter((r) => r !== region).map(
        (r) => peerIds[r],
      );
      return waitForState(
        pages[region],
        othersConnectedJs(others),
        DISCOVERY_BUDGET_MS,
      );
    }),
  );
  const states: Record<NetnsRegion, PeerState> = { eu, us, asia };
  for (const origin of REGIONS) {
    for (const target of REGIONS.filter((r) => r !== origin)) {
      expectKnownPeer(states[origin], peerIds[target]);
    }
  }
});

// --- (b) ring placement follows the scripted latency -----------------------

test('ring placement follows the scripted latency (80/160/120ms RTT)', async () => {
  const [eu, us, asia] = await Promise.all(
    REGIONS.map((region) => {
      const others = REGIONS.filter((r) => r !== region).map(
        (r) => peerIds[r],
      );
      return waitForState(
        pages[region],
        ringPlacedJs(others),
        DISCOVERY_BUDGET_MS,
      );
    }),
  );
  const states: Record<NetnsRegion, PeerState> = { eu, us, asia };
  for (const origin of REGIONS) {
    for (const target of REGIONS.filter((r) => r !== origin)) {
      expectRingAtLeast(
        states[origin],
        peerIds[target],
        EXPECTED_MIN_RING[pairKey(origin, target)],
      );
    }
  }
});

// --- (c)+(d) latency-ordered closest-node queries --------------------------

test('findClosestNode resolves the queried region from every origin', async () => {
  // Three origins in parallel x three targets sequentially per origin
  // (the demo's prompt() dialog seam is per page). routeQuery probes
  // candidates whose STORED rtt lies within [myRtt/2, myRtt*2] of the
  // originator's fresh measure; the TARGET's own node self-probes at 0ms
  // and always wins — so the identity assertion below is the strong one
  // that a same-host loopback rig could never support.
  const results: Record<
    NetnsRegion,
    Partial<Record<NetnsRegion, ClosestResult>>
  > = { eu: {}, us: {}, asia: {} };
  await Promise.all(
    REGIONS.map(async (origin) => {
      for (const target of REGIONS) {
        results[origin][target] = await findClosestNodeViaDemo(
          pages[origin],
          peerIds[target],
        );
      }
    }),
  );

  for (const origin of REGIONS) {
    for (const target of REGIONS) {
      const result = results[origin][target];
      if (!result) {
        throw new Error(
          `no closest-node result from ${origin} to ${target}`,
        );
      }
      expect(
        result.error,
        `closest from ${origin} to ${target} errored: ${result.error ?? 'none'}`,
      ).toBeUndefined();
      expect(
        result.closestPeerId,
        `closest to ${peerIds[target]} from ${origin}`,
      ).toBe(peerIds[target]);
      // (d) The accepted result never exceeds the origin's own scripted
      // RTT to the target's region — routeAcceptanceThreshold semantics,
      // asserted as an inequality, never an exact value.
      expect(
        result.closestRtt ?? Infinity,
        `closestRtt from ${origin} to ${target} is non-negative`,
      ).toBeGreaterThanOrEqual(0);
      expect(
        result.closestRtt ?? Number.POSITIVE_INFINITY,
        `closestRtt from ${origin} to ${target} within the scripted bound`,
      ).toBeLessThanOrEqual(pairRttBound(origin, target));
    }
  }
});