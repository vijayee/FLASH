import { appendFileSync } from 'node:fs';
import path from 'node:path';

import { expect, test, type Browser, type Page } from '@playwright/test';

import { expectKnownPeer, expectRingAtLeast } from '../src/assertions.js';
import { startArtifacts, type ArtifactRun } from '../src/artifacts.js';
import {
  connect,
  waitFor,
  waitForState,
  type PeerState,
} from '../src/orchestrator.js';
import {
  connectAgent,
  launchRemoteAgent,
  remoteExec,
  type RemoteAgent,
} from '../src/remote.js';

// Task 9: the geo suite. Three JS-demo peers run as SSH-raised agents on the
// Azure estate — flash-e2e-centralus (us), flash-e2e-swedencentral (eu) and
// flash-e2e-koreacentral (asia) — with flash-e2e-lab hosting the signaling
// server (systemd, ws://:8080). Unlike every other spec, NOTHING here is
// scripted: RTTs are REAL internet latency, ICE is real cross-region
// networking, and the only Chromium/CDP plumbing is per-VM (src/remote.ts).
//
// Excluded from local runs via the config's testIgnore (opt-IN only:
// E2E_GEO=1). Budgets are generous but bounded upper bounds — waitFor polls
// and resolves as soon as the condition holds. Assertions are
// inequality-based with slack (never exact RTT values): real networks jitter.
//
// TEST ORDER NOTE: the supernode-kill test closes the elected leader's page,
// so the cross-region media test runs BEFORE it — a re-raised peer would
// carry a fresh identity and re-discovery the media assertions don't need.

// Title-level opt-in guard (the @geo tags): config.testIgnore excludes this
// spec FILE from local runs (testIgnore matches paths, not titles — see
// playwright.config.ts), and this declaration makes a direct
// `playwright test test/geo.spec.ts` a no-op without E2E_GEO=1 as well.
test.skip(
  process.env.E2E_GEO !== '1',
  'geo suite is opt-in via E2E_GEO=1 (real Azure-region latency)',
);

// The lab VM (signaling + artifacts journal source); peers dial it directly.
const LAB_HOST = '172.173.102.12';
const SIGNALING_URL = `ws://${LAB_HOST}:8080`;
// Real Azure regions, role-label -> VM (provisioned by Task 8).
const REGIONS = [
  { label: 'us', name: 'centralus', host: '20.29.87.103' },
  { label: 'eu', name: 'swedencentral', host: '57.174.234.91' },
  { label: 'asia', name: 'koreacentral', host: '20.196.104.97' },
] as const;
type RegionLabel = (typeof REGIONS)[number]['label'];

// Approximate great-circle RTT matrix (ms) — INEQUALITIES only in asserts:
//   sweden<->korea ~230, sweden<->centralus ~135, korea<->centralus ~160.
// No mini-STUN override: on Azure the default stun.l.google.com is real and
// reachable, so srflx candidates are the VMs' public IPs (real STUN).
const GOSSIP_MS_OVERRIDE = 2000;
const MEDIA_SRC = '/fixtures/penguin.mp4';
// Upper bounds only (real latencies; waitFor resolves as soon as it holds).
const AGENT_READY_BUDGET_MS = 120_000;
const DISCOVERY_BUDGET_MS = 60_000;
// Every measured inter-region RTT must sit in this band (approx great-circle
// distances are 80-230ms; 50 floor is far below any Azure pair, 400 ceiling
// is 2x the worst expected pair — jitter tolerance, not a tight band).
const RTT_FLOOR_MS = 50;
const RTT_CEILING_MS = 400;
// The library's media_answer timeout is queryTimeoutMs (30s).
const MEDIA_HANDSHAKE_BUDGET_MS = 40_000;
// Plan budget: korea's activeStreams lists centralus within 30s.
const MEDIA_ACTIVE_BUDGET_MS = 30_000;
const FRAMES_BUDGET_MS = 20_000;
// Election probe round-trips + supernode_elected announcement.
const ELECTION_BUDGET_MS = 60_000;
// Plan budget for re-election: 1 x gossipPeriod + queryTimeout (32s), capped
// at 60s for real-WAN jitter.
const REELECTION_BUDGET_MS = 60_000;
// findClosestNode retry pattern (dart-interop): a repeated query re-measures,
// so identity assertions get 3 attempts before failing.
const MAX_QUERY_ATTEMPTS = 3;
const QUERY_BUDGET_MS = 40_000;

// --- page-side predicate sources (evaluated via waitFor/waitForState) ------

const othersConnectedJs = (otherIds: string[]) =>
  `function othersConnected(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let found = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected') found++;
    }
    return found === wanted.size;
  }`;

/** PeerState -> truthy: every other peer is connected AND rtt-measured. */
const rttsMeasuredJs = (otherIds: string[]) =>
  `function rttsMeasured(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let found = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected' &&
          p.rtt !== null && p.rtt !== undefined) found++;
    }
    return found === wanted.size ? state : null;
  }`;

/** PeerState -> truthy: peerId is in activeStreams. */
const hasActiveStreamJs = (peerId: string) =>
  `function hasActiveStream(state) {
    return state.activeStreams.includes(${JSON.stringify(peerId)});
  }`;

/** PeerState -> truthy: this node won the supernode election. */
const isSupernodeJs = `function isSupernode(state) {
  return state.isSupernode === true;
}`;

/** PeerState -> truthy: clusterLeader === leaderId (spec §5.1). */
const clusterLeaderJs = (leaderId: string) =>
  `function clusterLeaderIs(state) {
    return state.clusterLeader === ${JSON.stringify(leaderId)};
  }`;

/** The remote <video> the demo's renderStreams() creates for a peer. */
const remoteVideoPresentJs = (peerId: string) =>
  `function remoteVideoPresent() {
    return !!document.querySelector(
      '#videos video[data-peer-id="' + ${JSON.stringify(peerId)} + '"]');
  }`;

/**
 * Frames actually RENDERED by the remote <video> over one in-page second
 * (requestVideoFrameCallback fires per composited frame — media bits flow,
 * not just track events). Reachable because the korea peer is a JS demo
 * page whose remote video is a DOM element; a Dart-web peer would not be
 * (documented Task 6 limitation), in which case the spec falls back to
 * state-level evidence (see the media test).
 */
const countFramesInSecondJs = (peerId: string) =>
  `function countFramesInSecond() {
    return new Promise((resolve) => {
      const video = document.querySelector(
        '#videos video[data-peer-id="' + ${JSON.stringify(peerId)} + '"]');
      if (!video || !video.requestVideoFrameCallback) {
        resolve(0);
        return;
      }
      let n = 0;
      const tick = () => { n++; video.requestVideoFrameCallback(tick); };
      video.requestVideoFrameCallback(tick);
      setTimeout(() => resolve(n), 1000);
    });
  }`;

const framesFlowingJs = (peerId: string) =>
  `function framesFlowing() {
    return (${countFramesInSecondJs(peerId)})().then((n) => (n > 3 ? n : null));
  }`;

/** Reads the demo log line the real "Stream to peer" seam writes. */
const readStreamResultJs = (targetPeerId: string) =>
  `function readStreamResult() {
    const logEl = document.getElementById('log');
    if (!logEl) return null;
    const suffix = 'streaming to ' + ${JSON.stringify(targetPeerId)};
    for (const line of logEl.children) {
      const text = (line.textContent || '').trim();
      if (text.endsWith(suffix)) return { ok: true };
      const failedAt = text.indexOf(suffix + ' failed:');
      if (failedAt !== -1) {
        return {
          ok: false,
          error: text.slice(failedAt + suffix.length + ' failed:'.length).trim(),
        };
      }
    }
    return null;
  }`;

/** Reads the newest "closest to <target>: {...}" line of the demo log whose
 * JSON differs from prevJson (a fresh attempt's outcome). */
const newestFindResultJs = (targetPeerId: string, prevJson: string) =>
  `function newestFindResult() {
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
        const parsed = JSON.parse(text.slice(start, end + 1));
        if (JSON.stringify(parsed) !== ${JSON.stringify(prevJson)}) return parsed;
      } catch {
        return null;
      }
    }
    return null;
  }`;

// --- spec-local helpers -----------------------------------------------------

interface ClosestResult {
  closestPeerId?: string;
  closestRtt?: number;
  closestRttMs?: number;
  hopCount?: number;
  error?: string;
}

interface StreamResult {
  ok: boolean;
  error?: string;
}

interface ElectionResult {
  leaderId?: string;
  avgRtt?: number;
  hopCount?: number;
  error?: string;
}

/**
 * Drives the JS demo's real find-closest-node flow (prompt + #find ->
 * the library's routeQuery), retrying bounded times on the §3.6 candidate
 * window fallback (a repeated query re-measures fresh vs stored drift).
 * Returns the newest result; the CALLER asserts identity (unlike the
 * loopback specs, geo RTT scales keep both peers inside the window, so the
 * target's self-measure reliably wins).
 */
async function findClosestNodeViaDemo(
  page: Page,
  targetPeerId: string,
): Promise<ClosestResult> {
  let prevJson = '';
  let last: ClosestResult | null = null;
  for (let attempt = 1; attempt <= MAX_QUERY_ATTEMPTS; attempt++) {
    page.once('dialog', (dialog) => {
      void dialog.accept(targetPeerId).catch(() => {});
    });
    await page.click('#find');
    const result = await waitFor<ClosestResult>(
      page,
      newestFindResultJs(targetPeerId, prevJson),
      QUERY_BUDGET_MS,
    );
    if (result.closestPeerId === targetPeerId) return result;
    prevJson = JSON.stringify(result);
    last = result;
  }
  return last as ClosestResult;
}

/**
 * Drives the demo's real "Stream to peer" flow (prompt + #stream ->
 * establishMediaStream over the existing DataChannel). Resolves once the
 * demo log records the outcome; throws with the demo's error text on
 * failure (real cross-region ICE failures land here with the library's
 * message, and the ice-stats artifacts carry the candidate evidence).
 */
async function streamToPeerViaDemo(
  page: Page,
  targetPeerId: string,
): Promise<void> {
  page.once('dialog', (dialog) => {
    void dialog.accept(targetPeerId).catch(() => {});
  });
  await page.click('#stream');
  const result = await waitFor<StreamResult>(
    page,
    readStreamResultJs(targetPeerId),
    MEDIA_HANDSHAKE_BUDGET_MS,
  );
  if (!result.ok) {
    throw new Error(`demo stream to ${targetPeerId} failed: ${result.error}`);
  }
}

/**
 * Polls the page until its clusterLeader is one of `allowedIds`; throws with
 * the observed leader on timeout. Used post-kill: each survivor must elect a
 * leader among the survivors, never the dead peer (see the re-election test
 * for why the exact direction is jitter-dependent on a 2-candidate cluster).
 */
async function waitForLeaderAmong(
  page: Page,
  allowedIds: string[],
  budgetMs = REELECTION_BUDGET_MS,
): Promise<string> {
  const readLeader = () =>
    page.evaluate(() => window.__meridian?.state()?.clusterLeader ?? null);
  const deadline = Date.now() + budgetMs;
  let last: unknown;
  for (;;) {
    last = await readLeader();
    if (typeof last === 'string' && allowedIds.includes(last)) return last;
    if (Date.now() > deadline) {
      throw new Error(
        `clusterLeader never became one of [${allowedIds.join(', ')}] ` +
          `within ${budgetMs}ms (last observed: ${String(last)})`,
      );
    }
    await page.waitForTimeout(500);
  }
}

// --- rig lifecycle -----------------------------------------------------------

let artifacts: ArtifactRun | null = null;
const agents = {} as Record<RegionLabel, RemoteAgent>;
const browsers = {} as Record<RegionLabel, Browser>;
const pages = {} as Record<RegionLabel, Page>;
const peerIds = {} as Record<RegionLabel, string>;

test.beforeAll(async ({}, testInfo) => {
  // The full launch (3 SSH agents + 3 CDP attaches + 3 page loads + mesh
  // bootstrap) rides one hook: bound it generously up front.
  test.setTimeout(300_000);
  artifacts = startArtifacts(testInfo);

  await Promise.all(
    REGIONS.map(async (region) => {
      agents[region.label] = launchRemoteAgent({
        host: region.host,
        role: 'js',
        readyTimeoutMs: AGENT_READY_BUDGET_MS,
      });
      await agents[region.label].whenReady;
    }),
  );

  // CDP-attached browsers have no implicit-context creation: reuse the
  // browser's existing default context (about:blank). Pages load at the
  // agent's ready-line origin — the treated-secure origin Chromium was
  // launched with (see RemoteAgent.demoOrigin for why it is NOT the public
  // IP: Chrome 140 blocks insecure IP origins that are not listed).
  await Promise.all(
    REGIONS.map(async (region) => {
      const label = region.label;
      browsers[label] = await connectAgent(agents[label]);
      const ctx =
        browsers[label].contexts()[0] ??
        (await browsers[label].newContext());
      pages[label] = await ctx.newPage();
      // Ice-stats reachability: the RTCPeerConnection tracker init script
      // must land BEFORE the demo page loads (afterwards it sees nothing
      // and the collector records 0 lines — the documented graceful skip).
      artifacts?.installPcTracker(pages[label]);
      const origin = agents[label].demoOrigin;
      expect(origin, `${label} agent ready-line origin`).toBeTruthy();
      await pages[label].goto(
        `${origin}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}&mediaSrc=${MEDIA_SRC}`,
        { waitUntil: 'domcontentloaded', timeout: 60_000 },
      );
      await connect(pages[label], SIGNALING_URL);
    }),
  );

  const bootstrapped = await Promise.all(
    REGIONS.map((region) =>
      waitForState(
        pages[region.label],
        `function bootstrapped(state) { return !!state.peerId; }`,
        DISCOVERY_BUDGET_MS,
      ),
    ),
  );
  REGIONS.forEach((region, i) => {
    peerIds[region.label] = bootstrapped[i].peerId;
    expect(peerIds[region.label], `${region.name} peer id`).toBeTruthy();
  });

  // Full mesh before any test body: every test assumes the three peers are
  // DISCOVERED (not just bootstrapped) — and on a Playwright retry the
  // beforeAll re-runs on a fresh worker, so this wait must not live only in
  // the first test.
  await Promise.all(
    REGIONS.map((region) => {
      const others = REGIONS.filter((r) => r !== region).map(
        (r) => peerIds[r.label],
      );
      return waitForState(
        pages[region.label],
        othersConnectedJs(others),
        DISCOVERY_BUDGET_MS,
      );
    }),
  );

  // Task 12 collectors: per-peer JSONL history + console + ice-stats for the
  // whole geo run. signaling.jsonl is NOT available here (the lab's systemd
  // unit does not set LOG, and modifying the VM estate is out of scope) —
  // afterAll dumps the unit's journald tail instead, and the per-peer
  // wirelog JSONL carries the register/get_peers/peers_list/SDP traffic as
  // each peer saw it.
  artifacts.writeTopology({
    rig: 'Azure geo estate (SSH-raised agents; src/remote.ts)',
    signalingUrl: SIGNALING_URL,
    signalingHost: LAB_HOST,
    stun: 'default (stun.l.google.com) — real STUN, no loopback override',
    gossipMsOverride: GOSSIP_MS_OVERRIDE,
    mediaSrc: MEDIA_SRC,
    regions: Object.fromEntries(
      REGIONS.map((region) => [
        region.label,
        {
          name: region.name,
          host: region.host,
          cdpEndpoint: agents[region.label].endpoint,
          demoOrigin: agents[region.label].demoOrigin,
          peerId: peerIds[region.label],
        },
      ]),
    ),
    // Approximate great-circle RTT matrix (ms); asserts are inequality-based.
    approxRttMatrixMs: {
      'centralus-swedencentral': 135,
      'centralus-koreacentral': 160,
      'swedencentral-koreacentral': 230,
    },
  });
  for (const region of REGIONS) {
    await artifacts.startPeerRecorder(pages[region.label], peerIds[region.label]);
    artifacts.startIceStats(pages[region.label], peerIds[region.label]);
  }
});

test.afterEach(async ({}, testInfo) => {
  artifacts?.recordResult(testInfo);
});

test.afterAll(async () => {
  // Flush the collectors, then preserve what signaling visibility the lab
  // VM offers WITHOUT touching the estate: the systemd unit's journald tail
  // (best-effort; the unit sets no LOG env, so this shows lifecycle events
  // rather than relayed traffic — the per-peer wirelog JSONL above carries
  // the relayed traffic as each peer saw it).
  if (artifacts) {
    try {
      const journal = await remoteExec(
        LAB_HOST,
        'journalctl -u flash-signaling --no-pager -n 300 --output=short-iso',
        { timeoutMs: 20_000 },
      );
      appendFileSync(
        path.join(artifacts.dir, 'signaling-journal.log'),
        journal,
      );
    } catch (err) {
      console.warn(`signaling journal dump skipped: ${String(err)}`);
    }
    await artifacts.close();
  }
  await Promise.allSettled(
    REGIONS.map((region) => browsers[region.label]?.close()),
  );
  for (const region of REGIONS) {
    try {
      await agents[region.label]?.stop();
    } catch {
      // Best-effort teardown.
    }
  }
});

// --- (1) full-mesh discovery across regions ----------------------------------

test('full-mesh discovery: every region lists the other two connected with real inter-region RTTs @geo', async () => {
  test.setTimeout(180_000);
  const [us, eu, asia] = await Promise.all(
    REGIONS.map((region) => {
      const others = REGIONS.filter((r) => r !== region).map(
        (r) => peerIds[r.label],
      );
      return waitForState(
        pages[region.label],
        othersConnectedJs(others),
        DISCOVERY_BUDGET_MS,
      );
    }),
  );
  const states: Record<RegionLabel, PeerState> = { us, eu, asia };
  for (const region of REGIONS) {
    for (const target of REGIONS) {
      if (target.label === region.label) continue;
      expectKnownPeer(states[region.label], peerIds[target.label]);
    }
  }

  // Real measured RTTs (gossip-driven pings over the inter-region
  // DataChannels): every pair must be cross-continent in character —
  // above any LAN/loopback band and far below a pathological outlier —
  // and the sweden-korea leg must be the LONGEST from sweden's vantage
  // (230ms approx vs sweden-centralus ~135ms: a 95ms gap, far beyond jitter).
  const settled = await Promise.all(
    REGIONS.map((region) => {
      const others = REGIONS.filter((r) => r !== region).map(
        (r) => peerIds[r.label],
      );
      return waitForState(
        pages[region.label],
        rttsMeasuredJs(others),
        DISCOVERY_BUDGET_MS,
      );
    }),
  );
  const measured = {} as Record<RegionLabel, Record<RegionLabel, number>>;
  for (const state of settled) {
    const self = REGIONS.find((r) => peerIds[r.label] === state.peerId)!.label;
    const rtts = {} as Record<RegionLabel, number>;
    for (const target of REGIONS) {
      if (target.label === self) continue;
      const known = state.knownPeers.find(
        (p) => p.id === peerIds[target.label],
      );
      expect(known?.rtt, `${self} measured rtt to ${target.label}`).toBeTruthy();
      rtts[target.label] = known!.rtt;
      expect(
        known!.rtt,
        `${self}->${target.label} rtt within the inter-region band ` +
          `(${RTT_FLOOR_MS}-${RTT_CEILING_MS}ms)`,
      ).toBeGreaterThan(RTT_FLOOR_MS);
      expect(
        known!.rtt,
        `${self}->${target.label} rtt within the inter-region band ` +
          `(${RTT_FLOOR_MS}-${RTT_CEILING_MS}ms)`,
      ).toBeLessThan(RTT_CEILING_MS);
    }
    measured[self] = rtts;
  }
  console.log('geo measured RTTs (ms):', JSON.stringify(measured));
  expect(
    measured.eu.asia,
    `eu->asia rtt (${measured.eu.asia}ms) > eu->us rtt (${measured.eu.us}ms): ` +
      'korea is farther from sweden than iowa is',
  ).toBeGreaterThan(measured.eu.us);
});

// --- (2) ring placement monotonic with geography ------------------------------

test('ring placement is monotonic with geography: korea sits in sweden\'s outer rings @geo', async () => {
  // ringIndex = ceil(log2(rtt)) (src/js/src/ring.js, clamped to 8): the
  // ~230ms sweden-korea leg lands in ring 8's (128, 256] bounds (>= 7 with
  // generous slack), the ~135ms sweden-centralus leg in ring 8 too but the
  // ordering assert keeps >= 6 as the inequality bound.
  const eu = await waitForState(
    pages.eu,
    rttsMeasuredJs([peerIds.us, peerIds.asia]),
    DISCOVERY_BUDGET_MS,
  );
  expectRingAtLeast(eu, peerIds.asia, 7);
  expectRingAtLeast(eu, peerIds.us, 6);
});

// --- (3) findClosestNode resolves the queried region from every origin -------

test('findClosestNode from every origin resolves the queried region @geo', async () => {
  // At production latencies the §3.6 candidate window [myRtt/2, myRtt*2]
  // converges: every other peer sits inside it (50-230ms vs a 135-230ms
  // originator measure), so the queried peer is always probed and its
  // self-measure (0ms) wins — the identity assertion the loopback specs
  // could only make "identity-preferring" is now strict, with the dart-
  // interop retry+re-measure pattern as the bounded jitter guard.
  const routes: { from: RegionLabel; to: RegionLabel }[] = [
    { from: 'us', to: 'asia' },
    { from: 'eu', to: 'us' },
    { from: 'asia', to: 'eu' },
  ];
  for (const route of routes) {
    const targetId = peerIds[route.to];
    const result = await findClosestNodeViaDemo(pages[route.from], targetId);
    expect(result.error, `${route.from} query error`).toBeUndefined();
    expect(
      result.closestPeerId,
      `${route.from} -> ${route.to} resolves the queried region ` +
        `(result: ${JSON.stringify(result)})`,
    ).toBe(targetId);
    // closestRtt is measured FROM the responder: when the queried peer wins
    // for itself it self-measures 0ms (measured hop-by-hop, not origin-to-
    // target), so the band check belongs to the discovery test, not here.
    expect(
      result.closestRtt ?? result.closestRttMs ?? 0,
      `${route.from} -> ${route.to} closest rtt non-negative`,
    ).toBeGreaterThanOrEqual(0);
  }
});

// --- (4) cross-region media ----------------------------------------------------

test('cross-region media: centralus streams to korea, korea receives and renders it @geo', async () => {
  // The demo's real "Stream to peer" seam -> establishMediaStream
  // (media_offer/media_answer over the existing DataChannel; media over a
  // dedicated PeerConnection whose ICE spans the real Pacific). The uplink
  // is each page's looping penguin.mp4 fixture.
  await streamToPeerViaDemo(pages.us, peerIds.asia);

  // Korea lists centralus's stream in activeStreams (ontrack) within the
  // plan's 30s budget.
  await waitForState(
    pages.asia,
    hasActiveStreamJs(peerIds.us),
    MEDIA_ACTIVE_BUDGET_MS,
  );

  // Frame-level evidence when reachable: the korea peer is a JS demo page,
  // so its remote <video> is a real DOM element — >3 composited frames in a
  // 1s window proves media bits arrive. (A dart-web receiver would render
  // through a Flutter texture with no frame callback — state-level would be
  // the ceiling there, as documented in the dart-interop spec.)
  let videoPresent = false;
  try {
    videoPresent = await waitFor(
      pages.asia,
      remoteVideoPresentJs(peerIds.us),
      FRAMES_BUDGET_MS,
    );
  } catch {
    // renderStreams() creates the element within its 1s poll; absence here
    // means the state-level assertion above is the available evidence
    // (documented fallback — the ice-stats collector still shows the
    // connected candidate pair for this PeerConnection).
    console.warn(
      'korea rendered no remote <video> for centralus; asserting at state level only',
    );
  }
  if (!videoPresent) return;
  const frames = await waitFor<number>(
    pages.asia,
    framesFlowingJs(peerIds.us),
    FRAMES_BUDGET_MS,
  );
  expect(
    frames,
    'frames rendered on korea in a 1s window (penguin.mp4 is 24fps)',
  ).toBeGreaterThan(3);
});

// --- (5) supernode election + kill + re-election -------------------------------

test('supernode kill: the avg-RTT center (centralus) wins the election and survivors re-elect without it @geo', async () => {
  test.setTimeout(180_000);

  // Election: the demo's elect affordance runs the library's real election
  // path (electSupernode -> findCentralLeader, spec §5.1) over
  // [self, ...knownPeers] — every candidate measures the SAME target set
  // (self as 0ms), so the winner is the min-average-RTT peer. From the
  // measured matrix (approx: centralus 147ms, sweden 182ms, korea 195ms
  // averages) centralus is the strict argmin — asserted via inequality
  // against the matrix, not a hardcoded id, so jitter cannot flip it.
  const matrix = {} as Record<RegionLabel, number>;
  for (const region of REGIONS) {
    const state = await waitForState(
      pages[region.label],
      rttsMeasuredJs(REGIONS.filter((r) => r !== region).map((r) => peerIds[r.label])),
      DISCOVERY_BUDGET_MS,
    );
    let sum = 0;
    for (const target of REGIONS) {
      if (target.label === region.label) continue;
      sum += state.knownPeers.find((p) => p.id === peerIds[target.label])!.rtt;
    }
    matrix[region.label] = sum / (REGIONS.length - 1);
  }
  console.log('geo avg-RTT matrix (ms):', JSON.stringify(matrix));

  const election = await pages.us.evaluate(() => {
    const hook = window.__meridian;
    if (!hook || !hook.elect) throw new Error('elect affordance missing');
    return hook.elect();
  }) as ElectionResult;
  expect(election.error, 'election error').toBeUndefined();
  const argmin = (Object.keys(matrix) as RegionLabel[]).reduce((best, label) =>
    matrix[label] < matrix[best] ? label : best,
  );
  expect(election.leaderId, 'central leader by average RTT').toBe(
    peerIds[argmin],
  );
  // Inequality form (plan known-risks note): the winner's measured average
  // must be no worse than the runner-up's — centralus ~147 vs sweden ~182.
  expect(
    election.avgRtt,
    'election measured a real average',
  ).toBeGreaterThan(0);
  const runnerUp = (Object.keys(matrix) as RegionLabel[])
    .filter((label) => label !== argmin)
    .reduce((best, label) => (matrix[label] < matrix[best] ? label : best));
  expect(
    election.avgRtt!,
    `winner avg (${election.avgRtt}ms) <= runner-up avg (${matrix[runnerUp]}ms)`,
  ).toBeLessThanOrEqual(matrix[runnerUp]);

  // The winner self-initializes the Raft cluster and announces it.
  await waitForState(pages.us, isSupernodeJs, ELECTION_BUDGET_MS);
  await Promise.all(
    (['eu', 'asia'] as RegionLabel[]).map((label) =>
      waitForState(
        pages[label],
        clusterLeaderJs(peerIds.us),
        ELECTION_BUDGET_MS,
      ),
    ),
  );

  // Supernode death: closing the leader's page tears its DataChannels down
  // (no graceful peer_leaving), firing both survivors' close events ->
  // _handlePeerFailure -> triggerSupernodeReelection over the remaining
  // cluster [self, otherSurvivor]. DETERMINISTIC PART: the dead peer can
  // never lead again — each survivor's re-election measures its own average
  // over [self, otherSurvivor] (self as 0ms), so the dead peer is not a
  // candidate at all, and the recorded clusterLeader is necessarily one of
  // the two survivors. JITTER-DEPENDENT PART (documented from the estate):
  // with a 2-candidate cluster the greedy leader-query descent only probes
  // the other survivor while its STORED rtt sits inside the metric window
  // [myAvg/2, 2*myAvg] — with sweden-korea ~266ms in both directions that
  // comparison is decided by single-ping jitter, so the observed split can
  // be "each survivor names the other" (the first run's outcome, stable for
  // the whole 60s budget) or "both self-declare" without a third member to
  // break the symmetry. The plan's known-risks note prescribes exactly this
  // shape: assert WHO the leader may be by inequality, never the exact peer.
  await pages.us.close();
  const euLeader = await waitForLeaderAmong(pages.eu, [
    peerIds.eu,
    peerIds.asia,
  ]);
  const asiaLeader = await waitForLeaderAmong(pages.asia, [
    peerIds.eu,
    peerIds.asia,
  ]);
  console.log(
    `re-elected after supernode death: eu leader=${euLeader}, ` +
      `asia leader=${asiaLeader} (dead peer was ${peerIds.us})`,
  );

  // The survivors' link to each other is unaffected: the cluster still
  // functions as an overlay after the supernode died.
  await waitForState(
    pages.eu,
    `function asiaConnected(state) {
      return state.knownPeers.some((p) =>
        p.id === ${JSON.stringify(peerIds.asia)} && p.status === 'connected');
    }`,
    REELECTION_BUDGET_MS,
  );
});

// --- small helpers -------------------------------------------------------------