import { expect, test, type Browser, type Page } from '@playwright/test';

import { startArtifacts, type ArtifactRun } from '../src/artifacts.js';
import { connectOverCDP } from '../src/cdp.js';
import {
  launchRegion,
  netnsDown,
  type LaunchedRegion,
  type NetnsRegion,
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
  type WireLogEntry,
} from '../src/orchestrator.js';

// Task 4: media e2e over the Task 3 netns rig. Three Chromium peers, each in
// its own UNPRIVILEGED Linux network namespace with a per-region `tc netem`
// egress delay (see src/netns-launch.ts), discover each other through the
// real signaling server, then stream REAL media: every page loads the demo
// with `?mediaSrc=/fixtures/penguin.mp4`, so the Connect flow uplinks a
// looping 1280x720@24fps file (via video.captureStream()) instead of the
// fake-device getUserMedia fallback.
//
//   RTT(a <-> b) = halfDelayMs_a + halfDelayMs_b  (+0-2ms slirp overhead)
//   eu=60/us=20/asia=100 half-delays -> eu<->us 80, eu<->asia 160,
//   us<->asia 120. The MIDDLE region (us) minimizes the average RTT to the
//   other two — with the demo's ?elect= driving affordance the candidates
//   measure the same target set (self + both others, self as 0ms), so
//   us (avg ~67ms) strictly beats eu (~80ms) and asia (~93ms) and the
//   election winner is deterministic under netem.
//
// Assertions poll (waitFor/waitForState) — no sleeps; budgets are upper
// bounds that resolve as soon as the condition holds.

// E2E_SIGNALING_PORT mirrors the other specs' override (this machine has a
// foreign service on 8080).
const SIGNALING_PORT = Number(process.env.E2E_SIGNALING_PORT) || 8080;
const DEMO_PORT = 8090;
// Loopback mini-STUN (see local-query-routing.spec.ts for the srflx model).
const STUN_PORT = Number(process.env.E2E_STUN_PORT) || 3478;
const STUN_URL = `stun:10.0.2.2:${STUN_PORT}`;
const GOSSIP_MS_OVERRIDE = 2000;
// Served by scripts/serve.mjs from src/e2e/fixtures/ under /fixtures/.
const MEDIA_SRC = '/fixtures/penguin.mp4';
// Upper bounds only — waitFor polls and resolves as soon as it holds.
const DISCOVERY_BUDGET_MS = 60_000;
// The library's media_answer timeout is queryTimeoutMs (30s), so the demo
// seam's success/failure log line can take that long in the worst case.
const MEDIA_HANDSHAKE_BUDGET_MS = 40_000;
// Frames only flow once the media PeerConnection's ICE + DTLS complete over
// the netem path — an upper bound on that convergence, not a sleep.
const FRAMES_BUDGET_MS = 20_000;
// Election: single-ping RTT measures over ring channels + probe round-trips.
const ELECTION_BUDGET_MS = 30_000;

const REGIONS: NetnsRegion[] = ['eu', 'us', 'asia'];

// The scripted RTT matrix (half-delays eu=60/us=20/asia=100): full-mesh
// discovery must still hold before any media flows.
const othersConnectedJs = (otherIds: string[]) =>
  `function othersConnected(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let found = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected') found++;
    }
    return found === wanted.size;
  }`;

/** PeerState -> truthy: peerId is in activeStreams. */
const hasActiveStreamJs = (peerId: string) =>
  `function hasActiveStream(state) {
    return state.activeStreams.includes(${JSON.stringify(peerId)});
  }`;

/** PeerState -> truthy: peerId is NOT in activeStreams. */
const lacksActiveStreamJs = (peerId: string) =>
  `function lacksActiveStream(state) {
    return !state.activeStreams.includes(${JSON.stringify(peerId)});
  }`;

/** PeerState -> truthy: we were told to expect a relayed stream from peerId. */
const hasForwardedStreamJs = (sourcePeerId: string) =>
  `function hasForwardedStream(state) {
    return (state.forwardedStreams || []).includes(${JSON.stringify(sourcePeerId)});
  }`;

/** PeerState -> truthy: this node won the supernode election. */
const isSupernodeJs = `function isSupernode(state) {
  return state.isSupernode === true;
}`;

/** PeerState -> truthy: clusterLeader is peerId (spec §5.1 announcement). */
const clusterLeaderJs = (leaderId: string) =>
  `function clusterLeaderIs(state) {
    return state.clusterLeader === ${JSON.stringify(leaderId)};
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

/** recv forwarded_stream naming the source peer (spec §6.2 SFU signal). */
const hasForwardedStreamWireJs = (sourcePeerId: string) =>
  `function forwardedStreamWire(log) {
    return log.some((e) => e.dir === 'recv' && e.type === 'forwarded_stream' &&
      e.payload && e.payload.sourcePeerId === ${JSON.stringify(sourcePeerId)});
  }`;

/** recv media_close from the named sender (spec §7.1). */
const hasMediaCloseWireJs = (senderId: string) =>
  `function mediaCloseWire(log) {
    return log.some((e) => e.dir === 'recv' && e.type === 'media_close' &&
      e.payload && e.payload.senderId === ${JSON.stringify(senderId)});
  }`;

/** send media_close to the named peer. */
const hasMediaCloseSentWireJs = (targetPeerId: string) =>
  `function mediaCloseSentWire(log) {
    return log.some((e) => e.dir === 'send' && e.type === 'media_close' &&
      e.peerId === ${JSON.stringify(targetPeerId)});
  }`;

/**
 * The demo's local-preview slot (#local, index.html) doubles as the
 * ?mediaSrc= uplink element: it must still be playing the fixture file
 * when the node boots, proving the demo affordance armed the uplink.
 */
const fileUplinkPlayingJs = `function fileUplinkPlaying() {
  const video = document.getElementById('local');
  if (!video || !video.src || !video.src.endsWith('${MEDIA_SRC}')) return null;
  if (video.readyState < 2 || video.paused) return null;
  return { width: video.videoWidth, height: video.videoHeight };
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

/**
 * Counts the frames actually RENDERED by the remote <video> the demo's
 * renderStreams() created for a peer (data-peer-id) over one in-page second
 * — requestVideoFrameCallback fires per composited frame, so this proves
 * media bits arrive, not just that track events fired. Resolves 0 (immediately
 * falsy for waitFor) when the element is gone.
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
      const tick = () => {
        n++;
        video.requestVideoFrameCallback(tick);
      };
      video.requestVideoFrameCallback(tick);
      setTimeout(() => resolve(n), 1000);
    });
  }`;

/** Truthy once >3 frames rendered in a full 1s window (media is flowing). */
const framesFlowingJs = (peerId: string) =>
  `function framesFlowing() {
    return (${countFramesInSecondJs(peerId)})().then((n) => (n > 3 ? n : null));
  }`;

/**
 * Truthy when a full 1s window passed with at most 1 frame — after a close
 * the tracks stop (and the demo prunes the <video> within its 1s poll), so
 * the compositor produces no NEW frames.
 */
const framesStoppedJs = (peerId: string) =>
  `function framesStopped() {
    return (${countFramesInSecondJs(peerId)})().then((n) => (n <= 1 ? { n } : null));
  }`;

// --- suite-local helpers ---------------------------------------------------

interface StreamResult {
  ok: boolean;
  error?: string;
}

/**
 * Drives the demo's real "Stream to peer" flow: answers the prompt() dialog
 * with the target and clicks the demo's own Stream button, which runs the
 * library's establishMediaStream (media_offer/media_answer riding the
 * EXISTING peer DataChannel). Resolves once the demo log records the
 * outcome; throws with the demo's error text on failure.
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

// --- rig lifecycle ---------------------------------------------------------

let signaling: SignalingProcess;
let demo: DemoServer;
let miniStun: MiniStunServer;
let launched: LaunchedRegion[];
// Task 12: this spec opts into the per-run artifact collector (see
// src/artifacts.ts). The other specs are untouched — the collector is opt-in
// per spec, not a fixture the whole suite inherits.
let artifacts: ArtifactRun | null = null;
const browsers: Browser[] = [];
const pages = {} as Record<NetnsRegion, Page>;
const peerIds = {} as Record<NetnsRegion, string>;

const rigFor = (region: NetnsRegion): LaunchedRegion =>
  launched.find((r) => r.region === region)!;

// Same rig as local-query-routing.spec.ts (3 netns + slirp + 3 Chromium
// boots, 180s readiness deadline so the launcher's log-tail forensics can
// surface before a generic hook timeout would).
test.beforeAll(async ({}, testInfo) => {
  test.setTimeout(180_000);
  // Start the collector FIRST so the signaling server's JSONL log lands in
  // the run dir (relayed traffic + register/disconnect, Task 12).
  artifacts = startArtifacts(testInfo);
  signaling = startSignaling(SIGNALING_PORT, { logFile: artifacts.signalingPath });
  await signaling.ready;

  // HOST=0.0.0.0: the netns peers load the demo (and the media fixture)
  // through their slirp gateway (10.0.<i>.2 -> the host's loopback).
  demo = startServe({ host: '0.0.0.0', port: DEMO_PORT });
  await demo.ready;

  miniStun = startMiniStun({ port: STUN_PORT });
  await miniStun.ready;

  launched = await Promise.all(REGIONS.map((r) => launchRegion(r)));

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

  // Ice-stats reachability: the tracker init script must be installed BEFORE
  // the demo page loads (it wraps the RTCPeerConnection constructor; after
  // load it would see nothing and the ice-stats collector would record 0
  // lines — the documented graceful skip).
  for (const region of REGIONS) artifacts?.installPcTracker(pages[region]);

  // Every page loads with a REAL looping media source (penguin.mp4), so
  // every peer joins with a file-driven uplink instead of the fake device.
  await Promise.all(
    REGIONS.map(async (region) => {
      const spec = rigFor(region).spec;
      await openPeer(
        pages[region],
        `http://${spec.gateway}:${DEMO_PORT}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}` +
          `&stun=${STUN_URL}&mediaSrc=${MEDIA_SRC}`,
      );
      await connect(pages[region], `ws://${spec.gateway}:${SIGNALING_PORT}`);
    }),
  );

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

  // Per-peer artifacts for the rest of the run: 500ms state + wirelog JSONL,
  // console/pageerror stream, 1s getStats() digests (Task 12). All run on
  // their own intervals — nothing here is on a test's critical path.
  artifacts?.writeTopology({
    rig: 'unprivileged netns + tc netem egress (src/netns-launch.ts)',
    signalingUrl: `ws://<gateway>:${SIGNALING_PORT}`,
    stun: STUN_URL,
    gossipMsOverride: GOSSIP_MS_OVERRIDE,
    mediaSrc: MEDIA_SRC,
    regions: Object.fromEntries(
      REGIONS.map((region) => [
        region,
        {
          halfDelayMs: rigFor(region).spec.halfDelayMs,
          gateway: rigFor(region).spec.gateway,
          cdpHostPort: rigFor(region).spec.cdpHostPort,
          peerId: peerIds[region],
        },
      ]),
    ),
    // RTT(a <-> b) = halfDelayMs_a + halfDelayMs_b (+0-2ms slirp overhead).
    rttMatrixMs: {
      'eu-us': 80,
      'eu-asia': 160,
      'us-asia': 120,
    },
  });
  for (const region of REGIONS) {
    await artifacts?.startPeerRecorder(pages[region], peerIds[region]);
    artifacts?.startIceStats(pages[region], peerIds[region]);
  }
});

test.afterEach(async ({}, testInfo) => {
  // result.json: this test's name/outcome (TestInfo already knows both).
  artifacts?.recordResult(testInfo);
});

test.afterAll(async () => {
  // Flush + close every collector before the pages disappear.
  await artifacts?.close();
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

// --- (0) uplink affordance + discovery -------------------------------------

test('every peer boots with a looping file uplink and joins the full mesh', async () => {
  // Each page's ?mediaSrc= uplink: the #local slot is playing the fixture
  // file (decoded and rendering — readyState >= 2, not paused).
  const uplinks = await Promise.all(
    REGIONS.map((region) =>
      waitFor<{ width: number; height: number }>(
        pages[region],
        fileUplinkPlayingJs,
        DISCOVERY_BUDGET_MS,
      ),
    ),
  );
  for (const [i, region] of REGIONS.entries()) {
    expect(
      uplinks[i].width,
      `${region} file uplink has decoded frames`,
    ).toBeGreaterThan(0);
  }

  // Full mesh over the netns rig (signaling-driven discovery + gossip).
  await Promise.all(
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
});

// --- (a) direct media --------------------------------------------------------

test('direct media: frames actually flow from eu into us over the netns link', async () => {
  // The demo's real "Stream to peer" seam -> the library's
  // establishMediaStream (offer/answer over the existing DataChannel, media
  // over a dedicated PeerConnection).
  await streamToPeerViaDemo(pages.eu, peerIds.us);

  // us lists eu's stream in activeStreams (pc.ontrack) within the 10s spec
  // budget — an upper bound; waitFor resolves as soon as it appears.
  await waitForState(
    pages.us,
    hasActiveStreamJs(peerIds.eu),
    FRAMES_BUDGET_MS,
  );

  // Frames actually flow: >3 composited frames on the rendered remote
  // <video> within a full 1s window (penguin.mp4 is 24fps; a stalled or
  // black track would yield ~0).
  const frames = await waitFor<number>(
    pages.us,
    framesFlowingJs(peerIds.eu),
    FRAMES_BUDGET_MS,
  );
  expect(frames, 'frames rendered on us in a 1s window').toBeGreaterThan(3);
});

// --- (b) SFU relay -----------------------------------------------------------

test('SFU relay: us wins the central-leader election and forwards the stream announcement', async () => {
  // The library's own election gate (knownPeers.size >= 5 after 10s, spec
  // §9) cannot fire with a 3-peer overlay, so the demo's ?elect driving
  // affordance runs the SAME public election path (electSupernode ->
  // findCentralLeader, spec §5.1) over the whole cluster. With the scripted
  // RTT matrix the middle peer (us) strictly minimizes the average RTT:
  // us ~67ms vs eu ~80ms vs asia ~93ms (self measures 0ms for everyone).
  const election = await pages.us.evaluate(() => {
    const hook = window.__meridian;
    if (!hook || !hook.elect) throw new Error('elect affordance missing');
    return hook.elect();
  });
  expect(election.error, 'election error').toBeUndefined();
  expect(election.leaderId, 'central leader by average RTT').toBe(
    peerIds.us,
  );
  expect(election.avgRtt, 'election measured a real average').toBeGreaterThan(
    0,
  );

  // The winner self-initializes the Raft cluster and announces it; the
  // followers acknowledge the leader (spec §5.1).
  await waitForState(pages.us, isSupernodeJs, ELECTION_BUDGET_MS);
  await Promise.all(
    (['eu', 'asia'] as NetnsRegion[]).map((region) =>
      waitForState(
        pages[region],
        clusterLeaderJs(peerIds.us),
        ELECTION_BUDGET_MS,
      ),
    ),
  );

  // Stream asia -> us (the supernode). The supernode's SFU (setupMedia-
  // Forwarding ran on election) announces the freshly received stream to
  // the OTHER cluster member: eu must receive `forwarded_stream`.
  await streamToPeerViaDemo(pages.asia, peerIds.us);
  await waitForState(
    pages.us,
    hasActiveStreamJs(peerIds.asia),
    FRAMES_BUDGET_MS,
  );

  // eu (the third peer) got the signal — on the wire AND in the library's
  // forwarded-streams projection.
  await waitFor<WireLogEntry[]>(
    pages.eu,
    wireSeenJs(hasForwardedStreamWireJs(peerIds.asia)),
    ELECTION_BUDGET_MS,
  );
  await waitForState(
    pages.eu,
    hasForwardedStreamJs(peerIds.asia),
    ELECTION_BUDGET_MS,
  );

  // And the media itself really flows into the supernode: the relay's input
  // leg renders frames (the announcement is signal-level; the current
  // library does not re-transcode media to the third peer).
  const frames = await waitFor<number>(
    pages.us,
    framesFlowingJs(peerIds.asia),
    FRAMES_BUDGET_MS,
  );
  expect(frames, 'frames rendered on the supernode in a 1s window').toBeGreaterThan(
    3,
  );
});

// --- (c) close ---------------------------------------------------------------

test('close: media_close on the wire, stream removed, frames stop', async () => {
  // Precondition: asia -> us media from the SFU test is still live.
  await waitForState(
    pages.us,
    hasActiveStreamJs(peerIds.asia),
    FRAMES_BUDGET_MS,
  );

  // The library's public close path (spec §7.1): closeStream sends
  // media_close over the existing DataChannel and tears the media
  // PeerConnection down on both sides.
  await pages.us.evaluate((targetPeerId) => {
    const hook = window.__meridian;
    if (!hook || !hook.closeStream) {
      throw new Error('closeStream affordance missing');
    }
    return hook.closeStream(targetPeerId);
  }, peerIds.asia);

  // The closer: media_close on the wire and its activeStreams entry gone.
  await waitFor<WireLogEntry[]>(
    pages.us,
    wireSeenJs(hasMediaCloseSentWireJs(peerIds.asia)),
    FRAMES_BUDGET_MS,
  );
  await waitForState(
    pages.us,
    lacksActiveStreamJs(peerIds.asia),
    FRAMES_BUDGET_MS,
  );

  // The remote side: media_close received, stream removed.
  await waitFor<WireLogEntry[]>(
    pages.asia,
    wireSeenJs(hasMediaCloseWireJs(peerIds.us)),
    FRAMES_BUDGET_MS,
  );
  await waitForState(
    pages.asia,
    lacksActiveStreamJs(peerIds.us),
    FRAMES_BUDGET_MS,
  );

  // The rendered remote <video> stops producing frames: the tracks were
  // stopped and the demo prunes the element within its 1s poll, so a full
  // 1s window yields at most the one trailing composited frame.
  await waitFor<{ n: number }>(
    pages.us,
    framesStoppedJs(peerIds.asia),
    FRAMES_BUDGET_MS,
  );
});