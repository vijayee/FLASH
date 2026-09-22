import { chromium, expect, test, type Browser, type Page } from '@playwright/test';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { expectKnownPeer } from '../src/assertions.js';
import { connectOverCDP } from '../src/cdp.js';
import { launchRegion, netnsDown } from '../src/netns-launch.js';
import {
  connect,
  openPeer,
  startServe,
  startSignaling,
  waitFor,
  waitForState,
  type DemoServer,
  type PeerState,
  type SignalingProcess,
} from '../src/orchestrator.js';

// Task 6: cross-language interop. One JS peer (Chromium inside the `eu`
// netns rig) and the Dart example's Flutter web bundle (staged by
// scripts/build-dart-web.sh into src/e2e/build/dart-web and served on port
// 8091 by serve.mjs's DART_ROOT mode) discover each other through the real
// signaling server, answer closest-node queries across languages, and
// stream real media BOTH ways (JS -> Dart, then Dart -> JS after a clean
// close — the libraries keep one media connection per peer, so the reverse
// direction is exercised on a torn-down previous stream, not concurrently).
//
// The Dart tab runs in a plain host Chromium (secure context on 127.0.0.1,
// so getUserMedia/captureStream work under the fake-device launch flags).
// Its page is driven through the example's seams:
//   - window.__meridianState() — the JSON state snapshot (Task 1)
//   - window.__meridianAction(action, arg) — drives find/stream (Task 6;
//     CanvasKit renders no DOM widgets for Playwright to click)
//   - ?signaling=/?stun=/?gossipMs=/?mediaSrc= URL params (Task 6)
//
// ICE between the host tab and the netns peer: the netns Chromium does NOT
// get ?stun= here, so it uses its default STUN (mapped by
// --host-resolver-rules in netns-up.sh) — its srflx candidate is the HOST's
// egress IP + a slirp-mapped port, which the host tab reaches through the
// same local hairpin the netns specs rely on (and it can reach the host's
// host candidates through its gateway). The `eu` region runs with a 2ms
// half-delay override (launchRegion overrides): netns isolation without
// the geo matrix — RTT stays in the loopback band the assertions expect.

// E2E_SIGNALING_PORT mirrors the other specs' override (this machine has a
// foreign service on 8080).
const SIGNALING_PORT = Number(process.env.E2E_SIGNALING_PORT) || 8080;
const DEMO_PORT = 8090;
const DART_PORT = 8091;
// Staged Flutter web bundle (scripts/build-dart-web.sh output). The spec
// never rebuilds it: a missing bundle fails fast with the fix command.
const DART_WEB_ROOT = fileURLToPath(
  new URL('../build/dart-web', import.meta.url),
);
// Loopback-band netns peer: 2ms egress half-delay (see header for why the
// geo rig's default eu=60ms would break the <50ms loopback RTT band).
const EU_HALF_DELAY_MS = 2;
const GOSSIP_MS_OVERRIDE = 2000;
// Loopback RTT band (+ slirp/hairpin slack): asserted, never slept for.
const RTT_MAX_MS = 50;
// Upper bounds only — waitFor polls and resolves as soon as it holds.
const DISCOVERY_BUDGET_MS = 60_000;
// The library's media_answer timeout is 30s (queryTimeout), so the demo
// seam's success/failure record can take that long in the worst case.
const MEDIA_HANDSHAKE_BUDGET_MS = 40_000;
// Frames only flow once the media PeerConnection's ICE + DTLS complete.
const FRAMES_BUDGET_MS = 20_000;

// --- page-side predicate sources (evaluated via waitFor/waitForState) ------

/** PeerState -> truthy: peerId is known AND connected. */
const peerConnectedJs = (peerId: string) =>
  `function peerConnected(state) {
    return state.knownPeers.some(
      (p) => p.id === ${JSON.stringify(peerId)} && p.status === 'connected');
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

/**
 * KnownPeers entry for peerId once its rtt is measured (gossip-driven
 * pings); null before. The loopback band is asserted by the test, not the
 * predicate, so the failure message shows the actual measured value.
 */
const readRttJs = (peerId: string) =>
  `function readRtt() {
    const state = window.__meridianState
      ? JSON.parse(window.__meridianState())
      : window.__meridian.state();
    const known = state.knownPeers.find(
      (p) => p.id === ${JSON.stringify(peerId)});
    if (!known || known.rtt === null || known.rtt === undefined) return null;
    return known;
  }`;

/**
 * PeerState -> the lastFindResult once it resolved the given target AND its
 * JSON differs from `prevJson` (a fresh attempt's outcome, not the stale
 * one the previous retry produced).
 */
const newestDartFindResultJs = (targetPeerId: string, prevJson: string) =>
  `function newestDartFindResult(state) {
    const r = state.lastFindResult;
    if (!r || r.target !== ${JSON.stringify(targetPeerId)}) return null;
    return JSON.stringify(r) !== ${JSON.stringify(prevJson)} ? r : null;
  }`;

/** Reads the JS demo log line the real "Stream to peer" seam writes. */
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

/** PeerState -> the lastStreamResult once it resolved the given target. */
const dartStreamResultJs = (targetPeerId: string) =>
  `function dartStreamResult(state) {
    const r = state.lastStreamResult;
    if (!r || r.peerId !== ${JSON.stringify(targetPeerId)}) return null;
    return r;
  }`;

/**
 * Counts the frames actually RENDERED by the JS demo's remote <video> for a
 * peer over one in-page second (requestVideoFrameCallback fires per
 * composited frame) — proves media bits arrive, not just track events.
 * Used on the JS side only: the Dart example renders remote video through
 * RTCVideoView (a Flutter texture, not a DOM <video>), so no frame-callback
 * counter is reachable there (documented Task 6 limitation).
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

// --- suite-local helpers ---------------------------------------------------

interface ClosestResult {
  closestPeerId?: string;
  closestRtt?: number;
  closestRttMs?: number;
  hopCount?: number;
  error?: string;
}

/**
 * At loopback RTTs (10-30ms over the slirp hairpin, with single-ping
 * jitter), the spec §3.6 candidate window [myRtt/2, myRtt*2] — measured
 * FRESH against the STORED gossip-averaged rtt — can exclude the only
 * other peer, and the originator then legitimately answers "I'm closest"
 * (self). A repeated query re-measures and lands in-window, so both query
 * legs retry a bounded number of times before their identity assertions.
 */
const MAX_QUERY_ATTEMPTS = 3;

/** PeerState -> the newest find-result line whose JSON differs from prev. */
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

/**
 * Drives the JS demo's real find-closest-node flow: answers the prompt()
 * dialog with the target, clicks the demo's own Find button, and polls
 * the demo log for the result line (the library's routeQuery). Retries
 * bounded times on the loopback candidate-window fallback (see above).
 */
async function findClosestNodeViaDemo(
  page: Page,
  targetPeerId: string,
): Promise<ClosestResult> {
  let prevJson = '';
  for (let attempt = 1; attempt <= MAX_QUERY_ATTEMPTS; attempt++) {
    page.once('dialog', (dialog) => {
      void dialog.accept(targetPeerId).catch(() => {});
    });
    await page.click('#find');
    const result = await waitFor<ClosestResult>(
      page,
      newestFindResultJs(targetPeerId, prevJson),
      25_000,
    );
    if (result.closestPeerId === targetPeerId) return result;
    prevJson = JSON.stringify(result);
  }
  throw new Error(`find from JS to ${targetPeerId} never resolved the target`);
}

/**
 * Drives the Dart example's find through the __meridianAction seam (the
 * same handler its Find button calls), observing lastFindResult; retries
 * bounded times on the loopback candidate-window fallback (see above).
 */
async function findFromDart(
  page: Page,
  targetPeerId: string,
): Promise<PeerState> {
  let prevJson = '';
  for (let attempt = 1; attempt <= MAX_QUERY_ATTEMPTS; attempt++) {
    await driveDartAction(page, 'find', targetPeerId);
    const state = await waitForState(
      page,
      newestDartFindResultJs(targetPeerId, prevJson),
      35_000,
    );
    const result = state.lastFindResult;
    if (result?.closestPeerId === targetPeerId) return state;
    prevJson = JSON.stringify(result);
  }
  throw new Error(
    `find from Dart to ${targetPeerId} never resolved the target`,
  );
}

/** Fires the Dart example's `window.__meridianAction` drive seam (Task 6). */
async function driveDartAction(
  page: Page,
  action: 'find' | 'stream',
  arg: string,
): Promise<void> {
  const pair: [string, string] = [action, arg];
  await page.evaluate(
    ([actionName, actionArg]: [string, string]) => {
      const hook = (
        window as unknown as {
          __meridianAction?: (a: string, b: string) => void;
        }
      ).__meridianAction;
      if (!hook) throw new Error('__meridianAction is not installed');
      hook(actionName, actionArg);
    },
    pair,
  );
}

interface StreamResult {
  ok: boolean;
  error?: string;
}

/**
 * Drives the JS demo's real "Stream to peer" flow: answers the prompt()
 * dialog with the target and clicks the demo's own Stream button, which
 * runs the library's establishMediaStream (media_offer/media_answer riding
 * the EXISTING peer DataChannel). Resolves once the demo log records the
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
let dartDemo: DemoServer;
let launched: Awaited<ReturnType<typeof launchRegion>> | undefined;
let hostBrowser: Browser | undefined;
let euPage: Page | undefined;
let dartPage: Page | undefined;
let euPeerId = '';
let dartPeerId = '';

test.beforeAll(async () => {
  test.setTimeout(180_000);

  // The staged Dart bundle is a build artifact (gitignored): fail fast with
  // the fix command instead of a confusing 404-page boot failure.
  if (!existsSync(`${DART_WEB_ROOT}/index.html`)) {
    throw new Error(
      `staged dart-web bundle missing at ${DART_WEB_ROOT}; ` +
        `run src/e2e/scripts/build-dart-web.sh first`,
    );
  }

  signaling = startSignaling(SIGNALING_PORT);
  await signaling.ready;

  // HOST=0.0.0.0: the eu netns peer loads the JS demo through its slirp
  // gateway (10.0.2.2 -> the host's loopback). The Dart server stays
  // host-loopback: its only client is the host Chromium tab.
  demo = startServe({ host: '0.0.0.0', port: DEMO_PORT });
  await demo.ready;
  dartDemo = startServe({ port: DART_PORT, dartRoot: DART_WEB_ROOT });
  await dartDemo.ready;

  // The eu rig with a loopback-band delay override (see header).
  launched = await launchRegion('eu', { halfDelayMs: EU_HALF_DELAY_MS });

  // The Dart tab's host browser. mDNS candidate obfuscation must be off
  // (the netns peer could otherwise never resolve the host's host
  // candidates), plus the e2e media/autoplay flags.
  hostBrowser = await chromium.launch({
    args: [
      '--use-fake-ui-for-media-stream',
      '--use-fake-device-for-media-stream',
      '--autoplay-policy=no-user-gesture-required',
      '--no-sandbox',
      '--disable-features=WebRtcHideLocalIpsWithMdns',
      '--allow-loopback-in-peer-connection',
      '--no-proxy-server',
    ],
  });
  dartPage = await hostBrowser.newPage();

  // The eu peer: CDP-attached browsers have no implicit-context creation;
  // reuse the browser's existing default context (about:blank). The
  // browser handle is intentionally not closed — closing it only detaches
  // Playwright from the rig's Chromium; the rig itself is torn down below.
  const euBrowser = await connectOverCDP(launched.endpoint);
  const ctx = euBrowser.contexts()[0] ?? (await euBrowser.newContext());
  euPage = await ctx.newPage();

  // JS peer: no ?stun= (its default STUN gives the host-reachable srflx the
  // cross-namespace ICE needs — see header), with a real looping uplink for
  // the JS->Dart media leg.
  await openPeer(
    euPage,
    `http://10.0.2.2:${DEMO_PORT}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}` +
      `&mediaSrc=/fixtures/penguin.mp4`,
  );
  await connect(euPage, `ws://10.0.2.2:${SIGNALING_PORT}`);

  // Dart peer: auto-connects at page load through ?signaling=, with the
  // same looping file uplink via ?mediaSrc= (the example's Task 6
  // affordance, served from its own origin by the DART_ROOT server).
  await dartPage.goto(
    `http://127.0.0.1:${DART_PORT}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}` +
      `&mediaSrc=/fixtures/penguin.mp4&signaling=ws://127.0.0.1:${SIGNALING_PORT}`,
  );

  // Every node exists now (the Dart hook reports initialized:false before
  // that; the JS hook's state() is null) and has an id.
  const [eu, dart] = await Promise.all([
    waitForState(
      euPage,
      `function bootstrapped(state) { return !!state.peerId; }`,
      DISCOVERY_BUDGET_MS,
    ),
    waitForState(
      dartPage,
      `function bootstrapped(state) { return !!state.peerId; }`,
      DISCOVERY_BUDGET_MS,
    ),
  ]);
  euPeerId = eu.peerId;
  dartPeerId = dart.peerId;
  expect(euPeerId, 'eu peer id').toBeTruthy();
  expect(dartPeerId, 'dart peer id').toBeTruthy();
});

test.afterAll(async () => {
  try {
    await hostBrowser?.close();
  } catch {
    // Already dead.
  }
  if (launched) {
    try {
      await netnsDown('eu');
    } catch {
      // Best-effort: the down script is idempotent.
    }
  }
  for (const server of [demo, dartDemo, signaling]) {
    try {
      server?.stop();
    } catch {
      // Already dead.
    }
  }
});

// --- (a) cross-language discovery ------------------------------------------

test('cross-language discovery: the JS and Dart peers list each other connected', async () => {
  const [eu, dart] = await Promise.all([
    waitForState(euPage!, peerConnectedJs(dartPeerId), DISCOVERY_BUDGET_MS),
    waitForState(dartPage!, peerConnectedJs(euPeerId), DISCOVERY_BUDGET_MS),
  ]);
  expectKnownPeer(eu, dartPeerId);
  expectKnownPeer(dart, euPeerId);
});

// --- (b) cross-language closest-node queries -------------------------------

test('cross-language query: findClosestNode resolves the other language both ways', async () => {
  // JS -> Dart: the JS demo's real Find flow (prompt + button -> the
  // library's routeQuery; the Dart node self-probes at 0ms and wins).
  const jsResult = await findClosestNodeViaDemo(euPage!, dartPeerId);
  expect(jsResult.error, 'JS query error').toBeUndefined();
  expect(jsResult.closestPeerId, 'JS -> Dart closest peer').toBe(dartPeerId);
  expect(
    jsResult.closestRtt ?? jsResult.closestRttMs ?? 0,
    'JS -> Dart closestRtt non-negative',
  ).toBeGreaterThanOrEqual(0);

  // Dart -> JS: the example's __meridianAction seam runs the SAME handler
  // its Find button calls (node.findClosestNode); the outcome is observed
  // through the state hook's lastFindResult (the Dart side renders no DOM
  // log to scrape). findFromDart already retried the loopback
  // candidate-window fallback and only returns a state whose result
  // resolved the target.
  const dartState = await findFromDart(dartPage!, euPeerId);
  const dartResult = dartState.lastFindResult;
  expect(dartResult, 'Dart recorded a find result').toBeTruthy();
  expect(dartResult!.error, 'Dart query error').toBeUndefined();
  expect(dartResult!.closestPeerId, 'Dart -> JS closest peer').toBe(euPeerId);
  expect(
    dartResult!.closestRttMs ?? 0,
    'Dart -> JS closestRtt non-negative',
  ).toBeGreaterThanOrEqual(0);
});

// --- (c) cross-language media -----------------------------------------------

test('cross-language media: JS streams to the Dart peer', async () => {
  // The JS demo's real "Stream to peer" seam -> establishMediaStream
  // (media_offer/media_answer over the existing DataChannel; the uplink is
  // the looping penguin.mp4 fixture).
  await streamToPeerViaDemo(euPage!, dartPeerId);

  // The Dart node lists the JS peer's stream in activeStreams (its
  // onRemoteStreamAdded path). FRAME-LEVEL LIMITATION: the Dart example
  // renders remote video through RTCVideoView (a Flutter texture, not a
  // DOM <video>), so requestVideoFrameCallback is not reachable there —
  // the state field is the receive-side evidence at this seam.
  await waitForState(dartPage!, hasActiveStreamJs(euPeerId), FRAMES_BUDGET_MS);
});

test('cross-language media: the Dart peer streams back to JS', async () => {
  // Clean slate: the JS->Dart stream from the previous test holds the
  // libraries' single media-connection slot per peer, and an async teardown
  // could race a fresh handshake — so the reverse leg starts after an
  // explicit closeStream (spec §7.1: the closer sends media_close and both
  // sides tear down) and the close is observed on BOTH sides.
  await euPage!.evaluate((targetPeerId) => {
    const hook = window.__meridian;
    if (!hook || !hook.closeStream) {
      throw new Error('closeStream affordance missing');
    }
    return hook.closeStream(targetPeerId);
  }, dartPeerId);
  await waitForState(
    dartPage!,
    lacksActiveStreamJs(euPeerId),
    FRAMES_BUDGET_MS,
  );
  await waitForState(
    euPage!,
    lacksActiveStreamJs(dartPeerId),
    FRAMES_BUDGET_MS,
  );

  // Dart -> JS: the example's __meridianAction seam runs the same handler
  // its Stream button calls (node.establishMediaStream) — its uplink is
  // the looping penguin.mp4 file (its ?mediaSrc= affordance).
  await driveDartAction(dartPage!, 'stream', euPeerId);
  const dartState = await waitForState(
    dartPage!,
    dartStreamResultJs(euPeerId),
    MEDIA_HANDSHAKE_BUDGET_MS,
  );
  const streamResult = dartState.lastStreamResult;
  expect(streamResult, 'Dart recorded a stream result').toBeTruthy();
  expect(streamResult!.ok, `dart stream error: ${streamResult!.error}`).toBe(
    true,
  );

  // The JS peer receives it: activeStreams + frames actually rendered by
  // the demo's remote <video> (>3 composited frames in a 1s window —
  // penguin.mp4 is 24fps; a stalled track would yield ~0).
  await waitForState(euPage!, hasActiveStreamJs(dartPeerId), FRAMES_BUDGET_MS);
  const frames = await waitFor<number>(
    euPage!,
    framesFlowingJs(dartPeerId),
    FRAMES_BUDGET_MS,
  );
  expect(
    frames,
    'frames rendered on the JS peer in a 1s window',
  ).toBeGreaterThan(3);
});

// --- ring/RTT cross-check ---------------------------------------------------

test('ring placement: both sides measure a loopback-band RTT for each other', async () => {
  const [euEntry, dartEntry] = await Promise.all([
    waitFor<{ id: string; rtt: number }>(
      euPage!,
      readRttJs(dartPeerId),
      DISCOVERY_BUDGET_MS,
    ),
    waitFor<{ id: string; rtt: number }>(
      dartPage!,
      readRttJs(euPeerId),
      DISCOVERY_BUDGET_MS,
    ),
  ]);
  expect(euEntry.rtt, `eu's measured rtt to the Dart peer`).toBeGreaterThanOrEqual(
    0,
  );
  expect(
    euEntry.rtt,
    `eu's rtt to the Dart peer is loopback-band (< ${RTT_MAX_MS}ms)`,
  ).toBeLessThan(RTT_MAX_MS);
  expect(
    dartEntry.rtt,
    `dart's measured rtt to the JS peer`,
  ).toBeGreaterThanOrEqual(0);
  expect(
    dartEntry.rtt,
    `dart's rtt to the JS peer is loopback-band (< ${RTT_MAX_MS}ms)`,
  ).toBeLessThan(RTT_MAX_MS);
});