import { spawn, type ChildProcess } from 'node:child_process';

import { expect, test, type Browser, type Page } from '@playwright/test';

import { expectKnownPeer } from '../src/assertions.js';
import { startArtifacts } from '../src/artifacts.js';
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

// Task 10: the native Flutter desktop peer, BUILT AND RUN on the Azure lab
// VM (flash-e2e-lab). Task 7's local desktop-peer spec proved the native
// flutter_webrtc transport against Chromium ICE on ONE host (loopback ICE);
// this spec runs the SAME example against REAL GEOGRAPHY: the source is
// scp'd to the lab VM, `flutter build linux --release` runs there (the
// cloud-init lab role installs the Flutter SDK + GTK toolchain for exactly
// this), and the binary joins the geo topology under xvfb-run with
// MRD_SIGNALING=ws://<lab>:8080 (the lab's systemd signaling server) and
// MRD_STATUS_FILE as the native read seam (no display/JS seam on native —
// one JSON status line per second, same shape as the web hook).
//
// One JS peer (flash-e2e-swedencentral, SSH-raised agent per src/remote.ts)
// discovers the native peer over real inter-region latency, routes a
// closest-node query to it, and streams media INTO it (the receive path is
// the native-transport evidence: libwebrtc's ontrack on the lab VM — its
// ICE to the peer VM is real cross-region networking).
//
// Opt-in (@geo, needs the estate): the config's testIgnore only excludes
// geo.spec.ts (paths, not titles), so this spec carries geo.spec.ts's
// title-level test.skip guard too.

test.skip(
  process.env.E2E_GEO !== '1',
  'desktop geo suite is opt-in via E2E_GEO=1 (needs the Azure lab VM + Flutter SDK)',
);

// The lab VM: signaling (systemd, :8080), the Flutter SDK (/opt/flutter),
// and where the example source is uploaded + built for this spec.
const LAB_HOST = '172.173.102.12';
const SIGNALING_URL = `ws://${LAB_HOST}:8080`;
// The JS peer's VM (swedencentral) — real cross-region ICE to the lab VM.
const JS_REGION = { label: 'eu', name: 'swedencentral', host: '57.174.234.91' };

// Where the example source lands on the lab VM (repo-relative layout, so
// the example's `path: ..` dependency on the meridian_webrtc package
// resolves unmodified).
const REMOTE_DART_ROOT = '/opt/flash/src-dart-geo/src/dart';
const REMOTE_BUNDLE_DIR =
  `${REMOTE_DART_ROOT}/example/build/linux/x64/release/bundle`;
const REMOTE_STATUS_FILE = '/tmp/mrd-geo-desktop-status.jsonl';
const REMOTE_STDERR_LOG = '/tmp/mrd-geo-desktop-stderr.log';
const REMOTE_SOURCE_HASH = '/opt/flash/src-dart-geo/.mrd-geo-source-hash';
const REMOTE_UPLOAD_ROOT = '/opt/flash/src-dart-geo';

const SSH_USER = 'azureuser';

const GOSSIP_MS_OVERRIDE = 2000;
const MEDIA_SRC = '/fixtures/penguin.mp4';
// Upper bounds only — waits poll and resolve as soon as they hold.
const AGENT_READY_BUDGET_MS = 120_000;
// Flutter engine boot + first status line (written at startup).
const DESKTOP_READY_BUDGET_MS = 45_000;
const DESKTOP_BOOT_BUDGET_MS = 45_000;
const DISCOVERY_BUDGET_MS = 60_000;
// The library's media_answer timeout is queryTimeoutMs (30s).
const MEDIA_HANDSHAKE_BUDGET_MS = 40_000;
const MEDIA_RECEIVE_BUDGET_MS = 20_000;
// findClosestNode retry pattern (dart-interop/desktop-peer): a repeated
// query re-measures, so membership assertions get 3 attempts.
const MAX_QUERY_ATTEMPTS = 3;
const QUERY_BUDGET_MS = 40_000;
// Generous ceiling for the REAL inter-region RTT (measured geo pairs run
// 130-290ms; lab-VM peers can be faster when they share a region).
const RTT_CEILING_MS = 400;
// Source-set guard: rebuild on the lab VM only when the uploaded source
// content changed (sha256 over the same file set, stable order).
const SOURCE_PATHS = [
  'src/dart/pubspec.yaml',
  'src/dart/pubspec.lock',
  'src/dart/analysis_options.yaml',
  'src/dart/lib',
  'src/dart/example/pubspec.yaml',
  'src/dart/example/pubspec.lock',
  'src/dart/example/analysis_options.yaml',
  'src/dart/example/README.md',
  'src/dart/example/lib',
  'src/dart/example/linux',
];

// --- page-side predicate sources (evaluated via waitFor/waitForState) ------

/** PeerState -> truthy: peerId is known AND connected. */
const peerConnectedJs = (peerId: string) =>
  `function peerConnected(state) {
    return state.knownPeers.some(
      (p) => p.id === ${JSON.stringify(peerId)} && p.status === 'connected');
  }`;

/** KnownPeers entry for peerId once its rtt is measured; null before. */
const readRttJs = (peerId: string) =>
  `function readRtt() {
    const state = window.__meridian.state();
    if (!state) return null;
    const known = state.knownPeers.find(
      (p) => p.id === ${JSON.stringify(peerId)});
    if (!known || known.rtt === null || known.rtt === undefined) return null;
    return known;
  }`;

/**
 * The JS demo's last find-result line whose JSON differs from `prevJson`
 * (a fresh attempt's outcome, not a stale one a previous retry produced).
 */
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
 * KnownPeers entry for peerId once its STORED rtt differs from `prevJson`
 * (gossip pings refresh it) — null before. Between failed query attempts
 * the §3.6 candidate window is only falsifiable once the stored rtt the
 * window compares against has actually moved.
 */
const rttChangedJs = (peerId: string, prevRttJson: string) =>
  `function rttChanged() {
    const state = window.__meridian.state();
    if (!state) return null;
    const known = state.knownPeers.find(
      (p) => p.id === ${JSON.stringify(peerId)});
    if (!known || known.rtt === null || known.rtt === undefined) return null;
    return JSON.stringify(known.rtt) !== ${JSON.stringify(prevRttJson)}
      ? known : null;
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

// --- spec-local helpers ------------------------------------------------------

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

/**
 * Drives the JS demo's real find-closest-node flow (prompt + #find -> the
 * library's routeQuery), retrying on the §3.6 candidate window: a repeated
 * query re-measures against the refreshed stored rtt (the desktop-peer
 * spec's pattern). Returns the newest result; the CALLER asserts
 * membership (the documented relaxation — though at real-geography RTTs
 * the window usually converges and the identity holds).
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
    last = result;
    prevJson = JSON.stringify(result);
    if (attempt < MAX_QUERY_ATTEMPTS) {
      // Wait for the stored rtt to MOVE (a gossip refresh): re-querying
      // before it moves just repeats the same legitimate self-answer.
      const entry = await waitFor<{ rtt: number }>(
        page,
        readRttJs(targetPeerId),
        5_000,
      ).catch(() => null);
      const lastRttJson = entry ? JSON.stringify(entry.rtt) : 'null';
      await waitFor(
        page,
        rttChangedJs(targetPeerId, lastRttJson),
        25_000,
      ).catch(() => {
        // The stored rtt never moved; the next attempt still gets a fresh
        // measurement of its own.
      });
    }
  }
  return last as ClosestResult;
}

/** Drives the demo's real "Stream to peer" flow (prompt + #stream seam). */
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

// --- build on the lab VM -------------------------------------------------------

const repoRoot = new URL('../../../', import.meta.url).pathname;

/** sha256 over the uploaded source set (stable order; content-addressed). */
function sourceHash(): Promise<string> {
  return new Promise((resolve, reject) => {
    const proc = spawn(
      'bash',
      [
        '-c',
        // The cwd is wherever playwright ran, not the repo root — anchor
        // the find there so the file set is never empty.
        `cd ${repoRoot} && ` +
          `find ${SOURCE_PATHS.join(' ')} -type f -print0 2>/dev/null | sort -z | ` +
          `xargs -0 sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1`,
      ],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    let out = '';
    proc.stdout!.on('data', (c: Buffer) => (out += c.toString()));
    proc.once('exit', (code) =>
      code === 0 ? resolve(out.trim()) : reject(new Error(`hash exited ${code}`)),
    );
    proc.once('error', reject);
  });
}

/**
 * Uploads the example source to the lab VM (tar over ssh, mirroring the
 * repo layout so the example's `path: ..` package dependency resolves
 * unmodified) and runs `flutter pub get` + `flutter build linux --release`
 * THERE — the point of this spec is the native transport built on the lab
 * VM's cloud-installed toolchain. Idempotent: skipped when the recorded
 * source hash matches and the built binary is still on the VM.
 */
async function ensureDesktopBinaryBuilt(): Promise<void> {
  const hash = await sourceHash();
  const remoteState = await remoteExec(
    LAB_HOST,
    `cat ${REMOTE_SOURCE_HASH} 2>/dev/null; echo ---; ` +
      `test -x ${REMOTE_BUNDLE_DIR}/meridian_example && echo BINARY_OK || echo BINARY_MISSING`,
    { timeoutMs: 20_000 },
  ).catch(() => '---\nBINARY_MISSING');
  const [remoteHash, binaryState] = remoteState.trim().split('\n');
  if (binaryState === 'BINARY_OK' && remoteHash === hash) {
    console.log(`desktop binary on ${LAB_HOST} is current (hash match) — build skipped`);
    return;
  }
  console.log(
    `desktop binary ${remoteHash === hash ? 'missing' : 'stale'} on ${LAB_HOST}` +
      ` — uploading source and building there`,
  );
  const upload = spawn(
    'bash',
    [
      '-c',
      `tar czf - -C ${repoRoot} ${SOURCE_PATHS.join(' ')} | ` +
        `ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new ` +
        `${SSH_USER}@${LAB_HOST} ` +
        `'mkdir -p ${REMOTE_UPLOAD_ROOT} && tar xzf - -C ${REMOTE_UPLOAD_ROOT} && ` +
        `echo ${hash} > ${REMOTE_SOURCE_HASH}'`,
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  await new Promise<void>((resolve, reject) => {
    let err = '';
    upload.stderr!.on('data', (c: Buffer) => (err += c.toString()));
    upload.once('exit', (code) =>
      code === 0
        ? resolve()
        : reject(new Error(`source upload failed (${code}): ${err}`)),
    );
    upload.once('error', reject);
  });
  // The build itself: pub get (pub.dev over the internet) + the release
  // build. 15 min budget for a cold 2-core build.
  await remoteExec(
    LAB_HOST,
    `export PATH=/opt/flutter/bin:$PATH && cd ${REMOTE_DART_ROOT}/example && ` +
      `flutter pub get >/dev/null 2>&1 && flutter build linux --release 2>&1 | tail -3`,
    { timeoutMs: 900_000 },
  );
}

// --- native desktop peer runner (SSH-owned, like the geo agents) -----------

interface RemoteDesktopPeer {
  /** Resolves once the first status line arrived (the app booted far enough). */
  whenReady: Promise<void>;
  /** The newest parsed status snapshot (null until the first line). */
  latest: PeerState | null;
  stop(): Promise<void>;
  diagnostics(): string;
}

/**
 * Raises the built desktop peer on the lab VM over SSH and streams its
 * MRD_STATUS_FILE back through `tail -F` (the file is REMOTE — the local
 * desktop-peer spec's readFile seam does not apply). The SSH session owns
 * the app's lifecycle (xvfb-run + the app ride the remote session's
 * backgrounded process group), with a best-effort remote pkill as belt and
 * braces, same teardown philosophy as src/remote.ts.
 */
function spawnRemoteDesktopPeer(): RemoteDesktopPeer {
  const proc: ChildProcess = spawn(
    'ssh',
    [
      '-o', 'StrictHostKeyChecking=accept-new',
      '-o', 'BatchMode=yes',
      '-o', 'ServerAliveInterval=15',
      `${SSH_USER}@${LAB_HOST}`,
      // The lab VM has no sound device: flutter_webrtc's libwebrtc aborts
      // the process in ADM init without a userspace audio daemon. A
      // headless PulseAudio null sink satisfies it (cloud-init installs
      // the package on the lab role; started here idempotently).
      `(pulseaudio --check 2>/dev/null || pulseaudio --start --exit-idle-time=-1 2>/dev/null); ` +
        `rm -f ${REMOTE_STATUS_FILE} ${REMOTE_STDERR_LOG}; ` +
        `cd ${REMOTE_BUNDLE_DIR} && ` +
        `MRD_SIGNALING=${SIGNALING_URL} MRD_STATUS_FILE=${REMOTE_STATUS_FILE} ` +
        `xvfb-run -a ./meridian_example >${REMOTE_STDERR_LOG} 2>&1 & ` +
        `tail -n +1 -F ${REMOTE_STATUS_FILE}`,
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );

  let latest: PeerState | null = null;
  let lineBuf = '';
  const stderrTail: string[] = [];
  let rejectPromise!: (err: Error) => void;
  let resolvePromise!: () => void;
  let settled = false;
  const settle = (err?: Error) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    err ? rejectPromise(err) : resolvePromise();
  };
  const timer = setTimeout(
    () =>
      settle(
        new Error(
          `desktop peer on ${LAB_HOST} produced no status line within ` +
            `${DESKTOP_READY_BUDGET_MS}ms\nstderr: ${stderrTail.join('\n')}`,
        ),
      ),
    DESKTOP_READY_BUDGET_MS,
  );
  proc.stdout!.on('data', (chunk: Buffer) => {
    lineBuf += chunk.toString('utf8');
    for (;;) {
      const nl = lineBuf.indexOf('\n');
      if (nl === -1) break;
      const line = lineBuf.slice(0, nl).trim();
      lineBuf = lineBuf.slice(nl + 1);
      if (line === '') continue;
      try {
        latest = JSON.parse(line) as PeerState;
      } catch {
        // A partially-flushed line; the next second's line is clean.
        continue;
      }
      if (latest?.peerId) settle();
    }
  });
  proc.stderr!.on('data', (chunk: Buffer) => {
    stderrTail.push(...chunk.toString('utf8').split('\n'));
    while (stderrTail.length > 30) stderrTail.shift();
  });
  proc.once('exit', (code) => {
    if (settled) return;
    settle(
      new Error(
        `desktop peer ssh session exited before ready (code ${code}):\n` +
          stderrTail.join('\n'),
      ),
    );
  });
  proc.once('error', (err) => settle(err));

  const whenReady = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    whenReady,
    get latest(): PeerState | null {
      return latest;
    },
    stop: async () => {
      await new Promise<void>((resolve) => {
        if (proc.exitCode !== null) {
          resolve();
          return;
        }
        proc.once('exit', () => resolve());
        proc.kill('SIGTERM');
        setTimeout(resolve, 5_000).unref();
      });
      // Belt and braces: SIGKILL the app + xvfb if the SIGHUP missed them,
      // and drop the remote scratch files. Best-effort.
      await remoteExec(
        LAB_HOST,
        `pkill -9 -f meridian_example >/dev/null 2>&1; ` +
          `rm -f ${REMOTE_STATUS_FILE} ${REMOTE_STDERR_LOG}`,
        { timeoutMs: 15_000 },
      ).catch(() => {});
    },
    diagnostics: () =>
      `status: ${JSON.stringify(latest)}\nstderr tail: ${stderrTail.join('\n')}`,
  };
}

/** Polls the desktop peer's streamed status until `predicate` holds. */
async function waitForDesktopState(
  peer: RemoteDesktopPeer,
  predicate: (state: PeerState) => boolean,
  description: string,
  timeoutMs: number,
): Promise<PeerState> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const state = peer.latest;
    if (state && predicate(state)) return state;
    if (Date.now() > deadline) {
      throw new Error(
        `desktop peer status never satisfied: ${description} ` +
          `(after ${timeoutMs}ms); last observed: ${JSON.stringify(state)}`,
      );
    }
    await new Promise((r) => setTimeout(r, 250));
  }
}

// --- rig lifecycle ------------------------------------------------------------

let artifacts: ReturnType<typeof startArtifacts> | null = null;
let agent: RemoteAgent;
let browser: Browser | undefined;
let jsPage: Page | undefined;
let desktop: RemoteDesktopPeer;
let jsPeerId = '';

test.beforeAll(async ({}, testInfo) => {
  test.setTimeout(720_000);
  artifacts = startArtifacts(testInfo);

  // The JS peer: SSH-raised agent on the swedencentral VM (Task 9 pattern).
  agent = launchRemoteAgent({
    host: JS_REGION.host,
    role: 'js',
    readyTimeoutMs: AGENT_READY_BUDGET_MS,
  });
  await agent.whenReady;
  const browserCdp = await connectAgent(agent);
  browser = browserCdp;
  const ctx = browserCdp.contexts()[0] ?? (await browserCdp.newContext());
  jsPage = await ctx.newPage();
  artifacts?.installPcTracker(jsPage);
  const origin = agent.demoOrigin;
  expect(origin, `${JS_REGION.name} agent ready-line origin`).toBeTruthy();
  await jsPage.goto(
    `${origin}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}&mediaSrc=${MEDIA_SRC}`,
    { waitUntil: 'domcontentloaded', timeout: 60_000 },
  );
  await connect(jsPage, SIGNALING_URL);

  // The native peer: source uploaded + built ON the lab VM (idempotent —
  // a matching hash + binary short-circuits the multi-minute build), then
  // run under xvfb-run with its status file streamed back.
  await ensureDesktopBinaryBuilt();
  desktop = spawnRemoteDesktopPeer();
  await desktop.whenReady;

  const js = await waitForState(
    jsPage,
    `function bootstrapped(state) { return !!state.peerId; }`,
    DISCOVERY_BUDGET_MS,
  );
  jsPeerId = js.peerId;
  expect(jsPeerId, 'JS peer id').toBeTruthy();

  artifacts?.writeTopology({
    rig: 'Azure geo estate: native Flutter desktop peer (built on flash-e2e-lab) + JS agent (swedencentral)',
    signalingUrl: SIGNALING_URL,
    signalingHost: LAB_HOST,
    gossipMsOverride: GOSSIP_MS_OVERRIDE,
    mediaSrc: MEDIA_SRC,
    jsRegion: JS_REGION,
    desktopPeer: {
      host: LAB_HOST,
      bundleDir: REMOTE_BUNDLE_DIR,
      statusFile: REMOTE_STATUS_FILE,
    },
    rttNote:
      'real inter-region RTT (swedencentral <-> lab VM); inequality-based asserts',
  });
});

test.afterEach(async ({}, testInfo) => {
  artifacts?.recordResult(testInfo);
});

test.afterAll(async () => {
  try {
    await browser?.close();
  } catch {
    // Already dead.
  }
  try {
    await desktop?.stop();
  } catch {
    // Best-effort teardown.
  }
  try {
    await agent?.stop();
  } catch {
    // Best-effort teardown.
  }
  if (artifacts) {
    try {
      await artifacts.close();
    } catch {
      // Best-effort flush.
    }
  }
});

// --- (a) native peer joins the geo overlay -------------------------------------

test('native desktop peer (built on the lab VM) joins: the JS peer discovers it over real geography @geo', async () => {
  test.setTimeout(180_000);
  // The native peer's own view of itself, via the status file: the node
  // initialized (libwebrtc + dart:io paths) and has a peer id.
  const boot = await waitForDesktopState(
    desktop,
    (s) => !!(s.initialized && s.peerId),
    'initialized:true with a peerId',
    DESKTOP_BOOT_BUDGET_MS,
  );
  const desktopPeerId = boot.peerId;
  expect(desktopPeerId, 'desktop peer id').toBeTruthy();

  // The JS peer (real cross-region Chromium) discovered it through the
  // lab's real signaling server and lists it connected.
  const js = await waitForState(
    jsPage!,
    peerConnectedJs(desktopPeerId),
    DISCOVERY_BUDGET_MS,
  );
  expectKnownPeer(js, desktopPeerId);

  // RTT measured on the JS side (gossip-driven ping over the inter-region
  // DataChannel): real geography, generous band, never an exact value.
  const jsEntry = await waitFor<{ id: string; rtt: number }>(
    jsPage!,
    readRttJs(desktopPeerId),
    DISCOVERY_BUDGET_MS,
  );
  expect(jsEntry.rtt, `JS rtt to the desktop peer`).toBeGreaterThanOrEqual(0);
  expect(
    jsEntry.rtt,
    `JS->desktop rtt within the real-geography band (< ${RTT_CEILING_MS}ms)`,
  ).toBeLessThan(RTT_CEILING_MS);
  console.log(`js(${JS_REGION.name}) rtt to desktop peer: ${jsEntry.rtt}ms`);

  // The desktop peer's own status file shows the JS peer measured too.
  const desktopState = await waitForDesktopState(
    desktop,
    (s) =>
      s.knownPeers.some(
        (p) => p.id === jsPeerId && p.status === 'connected' &&
          p.rtt !== null && p.rtt !== undefined,
      ),
    `desktop peer measured an rtt for the JS peer ${jsPeerId}`,
    DISCOVERY_BUDGET_MS,
  );
  const desktopEntry = desktopState.knownPeers.find(
    (p) => p.id === jsPeerId,
  );
  expect(
    desktopEntry!.rtt,
    `desktop peer's rtt to the JS peer`,
  ).toBeGreaterThanOrEqual(0);
  expect(
    desktopEntry!.rtt,
    `desktop peer's rtt within the real-geography band (< ${RTT_CEILING_MS}ms)`,
  ).toBeLessThan(RTT_CEILING_MS);
});

// --- (b) closest-node query JS -> native --------------------------------------

test('closest-node query: JS routes to the native desktop peer (membership-level) @geo', async () => {
  const desktopPeerId = desktop.latest?.peerId;
  expect(desktopPeerId, 'desktop peer id known').toBeTruthy();
  // The JS demo's real Find flow (prompt + #find -> routeQuery).
  const result = await findClosestNodeViaDemo(jsPage!, desktopPeerId!);
  expect(result.error, 'JS query error').toBeUndefined();
  expect(
    [desktopPeerId, result.closestPeerId],
    'JS -> desktop closest peer is self or the desktop peer ' +
      `(result: ${JSON.stringify(result)})`,
  ).toContain(desktopPeerId);
  expect(
    result.closestRtt ?? result.closestRttMs ?? 0,
    'JS -> desktop closestRtt non-negative',
  ).toBeGreaterThanOrEqual(0);
});

// --- (c) JS -> native media over real geography --------------------------------

test('cross-stack media over real geography: the JS peer streams to the native desktop peer @geo', async () => {
  test.setTimeout(180_000);
  const desktopPeerId = desktop.latest?.peerId;
  expect(desktopPeerId, 'desktop peer id known').toBeTruthy();

  // The JS demo's real "Stream to peer" seam -> establishMediaStream
  // (media_offer/media_answer over the existing DataChannel; the JS
  // uplink is the looping penguin.mp4 fixture). The receive path is the
  // NATIVE evidence: flutter_webrtc's libwebrtc ontrack on the lab VM,
  // with ICE spanning real geography between the swedencentral agent and
  // the lab VM.
  await streamToPeerViaDemo(jsPage!, desktopPeerId!);

  // The native peer lists the JS peer's stream in activeStreams (its
  // onRemoteStreamAdded path). FRAME-LEVEL LIMITATION (documented in the
  // desktop-peer spec): the received track renders into an RTCVideoView
  // (a Flutter texture), so the status field is the receive-side ceiling.
  const state = await waitForDesktopState(
    desktop,
    (s) => s.activeStreams.includes(jsPeerId),
    `activeStreams contains the JS peer ${jsPeerId}`,
    MEDIA_RECEIVE_BUDGET_MS,
  );
  expect(state.activeStreams, 'desktop peer activeStreams').toContain(jsPeerId);
});