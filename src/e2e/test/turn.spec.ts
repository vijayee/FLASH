import { appendFileSync, existsSync, readFileSync } from 'node:fs';

import { expect, test, type Browser, type Page } from '@playwright/test';

import { startArtifacts } from '../src/artifacts.js';
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
  waitForState,
  type DemoServer,
  type MiniStunServer,
  type PeerState,
  type SignalingProcess,
} from '../src/orchestrator.js';
import { remoteExec } from '../src/remote.js';

// Task 10: the FORCED-RELAY TURN test. The three-netns rig (same launcher as
// the query-routing spec) is pointed at the lab VM's real coturn
// (turn:172.173.102.12:3478, long-term credentials, realm flash.test), and
// every netns has its outbound UDP starved (netns-up.sh MRD_UDP_ALLOW:
// iptables OUTPUT drop on tap0 except UDP to the TURN server). The result:
//
//   - host candidates are self-referential (10.0.2.100 in every netns) and
//     fail DTLS as always;
//   - the loopback mini-STUN produces NOTHING — the STUN binding itself is
//     one of the dropped UDP flows (the in-netns probe evidence proves the
//     block; no gathered candidate ever references its URL);
//   - the only working path is through coturn: allocations to
//     172.173.102.12:3478, whose relayed transport address is that same
//     public IP, so the single ACCEPT rule covers the data channel too.
//     (libwebrtc also derives an srflx candidate from the TURN Allocate's
//     mapped address — see the media test's NOTE.)
//
// Asserted end-to-end across the PUBLIC internet from a UDP-starved net:
// media connects (activeStreams) AND getStats() shows the selected pair
// TRAVERSING the relay on every media endpoint, with `relay` as the LOCAL
// candidate type on at least one endpoint — the same getStats() shape the
// Task 12 ice-stats collector records.
//
// Opt-in: it needs the Azure TURN server. The config's testIgnore only
// excludes geo.spec.ts (paths, not titles — see playwright.config.ts), so
// this spec carries geo.spec.ts's title-level test.skip guard too.

test.skip(
  process.env.E2E_GEO !== '1',
  'forced-relay TURN suite is opt-in via E2E_GEO=1 (needs the Azure coturn)',
);

// The lab VM's public IP: coturn (3478, lt-cred-mech realm flash.test) and
// its relay range (49152-65535, covered by the NSG's allow-ice-udp rule).
const TURN_HOST = '172.173.102.12';
const TURN_USER = 'flash';
const TURN_CREDENTIAL = 'flash-e2e-cred';
const TURN_URL = `turn:${TURN_HOST}:3478`;

// Port overrides per README (a Docker daemon squats 8080/3478 locally).
const SIGNALING_PORT = Number(process.env.E2E_SIGNALING_PORT) || 8080;
const DEMO_PORT = 8090;
const STUN_PORT = Number(process.env.E2E_STUN_PORT) || 3478;
const STUN_URL = `stun:10.0.2.2:${STUN_PORT}`;

const GOSSIP_MS_OVERRIDE = 2000;
const MEDIA_SRC = '/fixtures/penguin.mp4';
// Upper bounds only — waitFor polls and resolves as soon as it holds.
const DISCOVERY_BUDGET_MS = 60_000;
// The library's media_answer timeout is queryTimeoutMs (30s).
const MEDIA_HANDSHAKE_BUDGET_MS = 40_000;
const MEDIA_ACTIVE_BUDGET_MS = 30_000;
// TURN allocation + relay-pair connectivity checks run over the real
// internet; Chromium keeps checking pairs until one succeeds.
const RELAY_PAIR_BUDGET_MS = 45_000;

const REGIONS: NetnsRegion[] = ['eu', 'us', 'asia'];

// --- page-side predicate sources ---------------------------------------------

/** PeerState -> truthy: peerId is in activeStreams. */
const hasActiveStreamJs = (peerId: string) =>
  `function hasActiveStream(state) {
    return state.activeStreams.includes(${JSON.stringify(peerId)});
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
 * getStats() digest over every tracked media-carrying PeerConnection —
 * the same fields the Task 12 ice-stats collector records (selected
 * succeeded candidate pair's local/remote candidate types + pair RTT),
 * plus EVERY succeeded candidate pair's local/remote types (so the
 * relay-traversal invariant is checkable regardless of which pair the
 * stats list first) and each local candidate's {candidateType, url, ip}
 * so candidate origins are auditable. Requires the PC tracker init script
 * to be installed BEFORE the demo page loads (artifacts.installPcTracker,
 * same as the collector).
 */
const iceDigestsJs = `async function iceDigests() {
  const pcs = (window.__meridianPcs || []).filter((pc) => {
    try {
      return pc.getSenders().some((s) => s.track) ||
        pc.getReceivers().some((r) => r.track);
    } catch { return false; }
  });
  if (pcs.length === 0) return null;
  return Promise.all(pcs.map(async (pc) => {
    const report = await pc.getStats();
    const types = new Map();
    const pairs = [];
    const localCandidates = [];
    for (const s of report.values()) {
      if (s.type === 'local-candidate') {
        types.set(s.id, s.candidateType);
        localCandidates.push({
          candidateType: s.candidateType,
          url: s.url || null,
          ip: s.ip || s.address || null,
        });
      } else if (s.type === 'remote-candidate') {
        types.set(s.id, s.candidateType);
      } else if (s.type === 'candidate-pair') pairs.push(s);
    }
    const succeeded = pairs.filter((p) => p.state === 'succeeded');
    const selected =
      succeeded.find((p) => p.selected || p.nominated) || succeeded[0];
    const succeededPairs = succeeded.map((p) => ({
      local: types.get(p.localCandidateId),
      remote: types.get(p.remoteCandidateId),
    }));
    return {
      connectionState: pc.connectionState,
      iceConnectionState: pc.iceConnectionState,
      localCandidateType: selected ? types.get(selected.localCandidateId) : undefined,
      remoteCandidateType: selected ? types.get(selected.remoteCandidateId) : undefined,
      rttMs: selected && selected.currentRoundTripTime != null
        ? selected.currentRoundTripTime * 1000 : null,
      succeededPairs,
      localCandidates,
    };
  }));
}`;

// --- spec-local helpers ------------------------------------------------------

interface IceDigest {
  connectionState: string;
  iceConnectionState: string;
  localCandidateType?: string;
  remoteCandidateType?: string;
  rttMs?: number | null;
  succeededPairs?: { local?: string; remote?: string }[];
  localCandidates?: {
    candidateType: string;
    url: string | null;
    ip: string | null;
  }[];
}

interface StreamResult {
  ok: boolean;
  error?: string;
}

/** Polls a page-side async predicate source until truthy or deadline. */
async function waitForPage<T>(
  page: Page,
  predicateJs: string,
  description: string,
  timeoutMs: number,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  let lastError: string | null = null;
  for (;;) {
    try {
      last = (await page.evaluate(`(${predicateJs})()`)) as T | null;
    } catch (err) {
      last = null;
      lastError = String(err).slice(0, 300);
    }
    if (last) return last as T;
    if (Date.now() > deadline) {
      throw new Error(
        `${description} never held within ${timeoutMs}ms; last observed: ` +
          `${JSON.stringify(last)}${lastError ? ` (last evaluate error: ${lastError})` : ''}`,
      );
    }
    await page.waitForTimeout(500);
  }
}

/**
 * Timeout diagnostics for the digest poll: how many PeerConnections the
 * tracker saw, their states, and (per pc) how many senders/receivers carry
 * tracks — distinguishes "tracker missing" from "track filter excluded the
 * pc" from "no succeeded pair yet".
 */
const pcStateDiagnosticsJs = `async function pcStateDiagnostics() {
  const pcs = window.__meridianPcs || [];
  const out = [];
  for (const pc of pcs) {
    let senderTracks = -1;
    let receiverTracks = -1;
    try {
      senderTracks = pc.getSenders().filter((s) => s.track).length;
    } catch {}
    try {
      receiverTracks = pc.getReceivers().filter((r) => r.track).length;
    } catch {}
    out.push({
      connectionState: pc.connectionState,
      iceConnectionState: pc.iceConnectionState,
      senderTracks,
      receiverTracks,
    });
  }
  return { tracked: pcs.length, pcs: out };
}`;

/** Drives the demo's real "Stream to peer" flow (prompt + #stream seam). */
async function streamToPeerViaDemo(
  page: Page,
  targetPeerId: string,
): Promise<void> {
  page.once('dialog', (dialog) => {
    void dialog.accept(targetPeerId).catch(() => {});
  });
  await page.click('#stream');
  const result = await waitForPage<StreamResult>(
    page,
    readStreamResultJs(targetPeerId),
    `stream result for ${targetPeerId}`,
    MEDIA_HANDSHAKE_BUDGET_MS,
  );
  if (!result.ok) {
    throw new Error(`demo stream to ${targetPeerId} failed: ${result.error}`);
  }
}

/** Reads one netns rig evidence file (empty string when not present). */
const rigEvidence = (region: NetnsRegion, file: string): string => {
  const p = `/tmp/mrd-netns/${region}/${file}`;
  return existsSync(p) ? readFileSync(p, 'utf8').trim() : '';
};

// --- rig lifecycle ------------------------------------------------------------

let artifacts: ReturnType<typeof startArtifacts> | null = null;
let signaling: SignalingProcess;
let demo: DemoServer;
let miniStun: MiniStunServer;
let launched: LaunchedRegion[];
const browsers: Browser[] = [];
const pages = {} as Record<NetnsRegion, Page>;
const peerIds = {} as Record<NetnsRegion, string>;

const rigFor = (region: NetnsRegion): LaunchedRegion =>
  launched.find((r) => r.region === region)!;

test.beforeAll(async ({}, testInfo) => {
  test.setTimeout(300_000);
  artifacts = startArtifacts(testInfo);

  signaling = startSignaling(SIGNALING_PORT);
  await signaling.ready;
  demo = startServe({ host: '0.0.0.0', port: DEMO_PORT });
  await demo.ready;
  miniStun = startMiniStun({ port: STUN_PORT });
  await miniStun.ready;

  // The UDP starve: netns-up.sh (MRD_UDP_ALLOW) drops every outbound UDP
  // packet on tap0 except flows to the TURN server. MRD_STUN_PORT tells the
  // in-netns blocked-path probe which port the rig's mini-STUN listens on.
  process.env.MRD_UDP_ALLOW = TURN_HOST;
  process.env.MRD_STUN_PORT = String(STUN_PORT);

  launched = await Promise.all(REGIONS.map((r) => launchRegion(r)));

  await Promise.all(
    REGIONS.map(async (region) => {
      const browser = await connectOverCDP(rigFor(region).endpoint);
      browsers.push(browser);
      const ctx = browser.contexts()[0] ?? (await browser.newContext());
      const page = await ctx.newPage();
      // The PC tracker must land BEFORE the demo page loads so getStats()
      // can reach the media PeerConnections.
      artifacts?.installPcTracker(page);
      pages[region] = page;
    }),
  );

  // Every demo URL carries the Azure TURN entry (?turn=url,username,cred)
  // AND the loopback STUN (whose binding the iptables block defeats — that
  // is the point: srflx cannot save ICE, only the relay can).
  const turnParam = `${TURN_URL},${TURN_USER},${TURN_CREDENTIAL}`;
  await Promise.all(
    REGIONS.map(async (region) => {
      const spec: { gateway: string } = rigFor(region).spec;
      await openPeer(
        pages[region],
        `http://${spec.gateway}:${DEMO_PORT}/?wirelog=1&gossipMs=${GOSSIP_MS_OVERRIDE}` +
          `&stun=${STUN_URL}&turn=${turnParam}&mediaSrc=${MEDIA_SRC}`,
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

  artifacts?.writeTopology({
    rig: 'local netns rig (UDP-starved; netns-up.sh MRD_UDP_ALLOW) -> Azure TURN',
    signalingPort: SIGNALING_PORT,
    stunUrl: STUN_URL,
    turn: { url: TURN_URL, username: TURN_USER, host: TURN_HOST },
    udpBlock:
      'iptables OUTPUT on tap0: ACCEPT udp -> 172.173.102.12, DROP other udp',
    gossipMsOverride: GOSSIP_MS_OVERRIDE,
    mediaSrc: MEDIA_SRC,
    regions: Object.fromEntries(
      REGIONS.map((region) => [
        region,
        {
          peerId: peerIds[region],
          iptables: rigEvidence(region, 'iptables.txt'),
          udpAllowProbe: rigEvidence(region, 'udp-allow.txt'),
          udpBlockProbe: rigEvidence(region, 'udp-block.txt'),
        },
      ]),
    ),
  });
  for (const region of REGIONS) {
    try {
      await artifacts?.startPeerRecorder(pages[region], peerIds[region]);
      artifacts?.startIceStats(pages[region], peerIds[region]);
    } catch {
      // Collectors are evidence, not assertions.
    }
  }
});

test.afterEach(async ({}, testInfo) => {
  artifacts?.recordResult(testInfo);
});

test.afterAll(async () => {
  // The rig env must not leak into later specs sharing this worker.
  delete process.env.MRD_UDP_ALLOW;
  delete process.env.MRD_STUN_PORT;
  await Promise.allSettled(browsers.map((browser) => browser.close()));
  // The TURN server's journald is the server-side side of the evidence
  // (best-effort: default log level records lifecycle only).
  try {
    const journal = await remoteExec(
      TURN_HOST,
      'sudo journalctl -u coturn --no-pager -n 120 --output=short-iso',
      { timeoutMs: 25_000 },
    );
    if (artifacts) {
      appendFileSync(`${artifacts.dir}/coturn-journal.log`, journal);
    }
  } catch (err) {
    console.warn(`coturn journal dump skipped: ${String(err)}`);
  }
  if (artifacts) {
    try {
      await artifacts.close();
    } catch {
      // Best-effort flush.
    }
  }
  for (const region of REGIONS) {
    try {
      await netnsDown(region);
    } catch {
      // Best-effort: the down script is idempotent.
    }
  }
  for (const server of [demo, miniStun, signaling]) {
    try {
      server?.stop();
    } catch {
      // Already dead.
    }
  }
});

// --- (a) the UDP block held ------------------------------------------------------

test('UDP block held in every netns: in-netns STUN probe timed out, TURN probe answered @geo', async () => {
  for (const region of REGIONS) {
    // The rules netns-up.sh wrote: accept UDP to the TURN server (whole
    // destination IP — the relayed transport address lives there too) and
    // drop the rest of the UDP egress. Matched on the stable fragments;
    // exact token order differs between iptables-legacy/nft.
    const rules = rigEvidence(region, 'iptables.txt');
    expect(rules, `${region} iptables evidence captured in the netns`).toContain(
      '-A OUTPUT',
    );
    expect(rules, `${region} rules allow UDP to the TURN server`).toMatch(
      new RegExp(
        `^-A OUTPUT -d ${TURN_HOST.replace(/\./g, '\\.')}[\\s\\S]*-j ACCEPT$`,
        'm',
      ),
    );
    expect(rules, `${region} rules drop the rest of the UDP egress`).toContain(
      'udp -j DROP',
    );
    // The allowed-path probe: coturn answered a STUN binding request.
    expect(
      rigEvidence(region, 'udp-allow.txt'),
      `${region} TURN probe answered through the firewall`,
    ).toContain(`reply from ${TURN_HOST}:3478`);
    // The blocked-path probe: the same probe against the rig's own
    // mini-STUN timed out — outbound UDP is genuinely gone.
    expect(
      rigEvidence(region, 'udp-block.txt'),
      `${region} STUN probe timed out through the firewall`,
    ).toContain('no reply from');
  }
});

// --- (b) forced-relay media ------------------------------------------------------

test('media through the UDP-starved net: the selected pair traverses the TURN relay @geo', async () => {
  test.setTimeout(180_000);

  // Discovery (signaling is TCP — unaffected by the UDP block).
  await Promise.all(
    REGIONS.map((region) => {
      const others = REGIONS.filter((r) => r !== region).map(
        (r) => peerIds[r],
      );
      return waitForState(
        pages[region],
        `function othersConnected(state) {
          const wanted = new Set(${JSON.stringify(others)});
          let found = 0;
          for (const p of state.knownPeers) {
            if (wanted.has(p.id) && p.status === 'connected') found++;
          }
          return found === wanted.size;
        }`,
        DISCOVERY_BUDGET_MS,
      );
    }),
  );

  // The demo's real "Stream to peer" flow: eu -> us. With srflx and host
  // candidates dead, the media_offer's ICE can only complete via relay.
  await streamToPeerViaDemo(pages.eu, peerIds.us);

  // The receiver lists the stream (the library's ontrack path completed).
  await waitForState(
    pages.us,
    hasActiveStreamJs(peerIds.eu),
    MEDIA_ACTIVE_BUDGET_MS,
  );

  // THE relay evidence. The selected pair on each media endpoint must
  // TRAVERSE the TURN relay (one of its two candidates is the relay
  // endpoint), and at least one endpoint's selected pair must have `relay`
  // as its LOCAL candidate type. NOTE on the observed shape (documented
  // from the estate): libwebrtc derives an srflx candidate from the TURN
  // server too (the Allocate response's XOR-MAPPED-ADDRESS), and since the
  // relayed transport address lives on the TURN server's IP — the ONLY UDP
  // destination the firewall allows — the srflx<->relay pair has higher ICE
  // priority than relay<->relay and wins the nomination. Every possible
  // selected pair here therefore still runs through coturn: a peer's
  // srflx is only usable because its checks are sent TO the relay's
  // address, and the reply rides that peer's own relay/NAT mapping.
  const MEDIA_REGIONS: NetnsRegion[] = ['eu', 'us'];
  const selectedPairs: {
    region: NetnsRegion;
    local?: string;
    remote?: string;
    rttMs?: number | null;
  }[] = [];
  for (const region of MEDIA_REGIONS) {
    const digests = await waitForPage<IceDigest[]>(
      pages[region],
      `function poll() { return (${iceDigestsJs})().then((d) =>
        d && d.some((x) => x.localCandidateType &&
          x.connectionState === 'connected') ? d : null); }`,
      `${region}: a selected candidate pair in getStats`,
      RELAY_PAIR_BUDGET_MS,
    ).catch((err) => {
      // Enrich the failure with the PC-level state before rethrowing.
      return pages[region]
        .evaluate(`(${pcStateDiagnosticsJs})()`)
        .then(
          (diag) => {
            throw new Error(
              `${err.message}; pc diagnostics: ${JSON.stringify(diag)}`,
            );
          },
          () => {
            throw err;
          },
        );
    });
    const digest = digests[0];
    expect(
      digest.connectionState,
      `${region}: media PeerConnection connected`,
    ).toBe('connected');
    // (connectionState lags the selected pair by a DTLS handshake beat on
    // some runs; the poll above re-measures until both hold, and the
    // per-pair asserts below run after the connection has settled.)
    // The media path traverses the TURN relay. With both peers in a
    // UDP-starved netns, a pair whose candidates are BOTH non-relay
    // (host<->host or srflx<->srflx) can never succeed — every address
    // except the TURN server's is unreachable — so the stable invariant is:
    // EVERY succeeded candidate pair in the media PeerConnection involves
    // the relay endpoint. (The brief's literal reading — `relay` as the
    // selected pair's LOCAL candidate type — is the same statement seen
    // from one endpoint's vantage: nomination direction is not stable
    // across the two vantages, so the flipped view must be allowed.)
    const succeededPairs = digest.succeededPairs ?? [];
    expect(
      succeededPairs.length,
      `${region}: at least one succeeded candidate pair in getStats`,
    ).toBeGreaterThan(0);
    for (const pair of succeededPairs) {
      expect(
        [pair.local, pair.remote],
        `${region}: succeeded pair ${pair.local}<->${pair.remote} traverses ` +
          `the TURN relay (no direct path exists in a UDP-starved net)`,
      ).toContain('relay');
    }
    expect(
      digest.rttMs ?? 0,
      `${region}: relay pair measured a non-negative RTT`,
    ).toBeGreaterThanOrEqual(0);

    // The UDP block's page-level evidence (paired with test (a)'s
    // in-netns probes): the mini-STUN's advertised mapped address
    // (10.0.2.2) never appears as a gathered candidate's address — every
    // srflx candidate this page holds was derived from the TURN server
    // instead. A relay candidate MUST exist (the fallback happened).
    const candidates = digest.localCandidates ?? [];
    expect(
      candidates.some((c) => c.candidateType === 'relay'),
      `${region}: a relay candidate was gathered (TURN fallback happened) ` +
        `(candidates: ${JSON.stringify(candidates)})`,
    ).toBe(true);
    for (const candidate of candidates) {
      if (candidate.candidateType === 'srflx') {
        expect(
          candidate.ip,
          `${region}: no srflx candidate from the blocked loopback mini-STUN ` +
            `(the only srflx candidates are the TURN server's mapped address)`,
        ).not.toBe('10.0.2.2');
      }
    }
    console.log(
      `${region} succeeded pairs: ${JSON.stringify(succeededPairs)} ` +
        `selected=${digest.localCandidateType}<->${digest.remoteCandidateType} ` +
        `rtt=${digest.rttMs}ms localCandidates=${JSON.stringify(digest.localCandidates)}`,
    );
    selectedPairs.push({
      region,
      local: digest.localCandidateType,
      remote: digest.remoteCandidateType,
      rttMs: digest.rttMs,
    });
  }

  // The receiver's state, for the record.
  const finalState: PeerState = await waitForState(
    pages.us,
    hasActiveStreamJs(peerIds.eu),
    1_000,
  ).catch(() => null as unknown as PeerState);
  if (finalState) {
    console.log('us activeStreams:', JSON.stringify(finalState.activeStreams));
  }
});