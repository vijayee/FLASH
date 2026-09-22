import { expect, test, type Browser, type Page } from '@playwright/test';

import { connectOverCDP } from '../src/cdp.js';
import {
  killRegionChromium,
  launchRegion,
  netnsDown,
  type LaunchedRegion,
  type NetnsRegion,
} from '../src/netns-launch.js';
import {
  connect,
  openPeer,
  state,
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

// Task 5: failure + recovery e2e over the Task 3 netns rig. Three Chromium
// peers (eu/us/asia, per-region `tc netem` egress delays) discover each
// other, elect us as supernode (its scripted RTT matrix minimizes the
// average: us ~67ms vs eu ~80ms vs asia ~93ms), then suffer a supernode
// death, a tab death, and a pure silence:
//
//   (b1) elect(): the library's real election path (electSupernode ->
//        findCentralLeader, spec §5.1) deterministically picks us.
//   (b2) the elected supernode's TAB is closed. Both survivors'
//        DataChannel close events fire node._handlePeerFailure
//        (src/js/src/node.js) -> handleFailureRecovery ->
//        triggerSupernodeReelection (src/js/src/failures.js); the new
//        leader must be one of the SURVIVORS, never the dead peer.
//   (a)  a fresh third peer joins the two survivors, then ITS tab is
//        closed: both survivors mark it failed (strip rings, knownPeers
//        status 'failed', onPeerDisconnected -> the demo logs
//        "peer disconnected: <id>") WITHOUT any peer_leaving on the wire —
//        page.close() never runs the demo's graceful shutdown path.
//   (c)  pruneStalePeers, the LAST-LINE path: a peer that goes SILENT (no
//        leave message, no channel close event, no signaling push — the
//        server is pull-only) must be marked failed after 3 gossip periods
//        of lastSeen silence. See the (c) test's comment for why it kills
//        the browser PROCESS instead of closing the tab.
//
// TEST ORDER NOTE: the election (b1) MUST precede any peer death. The
// library keeps failed peers in knownPeers forever (pruneStalePeers only
// touches 'connected' entries), and the demo's elect() affordance re-runs
// over [self, ...knownPeers] — after any death an election would block on
// an ephemeral probe to the dead peer and error out. Likewise the
// supernode death (b2) must precede (a): once a cluster member dies while
// a supernode leads, the leader's Raft 'cluster_membership leave'
// replication drains the survivors' re-election candidate set. Peers whose
// tabs died are re-raised inside their STILL-RUNNING netns browsers.
//
// Assertions poll (waitFor/waitForState) — no sleeps; every budget is an
// upper bound that resolves as soon as the condition holds.

// E2E_SIGNALING_PORT mirrors the other specs' override (this machine has a
// foreign service on 8080).
const SIGNALING_PORT = Number(process.env.E2E_SIGNALING_PORT) || 8080;
const DEMO_PORT = 8090;
// Loopback mini-STUN (see local-query-routing.spec.ts for the srflx model).
const STUN_PORT = Number(process.env.E2E_STUN_PORT) || 3478;
const STUN_URL = `stun:10.0.2.2:${STUN_PORT}`;
// MERIDIAN_CONFIG.gossipPeriodMs override for the mesh (prune cutoff = 3 x
// period = 6s). The (c) test re-raises two peers with a SHORT period
// (500ms -> 1.5s cutoff) so the prune fires long before any SCTP inactivity
// teardown could blur the attribution.
const GOSSIP_MS_OVERRIDE = 2000;
const PRUNE_GOSSIP_MS_OVERRIDE = 500;
// queryTimeoutMs library default (pinned by src/js unit tests; deliberately
// not imported so this package never TS-resolves into the JS library).
const QUERY_TIMEOUT_MS = 30_000;
// Upper bounds only — waitFor polls and resolves as soon as it holds.
const DISCOVERY_BUDGET_MS = 60_000;
const ELECTION_BUDGET_MS = 30_000;
// DataChannel close events fire within milliseconds of the tab teardown;
// generous bound so a slow netem/slirp hop cannot flake the spec.
const FAILURE_BUDGET_MS = 15_000;
// Plan budget for the re-election: 1 x gossipPeriod + queryTimeout.
const REELECTION_BUDGET_MS = GOSSIP_MS_OVERRIDE + QUERY_TIMEOUT_MS;
// (c) short-gossip survivor: cutoff = 3 x 500ms; prune ticks every 500ms,
// so 'failed' must appear within a few seconds (plan bound: ~10s).
const PRUNE_BUDGET_MS = 10_000;
// (c) long-gossip witness (the original 2s-gossip mesh peer): cutoff 6s,
// prune ticks every 2s -> 'failed' by ~8s. Budgeted at 2x the plan's ~10s
// because the 60s ring-maintenance timer (refreshRings) may independently
// mark the silent peer failed via a 10s ping timeout — a legitimate spec
// §8 failure path, but one that can land later than the prune itself.
const PRUNE_LONG_BUDGET_MS = 20_000;

const REGIONS: NetnsRegion[] = ['eu', 'us', 'asia'];

// --- page-side predicate sources (evaluated via waitFor/waitForState) -----

/** PeerState -> truthy: every id in otherIds is known and connected. */
const othersConnectedJs = (otherIds: string[]) =>
  `function othersConnected(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let found = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected') found++;
    }
    return found === wanted.size;
  }`;

/** PeerState -> truthy: every other peer has a ring placement (rtt). */
const ringPlacedJs = (otherIds: string[]) =>
  `function ringPlaced(state) {
    const wanted = new Set(${JSON.stringify(otherIds)});
    let placed = 0;
    for (const p of state.knownPeers) {
      if (wanted.has(p.id) && p.status === 'connected' &&
          p.ringIndex !== null && p.ringIndex !== undefined) placed++;
    }
    return placed === wanted.size;
  }`;

/** PeerState -> truthy: peerId is known with the wanted status. */
const peerStatusJs = (peerId: string, status: string) =>
  `function peerStatus(state) {
    const known = state.knownPeers.find((p) => p.id === ${JSON.stringify(peerId)});
    return known && known.status === ${JSON.stringify(status)} ? state : null;
  }`;

/** PeerState -> truthy: no ring lists peerId as a primary member. */
const ringsExcludeJs = (peerId: string) =>
  `function ringsExclude(state) {
    for (const ring of state.rings) {
      if (ring.primary.includes(${JSON.stringify(peerId)})) return null;
    }
    return state;
  }`;

/** PeerState -> truthy: clusterLeader is set and is one of survivorIds. */
const leaderIsSurvivorJs = (survivorIds: string[]) =>
  `function leaderIsSurvivor(state) {
    return state.clusterLeader &&
      ${JSON.stringify(survivorIds)}.includes(state.clusterLeader)
      ? state
      : null;
  }`;

/** PeerState -> truthy: clusterLeader === leaderId (spec §5.1). */
const clusterLeaderJs = (leaderId: string) =>
  `function clusterLeaderIs(state) {
    return state.clusterLeader === ${JSON.stringify(leaderId)};
  }`;

/** PeerState -> truthy: this node won the supernode election. */
const isSupernodeJs = `function isSupernode(state) {
  return state.isSupernode === true;
}`;

/** Demo log (#log) -> truthy: some line contains the needle. */
const logContainsJs = (needle: string) =>
  `function logContains() {
    const logEl = document.getElementById('log');
    if (!logEl) return null;
    for (const line of logEl.children) {
      if ((line.textContent || '').includes(${JSON.stringify(needle)})) {
        return { line: line.textContent };
      }
    }
    return null;
  }`;

/**
 * WireLogEntry ring buffer -> truthy: NO received peer_leaving names
 * senderId. A tab close never runs the demo's graceful shutdown, so the
 * survivors must fail the peer without ever seeing its departure message —
 * the harsh path this spec deliberately exercises (the demo's Disconnect
 * button would send peer_leaving; this spec never drives it).
 */
const noPeerLeavingFromJs = (senderId: string) =>
  `function noPeerLeavingFrom() {
    const hook = window.__meridian;
    const snapshot = hook && hook.state ? hook.state() : null;
    const log = snapshot && snapshot.wireLog ? snapshot.wireLog : null;
    if (!log) return { entries: 0 };
    return log.some((e) => e.dir === 'recv' && e.type === 'peer_leaving' &&
        e.payload && e.payload.senderId === ${JSON.stringify(senderId)})
      ? null
      : { entries: log.length };
  }`;

// --- suite-local helpers ---------------------------------------------------

/**
 * Raises one fresh demo peer inside a region's EXISTING netns browser
 * (used to re-raise peers whose original tabs died in (b2)/(a)). Resolves
 * the new page. The ?gossipMs= override picks the node's gossip period
 * (and thus its prune cutoff).
 */
async function raisePeerInRegion(
  region: NetnsRegion,
  gossipMs: number,
): Promise<Page> {
  const spec = rigFor(region).spec;
  const browser = browsers[region];
  const ctx = browser.contexts()[0] ?? (await browser.newContext());
  const page = await ctx.newPage();
  await openPeer(
    page,
    `http://${spec.gateway}:${DEMO_PORT}/?wirelog=1&gossipMs=${gossipMs}` +
      `&stun=${STUN_URL}`,
  );
  await connect(page, `ws://${spec.gateway}:${SIGNALING_PORT}`);
  await waitForState(
    page,
    `function bootstrapped(state) { return !!state.peerId; }`,
    DISCOVERY_BUDGET_MS,
  );
  return page;
}

/** Union of every ring-primary peer id currently held by the node. */
const primaryIdsOf = (peerState: PeerState): Set<string> =>
  new Set(peerState.rings.flatMap((ring) => ring.primary));

// --- rig lifecycle ---------------------------------------------------------

let signaling: SignalingProcess;
let demo: DemoServer;
let miniStun: MiniStunServer;
let launched: LaunchedRegion[];
const browsers = {} as Record<NetnsRegion, Browser>;
const pages = {} as Record<NetnsRegion, Page>;
const peerIds = {} as Record<NetnsRegion, string>;

const rigFor = (region: NetnsRegion): LaunchedRegion =>
  launched.find((r) => r.region === region)!;

// Same rig as local-query-routing.spec.ts (3 netns + slirp + 3 Chromium
// boots, 180s readiness deadline so the launcher's log-tail forensics can
// surface before a generic hook timeout would).
test.beforeAll(async () => {
  test.setTimeout(180_000);
  signaling = startSignaling(SIGNALING_PORT);
  await signaling.ready;

  // HOST=0.0.0.0: the netns peers load the demo through their slirp
  // gateway (10.0.<i>.2 -> the host's loopback).
  demo = startServe({ host: '0.0.0.0', port: DEMO_PORT });
  await demo.ready;

  miniStun = startMiniStun({ port: STUN_PORT });
  await miniStun.ready;

  launched = await Promise.all(REGIONS.map((r) => launchRegion(r)));

  // CDP-attached browsers have no implicit-context creation: reuse the
  // browser's existing default context (about:blank). The per-region
  // browser handles are KEPT: dead tabs are re-raised inside them.
  await Promise.all(
    REGIONS.map(async (region) => {
      const browser = await connectOverCDP(rigFor(region).endpoint);
      browsers[region] = browser;
      const ctx = browser.contexts()[0] ?? (await browser.newContext());
      pages[region] = await ctx.newPage();
    }),
  );

  await Promise.all(
    REGIONS.map(async (region) => {
      const spec = rigFor(region).spec;
      await openPeer(
        pages[region],
        `http://${spec.gateway}:${DEMO_PORT}/?wirelog=1` +
          `&gossipMs=${GOSSIP_MS_OVERRIDE}&stun=${STUN_URL}`,
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
});

test.afterAll(async () => {
  await Promise.allSettled(
    REGIONS.map((region) => browsers[region]?.close()),
  );
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

// --- (0) full mesh + ring placement (preconditions for every failure) ------

test('full mesh: every region lists the other two as connected ring-placed peers', async () => {
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
  for (const region of REGIONS) {
    for (const target of REGIONS.filter((r) => r !== region)) {
      const known = states[region].knownPeers.find(
        (p) => p.id === peerIds[target],
      );
      expect(known?.status, `${region} knows ${target}`).toBe('connected');
    }
  }
  // Ring placement must exist before the election: the winner broadcasts
  // supernode_elected over its ring primaries (spec §5.1), so followers
  // only converge once they sit in the winner's rings.
  await Promise.all(
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
});

// --- (b1) deterministic election ---------------------------------------------

test('(b1) elect(): us wins the central-leader election and all three converge on it', async () => {
  // The demo's elect affordance runs the library's real election path
  // (electSupernode -> findCentralLeader, spec §5.1) over
  // [self, ...knownPeers]; every candidate measures the same target set
  // (self as 0ms), so with the scripted matrix us (avg ~67ms) strictly
  // beats eu (~80ms) and asia (~93ms) — deterministic under netem.
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
});

// --- (b2) supernode death: automatic re-election -----------------------------

test('(b2) closing the elected supernode’s tab: survivors re-elect a leader that is one of them', async () => {
  // Precondition from (b1): us is the elected supernode and eu/asia are
  // its Raft followers. Closing us's tab fires both survivors' DataChannel
  // close events -> _handlePeerFailure -> handleFailureRecovery ->
  // triggerSupernodeReelection over the remaining cluster members (eu's
  // candidates = [asia], asia's = [eu]).
  //
  // NOTE on the assertion shape: the demo's elect() affordance always
  // re-runs over [self, ...knownPeers], which STILL CONTAINS the dead
  // supernode (failed entries are kept, never deleted) — re-driving an
  // election that way would block on an ephemeral probe to the dead peer
  // and error out. So the assertion targets the library's AUTOMATIC path:
  // each survivor's clusterLeader must change to one of the SURVIVORS
  // (eu or asia), never stay at or return to the dead peer. Budgeted at
  // 1 x gossipPeriod + queryTimeout per the plan, on wall clock.
  await pages.us.close();

  await Promise.all(
    (['eu', 'asia'] as NetnsRegion[]).map((region) =>
      waitForState(
        pages[region],
        leaderIsSurvivorJs([peerIds.eu, peerIds.asia]),
        REELECTION_BUDGET_MS,
      ),
    ),
  );

  // The survivors' link to each other is unaffected: the cluster is still
  // a functioning overlay after the supernode died.
  await waitForState(
    pages.eu,
    peerStatusJs(peerIds.asia, 'connected'),
    FAILURE_BUDGET_MS,
  );
  await waitForState(
    pages.asia,
    peerStatusJs(peerIds.eu, 'connected'),
    FAILURE_BUDGET_MS,
  );
});

// --- (a) tab close: channel-close failure path -------------------------------

test('(a) closing a mesh peer’s tab: both survivors mark it failed, drop it from rings, log the disconnect', async () => {
  // Re-raise the us peer (its tab died in (b2)) so a full three-peer mesh
  // exists again: eu + asia (the two re-election survivors) + a fresh us
  // node in the same netns browser.
  const usPage = await raisePeerInRegion('us', GOSSIP_MS_OVERRIDE);
  pages.us = usPage;
  const us2Id = (await state(pages.us))!.peerId;
  expect(us2Id, 're-raised us peer id').toBeTruthy();
  expect(us2Id, 're-raised us gets a fresh id').not.toBe(peerIds.us);

  // The re-raised mesh must be connected AND ring-placed on the two
  // original survivors before the next death (ring placement gates the
  // refill snapshot below).
  await Promise.all(
    (['eu', 'asia'] as NetnsRegion[]).map((region) => {
      const others = [peerIds[region === 'eu' ? 'asia' : 'eu'], us2Id];
      return waitForState(
        pages[region],
        othersConnectedJs(others),
        DISCOVERY_BUDGET_MS,
      ).then(() =>
        waitForState(
          pages[region],
          ringPlacedJs(others),
          DISCOVERY_BUDGET_MS,
        ),
      );
    }),
  );

  // (d) Ring-refill fixture snapshot, taken BEFORE the death. With a
  // 3-peer overlay and nodesPerRing=8 every known peer is a ring PRIMARY;
  // secondaries (and thus promotable replacements) typically do not exist,
  // and the state projection does not expose secondaryMembers — so the
  // refill assertion below only fires if this fixture happens to have one
  // (a secondary promoted into a vacated slot appears as a NEW primary id).
  const prePrimaries = new Map<NetnsRegion, Set<string>>();
  for (const region of ['eu', 'us'] as NetnsRegion[]) {
    const before = await state(pages[region]);
    expect(before, `${region} state before the close`).toBeTruthy();
    prePrimaries.set(region, primaryIdsOf(before!));
  }

  // Harsh death: no graceful shutdown, no peer_leaving on the wire — only
  // the survivors' DataChannel close events.
  await pages.asia.close();

  // Both survivors route the close event through _handlePeerFailure: the
  // dead peer's knownPeers entry flips to 'failed'...
  await Promise.all(
    (['eu', 'us'] as NetnsRegion[]).map((region) =>
      waitForState(
        pages[region],
        peerStatusJs(peerIds.asia, 'failed'),
        FAILURE_BUDGET_MS,
      ),
    ),
  );
  // ...it is stripped from every ring of both survivors...
  await Promise.all(
    (['eu', 'us'] as NetnsRegion[]).map((region) =>
      waitForState(
        pages[region],
        ringsExcludeJs(peerIds.asia),
        FAILURE_BUDGET_MS,
      ),
    ),
  );
  // ...and onPeerDisconnected surfaced in the demo's log (both survivors).
  await Promise.all(
    (['eu', 'us'] as NetnsRegion[]).map((region) =>
      waitFor(
        pages[region],
        logContainsJs(`peer disconnected: ${peerIds.asia}`),
        FAILURE_BUDGET_MS,
      ),
    ),
  );
  // The harsh-path proof: no graceful peer_leaving naming asia was ever
  // received (closing the tab bypasses the demo's Disconnect path).
  await Promise.all(
    (['eu', 'us'] as NetnsRegion[]).map((region) =>
      waitFor(
        pages[region],
        noPeerLeavingFromJs(peerIds.asia),
        FAILURE_BUDGET_MS,
      ),
    ),
  );

  // (d) Conditional ring refill: any NEW primary that appears in a
  // survivor's rings after the death must be a survivor (a promoted
  // secondary replacing asia's slot). With 3 peers there is nobody to
  // promote, so this usually asserts nothing — it documents and guards the
  // refill path rather than inventing a secondary that the fixture lacks.
  const survivors = new Set([peerIds.eu, us2Id]);
  for (const region of ['eu', 'us'] as NetnsRegion[]) {
    const after = await state(pages[region]);
    expect(after, `${region} state after the close`).toBeTruthy();
    for (const id of primaryIdsOf(after!)) {
      if (prePrimaries.get(region)!.has(id)) {
        continue;
      }
      expect(
        survivors.has(id),
        `${region} promoted an unknown peer (${id}) into its rings`,
      ).toBe(true);
    }
  }
});

// --- (c) pruneStalePeers: the last-line silence path -------------------------

test('(c) a SILENT peer (killed browser: no leave, no channel close) is pruned to failed', async () => {
  // WHY A KILLED PROCESS, NOT A CLOSED TAB: with page.close() the tab's
  // SCTP stacks tear down and the survivors' DataChannel close events fire
  // _handlePeerFailure long before 3 gossip periods of silence elapse —
  // the prune path is UNREACHABLE through a tab close in practice (test
  // (a) covers that close path). To isolate pruneStalePeers, the victim's
  // browser PROCESS is SIGKILLed (killRegionChromium): WebRTC rides
  // UDP/SCTP, so a killed process emits no teardown packets — the
  // survivors see pure silence. There is no peer_leaving (the demo's
  // shutdown path never runs) and no signaling push (the server is
  // pull-only), so the ONLY way a survivor can learn of the death is
  // lastSeen-based pruning.
  //
  // The mesh is re-raised with a SHORT gossip period (500ms -> 3x period
  // cutoff = 1.5s) so the prune fires within seconds, far ahead of any
  // SCTP inactivity teardown that could blur the attribution. The original
  // eu peer (2s gossip -> 6s cutoff) stays up as the long-period witness.
  test.setTimeout(180_000);
  const reborn = {} as Record<'us' | 'asia', string>;
  for (const region of ['us', 'asia'] as const) {
    pages[region] = await raisePeerInRegion(region, PRUNE_GOSSIP_MS_OVERRIDE);
    const bootstrapped = await state(pages[region]);
    expect(bootstrapped, `${region} re-raised state`).toBeTruthy();
    reborn[region] = bootstrapped!.peerId;
  }

  // Precondition: the fresh short-gossip peers form a connected mesh with
  // each other AND the long-gossip eu witness (its prune needs an open
  // DataChannel to the victim).
  await waitForState(
    pages.us,
    peerStatusJs(reborn.asia, 'connected'),
    DISCOVERY_BUDGET_MS,
  );
  await waitForState(
    pages.asia,
    peerStatusJs(reborn.us, 'connected'),
    DISCOVERY_BUDGET_MS,
  );
  await waitForState(
    pages.eu,
    peerStatusJs(reborn.asia, 'connected'),
    DISCOVERY_BUDGET_MS,
  );

  // Pure silence: kill asia's whole browser process (its only remaining
  // node is the fresh short-gossip peer).
  killRegionChromium('asia');

  // Short-gossip survivor (us): the victim's lastSeen froze at death;
  // pruneStalePeers (cutoff 1.5s, ticks every 500ms) must flip it to
  // 'failed' within the plan's ~10s bound. No close event can fire (the
  // channel is still locally open) and no leave was sent, so this
  // transition IS the prune path. It also routes through the standard
  // _handlePeerFailure, so onPeerDisconnected logs it too.
  await waitForState(
    pages.us,
    peerStatusJs(reborn.asia, 'failed'),
    PRUNE_BUDGET_MS,
  );
  await waitFor(
    pages.us,
    logContainsJs(`peer disconnected: ${reborn.asia}`),
    PRUNE_BUDGET_MS,
  );

  // Long-gossip witness (eu, cutoff 6s): the same lastSeen safety net on
  // the original mesh's gossip period. Budgeted at 2x the plan's ~10s (see
  // PRUNE_LONG_BUDGET_MS): eu's 60s ring-maintenance timer may mark the
  // silent peer failed via its own 10s ring-probe timeout — also a spec
  // §8 failure path, but one that can land later than the prune itself.
  await waitForState(
    pages.eu,
    peerStatusJs(reborn.asia, 'failed'),
    PRUNE_LONG_BUDGET_MS,
  );
});