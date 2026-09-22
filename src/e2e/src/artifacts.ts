/**
 * Task 12: per-run artifact collector.
 *
 * Every run that opts in creates `src/e2e/runs/<UTC timestamp>-w<worker>-<spec
 * basename>/` populated with:
 *
 *   peers/<peerId>.jsonl      state snapshots every 500ms + wirelog event
 *                             lines beyond a watermark (peer-local seq + ms ts)
 *   console/<peerId>.log      page console + pageerror streams
 *   ice-stats/<peerId>.jsonl  getStats() snapshots every 1s while the page has
 *                             live PeerConnections
 *   signaling.jsonl           written by the signaling server itself when the
 *                             spec spawns it with `{ logFile: run.signalingPath }`
 *   topology.json             the region/delay config in force
 *   result.json               per-test name/outcome (via the TestInfo fixture)
 *
 * OPT-IN BY DESIGN: specs share `workers: 1`, so a collector that assumed
 * ownership of every page would force all six specs to change. Instead a spec
 * calls `startArtifacts(testInfo)` (from a `beforeAll` hook, which receives
 * the hook's own TestInfo) and then opts individual pages into the recorders.
 * Nothing is polled on the spec's critical path — every collector runs on its
 * own interval, appending to its stream; `stop()`/`close()` flush and close.
 */
import {
  createWriteStream,
  mkdirSync,
  writeFileSync,
  type WriteStream,
} from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Page, TestInfo } from '@playwright/test';

import type { PeerState, WireLogEntry } from './orchestrator.js';

/** One line of `peers/<peerId>.jsonl` (state snapshot or wirelog event). */
export interface PeerArtifactLine {
  /** Peer-local sequence number (increments across state + wire lines). */
  seq: number;
  /** Collector wall-clock ms (Date.now()). */
  ts: number;
  kind: 'state' | 'wire';
  state?: PeerState;
  /** Wirelog entry fields (when kind === 'wire'). */
  dir?: 'send' | 'recv';
  type?: string;
  peerId?: string;
  payload?: unknown;
  /** The wire entry's own in-page timestamp, for correlation with the demo. */
  wireTs?: number;
  /**
   * State lines only: heartbeat-class wire frames (raft keep-alives) skipped
   * since the last snapshot, aggregated per type instead of recorded.
   */
  wireSkipped?: Record<string, number>;
}

/** One line of `ice-stats/<peerId>.jsonl` (per-PC getStats() digest). */
export interface IceStatsLine {
  seq: number;
  ts: number;
  /** One digest per live PeerConnection in the page. */
  pcs: {
    pcIndex: number;
    connectionState: string;
    iceConnectionState: string;
    /** Selected/succeeded candidate pair's types + RTT, when ICE converged. */
    localCandidateType?: string;
    remoteCandidateType?: string;
    rttMs?: number | null;
    error?: string;
  }[];
}

/** Tear a collector down (flush + close its streams, remove listeners). */
export type StopFn = () => Promise<void>;

/**
 * Init script that tracks every RTCPeerConnection the page creates, so the
 * ice-stats sampler can reach them without a demo-side seam. It is a pure
 * pass-through wrapper (the real connection is constructed and returned by
 * the native constructor), and it installs itself at most once per page.
 */
const PC_TRACKER_JS = `function installPcTracker() {
  try {
    const Native = window.RTCPeerConnection;
    if (!Native || window.__meridianPcsTracked) return;
    window.__meridianPcsTracked = true;
    window.__meridianPcs = [];
    const Patched = function (...args) {
      const pc = new Native(...args);
      try { window.__meridianPcs.push(pc); } catch {}
      return pc;
    };
    Patched.prototype = Native.prototype;
    window.RTCPeerConnection = Patched;
  } catch {}
}installPcTracker();`;

/** Default ice-stats accessor: the tracker's connection list (may be empty). */
const DEFAULT_PC_ACCESSOR_JS = `function pcsAccessor() {
  return window.__meridianPcs || [];
}`;

/**
 * Same read as orchestrator.waitForState, but DELTA: only wirelog entries
 * newer than `lastWireTs` cross the CDP boundary (the demo's `state().wireLog`
 * is a live reference to a 2000-entry ring — shipping it whole every 500ms
 * stalls the page's main thread enough to distort RTT-based assertions).
 * `maxWireTs: -1` signals a ring reset (page reload): the next tick re-emits
 * the whole buffer.
 */
const READ_STATE_DELTA_JS = `function readStateDelta(lastWireTs) {
  const raw = window.__meridianState
    ? window.__meridianState()
    : (window.__meridian && window.__meridian.state
      ? window.__meridian.state()
      : null);
  if (!raw) return null;
  let state;
  try {
    state = typeof raw === 'string' ? JSON.parse(raw) : raw;
  } catch { return null; }
  if (!state || !state.peerId) return null;
  const wireLog = Array.isArray(state.wireLog) ? state.wireLog : [];
  // Heartbeat-class keep-alives (the Raft cluster emits several hundred
  // append_entries/vote frames per second once formed) are aggregated per
  // tick instead of shipped line-by-line: recording them would move megabytes
  // per second through CDP and distort the very RTT measurements the suite
  // asserts on. Everything else (gossip, probes, media, queries) is kept.
  const HEARTBEATS = {
    raft_append_entries: 1,
    raft_append_entries_response: 1,
    raft_request_vote: 1,
    raft_request_vote_response: 1,
  };
  const fresh = [];
  const wireSkipped = {};
  for (const entry of wireLog) {
    if (!entry || entry.ts <= lastWireTs) continue;
    if (HEARTBEATS[entry.type]) {
      wireSkipped[entry.type] = (wireSkipped[entry.type] || 0) + 1;
    } else {
      fresh.push(entry);
    }
  }
  delete state.wireLog;
  let maxWireTs = lastWireTs;
  for (const entry of wireLog) {
    if (entry && entry.ts > maxWireTs) maxWireTs = entry.ts;
  }
  const reset = wireLog.length > 0 && maxWireTs < lastWireTs;
  return { state, fresh, wireSkipped, maxWireTs: reset ? -1 : maxWireTs };
}`;

const PC_SAMPLE_JS = (accessorJs: string) => `(async function sampleIce() {
  let pcs;
  try { pcs = (${accessorJs})(); } catch { return null; }
  if (!Array.isArray(pcs)) return null;
  // Live-media gating: only connections that carry (or are negotiating) media
  // tracks get getStats()'d — the data-channel PeerConnections used for
  // discovery/probing stay untouched, so sampling cannot distort RTT-based
  // assertions. A media PC whose ICE is FAILING still has its senders/
  // receivers (ontrack fired at SDP time), so the failure playbook's
  // "ice-stats candidate types" path keeps working.
  pcs = pcs.filter((pc) => {
    try {
      return pc.getSenders().some((s) => s.track) ||
        pc.getReceivers().some((r) => r.track);
    } catch { return false; }
  });
  if (pcs.length === 0) return null;
  const digests = [];
  for (let i = 0; i < pcs.length; i++) {
    const pc = pcs[i];
    try {
      const report = await pc.getStats();
      const candidateTypes = new Map();
      const pairs = [];
      for (const s of report.values()) {
        if (s.type === 'local-candidate' || s.type === 'remote-candidate') {
          candidateTypes.set(s.id, s.candidateType);
        } else if (s.type === 'candidate-pair') {
          pairs.push(s);
        }
      }
      const succeeded = pairs.filter((p) => p.state === 'succeeded');
      const selected =
        succeeded.find((p) => p.selected || p.nominated) || succeeded[0];
      digests.push({
        pcIndex: i,
        connectionState: pc.connectionState,
        iceConnectionState: pc.iceConnectionState,
        localCandidateType: selected
          ? candidateTypes.get(selected.localCandidateId) : undefined,
        remoteCandidateType: selected
          ? candidateTypes.get(selected.remoteCandidateId) : undefined,
        rttMs: selected && selected.currentRoundTripTime != null
          ? selected.currentRoundTripTime * 1000 : null,
      });
    } catch (error) {
      digests.push({ pcIndex: i, error: String(error) });
    }
  }
  return digests.length > 0 ? digests : null;
})()`;
// NOTE: page.evaluate evaluates strings in EXPRESSION mode, so every helper
// above is a single parenthesized expression — a bare `function f(){} f()`
// script is a silent SyntaxError.

interface RunMeta {
  spec: string;
  workerIndex: number;
  startedAt: string;
}

interface RecordedTest {
  title: string;
  status: string;
  durationMs: number;
  error?: string;
}

export interface ArtifactRun {
  /** The run's artifact directory (`src/e2e/runs/<...>/`). */
  dir: string;
  /**
   * Where the signaling server should write its JSONL log for this run —
   * pass to `startSignaling(port, { logFile: run.signalingPath })` BEFORE
   * the server spawns, and the collector picks the file up as-is.
   */
  readonly signalingPath: string;
  /** Writes `topology.json` (the region/delay config actually in force). */
  writeTopology(topology: unknown): void;
  /**
   * Installs the RTCPeerConnection tracker init script on the page. MUST run
   * before the page loads (call between `newPage()` and `goto`) — afterwards
   * it cannot see connections the page already created, and `startIceStats`
   * simply records nothing (documented skip).
   */
  installPcTracker(page: Page): void;
  /**
   * Starts `peers/<peerId>.jsonl` (500ms state snapshots + wirelog events
   * beyond a watermark) and `console/<peerId>.log` (console + pageerror).
   * Returns a stop function (also called by `close()`).
   */
  startPeerRecorder(page: Page, peerId: string): Promise<StopFn>;
  /**
   * Starts `ice-stats/<peerId>.jsonl`: getStats() digests every 1s for
   * media-carrying PeerConnections only (discovery/probe PCs are untouched —
   * see PC_SAMPLE_JS). Installs the PC tracker opportunistically (only
   * effective pre-navigation). Pages without visible media PCs simply record
   * nothing — the helper skips itself gracefully rather than failing a test
   * that does not opt into media.
   */
  startIceStats(page: Page, peerId: string, pcAccessorJs?: string): StopFn;
  /**
   * Appends this test's name/outcome to `result.json` (call from afterEach,
   * where the TestInfo fixture already knows status + duration).
   */
  recordResult(testInfo: TestInfo): void;
  /** Stops every collector and writes the final `result.json`. */
  close(): Promise<void>;
}

class ArtifactRunImpl implements ArtifactRun {
  readonly dir: string;
  private readonly meta: RunMeta;
  private readonly streams = new Set<WriteStream>();
  private readonly recorders = new Set<StopFn>();
  private readonly tests: RecordedTest[] = [];
  private closed = false;

  constructor(dir: string, meta: RunMeta) {
    this.dir = dir;
    this.meta = meta;
  }

  get signalingPath(): string {
    return path.join(this.dir, 'signaling.jsonl');
  }

  writeTopology(topology: unknown): void {
    this.writeJson('topology.json', {
      run: this.meta,
      ...asObject(topology),
    });
  }

  installPcTracker(page: Page): void {
    void page.addInitScript(PC_TRACKER_JS).catch(() => {
      // A closed page has nothing left to instrument.
    });
  }

  async startPeerRecorder(page: Page, peerId: string): Promise<StopFn> {
    const peers = this.stream('peers', `${sanitize(peerId)}.jsonl`);
    const consoleLog = this.stream('console', `${sanitize(peerId)}.log`);

    const onConsole = (msg: { type: () => string; text: () => string }) => {
      consoleLog.write(
        `${new Date().toISOString()} [${msg.type()}] ${msg.text()}\n`,
      );
    };
    const onPageError = (error: Error) => {
      consoleLog.write(
        `${new Date().toISOString()} [pageerror] ${error.stack || error.message}\n`,
      );
    };
    page.on('console', onConsole);
    page.on('pageerror', onPageError);

    let seq = 0;
    let wireWatermark = 0;
    const writeLine = (line: PeerArtifactLine) =>
      peers.write(`${JSON.stringify(line)}\n`);

    const poll = setInterval(() => {
      void page
        .evaluate(`(${READ_STATE_DELTA_JS})(${wireWatermark})`)
        .then((rawDelta) => {
          const delta = rawDelta as {
            state: PeerState;
            fresh: WireLogEntry[];
            wireSkipped: Record<string, number>;
            maxWireTs: number;
          } | null;
          if (!delta || this.closed) return;
          writeLine({
            seq: seq++,
            ts: Date.now(),
            kind: 'state',
            state: delta.state,
            ...(Object.keys(delta.wireSkipped).length > 0
              ? { wireSkipped: delta.wireSkipped }
              : {}),
          });
          for (const entry of delta.fresh) {
            writeLine({
              seq: seq++,
              ts: Date.now(),
              kind: 'wire',
              dir: entry.dir,
              type: entry.type,
              peerId: entry.peerId,
              payload: entry.payload,
              wireTs: entry.ts,
            });
          }
          wireWatermark = delta.maxWireTs;
        })
        .catch(() => {
          // Navigating / closed page: skip this tick.
        });
    }, 500);

    const stop = async () => {
      clearInterval(poll);
      page.off('console', onConsole);
      page.off('pageerror', onPageError);
      this.recorders.delete(stop);
      closeStream(peers, this.streams);
      closeStream(consoleLog, this.streams);
    };
    this.recorders.add(stop);
    return stop;
  }

  startIceStats(page: Page, peerId: string, pcAccessorJs?: string): StopFn {
    // Only meaningful pre-load; post-load pages just record nothing (the
    // tracker flag keeps a duplicate install harmless).
    this.installPcTracker(page);
    const stats = this.stream('ice-stats', `${sanitize(peerId)}.jsonl`);
    const accessor = pcAccessorJs ?? DEFAULT_PC_ACCESSOR_JS;
    let seq = 0;
    const poll = setInterval(() => {
      void page
        .evaluate(PC_SAMPLE_JS(accessor))
        .then((digest) => {
          if (!digest || this.closed) return;
          stats.write(
            `${JSON.stringify({ seq: seq++, ts: Date.now(), peerId, pcs: digest })}\n`,
          );
        })
        .catch(() => {
          // Navigating / closed page: skip this tick.
        });
    }, 1000);
    const stop = async () => {
      clearInterval(poll);
      this.recorders.delete(stop);
      closeStream(stats, this.streams);
    };
    this.recorders.add(stop);
    return stop;
  }

  recordResult(testInfo: TestInfo): void {
    const recorded: RecordedTest = {
      title: testInfo.title,
      status: testInfo.status ?? 'unknown',
      durationMs: Math.round(testInfo.duration),
      ...(testInfo.error ? { error: String(testInfo.error.message) } : {}),
    };
    this.tests.push(recorded);
    this.writeResult();
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const stop of [...this.recorders]) await stop();
    this.writeResult();
    for (const stream of [...this.streams]) {
      closeStream(stream, this.streams);
    }
  }

  private stream(subdir: string, name: string): WriteStream {
    const dir = path.join(this.dir, subdir);
    mkdirSync(dir, { recursive: true });
    const stream = createWriteStream(path.join(dir, name), { flags: 'a' });
    this.streams.add(stream);
    return stream;
  }

  private writeResult(): void {
    this.writeJson('result.json', {
      spec: this.meta.spec,
      workerIndex: this.meta.workerIndex,
      startedAt: this.meta.startedAt,
      endedAt: new Date().toISOString(),
      tests: this.tests,
    });
  }

  private writeJson(name: string, value: unknown): void {
    try {
      writeFileSync(path.join(this.dir, name), `${JSON.stringify(value, null, 2)}\n`);
    } catch {
      // Artifact writing must never fail a test.
    }
  }
}

/** Creates the run dir (unique per timestamp + worker + spec) and collector. */
export function startArtifacts(testInfo: TestInfo): ArtifactRun {
  const runRoot = fileURLToPath(new URL('../runs/', import.meta.url));
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const spec = path.basename(testInfo.file);
  const dir = path.join(runRoot, `${stamp}-w${testInfo.workerIndex}-${spec}`);
  for (const sub of ['peers', 'console', 'ice-stats']) {
    mkdirSync(path.join(dir, sub), { recursive: true });
  }
  return new ArtifactRunImpl(dir, {
    spec,
    workerIndex: testInfo.workerIndex,
    startedAt: new Date().toISOString(),
  });
}

// --- small helpers ----------------------------------------------------------

function sanitize(peerId: string): string {
  return peerId.replace(/[^A-Za-z0-9._-]/g, '_');
}

function asObject(value: unknown): Record<string, unknown> {
  return value != null && typeof value === 'object'
    ? (value as Record<string, unknown>)
    : { value };
}

function closeStream(stream: WriteStream, all: Set<WriteStream>): void {
  if (!all.delete(stream)) return;
  stream.end();
}