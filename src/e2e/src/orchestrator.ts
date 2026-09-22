import { spawn, type ChildProcess } from 'node:child_process';
import { createConnection } from 'node:net';
import { fileURLToPath } from 'node:url';
import type { Page } from '@playwright/test';

/** One entry of the demo's `?wirelog=1` ring buffer (Task 1 demo seam). */
export interface WireLogEntry {
  dir: 'send' | 'recv';
  type: string;
  ts: number;
  peerId: string;
  payload: unknown;
}

/**
 * Snapshot of `window.__meridian.state()` in the JS demo. The Dart tab's
 * `window.__meridianState()` parses to this same shape (plus the
 * `initialized`/`error` affordances, absent on the JS side, whose `state()`
 * is `null` until the demo connects instead).
 */
export interface PeerState {
  peerId: string;
  knownPeers: {
    id: string;
    rtt: number;
    status: string;
    ringIndex: number | null;
  }[];
  rings: { index: number; primary: string[] }[];
  isSupernode: boolean;
  clusterLeader: string | null;
  activeStreams: string[];
  /** Peers a supernode told us to expect relayed streams from (spec §6.2). */
  forwardedStreams?: string[];
  wireLog?: WireLogEntry[];
  initialized?: boolean;
  error?: string | null;
  /**
   * Task 6 (Dart example only): the last `__meridianAction('find', target)`
   * outcome, observed by e2e through the state snapshot.
   */
  lastFindResult?: {
    target: string;
    closestPeerId?: string | null;
    closestRttMs?: number | null;
    error?: string;
  };
  /** Task 6 (Dart example only): the last `__meridianAction('stream', ...)`. */
  lastStreamResult?: { peerId: string; ok: boolean; error?: string };
}

declare global {
  interface Window {
    __meridian?: {
      peerId: () => string | null;
      connect: (url: string) => void;
      state: () => PeerState | null;
      /**
       * Task 4 affordance: drives the library's electSupernode over the
       * current cluster (self + all known peers). Resolves with the
       * findCentralLeader result ({leaderId, avgRtt, hopCount}); rejects on
       * the library's query timeout.
       */
      elect?: () => Promise<{
        leaderId?: string;
        avgRtt?: number;
        hopCount?: number;
        error?: string;
      }>;
      /** Task 4 affordance: node.closeStream (spec §7.1 media_close). */
      closeStream?: (peerId: string) => void;
    };
    __meridianState?: () => string;
  }
}

/** Handle on the spawned signaling server, for `beforeAll`/`afterAll`. */
export interface SignalingProcess {
  proc: ChildProcess;
  /** Resolves once the server's port accepts connections. */
  ready: Promise<void>;
  /** Kills the server process (idempotent). */
  stop: () => void;
}

/**
 * Boots the signaling server from the repo root (it serves no demo; peers
 * connect to the statically-served demo pages directly). `stdio` is ignored,
 * so a startup death (e.g. EADDRINUSE) surfaces only through `ready`
 * rejecting — await it in `beforeAll` and always `stop()` in afterAll:
 *
 * ```ts
 * let signaling: SignalingProcess;
 * test.beforeAll(async () => { signaling = startSignaling(); await signaling.ready; });
 * test.afterAll(() => { try { signaling?.stop(); } catch {} });
 * ```
 */
export function startSignaling(port = 8080): SignalingProcess {
  // src/e2e/src/orchestrator.ts -> repo root is three levels up.
  const proc = spawn('node', ['src/signaling-server/server.js'], {
    cwd: new URL('../../../', import.meta.url).pathname,
    env: { ...process.env, PORT: String(port) },
    stdio: 'ignore',
  });
  return { proc, ready: waitForPort(proc, port), stop: () => proc.kill() };
}

/**
 * Resolves when `port` accepts a TCP connection (polling, since the server
 * is stdio-mute); rejects if the process exits first or the port never
 * opens, so a dead server fails the suite instead of hanging it.
 */
function waitForPort(
  proc: ChildProcess,
  port: number,
  timeoutMs = 10_000,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let poll: NodeJS.Timeout | undefined;
    let timer: NodeJS.Timeout | undefined;
    const onExit = (code: number | null) =>
      settle(
        new Error(
          `signaling server exited before ready (code ${code}); ` +
            `is port ${port} already taken?`,
        ),
      );
    const settle = (error?: Error) => {
      clearInterval(poll);
      clearTimeout(timer);
      proc.off('exit', onExit);
      if (error) reject(error);
      else resolve();
    };
    timer = setTimeout(
      () =>
        settle(
          new Error(
            `signaling server did not open port ${port} within ${timeoutMs}ms`,
          ),
        ),
      timeoutMs,
    );
    poll = setInterval(() => {
      portAccepts(port).then((open) => {
        if (open) settle();
      });
    }, 100);
    proc.once('exit', onExit);
  });
}

/** Single TCP probe; `false` on any failure/timeout to connect. */
function portAccepts(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ port, host: '127.0.0.1' });
    const settle = (open: boolean) => {
      socket.destroy();
      resolve(open);
    };
    socket.once('connect', () => settle(true));
    socket.once('error', () => settle(false));
    socket.setTimeout(500, () => settle(false));
  });
}

/**
 * Polls a boolean/object predicate in the page until truthy or deadline.
 *
 * `predicateJs` must be a function *source* string: it is evaluated as
 * `(${predicateJs})()`, so a bare expression body will not run. Truthiness
 * semantics apply — 0, '' and null all mean "not ready". The last truthy
 * value (or the final observed value on timeout) is cast to `T`.
 */
export async function waitFor<T>(
  page: Page,
  predicateJs: string,
  timeoutMs = 15_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  for (;;) {
    last = (await page.evaluate(`(${predicateJs})()`)) as T | null;
    if (last) return last as T;
    if (Date.now() > deadline) {
      let observed: string;
      try {
        observed = JSON.stringify(last) ?? String(last);
      } catch {
        observed = String(last);
      }
      throw new Error(
        `waitFor timed out after ${timeoutMs}ms: ${predicateJs}; ` +
          `last observed value: ${observed}`,
      );
    }
    await page.waitForTimeout(250);
  }
}

/**
 * Waits for `window.__meridianState()` (Dart tab, JSON string) or the JS
 * demo's `window.__meridian.state()` to be present and for `predicateJs`
 * (a PeerState -> truthy function source, per waitFor) to hold, resolving
 * the parsed snapshot.
 */
export async function waitForState(
  page: Page,
  predicateJs: string,
  timeoutMs = 15_000,
): Promise<PeerState> {
  return waitFor<PeerState>(
    page,
    `function readState() {
      const raw = window.__meridianState
        ? window.__meridianState()
        : (window.__meridian && window.__meridian.state
          ? window.__meridian.state()
          : null);
      if (!raw) return null;
      const state = typeof raw === 'string' ? JSON.parse(raw) : raw;
      return (${predicateJs})(state) ? state : null;
    }`,
    timeoutMs,
  );
}

export const openPeer = (page: Page, demoUrl: string) => page.goto(demoUrl);

/** Drives the demo's Connect flow through the `window.__meridian` seam. */
export const connect = (page: Page, signalingUrl: string) =>
  page.evaluate((url) => {
    const hook = window.__meridian;
    if (!hook) throw new Error('window.__meridian is not installed');
    hook.connect(url);
  }, signalingUrl);

/**
 * Reads the JS demo's state snapshot. `null` before the demo page has
 * connected (the hook dereferences no node yet); the Dart tab's equivalent
 * is `waitForState` (its hook is installed at startup and reports
 * `initialized: false` rather than null).
 */
export const state = (page: Page) =>
  page.evaluate(
    () => window.__meridian?.state() ?? null,
  ) as Promise<PeerState | null>;

/** Handle on the spawned static demo server, for `beforeAll`/`afterAll`. */
export interface DemoServer {
  proc: ChildProcess;
  /** Resolves once the server's port accepts connections. */
  ready: Promise<void>;
  /** Kills the server process (idempotent). */
  stop: () => void;
}

export interface MiniStunServer {
  proc: ChildProcess;
  ready: Promise<void>;
  stop: () => void;
}

/**
 * Boots scripts/mini-stun.mjs — the loopback STUN responder the netns rig
 * points peers at via the demo's `?stun=` affordance. Readiness is a real
 * STUN binding round-trip (UDP has no listen-probe).
 */
export function startMiniStun(
  { port = 3478, mappedIp = '10.0.2.2' }: { port?: number; mappedIp?: string } = {},
): MiniStunServer {
  const script = fileURLToPath(new URL('../scripts/mini-stun.mjs', import.meta.url));
  const proc = spawn(process.execPath, [script, String(port)], {
    env: { ...process.env, MAPPED_IP: mappedIp },
    stdio: 'ignore',
  });
  const ready = (async () => {
    const { createSocket } = await import('node:dgram');
    const deadline = Date.now() + 10_000;
    for (;;) {
      if (proc.exitCode !== null) {
        throw new Error(`mini-stun exited (code ${proc.exitCode})`);
      }
      try {
        await new Promise<void>((resolve, reject) => {
          const sock = createSocket('udp4');
          const msg = Buffer.alloc(20);
          msg.writeUInt16BE(0x0001, 0);
          Buffer.from([0x21, 0x12, 0xa4, 0x42]).copy(msg, 4);
          Buffer.from('0123456789abcdef012345', 'hex').copy(msg, 8);
          const done = (err?: Error) => {
            clearTimeout(timer);
            sock.close();
            err ? reject(err) : resolve();
          };
          const timer = setTimeout(
            () => done(new Error('mini-stun probe timeout')),
            1000,
          );
          sock.on('message', () => done());
          sock.on('error', (e) => done(e));
          sock.send(msg, port, '127.0.0.1');
        });
        return;
      } catch {
        if (Date.now() > deadline) {
          throw new Error(`mini-stun never answered on ${port}`);
        }
        await new Promise((r) => setTimeout(r, 250));
      }
    }
  })();
  return { proc, ready, stop: () => proc.kill() };
}

/**
 * Boots scripts/serve.mjs — the static server for the JS demo pages
 * (`/` -> the demo, `/src/...` -> the library package root). `host` must
 * be 0.0.0.0 when netns peers (Task 3) must reach it: they dial the
 * server through their slirp gateway, which lands on the host's loopback.
 *
 * `dartRoot` (Task 6) switches the server into dart-web mode: it serves the
 * staged Flutter web bundle (`src/e2e/scripts/build-dart-web.sh` ->
 * src/e2e/build/dart-web) at `/` plus the shared `/fixtures/` base. Run it
 * on its own port (8091) so the JS demo instance stays untouched.
 */
export function startServe(
  {
    host = '127.0.0.1',
    port = 8090,
    dartRoot,
  }: { host?: string; port?: number; dartRoot?: string } = {},
): DemoServer {
  const script = fileURLToPath(
    new URL('../scripts/serve.mjs', import.meta.url),
  );
  const proc = spawn(process.execPath, [script], {
    env: {
      ...process.env,
      HOST: host,
      PORT: String(port),
      ...(dartRoot ? { DART_ROOT: dartRoot } : {}),
    },
    stdio: 'ignore',
  });
  return { proc, ready: waitForPort(proc, port), stop: () => proc.kill() };
}