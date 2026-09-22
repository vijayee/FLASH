import { spawn, type ChildProcess } from 'node:child_process';
import type { Page } from '@playwright/test';

/** One entry of the demo's `?wirelog=1` ring buffer (Task 1 demo seam). */
export interface WireLogEntry {
  dir: 'send' | 'recv';
  type: string;
  ts: number;
  peerId: string;
  payload: unknown;
}

/** Snapshot of `window.__meridian.state()` in the JS demo. */
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
  wireLog?: WireLogEntry[];
}

declare global {
  interface Window {
    __meridian?: {
      peerId: () => string | null;
      connect: (url: string) => void;
      state: () => PeerState | null;
    };
  }
}

/**
 * Boots the signaling server from the repo root (it serves no demo; peers
 * connect to the statically-served demo pages directly).
 */
export function startSignaling(port = 8080): ChildProcess {
  // src/e2e/src/orchestrator.ts -> repo root is three levels up.
  return spawn('node', ['src/signaling-server/server.js'], {
    cwd: new URL('../../../', import.meta.url).pathname,
    env: { ...process.env, PORT: String(port) },
    stdio: 'ignore',
  });
}

/** Polls a boolean/object predicate in the page until truthy or deadline. */
export async function waitFor<T>(
  page: Page,
  predicateJs: string,
  timeoutMs = 15_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = (await page.evaluate(`(${predicateJs})()`)) as T | null;
    if (value != null) return value;
    if (Date.now() > deadline) {
      throw new Error(`waitFor timed out: ${predicateJs}`);
    }
    await page.waitForTimeout(250);
  }
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
 * Reads the peer's state snapshot. `null` before the demo page has
 * connected (the hook dereferences no node yet).
 */
export const state = (page: Page) =>
  page.evaluate(
    () => window.__meridian?.state() ?? null,
  ) as Promise<PeerState | null>;