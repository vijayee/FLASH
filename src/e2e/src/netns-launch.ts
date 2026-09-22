/**
 * Task 3 latency-rig launcher: drives scripts/netns-up.sh / netns-down.sh
 * (which are 100% unprivileged — see their header for why rootlesskit's
 * builtin port driver could not be used on this machine and how the same
 * architecture is realized with `unshare -Urn --map-root-user` +
 * `slirp4netns --api-socket` host-forwarding instead).
 *
 * Latency model (verified: netem on tap0 delays only that namespace's
 * transmitted packets, i.e. egress; slirp overhead adds 0-2ms):
 *
 *   RTT(a <-> b) = halfDelayMs_a + halfDelayMs_b  (+0-2ms)
 *
 *   eu:   60ms   -> eu<->us   80ms  (ring 7, bounds (64, 128])
 *   us:   20ms   -> eu<->asia 160ms  (ring 8, bounds (128, 256])
 *   asia: 100ms  -> us<->asia 120ms  (ring 7)
 *
 * (This corrects the plan sketch's netem 20/20/60ms, which under the
 * egress-only model measures 40/80/80ms and would place the us and asia
 * peers a ring lower than the latency assertions expect.)
 */
import { spawn } from 'node:child_process';
import { readFileSync, existsSync, renameSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { chromium } from '@playwright/test';

import type { NetnsRegion } from './netns.js';

export type { NetnsRegion };
export type NetnsRegionName = NetnsRegion;

export interface RegionSpec {
  /** netem egress delay applied on the region's tap0 (ms). */
  halfDelayMs: number;
  /** Host port forwarding into the in-netns CDP relay (-> Chromium 9222). */
  cdpHostPort: number;
  /**
   * The slirp subnet shared by every region: slirp4netns 1.0.1 ignores
   * --cidr (its -c always assigns 10.0.2.100/24 with the 10.0.2.2
   * gateway, verified empirically), so every namespace's tap0 carries
   * that same address and dials the host (loopback) through it.
   */
  cidr: string;
  /** The netns tap0 address (slirp's -c-assigned .100 guest convention). */
  tapIp: string;
  /** slirp gateway inside the netns (maps to the host's loopback). */
  gateway: string;
}

export const REGIONS: Record<NetnsRegion, RegionSpec> = {
  eu: {
    halfDelayMs: 60,
    cdpHostPort: 9223,
    cidr: '10.0.2.0/24',
    tapIp: '10.0.2.100',
    gateway: '10.0.2.2',
  },
  us: {
    halfDelayMs: 20,
    cdpHostPort: 9224,
    cidr: '10.0.2.0/24',
    tapIp: '10.0.2.100',
    gateway: '10.0.2.2',
  },
  asia: {
    halfDelayMs: 100,
    cdpHostPort: 9225,
    cidr: '10.0.2.0/24',
    tapIp: '10.0.2.100',
    gateway: '10.0.2.2',
  },
};

export interface LaunchedRegion {
  region: NetnsRegion;
  /** CDP endpoint of the netns Chromium, reachable from the host. */
  endpoint: string;
  spec: RegionSpec;
  /** Tears the region's netns + slirp down (idempotent). */
  teardown: () => Promise<void>;
}

interface RigState {
  region: string;
  pid: number;
  slirpPid: number;
  endpoint: string;
  halfDelayMs: number;
  tapIp: string;
  gateway: string;
  cidr: string;
  cdpHostPort: number;
}

const STATE_ROOT = '/tmp/mrd-netns';

const scriptPath = (name: string) =>
  fileURLToPath(new URL(`../scripts/${name}`, import.meta.url));

/**
 * Brings one region's netns rig up (idempotent: an already-live region is
 * reused) and resolves once its Chromium CDP endpoint answers
 * `/json/version` through the slirp port forward. Rejects with the rig's
 * log tail on any failure, so a boot problem fails the suite loudly.
 *
 * Deterministic: the region's state is written to
 * `/tmp/mrd-netns/<region>.json` (pid, endpoint, half-delay, tap0 ip).
 */
export async function launchRegion(
  region: NetnsRegion,
  overrides: Partial<Pick<RegionSpec, 'halfDelayMs' | 'cdpHostPort'>> = {},
): Promise<LaunchedRegion> {
  const spec: RegionSpec = { ...REGIONS[region], ...overrides };
  const chromeBin =
    process.env.CHROME_BIN || chromium.executablePath();
  const up = fileURLToPath(new URL('../scripts/netns-up.sh', import.meta.url));
  const stateFile = `${STATE_ROOT}/${region}.json`;

  const rig = spawn(
    'bash',
    [
      up,
      region,
      String(spec.halfDelayMs),
      String(spec.cdpHostPort),
      chromeBin,
    ],
    // The rig streams its own progress into the test's console — that log
    // is also the forensics for a failed boot (chromium.log, slirp.log,
    // hostfwd.log all live under the state dir). NETNS_DIAG=1 additionally
    // makes the child write tc/addr/ping-gateway/host-demo evidence into
    // <state dir>/diag.txt at raise time.
    {
      stdio: ['ignore', 'inherit', 'inherit'],
      env: { ...process.env, NETNS_DIAG: process.env.NETNS_DIAG || '1' },
    },
  );

  // 180s: covers a cold raise (~10s Chromium boot + slirp attach + tap
  // configure) plus the CDP wait (the rig's own poll budget is 120s).
  const deadline = Date.now() + 180_000;
  let rigFailure: Error | null = null;
  /**
   * Rig forensics: tails of the up-script's logs, read BEFORE any
   * teardown sweep can remove them (netnsDown rm -rf's the state dir).
   */
  const readRigLogs = (region: string): string => {
    let out = '';
    for (const log of [
      'chromium.log',
      'slirp.log',
      'hostfwd.log',
      'relay.log',
      'tc.txt',
    ]) {
      const path = `${STATE_ROOT}/${region}/${log}`;
      if (!existsSync(path)) continue;
      try {
        const lines = readFileSync(path, 'utf8').trim().split('\n');
        out += `\n--- ${region}/${log} (last 12):\n${lines.slice(-12).join('\n')}`;
      } catch {
        // Unreadable: skip.
      }
    }
    return out;
  };
  const failLaunch = (region: string, message: string): void => {
    // Forensics are read here, BEFORE netnsDown's sweep removes them.
    rigFailure = new Error(
      `netns-up.sh ${region} ${message}; ` +
        `logs: ${STATE_ROOT}/${region}/{chromium,slirp,hostfwd,relay}.log\n` +
        readRigLogs(region),
    );
  };
  rig.once('exit', (code) => {
    // A completed `netns-up.sh` (state written, Chromium running as its
    // exec'd background child) exits 0 by design — that is the rig up,
    // not a failure. Only a non-zero exit (die) fails the launch here;
    // the die path already sweeps the region's orphans.
    if (code === 0) return;
    failLaunch(region, `exited early (code ${code})`);
  });
  const state = await (async () => {
    for (;;) {
      if (rigFailure) break;
      if (existsSync(stateFile)) {
        try {
          return JSON.parse(readFileSync(stateFile, 'utf8')) as RigState;
        } catch {
          // Written non-atomically? A completed write lands whole; retry.
        }
      }
      if (Date.now() > deadline) {
        failLaunch(region, 'did not come up within 180s');
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    // Preserve the failed rig's forensics (its logs die with the region
    // dir in netnsDown), then never leak a half-raised rig: sweep the
    // region's orphans (the down script is idempotent and marker-based).
    try {
      renameSync(`${STATE_ROOT}/${region}`, `${STATE_ROOT}/${region}.failed`);
    } catch {
      // Best-effort: the rig died before creating its dir.
    }
    try {
      await netnsDown(region);
    } catch {
      // Best-effort.
    }
    throw rigFailure;
  })();

  const endpoint = state.endpoint;
  if (!endpoint) {
    throw new Error(`netns rig for ${region} wrote no endpoint`);
  }

  return {
    region,
    endpoint,
    spec: {
      ...spec,
      halfDelayMs: state.halfDelayMs || spec.halfDelayMs,
      tapIp: state.tapIp || spec.tapIp,
      gateway: state.gateway || spec.gateway,
    },
    teardown: () => netnsDown(region),
  };
}

/** Tears one region's rig down (idempotent no-op without state). */
export async function netnsDown(region: NetnsRegion): Promise<void> {
  const down = fileURLToPath(
    new URL('../scripts/netns-down.sh', import.meta.url),
  );
  await new Promise<void>((resolve, reject) => {
    const proc = spawn('bash', [down, region], {
      stdio: ['ignore', 'inherit', 'inherit'],
    });
    proc.once('exit', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`netns-down.sh ${region} exited ${code}`));
    });
    proc.once('error', reject);
  });
}