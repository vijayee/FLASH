/**
 * Task 9: remote Chromium agents on the Azure geo estate.
 *
 * One agent per peer VM is raised over SSH (`sudo /opt/flash/agent/agent.sh
 * <js|dart>`): the script starts a CDP relay (0.0.0.0:9223 -> 127.0.0.1:9222
 * — new headless Chromium always binds the debug port to loopback) plus a
 * headless Chromium with the demo origin marked treated-secure, prints an
 * `agent ready:` line, and stays in the foreground. The SSH session IS the
 * agent's lifecycle: closing the client tears Chromium + relay down with it
 * (agent.sh's final `wait` rides the session; a belt-and-braces remote pkill
 * sweeps anything a HUP-ignoring Chromium left behind).
 *
 * The orchestrator then attaches over CDP (`connectOverCDP`, src/cdp.ts) and
 * creates pages through the browser's EXISTING default context — CDP-attached
 * browsers have no implicit-context creation (same constraint as the netns
 * rigs, Task 3/4/5).
 */
import { spawn, type ChildProcess } from 'node:child_process';

import type { Browser } from '@playwright/test';

import { connectOverCDP } from './cdp.js';

export interface RemoteAgentOptions {
  /** The VM's public IP (the agent + demo + CDP are all reached on it). */
  host: string;
  /** agent.sh role: the JS demo (:80) or the dart-web demo (:8090). */
  role: 'js' | 'dart';
  /** SSH login user (Azure cloud-init default). */
  user?: string;
  /** Budget for SSH connect + Chromium boot + CDP readiness. */
  readyTimeoutMs?: number;
}

export interface RemoteAgentReady {
  role: string;
  origin: string;
  cdp: number;
  relay: number;
  /** The remote Chromium PID (from the ready line; for forensics/kill). */
  pid: number;
}

export interface RemoteAgent {
  host: string;
  role: string;
  /**
   * Public CDP endpoint (through the agent's 0.0.0.0:9223 relay): pass to
   * `connectOverCDP`. NSG restricts 9222-9225 to the orchestrator IP.
   */
  endpoint: string;
  /** The parsed `agent ready:` line fields (null until readiness resolved). */
  ready: RemoteAgentReady | null;
  /**
   * The demo base URL to load pages at — the agent's own ready-line
   * `origin` (e.g. `http://10.0.0.4`, the VM's private address). This is
   * DELIBERATE, not a shortcut: the flag value of
   * `--unsafely-treat-insecure-origin-as-secure` is that same origin, and
   * Chrome 140 BLOCKS navigation to any other insecure IP origin with
   * net::ERR_BLOCKED_BY_CLIENT (verified empirically on the estate; IMDS
   * reports an empty publicIpAddress there, so agent.sh falls back to the
   * private address). The demo servers listen on 0.0.0.0, so the page
   * loads from the private origin fine; signaling (ws://lab:8080) and ICE
   * (host + srflx candidates over real interfaces) are origin-independent.
   */
  demoOrigin: string | null;
  /** Resolves once the agent announced readiness (await in beforeAll). */
  whenReady: Promise<void>;
  /**
   * Tears the agent down: closes the SSH client (the remote session's SIGHUP
   * ends agent.sh's `wait`, taking Chromium + relay with it), then
   * best-effort SIGKILLs anything left under the role's profile dir.
   * Idempotent.
   */
  stop(): Promise<void>;
  /** stdout/stderr forensics for a failed launch. */
  diagnostics(): string;
}

const SSH_USER = 'azureuser';

/** Parses `role=js origin=http://<ip> cdp=9222 relay=9223 pid=<n>`. */
function parseReadyLine(
  line: string,
  fallbackRole: string,
): RemoteAgentReady | null {
  const fields = Object.fromEntries(
    line
      .split(' ')
      .map((kv) => kv.split('=', 2))
      .filter((pair): pair is [string, string] => pair.length === 2),
  );
  const pid = Number(fields.pid);
  if (!fields.origin || !Number.isFinite(pid) || pid <= 0) return null;
  return {
    role: fields.role || fallbackRole,
    origin: fields.origin,
    cdp: Number(fields.cdp) || 9222,
    relay: Number(fields.relay) || 9223,
    pid,
  };
}

/**
 * Raises one VM's agent and resolves once its `agent ready:` line arrives
 * (agent.sh itself gates on Chromium's CDP endpoint answering
 * /json/version through the relay, so readiness here means CDP is usable).
 * Rejects — with the SSH output tail — if the session dies first or
 * readiness never lands within `readyTimeoutMs`.
 */
export function launchRemoteAgent({
  host,
  role,
  user = SSH_USER,
  readyTimeoutMs = 120_000,
}: RemoteAgentOptions): RemoteAgent {
  // The agent stays foreground by design: the ssh client runs for the whole
  // suite, and its stdout's `agent ready:` line is the readiness gate.
  const proc: ChildProcess = spawn(
    'ssh',
    [
      '-o', 'StrictHostKeyChecking=accept-new',
      '-o', 'BatchMode=yes',
      '-o', 'ServerAliveInterval=15',
      `${user}@${host}`,
      `sudo /opt/flash/agent/agent.sh ${role}`,
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );

  let stdout = '';
  let stderr = '';
  let ready: RemoteAgentReady | null = null;
  const fail: (err: Error) => void = (err) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    rejectPromise(err);
  };
  const succeed = () => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    resolvePromise();
  };
  let rejectPromise!: (err: Error) => void;
  let resolvePromise!: () => void;
  let settled = false;
  let timer: NodeJS.Timeout | undefined;
  const whenReady = new Promise<void>((resolve, reject) => {
    rejectPromise = reject;
    resolvePromise = resolve;
    timer = setTimeout(
      () =>
        fail(
          new Error(
            `agent ${role}@${host} not ready within ${readyTimeoutMs}ms\n` +
              `stdout: ${stdout}\nstderr: ${stderr}`,
          ),
        ),
      readyTimeoutMs,
    );
    proc.stdout!.on('data', (chunk: Buffer) => {
      stdout += chunk.toString();
      for (const line of stdout.split('\n')) {
        if (!line.startsWith('agent ready:')) continue;
        ready = parseReadyLine(
          line.slice('agent ready:'.length).trim(),
          role,
        );
        if (ready) succeed();
      }
    });
    proc.stderr!.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    proc.once('exit', (code) => {
      // A post-ready exit is the normal orchestrator-driven teardown.
      if (ready) return;
      fail(
        new Error(
          `agent ${role}@${host} exited before ready (code ${code})\n` +
            `stdout: ${stdout}\nstderr: ${stderr}`,
        ),
      );
    });
    proc.once('error', (err) =>
      fail(new Error(`agent ${role}@${host} spawn failed: ${err.message}`)),
    );
  });

  return {
    host,
    role,
    endpoint: `http://${host}:9223`,
    get ready(): RemoteAgentReady | null {
      return ready;
    },
    get demoOrigin(): string | null {
      return ready?.origin ?? null;
    },
    whenReady,
    async stop() {
      await new Promise<void>((resolve) => {
        if (proc.exitCode !== null) {
          resolve();
          return;
        }
        proc.once('exit', () => resolve());
        proc.kill('SIGTERM');
        // Do not hang afterAll on a wedged ssh client.
        setTimeout(resolve, 5_000).unref();
      });
      // Belt and braces: the SIGHUP path can miss Chromium, so SIGKILL
      // anything left under the role's profile dir. Bracketed patterns so
      // pkill's own remote bash command line never self-matches.
      // Best-effort — an already-dead VM fails this harmlessly.
      await remoteExec(
        host,
        `sudo pkill -9 -f 'agent/profile-${role.slice(0, 1)}[a-z]' || true; ` +
          `sudo pkill -9 -f 'cdp-[r]elay' || true`,
      ).catch(() => {});
    },
    diagnostics: () => `stdout: ${stdout}\nstderr: ${stderr}`,
  };
}

/**
 * One-shot SSH command runner (no PTY, bounded): used for the signaling
 * journal dump and any remote lifecycle pokes the geo suite needs.
 */
export function remoteExec(
  host: string,
  command: string,
  { timeoutMs = 30_000, user = SSH_USER } = {},
): Promise<string> {
  return new Promise((resolve, reject) => {
    const proc: ChildProcess = spawn(
      'ssh',
      [
        '-o', 'StrictHostKeyChecking=accept-new',
        '-o', 'BatchMode=yes',
        '-o', `ConnectTimeout=${Math.max(5, Math.ceil(timeoutMs / 1000))}`,
        `${user}@${host}`,
        command,
      ],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    let out = '';
    let err = '';
    proc.stdout!.on('data', (c: Buffer) => (out += c.toString()));
    proc.stderr!.on('data', (c: Buffer) => (err += c.toString()));
    const timer = setTimeout(
      () => {
        proc.kill('SIGKILL');
        reject(new Error(`ssh ${host} timed out: ${command}\n${err}`));
      },
      timeoutMs,
    );
    proc.once('exit', (code) => {
      clearTimeout(timer);
      if (code === 0) resolve(out);
      else reject(new Error(`ssh ${host} exited ${code}: ${command}\n${err}`));
    });
    proc.once('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

/**
 * CDP-attach for one agent. The returned Browser owns the connection —
 * closing it only detaches Playwright; the remote Chromium keeps running
 * until the agent's SSH session is torn down. Create pages through
 * `browser.contexts()[0]` (CDP browsers have no implicit-context creation).
 */
export async function connectAgent(agent: RemoteAgent): Promise<Browser> {
  return connectOverCDP(agent.endpoint);
}