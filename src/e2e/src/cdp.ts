import type { Browser } from '@playwright/test';
import { chromium } from '@playwright/test';

/**
 * Connects Playwright to a remote Chromium over its CDP endpoint — either a
 * netns peer (Task 3, `--remote-debugging-port=922x` on localhost) or a
 * Chromium agent on an Azure VM (Task 9). The returned Browser owns the
 * connection; closing it detaches from the agent without killing it.
 */
export function connectOverCDP(
  endpoint: string,
  // Cold netns Chromium can take tens of seconds to accept CDP; the
  // Playwright default (30s) is tight for the first boot.
  { timeoutMs = 60_000 }: { timeoutMs?: number } = {},
): Promise<Browser> {
  return chromium.connectOverCDP(endpoint, { timeout: timeoutMs });
}