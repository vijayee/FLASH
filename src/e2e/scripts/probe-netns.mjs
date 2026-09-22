import { chromium } from '@playwright/test';

const endpoint = process.argv[2] || 'http://127.0.0.1:9223';
const demo = process.argv[3] || 'http://10.0.2.2:8090/?wirelog=1';
const signalUrl = process.argv[4] || 'ws://10.0.2.2:18080';

const browser = await chromium.connectOverCDP(endpoint, { timeout: 30000 });
const ctx = browser.contexts()[0] ?? (await browser.newContext());
const page = await ctx.newPage();
page.on('console', (m) => console.log('[console]', m.type(), m.text().slice(0, 200)));
page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)));
page.on('requestfailed', (r) => console.log('[reqfail]', r.url(), r.failure()?.errorText));

await page.goto(demo, { timeout: 30000 });
console.log('--- loaded:', page.url());
await page.waitForTimeout(1500);
console.log('--- hook:', await page.evaluate(() => typeof window.__meridian));
await page.evaluate((u) => window.__meridian.connect(u), signalUrl);
await page.waitForTimeout(5000);
const st = await page.evaluate(() => window.__meridian?.state?.() ?? null);
console.log('--- state:', JSON.stringify(st?.knownPeers ?? st)?.slice(0, 400));
console.log('--- status el:', await page.evaluate(() => document.getElementById('status')?.textContent));
await page.close();
await browser.close();