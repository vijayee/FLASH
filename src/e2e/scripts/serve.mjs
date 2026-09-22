import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, join, resolve, extname, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

// src/e2e/scripts/serve.mjs
const scriptDir = dirname(fileURLToPath(import.meta.url));
// Repo root (src/e2e/scripts -> src/e2e -> src -> repo root): resolved from
// this file's location so the server's cwd never matters.
const repoRoot = resolve(scriptDir, '..', '..', '..');
// The JS demo pages (served at /).
const demoRoot = join(repoRoot, 'src', 'js', 'examples', 'browser');
// The demo imports the library via ../../src/index.js, which the browser
// resolves to /src/... at the origin root: fall back to the package root.
const libRoot = join(repoRoot, 'src', 'js');
// Task 4 media fixture (the looping ?mediaSrc= uplink file), served under
// /fixtures/... from the e2e package's fixtures dir.
const fixturesRoot = join(repoRoot, 'src', 'e2e', 'fixtures');
// Task 6: DART_ROOT switches this server into dart-web mode — it serves the
// STAGED Flutter web bundle (src/e2e/scripts/build-dart-web.sh copies
// src/dart/example/build/web to src/e2e/build/dart-web) at / plus the same
// /fixtures/ base (the Dart demo's ?mediaSrc= uplink loads the fixture from
// its own origin, which captureStream() requires). The default (JS demo)
// mode is unchanged.
const dartRoot = process.env.DART_ROOT
  ? resolve(process.env.DART_ROOT)
  : null;

const port = Number(process.env.PORT) || 8090;
const types = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.wasm': 'application/wasm',
  '.txt': 'text/plain; charset=utf-8',
  '.mp4': 'video/mp4',
};

// Browser-supplied URLs are untrusted: resolve the request path inside each
// base directory and refuse anything that escapes it (same hygiene as
// src/js/examples/browser/serve.mjs).
function isInside(base, pathname) {
  let resolved;
  try {
    resolved = resolve(base, '.' + pathname);
  } catch {
    return null;
  }
  if (resolved !== base && !resolved.startsWith(base + sep)) return null;
  return resolved;
}

async function serve(req, res) {
  const stripped = (req.url ?? '/').split('?')[0];
  const raw = stripped === '/' || stripped === '' ? '/index.html' : stripped;
  let pathname;
  try {
    pathname = decodeURIComponent(raw);
  } catch {
    res.writeHead(404);
    res.end('not found');
    return;
  }
  // Demo assets first (index.html, main.js); /src/... falls through to the
  // library package root; /fixtures/... to the e2e media fixture dir (the
  // prefix is stripped before the containment check, so the traversal
  // hygiene below still applies to whatever follows it). DART_ROOT mode
  // serves the staged Flutter bundle at / instead of the JS demo.
  const bases = dartRoot
    ? [
        { root: dartRoot, prefix: '' },
        { root: fixturesRoot, prefix: '/fixtures/' },
      ]
    : [
        { root: demoRoot, prefix: '' },
        { root: libRoot, prefix: '' },
        { root: fixturesRoot, prefix: '/fixtures/' },
      ];
  for (const { root, prefix } of bases) {
    let rel = pathname;
    if (prefix) {
      if (!pathname.startsWith(prefix)) continue;
      // Keep the leading slash: isInside resolves './<rel>' against the
      // base, so the remainder must stay root-relative.
      rel = '/' + pathname.slice(prefix.length);
    }
    const filePath = isInside(root, rel);
    if (!filePath) continue;
    try {
      const body = await readFile(filePath);
      const type = types[extname(pathname)] || 'application/octet-stream';
      res.writeHead(200, { 'content-type': type });
      res.end(body);
      return;
    } catch {
      // Not here (or a directory): try the next base.
    }
  }
  res.writeHead(404);
  res.end('not found');
}

// HOST env: 0.0.0.0 when the Task 3 netns rig serves the demo to the
// netns Chromiums (they reach this server through their slirp gateway,
// 10.0.<i>.2 -> the host's loopback); default stays loopback-only.
const host = process.env.HOST || '127.0.0.1';

createServer(serve).listen(port, host, () => {
  console.log(
    dartRoot
      ? `e2e dart-web server: http://${host}:${port} (root: ${dartRoot})`
      : `e2e demo server: http://${host}:${port} (signaling ws://localhost:8080)`,
  );
});