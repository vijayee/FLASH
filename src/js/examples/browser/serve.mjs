import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, join, resolve, extname, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const modulePath = fileURLToPath(import.meta.url);
const root = dirname(modulePath);
// The demo imports the library via ../../src/index.js, which the browser
// clamps to /src/... at the origin root: fall back to the package root.
const packageRoot = join(root, '..', '..');
const types = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css'
};

// Browser-supplied URLs are untrusted: resolve the request path inside the
// base directory and refuse anything that escapes it.
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
  const path = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  let pathname;
  try {
    pathname = decodeURIComponent(path);
  } catch {
    res.writeHead(404);
    res.end('not found');
    return;
  }
  for (const base of [root, packageRoot]) {
    const filePath = isInside(base, pathname);
    if (!filePath) continue;
    try {
      const body = await readFile(filePath);
      const type = types[extname(pathname)] || 'application/octet-stream';
      res.writeHead(200, { 'content-type': type });
      res.end(body);
      return;
    } catch {
      // Try the next base.
    }
  }
  res.writeHead(404);
  res.end('not found');
}

createServer(serve).listen(9000, '127.0.0.1', () => {
  console.log('Demo: http://127.0.0.1:9000 (signaling default ws://localhost:8080)');
});