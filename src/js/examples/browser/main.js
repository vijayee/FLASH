import { MeridianNode, MERIDIAN_CONFIG } from '../../src/index.js';
// Imported directly (not via index.js) only to wrap connect() for the
// optional ?wirelog=1 e2e recording below; no library behavior changes.
import { SignalingClient } from '../../src/signaling-client.js';
// Imported directly (not via index.js) only for the __meridian.elect() e2e
// driving affordance below: the library's public election entry point, run
// over the current cluster; no library behavior changes.
import { electSupernode } from '../../src/raft.js';

const $ = (id) => document.getElementById(id);
const logEl = $('log');
const MAX_LOG_ENTRIES = 200;
let node = null;
let statusTimer = null;
const renderedStreams = new Map(); // peerId -> rendered <video>

// --- e2e config affordances (Task 3; example-only) -----------------------
// ?gossipMs=<n> overrides MERIDIAN_CONFIG.gossipPeriodMs so a local e2e
// run converges in seconds instead of waiting out the 30s production
// gossip period. ?stun=a,b overrides stunServers (the netns rig points
// peers at a loopback mini-STUN through slirp). ?turn=url,username,cred
// (Task 10) appends ONE long-term-credential TURN entry to turnServers —
// the shape the library's buildIceServers spreads into the RTCIceServer
// list (src/js/src/rtc-utils.js: array of {urls, username, credential}).
// The turn URL may not contain a comma (turn: URLs never do), so a plain
// comma split parses all three fields. Invalid values fall back to the
// library defaults (empty turnServers).
const urlParams = new window.URLSearchParams(window.location.search);
const gossipMsParam = Number(urlParams.get('gossipMs'));
const mediaSrcParam = urlParams.get('mediaSrc');
const stunParam = urlParams.get('stun');
const turnParam = urlParams.get('turn');
const stunOverride = stunParam
  ? stunParam.split(',').map((s) => s.trim()).filter(Boolean)
  : null;
const turnFields = turnParam
  ? turnParam.split(',').map((s) => s.trim())
  : null;
const turnOverride =
  turnFields && turnFields[0] && turnFields[1] && turnFields[2]
    ? { urls: turnFields[0], username: turnFields[1], credential: turnFields[2] }
    : null;

// --- Overlay configuration panel -------------------------------------------
// The demo exposes the FULL MERIDIAN_CONFIG (src/js/src/config.js) as the
// collapsible #config-panel in index.html. The URL params above remain the
// source of truth for the e2e affordances: their parsed values pre-fill the
// panel inputs, and Connect builds the config FROM the inputs, so an e2e
// URL (?gossipMs=…&stun=…&turn=……) yields exactly the config the raw param
// parsing used to produce. Invalid values fall back to the library default.
// Empty stun means the library default list; empty turn means no relay.
const CONFIG_FIELDS = [
  ['ringsPerNode', 'Rings per node', 'int',
    'Latency-ordered rings this peer keeps; more rings = finer resolution, more connections.'],
  ['nodesPerRing', 'Nodes per ring', 'int',
    'Primary members stored per ring; extras degrade to secondary candidates.'],
  ['secondaryCandidates', 'Secondary candidates', 'int',
    'Backup members kept per ring for failover when a primary leaves.'],
  ['innermostRingRadius', 'Innermost ring radius (ms)', 'float',
    'RTT radius of ring 0 — the closest-latency band a peer maintains.'],
  ['ringMultiplicativeFactor', 'Ring factor (r)', 'float',
    'Each outer ring\'s RTT radius is the previous ring\'s times this.'],
  ['routeAcceptanceThreshold', 'Acceptance threshold (β)', 'float',
    'Fraction of queried rings that must answer before a routed query\'s result is accepted.'],
  ['probeTimeoutFactor', 'Probe timeout factor (ε)', 'float',
    'An RTT probe times out at this multiple of the peer\'s last measured RTT.'],
  ['gossipPeriodMs', 'Gossip period (ms)', 'int',
    'Interval between gossip exchanges; smaller values converge faster (e2e overrides this).'],
  ['ringReplacementPeriodMs', 'Ring replacement period (ms)', 'int',
    'Interval between maintenance sweeps that replace dead or slow ring members.'],
  ['maxEphemeralConnections', 'Max ephemeral connections', 'int',
    'Cap on short-lived probe connections opened for RTT measurement.'],
  ['maxHops', 'Max query hops', 'int',
    'Hop cap for routed closest-node / central-leader queries.'],
  ['ephemeralProbeTimeoutMs', 'Ephemeral probe timeout (ms)', 'int',
    'Timeout for one ephemeral probe connection.'],
  ['queryTimeoutMs', 'Query timeout (ms)', 'int',
    'Overall timeout for closest-node / central-leader queries.'],
];

// Panel pre-fill: library defaults, overridden by the URL params above.
const configPrefill = Object.fromEntries(
  CONFIG_FIELDS.map(([key]) => [key, MERIDIAN_CONFIG[key]])
);
if (Number.isFinite(gossipMsParam) && gossipMsParam > 0) {
  configPrefill.gossipPeriodMs = gossipMsParam;
}
configPrefill.stun = stunOverride
  ? stunParam
  : MERIDIAN_CONFIG.stunServers.join(',');
configPrefill.turn = turnOverride ? turnParam : '';

function addConfigRow(container, id, label, value, hint, disabled) {
  const row = document.createElement('div');
  row.className = 'config-row';
  const labelEl = document.createElement('label');
  labelEl.htmlFor = id;
  labelEl.textContent = label;
  const input = document.createElement('input');
  input.id = id;
  input.value = value;
  input.disabled = disabled === true;
  const what = document.createElement('span');
  what.className = 'what';
  what.textContent = hint;
  row.append(labelEl, input, what);
  container.append(row);
}

function buildConfigPanel() {
  const container = $('config-fields');
  for (const [key, label, , hint] of CONFIG_FIELDS) {
    addConfigRow(
      container,
      `config-${key}`,
      label,
      String(configPrefill[key]),
      hint
    );
  }
  addConfigRow(
    container,
    'config-stun',
    'STUN servers',
    configPrefill.stun,
    'Comma-separated STUN URLs for public-candidate discovery.'
  );
  addConfigRow(
    container,
    'config-turn',
    'TURN relay (url,username,credential)',
    configPrefill.turn,
    'One long-term-credential TURN entry for restrictive NATs; empty = no relay.'
  );
  addConfigRow(
    container,
    'config-mediaSrc',
    'Media source (uplink)',
    mediaSrcParam || '(getUserMedia / fake device)',
    'Display-only: chosen by the ?mediaSrc= URL param (looping file uplink).',
    true
  );
}
buildConfigPanel();

// Reads the (pre-filled) panel back into a MERIDIAN_CONFIG override set.
// Invalid or empty entries keep the library default, so hand-edited garbage
// can never poison the node.
function readConfigFromPanel() {
  const config = { ...MERIDIAN_CONFIG };
  for (const [key, , type] of CONFIG_FIELDS) {
    const raw = $(`config-${key}`).value.trim();
    if (!raw) continue;
    const value = Number(raw);
    if (!Number.isFinite(value)) continue;
    if (type === 'int' && !Number.isInteger(value)) continue;
    config[key] = value;
  }
  const stunRaw = $('config-stun').value.trim();
  if (stunRaw) {
    config.stunServers = stunRaw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
  }
  const turnRaw = $('config-turn').value.trim();
  if (turnRaw) {
    const fields = turnRaw.split(',').map((s) => s.trim());
    if (fields[0] && fields[1] && fields[2]) {
      config.turnServers = [
        { urls: fields[0], username: fields[1], credential: fields[2] },
      ];
    }
  }
  return config;
}

// --- e2e wire log (?wirelog=1) -------------------------------------------
// Optional bounded record of every DataChannel send/receive and signaling
// message, surfaced through window.__meridian.state().wireLog so a failed
// e2e run can be debugged from the page itself. Off by default: it only
// wraps the transport seams' send/onmessage, the library does the protocol.
const WIRE_LOG_MAX = 2000;
const wireLogEnabled =
  new window.URLSearchParams(window.location.search).get('wirelog') === '1';
const wireLogEntries = [];

function recordWire(dir, peerId, raw) {
  let type = 'unknown';
  let payload = raw;
  try {
    const decoded = JSON.parse(raw);
    if (decoded && typeof decoded === 'object') {
      payload = decoded;
      if (typeof decoded.type === 'string') type = decoded.type;
    }
  } catch {
    // Not JSON; keep the raw payload with the placeholder type.
  }
  if (wireLogEntries.length >= WIRE_LOG_MAX) wireLogEntries.shift();
  wireLogEntries.push({ dir, type, ts: Date.now(), peerId, payload });
}

// Signaling traffic funnels through the client's WebSocket (register,
// get_peers, offers, ICE), so wrapping the socket catches it all, including
// the bootstrap sends that bypass node._sendSignaling.
if (wireLogEnabled && !SignalingClient.prototype.connect.__wireLogged) {
  const origConnect = SignalingClient.prototype.connect;
  const wrappedConnect = function (url, node) {
    const bootstrapped = origConnect.call(this, url, node);
    const ws = this.ws;
    if (ws && !ws.__wireLogged) {
      ws.__wireLogged = true;
      const peerId = this.peerId;
      const origSend = ws.send.bind(ws);
      ws.send = (data) => {
        recordWire('send', peerId, String(data));
        origSend(data);
      };
      const origOnMessage = ws.onmessage;
      ws.onmessage = (event) => {
        recordWire('recv', peerId, String(event.data));
        origOnMessage(event);
      };
    }
    return bootstrapped;
  };
  wrappedConnect.__wireLogged = true;
  SignalingClient.prototype.connect = wrappedConnect;
}

// The node routes every protocol DataChannel (ring members, handshake
// channels) through this seam; wrapping it at the instance lets each
// channel's send and receive path be recorded without touching the library.
function installWireLog(target) {
  if (!wireLogEnabled) return;
  // The seam is library-internal and can move; degrade the wirelog (the
  // signaling half is still wrapped above) instead of killing the demo.
  if (typeof target._setupDataChannelHandlers !== 'function') {
    console.warn('[wirelog] setup seam not found; wirelog disabled');
    return;
  }
  const origSetup = target._setupDataChannelHandlers.bind(target);
  target._setupDataChannelHandlers = (dc, peerId) => {
    origSetup(dc, peerId);
    if (dc.__wireLogged) return;
    dc.__wireLogged = true;
    const origSend = dc.send.bind(dc);
    dc.send = (data) => {
      recordWire('send', peerId, String(data));
      origSend(data);
    };
    dc.addEventListener('message', (event) =>
      recordWire('recv', peerId, String(event.data)));
  };
}

function log(message) {
  const line = document.createElement('div');
  line.textContent = `${new Date().toISOString().slice(11, 23)}  ${message}`;
  logEl.prepend(line);
  // Newest entries are prepended: trim the oldest from the tail.
  while (logEl.childElementCount > MAX_LOG_ENTRIES) {
    logEl.lastElementChild.remove();
  }
}

function ringOccupancy() {
  return node.rings.reduce(
    (total, ring) => total + ring.primaryMembers.length,
    0
  );
}

function refreshStatus() {
  if (!node) return;
  $('status').textContent =
    `knownPeers: ${node.knownPeers.size}, ` +
    `ring slots: ${ringOccupancy()}, ` +
    `supernode: ${node.isSupernode ? 'yes' : 'no'}`;
  renderPeerInspector();
}

// Peer inspector (#peer-table): the node's knownPeers projected per row —
// short id, RTT, ring index (ring 0 = closest band), handshake status —
// sorted by ring then RTT. Fed from the same 1s poll as the status line.
function renderPeerInspector() {
  const tbody = $('peer-rows');
  tbody.textContent = '';
  const peers = [...node.knownPeers.entries()].sort(
    (a, b) =>
      (a[1].ringIndex ?? Number.MAX_SAFE_INTEGER) -
        (b[1].ringIndex ?? Number.MAX_SAFE_INTEGER) ||
      (a[1].rtt ?? Number.MAX_SAFE_INTEGER) -
        (b[1].rtt ?? Number.MAX_SAFE_INTEGER)
  );
  for (const [id, peer] of peers) {
    const row = document.createElement('tr');
    const cells = [
      id.slice(0, 8),
      peer.rtt == null ? '?' : Math.round(peer.rtt),
      peer.ringIndex == null ? '-' : peer.ringIndex,
      peer.status,
    ];
    for (const cell of cells) {
      const td = document.createElement('td');
      td.textContent = String(cell);
      row.append(td);
    }
    tbody.append(row);
  }
}

// Remote MediaStreams surface on node.activeStreams (populated by the
// node's ontrack wiring); a light poll renders any that appear and prunes
// any whose entry has been removed (media_close / peer failure).
function renderStreams() {
  if (!node) return;
  for (const [peerId, stream] of node.activeStreams) {
    if (renderedStreams.has(peerId)) continue;
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    video.srcObject = stream;
    video.dataset.peerId = peerId;
    $('videos').append(video);
    renderedStreams.set(peerId, video);
    log(`rendering stream from ${peerId}`);
  }
  for (const peerId of [...renderedStreams.keys()]) {
    if (node.activeStreams.has(peerId)) continue;
    renderedStreams.get(peerId).remove();
    renderedStreams.delete(peerId);
    log(`stream from ${peerId} closed`);
  }
}

function setConnected(connected) {
  $('connect').disabled = connected;
  $('disconnect').disabled = !connected;
  $('find').disabled = !connected;
  $('stream').disabled = !connected;
  // The overlay config is baked into the live node: once a node exists the
  // panel is locked until a page reload (the hint says so).
  if (connected) {
    for (const input of document.querySelectorAll('#config-fields input')) {
      input.disabled = true;
    }
  }
}

function wireHandlers() {
  node.handlers.onPeerDisconnected = (peerId) =>
    log(`peer disconnected: ${peerId}`);
  node.handlers.onSupernodeElected = (peerId) =>
    log(`supernode elected: ${peerId}`);
  node.handlers.onStreamRequest = (peerId) =>
    log(`stream requested by ${peerId}`);
  node.handlers.onStreamOffer = (peerId) => {
    log(`stream offered by ${peerId}`);
    renderStreams();
  };
}

// randomUUID exists only in secure contexts; the demo also runs against
// plain-http origins (netns/Azure peers via slirp), so fall back.
function uuidV4() {
  if (crypto.randomUUID) return crypto.randomUUID();
  return ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, (c) =>
    (
      c ^
      (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (c / 4)))
    ).toString(16)
  );
}

// ?mediaSrc=<url> (Task 4, example-only): when present, the Connect flow
// uses a looping <video> playing the file as the uplink (captureStream())
// instead of getUserMedia — e2e's deterministic media source. The element
// must be RENDERED for the browser to decode/play it (a display:none video
// never starts in headless Chromium), so the demo's own local-preview slot
// #local (given `loop` in index.html) is reused. Falls back to getUserMedia
// (fake device under the e2e launch flags) if the file never plays.
async function startFileUplink(src) {
  const video = $('local');
  video.src = src;
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('mediaSrc video did not start within 20s')),
        20_000
      );
      video.addEventListener(
        'error',
        () => {
          clearTimeout(timer);
          reject(
            new Error(
              'mediaSrc video failed: ' +
                (video.error ? video.error.message : 'unknown')
            )
          );
        },
        { once: true }
      );
      video.addEventListener(
        'playing',
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true }
      );
      video.play().catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
  } catch (err) {
    video.removeAttribute('src');
    throw err;
  }
  return video.captureStream();
}

async function connect() {
  const url = $('signal-url').value || 'ws://localhost:8080';
  const peerId = uuidV4();
  $('peer-id').textContent = peerId;

  let uplink = null;
  if (mediaSrcParam) {
    try {
      uplink = await startFileUplink(mediaSrcParam);
      log(`uplink: looping ${mediaSrcParam}`);
    } catch (err) {
      log(`mediaSrc uplink failed (${err.message}); falling back`);
    }
  }
  if (!uplink) {
    try {
      uplink = await navigator.mediaDevices.getUserMedia({
        video: true,
        audio: true
      });
      $('local').srcObject = uplink;
    } catch (err) {
      log(`no local media (${err.name}); joining without an uplink`);
    }
  }

  node = new MeridianNode(peerId, null, readConfigFromPanel());
  wireHandlers();
  installWireLog(node);
  await node.initialize(url, uplink);
  log(`connected as ${peerId} via ${url}`);

  setConnected(true);
  statusTimer = setInterval(() => {
    refreshStatus();
    renderStreams();
  }, 1000);
}

function disconnect() {
  if (!node) return;
  const peerId = node.peerId;
  if (statusTimer) clearInterval(statusTimer);
  statusTimer = null;
  node.uplinkStream?.getTracks().forEach((track) => track.stop());
  node.shutdown();
  node = null;
  for (const video of renderedStreams.values()) video.remove();
  renderedStreams.clear();
  $('local').srcObject = null;
  // A ?mediaSrc= uplink rides the same slot; stop its file playback too.
  $('local').removeAttribute('src');
  $('peer-id').textContent = '(not connected)';
  $('status').textContent = 'idle';
  $('peer-rows').textContent = '';
  setConnected(false);
  log(`disconnected ${peerId}`);
}

async function findClosest() {
  const target = prompt('Target peer id:');
  if (!target || !node) return;
  log(`finding closest node to ${target}...`);
  try {
    const result = await node.findClosestNode(target, 'peer');
    $('status').textContent = JSON.stringify(result);
    log(`closest to ${target}: ${JSON.stringify(result)}`);
  } catch (err) {
    log(`closest-node query failed: ${err.message}`);
  }
}

async function streamToPeer() {
  const target = prompt('Peer id to stream to:');
  if (!target || !node) return;
  try {
    await node.establishMediaStream(target);
    log(`streaming to ${target}`);
    for (let i = 0; i < 15; i++) {
      renderStreams();
      if (renderedStreams.has(target)) break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  } catch (err) {
    log(`streaming to ${target} failed: ${err.message}`);
  }
}

$('connect').addEventListener('click', () => {
  // One live node at a time: dialing twice must not leak the first node's
  // timers or stack a second status poll. Re-enabled on failure.
  $('connect').disabled = true;
  connect().catch((err) => {
    log(`connect failed: ${err.message}`);
    if (node) disconnect();
    else {
      setConnected(false);
      $('peer-id').textContent = '(not connected)';
    }
  });
});
$('disconnect').addEventListener('click', () =>
  disconnect()
);
$('find').addEventListener('click', () =>
  findClosest().catch((err) => log(`error: ${err.message}`))
);
$('stream').addEventListener('click', () =>
  streamToPeer().catch((err) => log(`error: ${err.message}`))
);

// --- e2e state hook (window.__meridian) -----------------------------------
// Read-only projections over the live node's fields, plus a small driving
// affordance: connect(url) fills the URL input and clicks the real Connect
// button so e2e runs the exact manual flow (ids per index.html). Peer
// fields read as null/empty until a node exists.
window.__meridian = {
  peerId: () => node && node.peerId,
  connect: (url) => {
    $('signal-url').value = url;
    $('connect').click();
  },
  state: () => {
    if (!node) return null;
    return {
      peerId: node.peerId,
      knownPeers: [...node.knownPeers.entries()].map(([id, p]) => ({
        id,
        rtt: p.rtt,
        status: p.status,
        ringIndex: p.ringIndex,
      })),
      rings: node.rings.map((r) => ({
        index: r.index,
        primary: r.primaryMembers.map((m) => m.peerId),
      })),
      isSupernode: node.isSupernode,
      clusterLeader: node.clusterLeader,
      activeStreams: [...node.activeStreams.keys()],
      forwardedStreams: [...node._forwardedStreams],
      wireLog: wireLogEntries,
    };
  },
  /**
   * e2e driving affordance (Task 4): runs the library's real supernode
   * election (electSupernode -> findCentralLeader, spec §5.1) over the
   * current cluster. Candidates include ourselves so every candidate
   * measures the SAME target set (self-measures as 0): the spec §3.7
   * average-RTT metric then ranks candidates fairly and the winner is
   * deterministic. The library's own _maybeElectSupernode gate (knownPeers
   * >= 5, spec §9) stays untouched — small e2e overlays trigger here.
   */
  elect: () => {
    if (!node) throw new Error('not connected');
    return electSupernode(node, [node.peerId, ...node.knownPeers.keys()]);
  },
  /** e2e driving affordance (Task 4): node.closeStream (spec §7.1). */
  closeStream: (peerId) => {
    if (!node) throw new Error('not connected');
    return node.closeStream(peerId);
  },
};