import { MeridianNode, MERIDIAN_CONFIG } from '../../src/index.js';
// Imported directly (not via index.js) only to wrap connect() for the
// optional ?wirelog=1 e2e recording below; no library behavior changes.
import { SignalingClient } from '../../src/signaling-client.js';

const $ = (id) => document.getElementById(id);
const logEl = $('log');
const MAX_LOG_ENTRIES = 200;
let node = null;
let statusTimer = null;
const renderedStreams = new Map(); // peerId -> rendered <video>

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

async function connect() {
  const url = $('signal-url').value || 'ws://localhost:8080';
  const peerId = crypto.randomUUID();
  $('peer-id').textContent = peerId;

  let uplink;
  try {
    uplink = await navigator.mediaDevices.getUserMedia({
      video: true,
      audio: true
    });
    $('local').srcObject = uplink;
  } catch (err) {
    log(`no local media (${err.name}); joining without an uplink`);
  }

  node = new MeridianNode(peerId, null, MERIDIAN_CONFIG);
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
  $('peer-id').textContent = '(not connected)';
  $('status').textContent = 'idle';
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
      wireLog: wireLogEntries,
    };
  },
};