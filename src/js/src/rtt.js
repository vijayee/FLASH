import { MESSAGE_TYPES } from './message-types.js';
import { createPeerConnection } from './rtc-utils.js';
import { MERIDIAN_CONFIG } from './config.js';

const RTT_PROBE_TIMEOUT_MS = 10000;

// Channels with the PING responder already installed (pooled channels are
// reused across probes).
const PROBE_WIRED = new WeakSet();

/**
 * Minimal PING responder for channels we only probe with (the full message
 * dispatch lives on the node's ring channels). Without it the remote's own
 * RTT measure would time out waiting for a PONG.
 */
function installProbeResponder(dataChannel) {
  if (PROBE_WIRED.has(dataChannel)) return;
  PROBE_WIRED.add(dataChannel);
  dataChannel.addEventListener('message', (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }
    if (!msg || msg.type !== MESSAGE_TYPES.PING) return;
    try {
      dataChannel.send(
        JSON.stringify({ type: MESSAGE_TYPES.PONG, id: msg.id, t: msg.t })
      );
    } catch {
      // Channel may have just closed.
    }
  });
}

/**
 * Fast path (spec §3.2): measure RTT over an existing DataChannel using a
 * correlated ping/pong exchange, timed with performance.now().
 */
export function measureRttOverDataChannel(dataChannel) {
  return new Promise((resolve, reject) => {
    if (dataChannel.readyState === 'closed') {
      reject(new Error('DataChannel is closed'));
      return;
    }

    const id = crypto.randomUUID();
    const start = performance.now();
    let timeout;
    let settled = false;

    const cleanup = () => {
      clearTimeout(timeout);
      dataChannel.removeEventListener('message', handler);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };
    const succeed = (rtt) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(rtt);
    };

    function handler(event) {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === MESSAGE_TYPES.PONG && msg.id === id) {
          succeed(performance.now() - start);
        }
      } catch {
        // Ignore malformed messages.
      }
    }

    timeout = setTimeout(() => {
      fail(new Error('RTT probe timeout'));
    }, RTT_PROBE_TIMEOUT_MS);

    dataChannel.addEventListener('message', handler);
    try {
      dataChannel.send(JSON.stringify({ type: MESSAGE_TYPES.PING, id, t: start }));
    } catch (err) {
      fail(err);
    }
  });
}

// Per-peer in-flight guard: concurrent probes to the same peer would
// cross-apply each other's probe_answer.
const ACTIVE_PROBES = new Map(); // peerId -> correlation id

/**
 * Medium path (spec §3.2): probe a peer we have no DataChannel with yet by
 * establishing an ephemeral WebRTC connection. Reuses a pooled connection
 * when available. The signaling exchange is handled by a transient listener
 * on the signaling channel (probe_offer / probe_answer / ice_candidate),
 * which is removed once the probe settles.
 */
export function probePeerViaEphemeralConnection(
  peerId,
  signalChannel,
  config,
  pool,
  selfPeerId
) {
  const pooled = pool.find(peerId);
  if (pooled) {
    pooled.lastUsed = Date.now();
    installProbeResponder(pooled.dc);
    return measureRttOverDataChannel(pooled.dc);
  }

  if (ACTIVE_PROBES.has(peerId)) {
    return Promise.reject(new Error('Probe already in flight for ' + peerId));
  }
  const probeId = crypto.randomUUID();
  ACTIVE_PROBES.set(peerId, probeId);

  const pc = createPeerConnection(config);
  const dc = pc.createDataChannel('probe-' + crypto.randomUUID());
  installProbeResponder(dc);

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;
    let signalHandler;
    const pendingRemoteCandidates = [];

    const cleanup = () => {
      clearTimeout(timeout);
      signalChannel.removeEventListener('message', signalHandler);
      if (ACTIVE_PROBES.get(peerId) === probeId) ACTIVE_PROBES.delete(peerId);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      try {
        pc.close();
      } catch {
        // Already closed.
      }
      reject(err);
    };
    const succeed = (rtt) => {
      if (settled) return;
      settled = true;
      cleanup();
      pool.add({
        targetId: peerId,
        pc,
        dc,
        lastUsed: Date.now(),
        createdAt: Date.now()
      });
      resolve(rtt);
    };

    timeout = setTimeout(() => {
      fail(new Error('Ephemeral probe timeout'));
    }, config.ephemeralProbeTimeoutMs);

    const whenOpen = () => {
      if (settled) return;
      measureRttOverDataChannel(dc).then(succeed).catch(fail);
    };
    if (dc.readyState === 'open') {
      whenOpen();
    } else {
      dc.onopen = whenOpen;
    }

    signalHandler = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      if (msg.target !== selfPeerId || msg.senderId !== peerId) return;

      if (msg.type === 'probe_answer') {
        // Only the SDP we ourselves offered may be applied to this pc. The
        // echoed probeId (when present) must be ours; peers that drop the
        // field fall back to senderId matching above.
        if (msg.probeId && msg.probeId !== probeId) return;
        if (pc.signalingState !== 'have-local-offer') return;
        pc.setRemoteDescription(new RTCSessionDescription(msg.sdp))
          .then(() => {
            for (const candidate of pendingRemoteCandidates.splice(0)) {
              pc.addIceCandidate(candidate).catch(() => {});
            }
          })
          .catch(fail);
      } else if (msg.type === 'ice_candidate' && msg.candidate) {
        if (pc.remoteDescription) {
          pc.addIceCandidate(msg.candidate).catch(() => {});
        } else {
          pendingRemoteCandidates.push(msg.candidate);
        }
      }
    };
    signalChannel.addEventListener('message', signalHandler);

    // Trickle ICE: forward every candidate as it is gathered.
    pc.onicecandidate = (event) => {
      if (!event.candidate) return;
      try {
        signalChannel.send(
          JSON.stringify({
            type: 'ice_candidate',
            target: peerId,
            senderId: selfPeerId,
            candidate: event.candidate
          })
        );
      } catch (err) {
        fail(err);
      }
    };

    pc.createOffer()
      .then((offer) => pc.setLocalDescription(offer))
      .then(() => {
        signalChannel.send(
          JSON.stringify({
            type: 'probe_offer',
            target: peerId,
            senderId: selfPeerId,
            probeId,
            sdp: pc.localDescription
          })
        );
      })
      .catch(fail);
  });
}

/**
 * Slow path (spec §3.2): measure RTT to a web server via Image favicon
 * timing, which works without CORS. A 404 still yields a usable timing.
 */
export function probeHttpTarget(url, timeoutMs = MERIDIAN_CONFIG.ephemeralProbeTimeoutMs) {
  const start = performance.now();

  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      resolve(performance.now() - start);
    };
    const img = new Image();
    img.onload = finish;
    img.onerror = finish;
    setTimeout(finish, timeoutMs);
    img.src = url + '/favicon.ico?t=' + start;
  });
}