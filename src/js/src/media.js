import { MESSAGE_TYPES } from './message-types.js';
import { createPeerConnection } from './rtc-utils.js';

// Cap on waiting for ICE gathering before sending the SDP anyway
// (non-trickle media handshake: candidates ride inside the SDP over the
// existing DataChannel, since the spec defines no media ICE message).
const ICE_GATHER_TIMEOUT_MS = 3000;

/**
 * Establishes a media stream to a connected peer (spec §6.1): the offer/
 * answer exchange travels over the EXISTING DataChannel; media flows over
 * a dedicated PeerConnection built through the shared rtc-utils factory
 * (stun + turn). Resolves with our own PeerConnection once the answer is
 * applied.
 */
export async function establishMediaStream(node, targetPeerId) {
  const known = node.knownPeers.get(targetPeerId);
  if (!known || !known.dataChannel) {
    throw new Error('Target peer not connected');
  }
  const channel = known.dataChannel;

  // Re-establishment replaces any previous media connection to this peer.
  const previous = node._mediaConnections.get(targetPeerId);
  if (previous) {
    try {
      previous.close();
    } catch {
      // Already closed.
    }
    node._mediaConnections.delete(targetPeerId);
  }

  const pc = createPeerConnection(node.config, node.rtcFactory);
  node._mediaConnections.set(targetPeerId, pc);
  wireMediaPeer(node, targetPeerId, pc);

  addUplinkTracks(node, pc);

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await iceGatheringComplete(node, pc);

  channel.send(
    JSON.stringify({
      type: MESSAGE_TYPES.MEDIA_OFFER,
      senderId: node.peerId,
      sdp: pc.localDescription
    })
  );

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;

    const cleanup = () => {
      clearTimeout(timeout);
      channel.removeEventListener('message', handler);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      closeMediaTo(node, targetPeerId);
      reject(err);
    };
    const succeed = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(pc);
    };

    const handler = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      if (
        msg.type === MESSAGE_TYPES.MEDIA_ANSWER &&
        msg.senderId === targetPeerId
      ) {
        pc.setRemoteDescription(new RTCSessionDescription(msg.sdp))
          .then(succeed)
          .catch(fail);
      }
    };

    channel.addEventListener('message', handler);
    timeout = setTimeout(() => {
      fail(new Error('media_answer timeout'));
    }, node.config.queryTimeoutMs);
  });
}

/**
 * Answers an incoming media offer (spec §6.1). When we have no uplink of
 * our own, the onStreamRequest handler fires first so the embedder can
 * decide (it may synchronously set node.uplinkStream to serve the stream).
 */
export async function handleMediaOffer(node, msg, dataChannel) {
  if (!msg || typeof msg.senderId !== 'string' || !msg.sdp) return;
  const peerId = msg.senderId;

  if (!node.uplinkStream && node.handlers.onStreamRequest) {
    // Consumer decides whether to serve this stream request.
    try {
      node.handlers.onStreamRequest(peerId);
    } catch {
      // An application handler error must not break the answer.
    }
  }

  const pc = createPeerConnection(node.config, node.rtcFactory);
  node._mediaConnections.set(peerId, pc);
  wireMediaPeer(node, peerId, pc);

  addUplinkTracks(node, pc);

  await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  await iceGatheringComplete(node, pc);

  try {
    dataChannel.send(
      JSON.stringify({
        type: MESSAGE_TYPES.MEDIA_ANSWER,
        senderId: node.peerId,
        sdp: pc.localDescription
      })
    );
  } catch {
    // Channel may have just closed; the offerer times out.
  }
}

/**
 * Closes our inbound stream from a peer (spec §7.1 media_close).
 */
export function handleMediaClose(node, msg) {
  if (!msg || typeof msg.senderId !== 'string') return;
  closeMediaTo(node, msg.senderId);
}

/**
 * Politely ends our outbound stream to a peer and tears the media
 * connection down on both sides.
 */
export function closeStream(node, peerId) {
  const known = node.knownPeers.get(peerId);
  if (known && known.dataChannel) {
    try {
      known.dataChannel.send(
        JSON.stringify({
          type: MESSAGE_TYPES.MEDIA_CLOSE,
          senderId: node.peerId
        })
      );
    } catch {
      // Channel may be dead; we still tear our side down.
    }
  }
  closeMediaTo(node, peerId);
}

/**
 * Stops a peer's stream locally and closes the media PeerConnection we
 * hold for it.
 */
export function closeMediaTo(node, peerId) {
  const stream = node.activeStreams.get(peerId);
  if (stream) {
    try {
      stream.getTracks().forEach((track) => track.stop());
    } catch {
      // Tracks may already be stopped.
    }
    node.activeStreams.delete(peerId);
  }
  const pc = node._mediaConnections.get(peerId);
  if (pc) {
    try {
      pc.close();
    } catch {
      // Already closed.
    }
    node._mediaConnections.delete(peerId);
  }
}

/**
 * Activates SFU mode (spec §6.2): every stream we receive as a supernode
 * is signalled to the other cluster members for forwarding.
 */
export function setupMediaForwarding(node) {
  if (!node.isSupernode) return;
  node._mediaForwarding = true;
}

/**
 * Supernode SFU (spec §6.2): announce a freshly received remote stream to
 * the other cluster members so they expect the forwarded media.
 */
export function forwardStreamToCluster(node, sourcePeerId, stream) {
  if (!node.isSupernode || !node._mediaForwarding) return;
  const cluster = node.supernodeCluster;
  if (!cluster) return;

  for (const member of cluster.members) {
    if (member === sourcePeerId || member === node.peerId) continue;
    const known = node.knownPeers.get(member);
    if (!known || !known.dataChannel) continue;
    try {
      known.dataChannel.send(
        JSON.stringify({
          type: MESSAGE_TYPES.FORWARDED_STREAM,
          sourcePeerId,
          streamId: stream ? stream.id : null
        })
      );
    } catch {
      // Channel may be dead.
    }
  }
}

/**
 * Handles a supernode's `forwarded_stream` signal: a stream originated by
 * sourcePeerId will arrive relayed through the cluster leader.
 */
export function handleForwardedStream(node, msg) {
  if (!msg || typeof msg.sourcePeerId !== 'string') return;
  node._forwardedStreams.add(msg.sourcePeerId);
  if (node.handlers.onStreamOffer) {
    try {
      node.handlers.onStreamOffer(msg.sourcePeerId, msg.streamId);
    } catch {
      // Application handler errors must not break the dispatch.
    }
  }
}

function wireMediaPeer(node, peerId, pc) {
  pc.ontrack = (event) => {
    const stream = event.streams && event.streams[0];
    if (!stream) return;
    node.activeStreams.set(peerId, stream);
    forwardStreamToCluster(node, peerId, stream);
  };
  pc.onconnectionstatechange = () => {
    if (
      !node._shuttingDown &&
      (pc.connectionState === 'failed' || pc.connectionState === 'closed')
    ) {
      closeMediaTo(node, peerId);
    }
  };
}

function addUplinkTracks(node, pc) {
  if (!node.uplinkStream) return;
  for (const track of node.uplinkStream.getTracks()) {
    pc.addTrack(track, node.uplinkStream);
  }
}

function iceGatheringComplete(node, pc) {
  if (pc.iceGatheringState === 'complete') {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let timeout;
    const done = () => {
      clearTimeout(timeout);
      pc.removeEventListener('icegatheringstatechange', listener);
      resolve();
    };
    const listener = () => {
      if (pc.iceGatheringState === 'complete') done();
    };
    // Send whatever candidates we have by the cap; a slow TURN path is
    // better than a handshake that never completes.
    timeout = setTimeout(done, ICE_GATHER_TIMEOUT_MS);
    pc.addEventListener('icegatheringstatechange', listener);
  });
}