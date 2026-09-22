import { MESSAGE_TYPES } from './message-types.js';
import { measureRttOverDataChannel } from './rtt.js';

function pickRandom(list) {
  return list[Math.floor(Math.random() * list.length)];
}

/**
 * Anti-entropy push gossip (spec §3.5): for each ring, send one random peer
 * sample per ring to one random primary member of that ring.
 */
export async function runGossipCycle(node) {
  const now = Date.now();

  for (const ring of node.rings) {
    if (ring.primaryMembers.length === 0) continue;

    const target = pickRandom(ring.primaryMembers);

    const ringSamples = {};
    for (const r of node.rings) {
      if (r.primaryMembers.length > 0) {
        ringSamples[r.index] = pickRandom(r.primaryMembers).peerId;
      }
    }

    try {
      target.dataChannel.send(
        JSON.stringify({
          type: MESSAGE_TYPES.GOSSIP,
          senderId: node.peerId,
          timestamp: now,
          ringSamples
        })
      );
    } catch {
      // DataChannel may be dead.
      node._handlePeerFailure(target.peerId);
    }
  }

  node.lastGossipTime = now;
}

/**
 * Handles an incoming gossip message: re-measures the RTT to the sender and
 * initiates connections to peers we do not yet know.
 */
export async function handleGossip(node, message) {
  const senderId = message.senderId;
  if (senderId === node.peerId) return;

  const sender = node.knownPeers.get(senderId);
  if (sender && sender.dataChannel) {
    try {
      const rtt = await measureRttOverDataChannel(sender.dataChannel);
      sender.rtt = rtt;
      sender.lastSeen = Date.now();
    } catch {
      // Handled by ring refresh.
    }
  }

  for (const peerId of Object.values(message.ringSamples || {})) {
    if (peerId === node.peerId) continue;

    const known = node.knownPeers.get(peerId);
    if (known) {
      known.lastSeen = Date.now();
      // Attempt a reconnect for peers that lost their connection.
      if (known.status !== 'connected' && !node.pendingConnections.has(peerId)) {
        node.pendingConnections.add(peerId);
        node
          ._establishConnectionToPeer(peerId)
          .catch((err) => {
            console.warn('Failed to reconnect to peer:', peerId, err);
          })
          .finally(() => {
            node.pendingConnections.delete(peerId);
          });
      }
      continue;
    }

    if (node.pendingConnections.has(peerId)) continue;

    node.pendingConnections.add(peerId);
    node
      ._establishConnectionToPeer(peerId)
      .catch((err) => {
        console.warn('Failed to connect to discovered peer:', peerId, err);
      })
      .finally(() => {
        node.pendingConnections.delete(peerId);
      });
  }
}