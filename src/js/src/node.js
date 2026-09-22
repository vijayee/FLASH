import { MERIDIAN_CONFIG } from './config.js';
import { MESSAGE_TYPES } from './message-types.js';
import { ConnectionPool } from './connection-pool.js';
import { measureRttOverDataChannel } from './rtt.js';
import { createPeerConnection } from './rtc-utils.js';
import { getRingBounds } from './ring.js';
import { addPeerToRing, optimizeRing, refreshRings, moveMember } from './ring-manager.js';
import { runGossipCycle, handleGossip } from './gossip.js';
import { SignalingClient } from './signaling-client.js';
import {
  findClosestNode,
  findCentralLeader,
  findNodesSatisfyingConstraints,
  routeQuery,
  routeLeaderQuery,
  routeConstraintQuery,
  handleProbeRequest,
  handleProbeRequestAvg,
  handleProbeRequestConstraints,
  handleQueryResult
} from './query-routing.js';
import {
  initRaftState,
  handleAppendEntries,
  handleAppendEntriesResponse,
  handleRequestVote,
  handleRequestVoteResponse,
  electSupernode,
  handleSupernodeElected,
  shutdownRaft
} from './raft.js';
import {
  establishMediaStream,
  handleMediaOffer,
  handleMediaClose,
  closeStream,
  handleForwardedStream
} from './media.js';
import { handleFailureRecovery, pruneStalePeers } from './failures.js';

const CONNECTION_ESTABLISHMENT_TIMEOUT_MS = 10000;
const POOL_CLEANUP_INTERVAL_MS = 60000;
const ELECTION_CHECK_DELAY_MS = 10000;
const ELECTION_MIN_KNOWN_PEERS = 5;

/**
 * Main Meridian overlay node (spec §2.2). Owns the rings, known peers,
 * connection state, and the DataChannel message dispatch. Query routing,
 * supernode/Raft, and media streaming plug in at the documented seams in a
 * later task.
 */
export class MeridianNode {
  constructor(peerId, signalChannel, config = MERIDIAN_CONFIG) {
    this.peerId = peerId;
    this.signalChannel = signalChannel;
    this.config = { ...MERIDIAN_CONFIG, ...config };

    // Optional injection point for tests: an object with
    // createPeerConnection(iceServers) returning a browser-like
    // RTCPeerConnection. Defaults to the global RTCPeerConnection.
    this.rtcFactory = this.config.rtcFactory || null;

    // Rings: array of Ring objects.
    this.rings = [];
    for (let i = 0; i < this.config.ringsPerNode; i++) {
      this.rings.push(this._createRing(i));
    }

    // All known peers (includes ring members + extras).
    this.knownPeers = new Map(); // peerId -> KnownPeer

    // Connection pool for ephemeral probes.
    this.connectionPool = new ConnectionPool(this.config.maxEphemeralConnections);

    // PeerConnections we own, keyed by peerId (ring + in-handshake).
    this._peerConnections = new Map();
    // WebRTC handshakes in flight, keyed by remote peerId.
    this._pendingPeerConnections = new Map(); // peerId -> { pc, pendingCandidates, remoteDescriptionSet }
    // ICE candidates arriving before their connection exists.
    this._earlyCandidates = new Map(); // peerId -> candidate[]

    // DataChannels with the protocol responder already installed; guards
    // double-wiring (addPeerToRing re-runs _setupDataChannelHandlers).
    this._wiredChannels = new WeakSet();

    this._refreshingRings = false;
    this._shuttingDown = false;
    this._initialized = false;

    // Supernode state (Raft arrives in a later task).
    this.isSupernode = false;
    this.supernodeCluster = null;
    this.clusterLeader = null;

    // Streaming state (media.js).
    this.activeStreams = new Map(); // peerId -> MediaStream
    this.uplinkStream = null;
    this.supernodeDc = null;
    // Media PeerConnections we own, keyed by peerId (media.js).
    this._mediaConnections = new Map();
    // Set when SFU forwarding is active (spec §6.2).
    this._mediaForwarding = false;
    // Peers a supernode told us to expect relayed streams from.
    this._forwardedStreams = new Set();

    // Pending operations.
    this.pendingProbes = new Map(); // queryId -> { resolve, reject, timer }
    this.pendingConnections = new Set(); // peerIds being connected to
    // Where to return query_result for queries we forwarded hop-by-hop
    // (query-routing.js), keyed by queryId -> DataChannel.
    this._queryBackRoutes = new Map();

    // Gossip state.
    this.lastGossipTime = 0;
    this.gossipInterval = null;

    // Maintenance timers.
    this.ringReplacementInterval = null;
    this._poolCleanupInterval = null;
    this._electionCheckTimer = null;

    // Signaling bootstrap client.
    this.signalingClient = null;

    // Event handlers.
    this.handlers = {
      onStreamRequest: null,
      onStreamOffer: null,
      onSupernodeElected: null,
      onPeerDisconnected: null
    };

    // Seam: messages whose handler families arrive in the next task
    // (query routing, Raft/supernode, media) surface here.
    this._onUnhandledMessage = null;

    // Optional debug sink for errors we must not swallow silently:
    // (message, ...meta).
    this._debug = null;
  }

  _createRing(index) {
    const bounds = getRingBounds(index, this.config);
    return {
      index,
      innerRadius: bounds.inner,
      outerRadius: bounds.outer,
      primaryMembers: [],
      secondaryMembers: []
    };
  }

  _createPeerConnection() {
    return createPeerConnection(this.config, this.rtcFactory);
  }

  _sendSignaling(message) {
    try {
      this.signalChannel && this.signalChannel.send(JSON.stringify(message));
    } catch {
      // Signaling channel may be closed; the peer will time out.
    }
  }

  // --- Ring management (delegates to ring-manager.js) ---

  async addPeerToRing(peerId, dataChannel, rtt) {
    return addPeerToRing(this, peerId, dataChannel, rtt);
  }

  _optimizeRing(ringIndex) {
    optimizeRing(ringIndex, this.rings, this.config);
  }

  async refreshRings() {
    return refreshRings(this);
  }

  _moveMember(member, fromRingIndex, toRingIndex) {
    moveMember(this, member, fromRingIndex, toRingIndex);
  }

  // --- Gossip (delegates to gossip.js) ---

  async runGossipCycle() {
    return runGossipCycle(this);
  }

  async handleGossip(message) {
    return handleGossip(this, message);
  }

  // --- Query routing (delegates to query-routing.js, spec §3.6-§3.8) ---

  findClosestNode(target, targetType) {
    return findClosestNode(this, target, targetType);
  }

  findCentralLeader(peerIds) {
    return findCentralLeader(this, peerIds);
  }

  findNodesSatisfyingConstraints(constraints) {
    return findNodesSatisfyingConstraints(this, constraints);
  }

  // --- Supernode / Raft (delegates to raft.js, spec §5) ---

  _initRaftState(clusterMembers, options) {
    return initRaftState(this, clusterMembers, options);
  }

  // --- Streaming (delegates to media.js, spec §6) ---

  establishMediaStream(targetPeerId) {
    return establishMediaStream(this, targetPeerId);
  }

  closeStream(peerId) {
    return closeStream(this, peerId);
  }

  // --- Signaling-driven connection handling ---

  handlePeersList(msg) {
    for (const peerId of msg.peers || []) {
      if (peerId === this.peerId) continue;
      if (this.knownPeers.has(peerId) || this.pendingConnections.has(peerId)) {
        continue;
      }
      this.pendingConnections.add(peerId);
      this._establishConnectionToPeer(peerId)
        .catch(() => {})
        .finally(() => {
          this.pendingConnections.delete(peerId);
        });
    }
  }

  /**
   * We are the offerer: create the connection, trickle our ICE candidates,
   * send the offer at once, and integrate the peer once the channel opens.
   */
  _establishConnectionToPeer(peerId) {
    // One live connection per peer: refuse to dial over an existing open
    // or in-flight connection (glare is resolved on the answering side).
    if (
      this._pendingPeerConnections.has(peerId) ||
      this._peerConnections.has(peerId)
    ) {
      return Promise.resolve();
    }

    const pc = this._createPeerConnection();
    const dc = pc.createDataChannel('meridian-' + crypto.randomUUID());
    const entry = { pc, pendingCandidates: [], remoteDescriptionSet: false };
    this._pendingPeerConnections.set(peerId, entry);
    this._peerConnections.set(peerId, pc);

    return new Promise((resolve, reject) => {
      let settled = false;
      const settleReject = (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        // A glare rollback may already have replaced this entry; only tear
        // down while ours is still the tracked connection.
        if (this._pendingPeerConnections.get(peerId) === entry) {
          this._failConnection(peerId, err);
        }
        reject(err);
      };

      const timeout = setTimeout(() => {
        settleReject(new Error('Connection establishment timeout'));
      }, CONNECTION_ESTABLISHMENT_TIMEOUT_MS);

      pc.onconnectionstatechange = () => {
        if (
          pc.connectionState === 'failed' ||
          pc.connectionState === 'closed'
        ) {
          settleReject(new Error('PeerConnection ' + pc.connectionState));
        }
      };

      pc.onicecandidate = (event) => {
        if (!event.candidate) return;
        // Trickle ICE: send every candidate as it is gathered.
        this._sendSignaling({
          type: 'ice_candidate',
          target: peerId,
          senderId: this.peerId,
          candidate: event.candidate
        });
      };

      const integrate = async () => {
        if (settled) return;
        try {
          // The protocol responder must be live before measuring RTT: the
          // remote only answers our PING once our PONG responder exists.
          this._setupDataChannelHandlers(dc, peerId);
          const rtt = await measureRttOverDataChannel(dc);
          await this.addPeerToRing(peerId, dc, rtt);
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          if (this._pendingPeerConnections.get(peerId) === entry) {
            this._pendingPeerConnections.delete(peerId);
          }
          resolve();
        } catch (err) {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          if (this._pendingPeerConnections.get(peerId) === entry) {
            this._failConnection(peerId, err);
          }
          reject(err);
        }
      };
      if (dc.readyState === 'open') {
        integrate();
      } else {
        dc.onopen = integrate;
      }

      pc.createOffer()
        .then((offer) => pc.setLocalDescription(offer))
        .then(() => {
          this._sendSignaling({
            type: 'connect_offer',
            target: peerId,
            senderId: this.peerId,
            sdp: pc.localDescription
          });
        })
        .catch(settleReject);
    });
  }

  /**
   * We received an offer (connect_offer or probe_offer): answer it and SEND
   * the answer immediately (spec-fix: the answer must actually be sent),
   * with ICE candidates trickling separately.
   */
  async _answerOffer(msg, answerType) {
    const peerId = msg.senderId;

    // Glare (every peer dials every peer from the same peers_list, so both
    // sides may offer at once): resolve deterministically. The
    // lexicographically greater peerId is impolite and ignores the incoming
    // offer (its own offer wins); the lesser rolls back its own offer and
    // answers, so exactly one connection survives per pair.
    if (
      this._pendingPeerConnections.has(peerId) ||
      this._peerConnections.has(peerId)
    ) {
      if (this.peerId > peerId) return;
      const stale = this._pendingPeerConnections.get(peerId);
      this._failConnection(peerId);
      if (stale && stale.pendingCandidates.length > 0) {
        // The remote's trickled candidates belong to the ICE session we are
        // now answering; keep them for the replacement pc.
        this._earlyCandidates.set(peerId, stale.pendingCandidates);
      }
    }

    const pc = this._createPeerConnection();
    const entry = { pc, pendingCandidates: [], remoteDescriptionSet: false };
    this._pendingPeerConnections.set(peerId, entry);
    this._peerConnections.set(peerId, pc);

    // A channel that never opens must not leak the pc until shutdown.
    entry.timeout = setTimeout(() => {
      if (this._pendingPeerConnections.get(peerId) === entry) {
        this._handlePeerFailure(peerId);
      }
    }, CONNECTION_ESTABLISHMENT_TIMEOUT_MS);

    pc.onconnectionstatechange = () => {
      if (
        pc.connectionState === 'failed' ||
        pc.connectionState === 'closed'
      ) {
        this._handlePeerFailure(peerId);
      }
    };
    pc.ondatachannel = (event) => {
      this._integrateIncomingDataChannel(peerId, event.channel);
    };
    pc.onicecandidate = (event) => {
      if (!event.candidate) return;
      this._sendSignaling({
        type: 'ice_candidate',
        target: peerId,
        senderId: this.peerId,
        candidate: event.candidate
      });
    };

    try {
      await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
      entry.remoteDescriptionSet = true;
      this._flushPendingCandidates(peerId, entry);
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      const answerMsg = {
        type: answerType,
        target: peerId,
        senderId: this.peerId,
        sdp: pc.localDescription
      };
      // Correlation id echoed for the offerer's probe matching; the
      // signaling server relays payloads verbatim.
      if (msg.probeId) answerMsg.probeId = msg.probeId;
      this._sendSignaling(answerMsg);
    } catch (err) {
      this._failConnection(peerId, err);
      throw err;
    }
  }

  handleIncomingConnection(msg) {
    return this._answerOffer(msg, 'connect_answer');
  }

  handleProbeOffer(msg) {
    return this._answerOffer(msg, 'probe_answer');
  }

  /**
   * We are the offerer of a connect handshake: the answer is applied here,
   * with ICE candidates buffered until the remote description is set.
   */
  handleConnectAnswer(msg) {
    if (
      !msg.sdp ||
      typeof msg.sdp.type !== 'string' ||
      typeof msg.sdp.sdp !== 'string'
    ) {
      // Malformed answer: surface on the debug seam instead of throwing on
      // the dispatch path or swallowing it silently.
      if (this._debug) this._debug('invalid connect_answer sdp', msg.senderId);
      return;
    }
    const entry = this._pendingPeerConnections.get(msg.senderId);
    if (!entry) return;
    entry.pc
      .setRemoteDescription(new RTCSessionDescription(msg.sdp))
      .then(() => {
        entry.remoteDescriptionSet = true;
        this._flushPendingCandidates(msg.senderId, entry);
      })
      .catch((err) => {
        if (this._debug) {
          this._debug('connect_answer rejected', msg.senderId, String(err));
        }
      });
  }

  /**
   * Seam: probes we initiate flow through probePeerViaEphemeralConnection,
   * which installs its own transient answer listener, so nothing is pending
   * here yet. Node-originated probe flows (query routing) hook in here in
   * the next task.
   */
  handleProbeAnswer() {}

  handleIceCandidate(msg) {
    const candidate = msg.candidate;
    if (!candidate) return;

    const entry = this._pendingPeerConnections.get(msg.senderId);
    if (entry) {
      if (entry.remoteDescriptionSet) {
        entry.pc.addIceCandidate(candidate).catch(() => {});
      } else {
        entry.pendingCandidates.push(candidate);
      }
      return;
    }

    // The handshake pc may not exist yet (still being created); buffer only
    // when a connection attempt to this peer is actually in flight.
    if (this.pendingConnections.has(msg.senderId)) {
      const buffered = this._earlyCandidates.get(msg.senderId) || [];
      buffered.push(candidate);
      this._earlyCandidates.set(msg.senderId, buffered);
    }
  }

  _flushPendingCandidates(peerId, entry) {
    const early = this._earlyCandidates.get(peerId);
    if (early) {
      entry.pendingCandidates.push(...early);
      this._earlyCandidates.delete(peerId);
    }
    for (const candidate of entry.pendingCandidates) {
      entry.pc.addIceCandidate(candidate).catch(() => {});
    }
    entry.pendingCandidates = [];
  }

  /**
   * Centralized failure teardown for a connection to a peer: closes every
   * tracked PeerConnection and clears the per-peer map entries.
   */
  _failConnection(peerId, err = null) {
    const pending = this._pendingPeerConnections.get(peerId);
    const mapped = this._peerConnections.get(peerId);
    this._pendingPeerConnections.delete(peerId);
    this._peerConnections.delete(peerId);
    this._earlyCandidates.delete(peerId);
    for (const pc of new Set([pending && pending.pc, mapped])) {
      if (!pc) continue;
      try {
        pc.close();
      } catch {
        // Already closed.
      }
    }
    if (err && this._debug) {
      this._debug('connection failed: ' + peerId, String(err));
    }
  }

  /**
   * Measures the RTT over a freshly opened inbound DataChannel and enrolls
   * the peer into the rings.
   */
  async _integrateIncomingDataChannel(peerId, dataChannel) {
    const entry = this._pendingPeerConnections.get(peerId);
    const integrate = async () => {
      // The answerer-side timeout may already have torn this peer down.
      if (entry && this._pendingPeerConnections.get(peerId) !== entry) return;
      try {
        // The protocol responder must be live before measuring RTT: this
        // peer is in no ring yet but must answer our PING regardless.
        this._setupDataChannelHandlers(dataChannel, peerId);
        const rtt = await measureRttOverDataChannel(dataChannel);
        await this.addPeerToRing(peerId, dataChannel, rtt);
        this._pendingPeerConnections.delete(peerId);
        if (entry && entry.timeout) clearTimeout(entry.timeout);
      } catch (err) {
        if (entry && entry.timeout) clearTimeout(entry.timeout);
        this._failConnection(peerId, err);
        this._handlePeerFailure(peerId);
      }
    };
    if (dataChannel.readyState === 'open') {
      integrate();
    } else {
      dataChannel.onopen = integrate;
    }
  }

  // --- DataChannel protocol (spec §7.2) ---

  _setupDataChannelHandlers(dataChannel, peerId) {
    if (this._wiredChannels.has(dataChannel)) return;
    this._wiredChannels.add(dataChannel);

    dataChannel.addEventListener('message', (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return; // Ignore malformed messages.
      }
      if (!msg || typeof msg.type !== 'string') return;

      // Any traffic proves the peer alive (feeds lastSeen-based pruning).
      const known = this.knownPeers.get(peerId);
      if (known) known.lastSeen = Date.now();

      switch (msg.type) {
        case MESSAGE_TYPES.PING:
          try {
            dataChannel.send(
              JSON.stringify({ type: MESSAGE_TYPES.PONG, id: msg.id, t: msg.t })
            );
          } catch {
            // Channel may have just closed.
          }
          break;

        case MESSAGE_TYPES.PONG:
          // Correlated by the pending measureRttOverDataChannel listener.
          break;

        case MESSAGE_TYPES.GOSSIP:
          this.handleGossip(msg).catch(() => {});
          break;

        case MESSAGE_TYPES.PEER_LEAVING:
          this._handlePeerFailure(msg.senderId);
          break;

        // --- Query routing (spec §3.6-§3.8) ---

        case MESSAGE_TYPES.QUERY_FORWARD: {
          const query = msg.query;
          if (query && typeof query.queryId === 'string') {
            // Back-routing: our result must travel back over the channel
            // this hop arrived on.
            query.requesterDc = dataChannel;
            routeQuery(this, query).catch(() => {});
          }
          break;
        }

        case MESSAGE_TYPES.LEADER_QUERY_FORWARD: {
          const query = msg.query;
          if (query && typeof query.queryId === 'string') {
            query.requesterDc = dataChannel;
            routeLeaderQuery(this, query).catch(() => {});
          }
          break;
        }

        case MESSAGE_TYPES.CONSTRAINT_QUERY_FORWARD: {
          const query = msg.query;
          if (query && typeof query.queryId === 'string') {
            query.requesterDc = dataChannel;
            routeConstraintQuery(this, query).catch(() => {});
          }
          break;
        }

        case MESSAGE_TYPES.PROBE_REQUEST:
          handleProbeRequest(this, msg, dataChannel).catch(() => {});
          break;

        case MESSAGE_TYPES.PROBE_REQUEST_AVG:
          handleProbeRequestAvg(this, msg, dataChannel).catch(() => {});
          break;

        case MESSAGE_TYPES.PROBE_REQUEST_CONSTRAINTS:
          handleProbeRequestConstraints(this, msg, dataChannel).catch(() => {});
          break;

        case MESSAGE_TYPES.QUERY_RESULT:
          // probe_result / probe_result_avg / probe_result_constraints /
          // media_answer are consumed by their own one-shot listeners.
          handleQueryResult(this, msg);
          break;

        // --- Media (spec §6) ---

        case MESSAGE_TYPES.MEDIA_OFFER:
          handleMediaOffer(this, msg, dataChannel).catch(() => {});
          break;

        case MESSAGE_TYPES.FORWARDED_STREAM:
          handleForwardedStream(this, msg);
          break;

        case MESSAGE_TYPES.MEDIA_CLOSE:
          handleMediaClose(this, msg);
          break;

        // --- Supernode / Raft (spec §5) ---

        case MESSAGE_TYPES.SUPERNODE_ELECTED:
          handleSupernodeElected(this, msg);
          break;

        case MESSAGE_TYPES.RAFT_APPEND_ENTRIES:
          handleAppendEntries(this, msg, dataChannel);
          break;

        case MESSAGE_TYPES.RAFT_APPEND_ENTRIES_RESPONSE:
          handleAppendEntriesResponse(this, msg, peerId);
          break;

        case MESSAGE_TYPES.RAFT_REQUEST_VOTE:
          handleRequestVote(this, msg, dataChannel);
          break;

        case MESSAGE_TYPES.RAFT_REQUEST_VOTE_RESPONSE:
          handleRequestVoteResponse(this, msg, peerId);
          break;

        default:
          // Seam for message types an embedder adds at runtime.
          if (this._onUnhandledMessage) {
            this._onUnhandledMessage(msg, dataChannel, peerId);
          }
          break;
      }
    });

    dataChannel.addEventListener('close', () => {
      this._handlePeerFailure(peerId);
    });
  }

  /**
   * Core failure handling (spec §8): remove the peer from all rings,
   * promote secondaries, update knownPeers. Raft and streaming recovery
   * hooks plug in here in the next task.
   */
  _handlePeerFailure(peerId) {
    if (!peerId || peerId === this.peerId) return;
    // Graceful shutdown tears everything down itself; close events fired
    // during it must not trigger disconnect handling.
    if (this._shuttingDown) return;

    for (const ring of this.rings) {
      ring.primaryMembers = ring.primaryMembers.filter((m) => m.peerId !== peerId);
      ring.secondaryMembers = ring.secondaryMembers.filter((m) => m.peerId !== peerId);
    }

    // Promote secondary candidates to fill primaries.
    for (const ring of this.rings) {
      while (
        ring.primaryMembers.length < this.config.nodesPerRing &&
        ring.secondaryMembers.length > 0
      ) {
        ring.primaryMembers.push(ring.secondaryMembers.shift());
      }
    }

    const known = this.knownPeers.get(peerId);
    if (known) {
      known.status = 'failed';
      known.dataChannel = null;
    }

    const pc = this._peerConnections.get(peerId);
    if (pc) {
      try {
        pc.close();
      } catch {
        // Already closed.
      }
      this._peerConnections.delete(peerId);
    }
    this._pendingPeerConnections.delete(peerId);
    this._earlyCandidates.delete(peerId);

    // Spec §8 recovery layered on the core cleanup above (spec §8): never
    // awaited — the app notification below must not wait on a 30s query.
    handleFailureRecovery(this, peerId).catch(() => {});

    if (this.handlers.onPeerDisconnected) {
      this.handlers.onPeerDisconnected(peerId);
    }
  }

  /**
   * Supernode election check (spec §5.1, §9): runs 10s after bootstrap
   * once the overlay knows enough peers. Central-leader election decides;
   * the winner self-initializes the Raft cluster and announces it.
   */
  _maybeElectSupernode() {
    if (this._shuttingDown || this.isSupernode || this.supernodeCluster) {
      return;
    }
    if (this.knownPeers.size < ELECTION_MIN_KNOWN_PEERS) return;

    const clusterPeers = Array.from(this.knownPeers.keys()).slice(0, 20);
    electSupernode(this, clusterPeers).catch(() => {
      if (this._debug) this._debug('supernode election failed');
    });
  }

  /**
   * Full initialization sequence (spec §9).
   */
  async initialize(signalServerUrl, mediaStream = null) {
    if (this._initialized) {
      throw new Error('MeridianNode.initialize() already called');
    }
    this._initialized = true;
    this.uplinkStream = mediaStream || null;

    // 1. Connect to the signaling server and bootstrap. A
    // constructor-provided signalChannel is reused as-is (the embedder
    // owns bootstrap over it).
    if (!this.signalChannel) {
      const client = new SignalingClient(this.peerId);
      this.signalingClient = client;
      const timeoutMs = this.config.queryTimeoutMs || 15000;
      try {
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => {
            reject(new Error('Signaling bootstrap timeout'));
          }, timeoutMs);
          client
            .connect(signalServerUrl, this)
            .then(() => {
              clearTimeout(timer);
              resolve();
            })
            .catch((err) => {
              clearTimeout(timer);
              reject(err);
            });
        });
      } catch (err) {
        client.close();
        throw err;
      }
      this.signalChannel = client.ws;
    }

    // 2. Start the gossip protocol (each cycle also prunes peers we have
    // not heard from for three gossip periods, spec §8).
    this.gossipInterval = setInterval(() => {
      this.runGossipCycle().catch(() => {});
      pruneStalePeers(this);
    }, this.config.gossipPeriodMs);

    // 3. Start ring maintenance.
    this.ringReplacementInterval = setInterval(() => {
      this.refreshRings().catch(() => {});
    }, this.config.ringReplacementPeriodMs);

    // 4. Start connection pool cleanup.
    this._poolCleanupInterval = setInterval(() => {
      this.connectionPool.cleanup();
    }, POOL_CLEANUP_INTERVAL_MS);

    // 5. After initial ring population, evaluate supernode eligibility.
    this._electionCheckTimer = setTimeout(() => {
      if (this.knownPeers.size >= ELECTION_MIN_KNOWN_PEERS) {
        this._maybeElectSupernode();
      }
    }, ELECTION_CHECK_DELAY_MS);

    return this.peerId;
  }

  _broadcastPeerLeaving() {
    const payload = JSON.stringify({
      type: MESSAGE_TYPES.PEER_LEAVING,
      senderId: this.peerId
    });
    for (const ring of this.rings) {
      for (const member of [...ring.primaryMembers, ...ring.secondaryMembers]) {
        try {
          member.dataChannel.send(payload);
        } catch {
          // Channel may be dead.
        }
      }
    }
  }

  /**
   * Graceful shutdown (spec §9).
   */
  shutdown() {
    // Suppress peer-failure handling while we tear everything down.
    this._shuttingDown = true;

    // Broadcast departure.
    this._broadcastPeerLeaving();

    // Deregister from the signaling server.
    try {
      this.signalingClient && this.signalingClient.send({
        type: 'disconnect',
        peerId: this.peerId
      });
    } catch {
      // Signaling channel may be closed.
    }

    // Close all ring DataChannels.
    for (const ring of this.rings) {
      for (const member of [...ring.primaryMembers, ...ring.secondaryMembers]) {
        try {
          member.dataChannel.close();
        } catch {
          // Already closed.
        }
      }
    }

    // Drain in-flight connection attempts.
    this.pendingConnections.clear();

    // Close pooled and in-flight PeerConnections (the pending map holds
    // handshakes that never completed).
    this.connectionPool.close();
    const pcs = new Set(this._peerConnections.values());
    for (const entry of this._pendingPeerConnections.values()) {
      pcs.add(entry.pc);
    }
    for (const pc of pcs) {
      try {
        pc.close();
      } catch {
        // Already closed.
      }
    }
    this._peerConnections.clear();
    this._pendingPeerConnections.clear();
    this._earlyCandidates.clear();

    // Close media streams.
    for (const stream of this.activeStreams.values()) {
      stream.getTracks().forEach((t) => t.stop());
    }

    // Clear timers (Raft election + heartbeat included).
    if (this.gossipInterval) clearInterval(this.gossipInterval);
    if (this.ringReplacementInterval) clearInterval(this.ringReplacementInterval);
    if (this._poolCleanupInterval) clearInterval(this._poolCleanupInterval);
    if (this._electionCheckTimer) clearTimeout(this._electionCheckTimer);
    shutdownRaft(this);

    // Close media PeerConnections (active stream tracks are stopped above).
    for (const pc of this._mediaConnections.values()) {
      try {
        pc.close();
      } catch {
        // Already closed.
      }
    }
    this._mediaConnections.clear();

    // Drain pending query promises so callers do not hang until timeout.
    for (const pending of this.pendingProbes.values()) {
      clearTimeout(pending.timer);
      try {
        pending.reject(new Error('Node shutting down'));
      } catch {
        // Resolver already settled.
      }
    }
    this.pendingProbes.clear();
    this._queryBackRoutes.clear();

    // Close the signaling channel.
    if (this.signalingClient) {
      this.signalingClient.close();
    }
  }
}