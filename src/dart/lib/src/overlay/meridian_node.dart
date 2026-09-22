import 'dart:async';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:logging/logging.dart';

import '../config/meridian_config.dart';
import '../consensus/raft_consensus.dart';
import '../data_channel_handler.dart';
import '../message_types.dart';
import '../models/connection_pool.dart';
import '../models/peer_state.dart';
import '../models/query_types.dart';
import '../models/raft_state.dart';
import '../models/ring.dart';
import '../signaling/signaling_client.dart';
import '../streaming/stream_manager.dart';
import '../utils/crypto_utils.dart';
import '../utils/timer_utils.dart';
import '../webrtc/peer_connection_manager.dart';
import '../webrtc/rtc_utils.dart';
import 'failures.dart';
import 'gossip_protocol.dart';
import 'query_routing.dart';
import 'ring_manager.dart';
import 'rtt_measurement.dart';

/// Main Meridian overlay node (spec §2.2, §5.1). Owns the rings, known
/// peers, connection state and the DataChannel dispatch wiring. Query
/// routing, supernode/Raft and media streaming plug in at the documented
/// seams in a later task.
class MeridianNode {
  static final Logger _logger = Logger('meridian_webrtc.node');

  final String peerId;
  final MeridianConfig config;

  /// Injectable millisecond clock (test seam): RTT ping/pong timestamps read
  /// from here, so tests script deterministic RTTs. Defaults to [nowMs].
  final double Function() clock;

  /// One ring per [MeridianConfig.ringsPerNode], built from
  /// [getRingBounds]: exponentially increasing radii, outermost unbounded.
  late final List<Ring> rings;

  /// All known peers (includes ring members and extras).
  final Map<String, KnownPeer> knownPeers = {};

  /// Pool of established ephemeral probe connections.
  late final ConnectionPool connectionPool;

  /// Queries awaiting their result (correlated by queryId; query
  /// routing owns their timeouts).
  final Map<String, Completer<QueryResult>> pendingQueries = {};

  /// peerIds with a connection attempt in flight.
  final Set<String> pendingConnections = {};

  // Supernode state (managed by consensus/raft_consensus.dart).
  bool isSupernode = false;
  String? clusterLeader;
  SupernodeCluster? supernodeCluster;

  // Streaming state (managed by streaming/stream_manager.dart): active
  // inbound streams and our own media PeerConnections, keyed by peerId.
  rtc.MediaStream? localStream;
  final Map<String, rtc.MediaStream> activeStreams = {};
  final Map<String, rtc.RTCPeerConnection> mediaConnections = {};

  /// True once SFU forwarding is active (spec §6.2).
  bool mediaForwarding = false;

  /// PeerIds a supernode told us to expect relayed streams from.
  final Set<String> forwardedStreams = {};

  /// Where to return query_result for queries we forwarded hop-by-hop
  /// (query routing), keyed by queryId. A live DataChannel cannot survive
  /// JSON round-trips, so this map stays local and is rewritten at every
  /// hop; drained on resolve, LRU-capped at 200 and on shutdown.
  final Map<String, rtc.RTCDataChannel> queryBackRoutes = {};

  /// Live signaling sink; published by [SignalingClient.connect] before
  /// register so bootstrap-triggered offers and ICE reach the server.
  SignalSink? signalChannel;
  SignalingClient? signalingClient;

  late final RingManager ringManager;
  late final GossipProtocol gossipProtocol;
  late final RttMeasurement rttMeasurement;
  late final PeerConnectionManager pcManager;
  late final DataChannelHandler dcHandler;

  /// PeerConnections we own, keyed by remote peerId (ring + in-handshake).
  final Map<String, rtc.RTCPeerConnection> peerConnections = {};

  /// WebRTC handshakes in flight, keyed by remote peerId.
  final Map<String, PendingConnection> pendingPeerConnections = {};

  /// ICE candidates arriving before their connection exists.
  final Map<String, List<rtc.RTCIceCandidate>> earlyCandidates = {};

  /// Timestamp of the last outgoing gossip cycle (ms epoch).
  int lastGossipTime = 0;

  /// Event seams for the embedding application.
  void Function(String supernodeId)? onSupernodeElected;
  void Function(String peerId)? onPeerDisconnected;
  void Function(String peerId, rtc.MediaStream stream)? onRemoteStreamAdded;
  void Function(String peerId)? onRemoteStreamRemoved;

  /// Media seams (media.js): the embedder decides whether to serve a
  /// stream request (it may set [localStream] synchronously) and learns
  /// about supernode-relayed stream offers.
  void Function(String peerId)? onStreamRequest;
  void Function(String sourcePeerId, String? streamId)? onStreamOffer;

  /// State-machine hooks driven by committed Raft log entries
  /// (consensus/raft_consensus.dart).
  void Function(String streamId, Map<String, dynamic> metadata)?
      updateStreamMetadata;
  void Function(Map<String, dynamic> change)? handleTopologyChange;

  /// Test seam: media PeerConnections are created through here when set;
  /// defaults to the shared [PeerConnectionManager.createConnection]
  /// factory.
  Future<rtc.RTCPeerConnection> Function()? mediaConnectionFactory;

  /// Seam for message families that arrive in the next task (query routing,
  /// Raft/supernode, media) and for types an embedder adds at runtime.
  void Function(
    Map<String, dynamic> message,
    rtc.RTCDataChannel channel,
    String peerId,
  )? onUnhandledMessage;

  /// True while [dispose] is tearing the node down (close events fired
  /// during teardown must not read as peer failures).
  bool get shuttingDown => _shuttingDown;

  late final QueryRouting queryRouting;
  late final RaftConsensus raftConsensus;
  late final StreamManager streamManager;
  Timer? _gossipTimer;
  Timer? _ringReplacementTimer;
  Timer? _poolCleanupTimer;
  Timer? _electionTimer;
  bool _shuttingDown = false;
  bool _initialized = false;

  MeridianNode(
      {String? peerId, MeridianConfig? config, double Function()? clock})
      : peerId = peerId ?? uuidV4(),
        config = config ?? const MeridianConfig(),
        clock = clock ?? nowMs,
        connectionPool = ConnectionPool(
            maxSize:
                (config ?? const MeridianConfig()).maxEphemeralConnections) {
    _initRings();
    ringManager = RingManager(this);
    gossipProtocol = GossipProtocol(this);
    rttMeasurement = RttMeasurement(this);
    pcManager = PeerConnectionManager(this);
    dcHandler = DataChannelHandler(this);
    queryRouting = QueryRouting(this);
    raftConsensus = RaftConsensus(this);
    streamManager = StreamManager(this);
  }

  void _initRings() {
    final list = <Ring>[];
    for (var i = 0; i < config.ringsPerNode; i++) {
      final bounds = getRingBounds(i, config);
      list.add(Ring(
        index: i,
        innerRadiusMs: bounds.inner,
        outerRadiusMs: bounds.outer,
      ));
    }
    rings = list;
  }

  /// Full initialization sequence (spec §9): connect + bootstrap via
  /// signaling, then start the gossip, ring-maintenance and pool-cleanup
  /// timers, and schedule the supernode eligibility check.
  Future<void> initialize(String signalingUrl,
      {rtc.MediaStream? mediaStream}) async {
    if (_initialized) {
      throw StateError('MeridianNode.initialize() already called');
    }
    _initialized = true;
    localStream = mediaStream;

    final client = SignalingClient(node: this);
    signalingClient = client;
    try {
      await client.connect(signalingUrl);
    } catch (err) {
      client.close();
      signalingClient = null;
      _logger.severe('signaling bootstrap failed: $err');
      rethrow;
    }

    _gossipTimer = Timer.periodic(config.gossipPeriod, (_) {
      unawaited(gossipProtocol.runGossipCycle().catchError((_) {}));
      // Spec §8: each gossip cycle also prunes peers we have not heard
      // from for three gossip periods.
      pruneStalePeers(this);
    });

    _ringReplacementTimer = Timer.periodic(config.ringReplacementPeriod, (_) {
      unawaited(refreshRings().catchError((_) {}));
    });

    _poolCleanupTimer = Timer.periodic(poolCleanupInterval, (_) {
      connectionPool.cleanup();
    });

    _electionTimer = Timer(electionCheckDelay, () {
      if (knownPeers.length >= electionMinKnownPeers) {
        maybeElectSupernode();
      }
    });
  }

  /// Graceful shutdown (spec §9): broadcast departure, deregister, stop
  /// timers, close channels and connections, drain pending operations and
  /// stop local media.
  Future<void> dispose() async {
    if (_shuttingDown) return;
    // Close events fired during teardown must not trigger peer-failure
    // handling.
    _shuttingDown = true;

    // 1. Broadcast departure and deregister.
    broadcastPeerLeaving();
    sendSignaling({'type': 'disconnect', 'peerId': peerId});

    // 2. Stop timers.
    _gossipTimer?.cancel();
    _ringReplacementTimer?.cancel();
    _poolCleanupTimer?.cancel();
    _electionTimer?.cancel();

    // 3. Close ring DataChannels.
    for (final ring in rings) {
      for (final member in [...ring.primaryMembers, ...ring.secondaryMembers]) {
        try {
          await member.dataChannel?.close();
        } catch (_) {
          // Already closed.
        }
      }
    }

    // 4. Drain in-flight connection attempts.
    pendingConnections.clear();

    // 5. Close pooled and in-flight PeerConnections (the pending map holds
    // handshakes that never completed).
    connectionPool.dispose();
    for (final pc in {
      for (final entry in pendingPeerConnections.values) entry.pc,
      ...peerConnections.values,
    }) {
      try {
        unawaited(pc.close());
      } catch (_) {
        // Already closed.
      }
    }
    peerConnections.clear();
    for (final entry in pendingPeerConnections.values) {
      entry.timeout?.cancel();
    }
    pendingPeerConnections.clear();
    earlyCandidates.clear();

    // 6. Drain pending queries so callers do not hang until timeout.
    for (final completer in pendingQueries.values) {
      if (!completer.isCompleted) {
        completer.completeError(StateError('Node shutting down'));
      }
    }
    pendingQueries.clear();

    // 6b. Shut Raft timers down, drain query/probe back-routes and close
    // every media connection (pending media handshakes included).
    raftConsensus.shutdown();
    queryRouting.shutdown();
    streamManager.dispose();
    queryBackRoutes.clear();
    for (final stream in activeStreams.values) {
      for (final track in stream.getTracks()) {
        try {
          unawaited(track.stop());
        } catch (_) {
          // Track may already be stopped.
        }
      }
    }
    activeStreams.clear();

    // 7. Stop local media tracks.
    if (localStream != null) {
      for (final track in localStream!.getTracks()) {
        try {
          unawaited(track.stop());
        } catch (_) {
          // Track may already be stopped.
        }
      }
    }

    // 8. Close the signaling channel.
    signalingClient?.close();
  }

  /// Bootstraps connections to every peer the server knows that we do not.
  void handlePeersList(Map<String, dynamic> msg) {
    final peers = msg['peers'];
    if (peers is! List) return;
    for (final peerId in peers) {
      if (peerId is! String) continue;
      if (peerId == this.peerId) continue;
      if (knownPeers.containsKey(peerId) ||
          pendingConnections.contains(peerId)) {
        continue;
      }
      pendingConnections.add(peerId);
      unawaited(
        pcManager.establishConnection(peerId).catchError((_) {}).whenComplete(
              () => pendingConnections.remove(peerId),
            ),
      );
    }
  }

  /// Supernode election check (spec §5.1, §9): runs once, 10s after
  /// bootstrap, when enough peers are known. The central-leader query
  /// decides; the winner self-initializes the Raft cluster and announces
  /// it ([RaftConsensus.electSupernode]).
  void maybeElectSupernode() {
    if (_shuttingDown || isSupernode || supernodeCluster != null) return;
    if (knownPeers.length < electionMinKnownPeers) return;

    final clusterPeers = knownPeers.keys.take(20).toList();
    unawaited(
      raftConsensus.electSupernode(clusterPeers).then(
        (_) {},
        onError: (Object error) {
          _logger.warning('supernode election failed: $error');
        },
      ),
    );
  }

  /// Periodic ring maintenance (delegates to [RingManager.refreshRings]).
  Future<void> refreshRings() => ringManager.refreshRings();

  /// Core failure handling (spec §8): removes the peer from all rings,
  /// promotes secondary candidates, updates knownPeers, and tears down every
  /// tracked connection to it. Streaming and Raft recovery hooks plug in here
  /// in the next task.
  void handlePeerFailure(String peerId) {
    if (peerId.isEmpty || peerId == this.peerId) return;
    // Graceful shutdown tears everything down itself; close events fired
    // during it must not trigger disconnect handling.
    if (_shuttingDown) return;

    for (final ring in rings) {
      ring.primaryMembers.removeWhere((m) => m.peerId == peerId);
      ring.secondaryMembers.removeWhere((m) => m.peerId == peerId);
    }

    // Promote secondary candidates to fill primaries.
    for (final ring in rings) {
      while (ring.primaryMembers.length < config.nodesPerRing &&
          ring.secondaryMembers.isNotEmpty) {
        ring.primaryMembers.add(ring.secondaryMembers.removeAt(0));
      }
    }

    final known = knownPeers[peerId];
    if (known != null) {
      known.status = PeerStatus.failed;
      known.dataChannel = null;
    }

    final pc = peerConnections.remove(peerId);
    if (pc != null) {
      try {
        pc.close();
      } catch (_) {
        // Already closed.
      }
    }
    pendingPeerConnections.remove(peerId)?.timeout?.cancel();
    earlyCandidates.remove(peerId);

    // Spec §8 recovery layered on the core cleanup above; never awaited —
    // the app notification below must not wait on a 30s election query.
    unawaited(handleFailureRecovery(this, peerId).catchError((Object _) {}));

    onPeerDisconnected?.call(peerId);
  }

  /// Centralized failure teardown for a connection to a peer: closes every
  /// tracked PeerConnection and clears the per-peer map entries. Used from
  /// every handshake failure path.
  void failConnection(String peerId, [Object? error]) {
    final pending = pendingPeerConnections.remove(peerId);
    final mapped = peerConnections.remove(peerId);
    earlyCandidates.remove(peerId);
    for (final pc in {
      if (pending != null) pending.pc,
      if (mapped != null) mapped,
    }) {
      try {
        pc.close();
      } catch (_) {
        // Already closed.
      }
    }
    pending?.timeout?.cancel();
    if (error != null) {
      _logger.warning('connection failed: $peerId', error);
    }
  }

  /// Routes a trickled ICE candidate to the handshake connection for
  /// [msg['senderId']], applying it once the remote description is set,
  /// buffering it on the connection otherwise, or buffering it while the
  /// connection is still being created.
  void handleIceCandidate(Map<String, dynamic> msg) {
    final candidate = candidateFromMap(msg['candidate']);
    if (candidate == null) return;
    final senderId = msg['senderId'];
    if (senderId is! String) return;

    final entry = pendingPeerConnections[senderId];
    if (entry != null) {
      if (entry.remoteDescriptionSet) {
        entry.pc.addCandidate(candidate).catchError((_) {});
      } else {
        entry.pendingCandidates.add(candidate);
      }
      return;
    }

    // An ephemeral probe to this peer may own the candidates.
    if (rttMeasurement.handleEphemeralIceCandidate(senderId, candidate)) return;

    // The handshake pc may not exist yet (still being created); buffer only
    // when a connection attempt to this peer is actually in flight.
    if (pendingConnections.contains(senderId)) {
      earlyCandidates.putIfAbsent(senderId, () => []).add(candidate);
    }
  }

  /// Applies buffered early candidates once the remote description is set.
  void flushPendingCandidates(String peerId, PendingConnection entry) {
    final early = earlyCandidates.remove(peerId);
    if (early != null) {
      entry.pendingCandidates.addAll(early);
    }
    for (final candidate in entry.pendingCandidates) {
      entry.pc.addCandidate(candidate).catchError((_) {});
    }
    entry.pendingCandidates.clear();
  }

  /// Measures the RTT over a freshly opened inbound DataChannel and enrolls
  /// the peer into the rings. Used on the answering side of connect/probe
  /// offers.
  Future<void> integrateIncomingDataChannel(
      String peerId, rtc.RTCDataChannel dc) async {
    final entry = pendingPeerConnections[peerId];

    Future<void> integrate() async {
      // The answerer-side timeout may already have torn this peer down, or a
      // glare rollback replaced the connection we are integrating.
      if (entry != null && pendingPeerConnections[peerId] != entry) return;
      try {
        // The protocol responder must be live before measuring RTT: this
        // peer is in no ring yet but must answer our PING regardless.
        dcHandler.setupHandlers(dc, peerId);
        final rtt = await rttMeasurement.measureOverDataChannel(dc);
        await ringManager.addPeerToRing(peerId, dc, rtt);
        if (pendingPeerConnections[peerId] == entry) {
          pendingPeerConnections.remove(peerId);
        }
        entry?.timeout?.cancel();
      } catch (err) {
        entry?.timeout?.cancel();
        failConnection(peerId, err);
        handlePeerFailure(peerId);
      }
    }

    if (dc.state == rtc.RTCDataChannelState.RTCDataChannelOpen) {
      unawaited(integrate());
    } else {
      dc.onDataChannelState = (state) {
        if (state == rtc.RTCDataChannelState.RTCDataChannelOpen) {
          unawaited(integrate());
        }
      };
    }
  }

  /// Applies a probe_answer to the in-flight ephemeral probe for its sender.
  void handleProbeAnswer(Map<String, dynamic> msg) {
    rttMeasurement.handleProbeAnswer(msg);
  }

  /// Relays [message] to the signaling server; safe to call when the
  /// channel is down (the peer will time out instead).
  void sendSignaling(Map<String, dynamic> message) {
    try {
      signalChannel?.send(message);
    } catch (_) {
      // Signaling channel may be closed.
    }
  }

  // --- Query routing (delegates to [QueryRouting], spec §3.6-§3.8) ---

  /// Finds the peer closest to [target] (spec §3.6).
  Future<QueryResult> findClosestNode(String target, {String? targetType}) =>
      queryRouting.findClosestNode(target, targetType: targetType);

  /// Finds the peer minimizing average latency to [peerIds] (spec §3.7).
  Future<QueryResult> findCentralLeader(List<String> peerIds) =>
      queryRouting.findCentralLeader(peerIds);

  /// Finds nodes satisfying all [constraints] (spec §3.8).
  Future<QueryResult> findNodesSatisfyingConstraints(
    List<Constraint> constraints,
  ) =>
      queryRouting.findNodesSatisfyingConstraints(constraints);

  // --- Streaming (delegates to [StreamManager], spec §6) ---

  /// Establishes a media stream to a connected peer (spec §6.1).
  Future<rtc.RTCPeerConnection> establishMediaStream(String targetPeerId) =>
      streamManager.establishMediaStream(targetPeerId);

  /// Politely ends our outbound stream to [peerId] (spec §7.1).
  void closeStream(String peerId) => streamManager.closeStream(peerId);

  /// The live ring membership entry for [peerId], if it is enrolled in
  /// any ring (used by query routing to forward along member channels).
  RingMember? ringMember(String peerId) {
    for (final ring in rings) {
      for (final member in ring.primaryMembers) {
        if (member.peerId == peerId) return member;
      }
      for (final member in ring.secondaryMembers) {
        if (member.peerId == peerId) return member;
      }
    }
    return null;
  }

  /// Broadcasts a graceful-departure notice over every ring DataChannel.
  void broadcastPeerLeaving() {
    for (final ring in rings) {
      for (final member in [...ring.primaryMembers, ...ring.secondaryMembers]) {
        final dc = member.dataChannel;
        if (dc == null) continue;
        sendChannelMessage(dc, {
          'type': MeridianMessageTypes.peerLeaving,
          'senderId': peerId,
        });
      }
    }
  }
}
