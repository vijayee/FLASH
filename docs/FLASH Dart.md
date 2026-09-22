# Meridian-WebRTC: Flutter/Dart Implementation Prompt

Below is a complete implementation prompt for Claude Code (or any AI coding assistant) to build the entire Meridian-WebRTC system in Flutter/Dart. It's structured as a single, comprehensive specification that can be handed to an AI code generator.

---

## PROMPT START

Implement a complete browser-based P2P video/audio streaming system in Flutter/Dart using the `flutter_webrtc` package. The system uses **Meridian's overlay methodology** — multi-resolution rings, gossip-based discovery, query routing via direct measurements — adapted entirely to **WebRTC primitives** (DataChannels for control, MediaTracks for media). No raw UDP sockets, no native code, no plugins beyond `flutter_webrtc`.

The system must run on **all platforms** Flutter supports: Android, iOS, Web, macOS, Windows, Linux — from a single codebase.

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                    Application Layer                          │
│  Video/audio streaming UI, peer management, room management  │
├──────────────────────────────────────────────────────────────┤
│                    Meridian Overlay Layer                     │
│  Multi-resolution rings, gossip protocol, query routing      │
├──────────────────────────────────────────────────────────────┤
│                    Consensus Layer (Supernodes only)          │
│  Raft for control plane state replication                    │
├──────────────────────────────────────────────────────────────┤
│                    WebRTC Transport Layer                     │
│  RTCPeerConnection, DataChannel, MediaTrack, ICE/STUN/TURN  │
├──────────────────────────────────────────────────────────────┤
│                    Signaling Layer                            │
│  Minimal: ICE handshake relay + bootstrap peer discovery     │
└──────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

- **All communication over WebRTC**: DataChannels for control plane, MediaTracks for streaming
- **Every node is equal**: Any Flutter instance (mobile/desktop/web) can be a supernode via Meridian's central leader election
- **No raw sockets**: Probing uses DataChannel ping-pong (existing connections) and ephemeral WebRTC (new connections)
- **Minimal signaling server**: Only for ICE handshake relay and initial bootstrap
- **Raft for supernode control plane**: Consistent state across supernode clusters
- **Single codebase**: All platforms share 100% of Meridian overlay logic; only WebRTC transport uses platform-specific plugin

---

## 2. Dependencies (pubspec.yaml)

```yaml
name: meridian_webrtc
description: P2P video/audio streaming using Meridian overlay over WebRTC
version: 0.1.0

environment:
  sdk: '>=3.0.0 <4.0.0'

dependencies:
  flutter:
    sdk: flutter
  flutter_webrtc: ^0.12.0
  web_socket_channel: ^3.0.0
  uuid: ^4.0.0
  collection: ^1.18.0
  logging: ^1.2.0
  provider: ^6.1.0  # For state management

dev_dependencies:
  flutter_test:
    sdk: flutter
  flutter_lints: ^4.0.0
```

---

## 3. File Structure

```plaintext
lib/
├── main.dart                          # App entry point
├── config/
│   └── meridian_config.dart           # System-wide constants
├── models/
│   ├── peer_state.dart                # PeerState, KnownPeer, RingMember
│   ├── ring.dart                      # Ring data structure
│   ├── connection_pool.dart           # Ephemeral connection pool
│   ├── query_types.dart               # Query, QueryResult, Constraint
│   └── raft_state.dart                # Raft state structures
├── overlay/
│   ├── meridian_node.dart             # Main MeridianNode class
│   ├── ring_manager.dart              # Ring operations, hypervolume optimization
│   ├── gossip_protocol.dart           # Gossip cycle and handlers
│   ├── query_routing.dart             # Closest node, leader election, multi-constraint
│   └── rtt_measurement.dart           # RTT measurement over DC and ephemeral connections
├── webrtc/
│   ├── peer_connection_manager.dart   # RTCPeerConnection lifecycle
│   ├── data_channel_handler.dart      # Message protocol dispatch
│   ├── media_manager.dart             # Media stream management
│   └── ice_handler.dart               # ICE/STUN/TURN configuration
├── consensus/
│   └── raft_consensus.dart            # Raft implementation
├── signaling/
│   ├── signaling_client.dart          # WebSocket signaling client
│   └── signaling_server.dart          # Minimal signaling server (for testing)
├── streaming/
│   ├── stream_manager.dart            # Media stream establishment
│   └── sfu_forwarder.dart             # Supernode media forwarding
├── ui/
│   ├── home_screen.dart               # Main screen
│   ├── video_grid.dart                # Grid of remote video streams
│   ├── peer_list.dart                 # List of discovered peers
│   └── controls.dart                  # Stream controls (mute, disconnect)
└── utils/
    ├── crypto_utils.dart              # UUID generation, hashing
    └── timer_utils.dart               # Timer management
```

---

## 4. Data Structures (models/)

### 4.1 meridian_config.dart

```dart
class MeridianConfig {
  final int ringsPerNode;               // 9
  final int nodesPerRing;               // 8
  final int secondaryCandidates;        // 4
  final double innermostRingRadiusMs;   // 1.0
  final double ringMultiplicativeFactor; // 2.0
  final double routeAcceptanceThreshold; // 0.5 (β)
  final double probeTimeoutFactor;      // 2.0 (ε)
  final Duration gossipPeriod;          // 30 seconds
  final Duration ringReplacementPeriod; // 60 seconds
  final int maxEphemeralConnections;    // 10
  final List<String> stunServers;
  final List<TurnServerConfig> turnServers;
  final int maxHops;                    // 32
  final Duration ephemeralProbeTimeout; // 5 seconds
  final Duration queryTimeout;          // 30 seconds

  const MeridianConfig({
    this.ringsPerNode = 9,
    this.nodesPerRing = 8,
    this.secondaryCandidates = 4,
    this.innermostRingRadiusMs = 1.0,
    this.ringMultiplicativeFactor = 2.0,
    this.routeAcceptanceThreshold = 0.5,
    this.probeTimeoutFactor = 2.0,
    this.gossipPeriod = Duration(seconds: 30),
    this.ringReplacementPeriod = Duration(seconds: 60),
    this.maxEphemeralConnections = 10,
    this.stunServers = const ['stun:stun.l.google.com:19302'],
    this.turnServers = const [],
    this.maxHops = 32,
    this.ephemeralProbeTimeout = Duration(seconds: 5),
    this.queryTimeout = Duration(seconds: 30),
  });
}

class TurnServerConfig {
  final String url;
  final String username;
  final String credential;

  const TurnServerConfig({
    required this.url,
    required this.username,
    required this.credential,
  });
}
```

### 4.2 ring.dart

```dart
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

class Ring {
  final int index;
  final double innerRadiusMs;
  final double outerRadiusMs;
  final List<RingMember> primaryMembers;
  final List<RingMember> secondaryMembers;

  Ring({
    required this.index,
    required this.innerRadiusMs,
    required this.outerRadiusMs,
    List<RingMember>? primaryMembers,
    List<RingMember>? secondaryMembers,
  })  : primaryMembers = primaryMembers ?? [],
        secondaryMembers = secondaryMembers ?? [];
}

class RingMember {
  final String peerId;
  rtc.RTCDataChannel? dataChannel;
  double rttMs;
  DateTime lastProbed;
  String iceCandidateType; // 'host' | 'srflx' | 'relay' | 'prflx'
  String natType;          // 'same_subnet' | 'cone_nat' | 'symmetric_nat' | ...
  bool isSupernode;
  bool isFirewalled;
  DateTime joinedAt;

  RingMember({
    required this.peerId,
    this.dataChannel,
    required this.rttMs,
    DateTime? lastProbed,
    this.iceCandidateType = 'host',
    this.natType = 'unknown',
    this.isSupernode = false,
    this.isFirewalled = false,
    DateTime? joinedAt,
  })  : lastProbed = lastProbed ?? DateTime.now(),
        joinedAt = joinedAt ?? DateTime.now();
}
```

### 4.3 peer_state.dart

```dart
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

enum PeerStatus { discovered, connecting, connected, failed }

class KnownPeer {
  final String peerId;
  rtc.RTCDataChannel? dataChannel;
  double? rttMs;
  DateTime lastSeen;
  bool isSupernode;
  int? ringIndex;
  PeerStatus status;

  KnownPeer({
    required this.peerId,
    this.dataChannel,
    this.rttMs,
    DateTime? lastSeen,
    this.isSupernode = false,
    this.ringIndex,
    this.status = PeerStatus.discovered,
  }) : lastSeen = lastSeen ?? DateTime.now();
}
```

### 4.4 connection_pool.dart

```dart
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

class ConnectionPoolEntry {
  final String targetId;
  final rtc.RTCPeerConnection pc;
  final rtc.RTCDataChannel dc;
  DateTime lastUsed;
  final DateTime createdAt;

  ConnectionPoolEntry({
    required this.targetId,
    required this.pc,
    required this.dc,
    DateTime? lastUsed,
    DateTime? createdAt,
  })  : lastUsed = lastUsed ?? DateTime.now(),
        createdAt = createdAt ?? DateTime.now();
}

class ConnectionPool {
  final List<ConnectionPoolEntry> entries;
  final int maxSize;

  ConnectionPool({this.maxSize = 10}) : entries = [];

  ConnectionPoolEntry? find(String targetId) {
    try {
      return entries.firstWhere((e) => e.targetId == targetId);
    } catch (_) {
      return null;
    }
  }

  void add(ConnectionPoolEntry entry) {
    if (entries.length >= maxSize) {
      // Evict oldest
      entries.sort((a, b) => a.lastUsed.compareTo(b.lastUsed));
      final oldest = entries.removeAt(0);
      oldest.pc.close();
    }
    entries.add(entry);
  }

  void cleanup({Duration maxAge = const Duration(minutes: 2)}) {
    final now = DateTime.now();
    entries.removeWhere((entry) {
      if (now.difference(entry.lastUsed) > maxAge) {
        entry.pc.close();
        return true;
      }
      return false;
    });
  }

  void dispose() {
    for (final entry in entries) {
      entry.pc.close();
    }
    entries.clear();
  }
}
```

### 4.5 query_types.dart

```dart
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

enum QueryType { closestNode, leaderElection, multiConstraint }

class Query {
  final String queryId;
  final QueryType type;
  final String? target;
  final String? targetType; // 'peer' | 'http' | 'https'
  final List<String>? targets; // For leader election
  final List<Constraint>? constraints; // For multi-constraint
  int hopCount;
  final String originator;
  rtc.RTCDataChannel? requesterDc;
  final DateTime timestamp;

  Query({
    required this.queryId,
    required this.type,
    this.target,
    this.targetType,
    this.targets,
    this.constraints,
    this.hopCount = 0,
    required this.originator,
    this.requesterDc,
    DateTime? timestamp,
  }) : timestamp = timestamp ?? DateTime.now();
}

class Constraint {
  final String target;
  final double maxLatencyMs;

  const Constraint({required this.target, required this.maxLatencyMs});
}

class QueryResult {
  final String queryId;
  final String? closestPeerId;
  final double? closestRttMs;
  final String? leaderId;
  final double? avgRttMs;
  final List<String>? satisfyingPeers;
  final int hopCount;
  final String? error;

  QueryResult({
    required this.queryId,
    this.closestPeerId,
    this.closestRttMs,
    this.leaderId,
    this.avgRttMs,
    this.satisfyingPeers,
    this.hopCount = 0,
    this.error,
  });
}
```

### 4.6 raft_state.dart

```dart
enum RaftState { follower, candidate, leader }

class RaftLogEntry {
  final int term;
  final int index;
  final Map<String, dynamic> command;

  RaftLogEntry({
    required this.term,
    required this.index,
    required this.command,
  });
}

class SupernodeCluster {
  final String clusterId;
  List<String> members;
  String? leaderId;
  
  // Persistent state
  int currentTerm;
  String? votedFor;
  List<RaftLogEntry> log;
  
  // Volatile state
  int commitIndex;
  int lastApplied;
  
  // Leader state
  final Map<String, int> nextIndex;
  final Map<String, int> matchIndex;
  
  // Timers
  Duration electionTimeout;
  Duration heartbeatInterval;
  
  // Internal
  RaftState raftState;

  SupernodeCluster({
    required this.clusterId,
    required this.members,
    this.leaderId,
    this.currentTerm = 0,
    this.votedFor,
    List<RaftLogEntry>? log,
    this.commitIndex = 0,
    this.lastApplied = 0,
    this.raftState = RaftState.follower,
    Duration? electionTimeout,
    this.heartbeatInterval = const Duration(milliseconds: 50),
  })  : log = log ?? [],
        nextIndex = {},
        matchIndex = {},
        electionTimeout = electionTimeout ?? 
            Duration(milliseconds: 150 + (DateTime.now().millisecondsSinceEpoch % 150));
}
```

---

## 5. Core Algorithms (overlay/)

### 5.1 meridian_node.dart — Main Node Class

```dart
import 'dart:async';
import 'dart:math';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:uuid/uuid.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import '../config/meridian_config.dart';
import '../models/peer_state.dart';
import '../models/ring.dart';
import '../models/connection_pool.dart';
import '../models/query_types.dart';
import '../models/raft_state.dart';
import 'ring_manager.dart';
import 'gossip_protocol.dart';
import 'query_routing.dart';
import 'rtt_measurement.dart';
import '../webrtc/peer_connection_manager.dart';
import '../webrtc/data_channel_handler.dart';
import '../webrtc/media_manager.dart';
import '../consensus/raft_consensus.dart';
import '../signaling/signaling_client.dart';

class MeridianNode {
  final String peerId;
  final MeridianConfig config;
  final Uuid _uuid = const Uuid();
  
  // Core state
  late final List<Ring> rings;
  final Map<String, KnownPeer> knownPeers = {};
  late final ConnectionPool connectionPool;
  
  // Signaling
  SignalingClient? _signalClient;
  
  // Subsystems
  late final RingManager ringManager;
  late final GossipProtocol gossipProtocol;
  late final QueryRouting queryRouting;
  late final RttMeasurement rttMeasurement;
  late final PeerConnectionManager pcManager;
  late final DataChannelHandler dcHandler;
  late final MediaManager mediaManager;
  late final RaftConsensus? raftConsensus;
  
  // Supernode state
  bool isSupernode = false;
  String? clusterLeader;
  SupernodeCluster? supernodeCluster;
  
  // Media state
  rtc.MediaStream? localStream;
  final Map<String, rtc.MediaStream> remoteStreams = {};
  
  // Pending operations
  final Map<String, Completer<QueryResult>> pendingQueries = {};
  final Set<String> pendingConnections = {};
  
  // Timers
  Timer? _gossipTimer;
  Timer? _ringReplacementTimer;
  Timer? _poolCleanupTimer;
  
  // Event callbacks
  void Function(String supernodeId)? onSupernodeElected;
  void Function(String peerId)? onPeerDisconnected;
  void Function(String peerId, rtc.MediaStream stream)? onRemoteStreamAdded;
  void Function(String peerId)? onRemoteStreamRemoved;
  
  MeridianNode({
    String? peerId,
    MeridianConfig? config,
  })  : peerId = peerId ?? const Uuid().v4(),
        config = config ?? const MeridianConfig(),
        connectionPool = ConnectionPool(maxSize: config?.maxEphemeralConnections ?? 10) {
    _initRings();
    ringManager = RingManager(this);
    gossipProtocol = GossipProtocol(this);
    queryRouting = QueryRouting(this);
    rttMeasurement = RttMeasurement(this);
    pcManager = PeerConnectionManager(this);
    dcHandler = DataChannelHandler(this);
    mediaManager = MediaManager(this);
    raftConsensus = RaftConsensus(this);
  }
  
  void _initRings() {
    rings = [];
    for (int i = 0; i < config.ringsPerNode; i++) {
      rings.add(Ring(
        index: i,
        innerRadiusMs: i == 0 ? 1.0 : pow(config.ringMultiplicativeFactor, i - 1).toDouble(),
        outerRadiusMs: i < config.ringsPerNode - 1
            ? pow(config.ringMultiplicativeFactor, i).toDouble()
            : double.infinity,
      ));
    }
  }
  
  /// Initialize the node: connect to signaling, start timers
  Future<void> initialize(String signalingUrl, {rtc.MediaStream? mediaStream}) async {
    localStream = mediaStream;
    
    // Connect to signaling server
    _signalClient = SignalingClient(url: signalingUrl, node: this);
    await _signalClient!.connect();
    
    // Start gossip protocol
    _gossipTimer = Timer.periodic(config.gossipPeriod, (_) {
      gossipProtocol.runGossipCycle();
    });
    
    // Start ring maintenance
    _ringReplacementTimer = Timer.periodic(config.ringReplacementPeriod, (_) {
      ringManager.refreshRings();
    });
    
    // Start connection pool cleanup
    _poolCleanupTimer = Timer.periodic(const Duration(minutes: 1), (_) {
      connectionPool.cleanup();
    });
    
    // After initial population, attempt supernode election
    Future.delayed(const Duration(seconds: 10), () {
      if (knownPeers.length >= 5) {
        _attemptSupernodeElection();
      }
    });
  }
  
  /// Graceful shutdown
  Future<void> dispose() async {
    // Broadcast departure
    _broadcastToAll({'type': 'peer_leaving', 'senderId': peerId});
    
    // Cancel timers
    _gossipTimer?.cancel();
    _ringReplacementTimer?.cancel();
    _poolCleanupTimer?.cancel();
    
    // Dispose subsystems
    raftConsensus?.dispose();
    pcManager.dispose();
    connectionPool.dispose();
    mediaManager.dispose();
    
    // Close signaling
    await _signalClient?.disconnect();
    
    // Stop local media
    if (localStream != null) {
      for (final track in localStream!.getTracks()) {
        track.stop();
      }
    }
  }
  
  void _broadcastToAll(Map<String, dynamic> message) {
    for (final ring in rings) {
      for (final member in ring.primaryMembers) {
        try {
          member.dataChannel?.send(rtc.RTCDataChannelMessage(
            const JsonEncoder().convert(message)
          ));
        } catch (_) {}
      }
    }
  }
  
  Future<void> _attemptSupernodeElection() async {
    final clusterPeers = knownPeers.keys.take(20).toList();
    final result = await queryRouting.findCentralLeader(clusterPeers);
    
    if (result.leaderId == peerId) {
      isSupernode = true;
      clusterLeader = peerId;
      raftConsensus!.initState(clusterPeers);
      onSupernodeElected?.call(peerId);
    } else {
      clusterLeader = result.leaderId;
    }
  }
}
```

### 5.2 ring_manager.dart — Ring Operations

```dart
import 'dart:math';
import '../models/ring.dart';
import 'meridian_node.dart';

class RingManager {
  final MeridianNode node;
  
  RingManager(this.node);
  
  /// Calculate which ring a peer belongs to based on RTT
  int calculateRingIndex(double rttMs) {
    if (rttMs <= node.config.innermostRingRadiusMs) return 0;
    
    final index = (log(rttMs / node.config.innermostRingRadiusMs) /
        log(node.config.ringMultiplicativeFactor))
        .ceil();
    
    return min(index, node.config.ringsPerNode - 1);
  }
  
  /// Add a peer to the appropriate ring
  Future<void> addPeerToRing(
    String peerId,
    rtc.RTCDataChannel dataChannel,
    double rttMs,
  ) async {
    final ringIndex = calculateRingIndex(rttMs);
    final ring = node.rings[ringIndex];
    
    final member = RingMember(
      peerId: peerId,
      dataChannel: dataChannel,
      rttMs: rttMs,
    );
    
    if (ring.primaryMembers.length < node.config.nodesPerRing) {
      ring.primaryMembers.add(member);
    } else {
      ring.secondaryMembers.add(member);
      if (ring.secondaryMembers.length > node.config.secondaryCandidates) {
        ring.secondaryMembers.removeAt(0);
      }
      _optimizeRing(ringIndex);
    }
    
    // Store in known peers
    node.knownPeers[peerId] = KnownPeer(
      peerId: peerId,
      dataChannel: dataChannel,
      rttMs: rttMs,
      ringIndex: ringIndex,
      status: PeerStatus.connected,
    );
    
    // Set up DataChannel handlers
    node.dcHandler.setupHandlers(dataChannel, peerId);
  }
  
  /// Hypervolume-based ring optimization
  void _optimizeRing(int ringIndex) {
    final ring = node.rings[ringIndex];
    final allCandidates = [
      ...ring.primaryMembers,
      ...ring.secondaryMembers,
    ];
    
    if (allCandidates.length <= node.config.nodesPerRing) return;
    
    // Build local coordinate space using RTTs
    final coordinates = <String, List<double>>{};
    for (final a in allCandidates) {
      final vec = <double>[];
      for (final b in allCandidates) {
        vec.add(a.peerId == b.peerId ? 0 : b.rttMs);
      }
      coordinates[a.peerId] = vec;
    }
    
    // Greedy selection: iteratively drop the peer whose removal
    // reduces hypervolume the least
    var selected = allCandidates.map((c) => c.peerId).toList();
    
    while (selected.length > node.config.nodesPerRing) {
      String? worstPeer;
      double smallestReduction = double.infinity;
      
      for (final peerId in selected) {
        final without = selected.where((id) => id != peerId).toList();
        final volWith = _computeHypervolume(selected, coordinates);
        final volWithout = _computeHypervolume(without, coordinates);
        final reduction = volWith - volWithout;
        
        if (reduction < smallestReduction) {
          smallestReduction = reduction;
          worstPeer = peerId;
        }
      }
      
      if (worstPeer != null) {
        selected.remove(worstPeer);
      }
    }
    
    // Promote selected to primary, demote others to secondary
    final newPrimary = <RingMember>[];
    final newSecondary = <RingMember>[];
    
    for (final candidate in allCandidates) {
      if (selected.contains(candidate.peerId)) {
        newPrimary.add(candidate);
      } else {
        newSecondary.add(candidate);
      }
    }
    
    ring.primaryMembers
      ..clear()
      ..addAll(newPrimary);
    ring.secondaryMembers
      ..clear()
      ..addAll(newSecondary.take(node.config.secondaryCandidates));
  }
  
  double _computeHypervolume(
    List<String> peerIds,
    Map<String, List<double>> coordinates,
  ) {
    if (peerIds.isEmpty) return 0;
    if (peerIds.length == 1) return 1;
    
    double volume = 1;
    final dims = coordinates[peerIds.first]!.length;
    
    for (int d = 0; d < dims; d++) {
      double minVal = double.infinity;
      double maxVal = double.negativeInfinity;
      
      for (final peerId in peerIds) {
        final val = coordinates[peerId]![d];
        if (val < minVal) minVal = val;
        if (val > maxVal) maxVal = val;
      }
      
      volume *= (maxVal - minVal + 1);
    }
    
    return volume;
  }
  
  /// Refresh all rings: re-measure RTTs, re-evaluate placement
  Future<void> refreshRings() async {
    for (int i = 0; i < node.rings.length; i++) {
      final ring = node.rings[i];
      final membersToCheck = List<RingMember>.from(ring.primaryMembers);
      
      for (final member in membersToCheck) {
        try {
          if (member.dataChannel != null) {
            final newRtt = await node.rttMeasurement.measureOverDataChannel(
              member.dataChannel!
            );
            member.rttMs = newRtt;
            member.lastProbed = DateTime.now();
            
            // Check if peer should move to a different ring
            final correctRing = calculateRingIndex(newRtt);
            if (correctRing != i) {
              _moveMember(member, i, correctRing);
            }
          }
        } catch (e) {
          _handleMemberFailure(member.peerId, i);
        }
      }
      
      _optimizeRing(i);
    }
  }
  
  void _moveMember(RingMember member, int fromIndex, int toIndex) {
    final fromRing = node.rings[fromIndex];
    final toRing = node.rings[toIndex];
    
    fromRing.primaryMembers.removeWhere((m) => m.peerId == member.peerId);
    
    // Promote secondary if available
    if (fromRing.secondaryMembers.isNotEmpty) {
      fromRing.primaryMembers.add(fromRing.secondaryMembers.removeAt(0));
    }
    
    // Add to target ring
    if (toRing.primaryMembers.length < node.config.nodesPerRing) {
      toRing.primaryMembers.add(member);
    } else {
      toRing.secondaryMembers.add(member);
      _optimizeRing(toIndex);
    }
    
    // Update known peer
    final known = node.knownPeers[member.peerId];
    if (known != null) {
      known.ringIndex = toIndex;
    }
  }
  
  void _handleMemberFailure(String peerId, int ringIndex) {
    final ring = node.rings[ringIndex];
    ring.primaryMembers.removeWhere((m) => m.peerId == peerId);
    ring.secondaryMembers.removeWhere((m) => m.peerId == peerId);
    
    // Promote secondary
    while (ring.primaryMembers.length < node.config.nodesPerRing &&
        ring.secondaryMembers.isNotEmpty) {
      ring.primaryMembers.add(ring.secondaryMembers.removeAt(0));
    }
    
    node._handlePeerFailure(peerId);
  }
}
```

### 5.3 rtt_measurement.dart — RTT Measurement

```dart
import 'dart:async';
import 'dart:convert';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import '../models/connection_pool.dart';
import 'meridian_node.dart';

class RttMeasurement {
  final MeridianNode node;
  final Map<String, Completer<double>> _pendingPongs = {};
  
  RttMeasurement(this.node);
  
  /// Fast path: measure RTT over an existing DataChannel
  Future<double> measureOverDataChannel(rtc.RTCDataChannel dc) async {
    final id = node._uuid.v4();
    final completer = Completer<double>();
    final start = DateTime.now().millisecondsSinceEpoch.toDouble();
    
    _pendingPongs[id] = completer;
    
    // Timeout
    Timer(const Duration(seconds: 10), () {
      if (!completer.isCompleted) {
        _pendingPongs.remove(id);
        completer.completeError(Exception('RTT probe timeout'));
      }
    });
    
    dc.send(rtc.RTCDataChannelMessage(
      jsonEncode({'type': 'ping', 'id': id, 't': start})
    ));
    
    return completer.future;
  }
  
  /// Handle incoming ping — send pong
  void handlePing(rtc.RTCDataChannel dc, Map<String, dynamic> msg) {
    dc.send(rtc.RTCDataChannelMessage(jsonEncode({
      'type': 'pong',
      'id': msg['id'],
      't': msg['t'],
    })));
  }
  
  /// Handle incoming pong — resolve pending measurement
  void handlePong(Map<String, dynamic> msg) {
    final id = msg['id'] as String;
    final completer = _pendingPongs.remove(id);
    if (completer != null && !completer.isCompleted) {
      final now = DateTime.now().millisecondsSinceEpoch.toDouble();
      final start = (msg['t'] as num).toDouble();
      completer.complete(now - start);
    }
  }
  
  /// Medium path: probe a peer via ephemeral WebRTC connection
  Future<double> probePeerViaEphemeral(String peerId) async {
    // Check connection pool first
    final poolEntry = node.connectionPool.find(peerId);
    if (poolEntry != null) {
      poolEntry.lastUsed = DateTime.now();
      return measureOverDataChannel(poolEntry.dc);
    }
    
    // Create ephemeral connection
    final pc = await node.pcManager.createEphemeralConnection();
    final dc = await pc.createDataChannel('probe-${node._uuid.v4()}');
    
    final completer = Completer<double>();
    final timeout = Timer(node.config.ephemeralProbeTimeout, () {
      if (!completer.isCompleted) {
        pc.close();
        completer.completeError(Exception('Ephemeral probe timeout'));
      }
    });
    
    dc.onOpen = () async {
      try {
        final rtt = await measureOverDataChannel(dc);
        node.connectionPool.add(ConnectionPoolEntry(
          targetId: peerId,
          pc: pc,
          dc: dc,
        ));
        timeout.cancel();
        if (!completer.isCompleted) completer.complete(rtt);
      } catch (e) {
        pc.close();
        timeout.cancel();
        if (!completer.isCompleted) completer.completeError(e);
      }
    };
    
    // Create offer and send via signaling
    final offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    
    node._signalClient?.send({
      'type': 'probe_offer',
      'target': peerId,
      'senderId': node.peerId,
      'sdp': pc.localDescription?.toMap(),
    });
    
    return completer.future;
  }
  
  /// Slow path: measure RTT to an HTTP target
  Future<double> probeHttpTarget(String url) async {
    // In Flutter, use an HTTP head request or similar
    final stopwatch = Stopwatch()..start();
    try {
      final uri = Uri.parse(url);
      // Use dart:io HttpClient on native, or http package on web
      // For simplicity, we use a basic approach
      final client = HttpClient();
      await client.head(uri.host, uri.port, uri.path);
      client.close();
      return stopwatch.elapsedMilliseconds.toDouble();
    } catch (e) {
      return double.infinity;
    }
  }
  
  /// Measure RTT to an arbitrary target (dispatches to appropriate method)
  Future<double> measureToTarget(String target, String targetType) async {
    switch (targetType) {
      case 'peer':
        final known = node.knownPeers[target];
        if (known?.dataChannel != null) {
          return measureOverDataChannel(known!.dataChannel!);
        }
        return probePeerViaEphemeral(target);
      case 'http':
      case 'https':
        return probeHttpTarget(target);
      default:
        throw Exception('Unknown target type: $targetType');
    }
  }
}
```

### 5.4 gossip_protocol.dart — Gossip Protocol

```dart
import 'dart:convert';
import 'dart:math';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'meridian_node.dart';

class GossipProtocol {
  final MeridianNode node;
  
  GossipProtocol(this.node);
  
  /// Run one gossip cycle: send a random sample from each ring
  /// to one random peer from each ring
  Future<void> runGossipCycle() async {
    for (final ring in node.rings) {
      if (ring.primaryMembers.isEmpty) continue;
      
      // Pick random target from this ring
      final target = ring.primaryMembers[Random().nextInt(ring.primaryMembers.length)];
      
      // Build gossip payload: one random peer from each ring
      final ringSamples = <int, String>{};
      for (final r in node.rings) {
        if (r.primaryMembers.isNotEmpty) {
          final sample = r.primaryMembers[Random().nextInt(r.primaryMembers.length)];
          ringSamples[r.index] = sample.peerId;
        }
      }
      
      try {
        target.dataChannel?.send(rtc.RTCDataChannelMessage(jsonEncode({
          'type': 'gossip',
          'senderId': node.peerId,
          'timestamp': DateTime.now().millisecondsSinceEpoch,
          'ringSamples': ringSamples.map((k, v) => MapEntry(k.toString(), v)),
        })));
      } catch (_) {
        // DataChannel may be dead — will be caught by ring refresh
      }
    }
  }
  
  /// Handle incoming gossip message
  Future<void> handleGossip(Map<String, dynamic> msg) async {
    final senderId = msg['senderId'] as String;
    
    // Measure RTT to sender
    final known = node.knownPeers[senderId];
    if (known?.dataChannel != null) {
      try {
        final rtt = await node.rttMeasurement.measureOverDataChannel(
          known!.dataChannel!
        );
        known.rttMs = rtt;
        known.lastSeen = DateTime.now();
      } catch (_) {}
    }
    
    // Process each peer in the gossip sample
    final ringSamples = msg['ringSamples'] as Map<String, dynamic>;
    for (final entry in ringSamples.entries) {
      final peerId = entry.value as String;
      if (peerId == node.peerId) continue;
      if (node.knownPeers.containsKey(peerId)) {
        node.knownPeers[peerId]!.lastSeen = DateTime.now();
        continue;
      }
      
      // New peer discovered — initiate connection
      if (!node.pendingConnections.contains(peerId)) {
        node.pendingConnections.add(peerId);
        try {
          await node.pcManager.establishConnection(peerId);
        } catch (e) {
          // Connection failed, will be retried on next gossip
        } finally {
          node.pendingConnections.remove(peerId);
        }
      }
    }
  }
}
```

### 5.5 query_routing.dart — Query Routing

```dart
import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import '../models/query_types.dart';
import 'meridian_node.dart';

class QueryRouting {
  final MeridianNode node;
  
  QueryRouting(this.node);
  
  /// Find the closest node to a target
  Future<QueryResult> findClosestNode(String target, {String targetType = 'peer'}) async {
    final queryId = node._uuid.v4();
    final completer = Completer<QueryResult>();
    
    final timeout = Timer(node.config.queryTimeout, () {
      if (!completer.isCompleted) {
        node.pendingQueries.remove(queryId);
        completer.complete(QueryResult(
          queryId: queryId,
          error: 'Query timeout',
        ));
      }
    });
    
    node.pendingQueries[queryId] = completer;
    
    // Start query at this node
    _routeQuery(Query(
      queryId: queryId,
      type: QueryType.closestNode,
      target: target,
      targetType: targetType,
      originator: node.peerId,
    ));
    
    return completer.future;
  }
  
  /// Route a closest-node query one hop
  Future<void> _routeQuery(Query query) async {
    if (query.hopCount > node.config.maxHops) {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        error: 'Max hops exceeded',
      ));
      return;
    }
    
    // Measure this node's RTT to the target
    double myRtt;
    try {
      myRtt = await node.rttMeasurement.measureToTarget(
        query.target!, query.targetType ?? 'peer'
      );
    } catch (e) {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        error: 'Cannot measure target',
      ));
      return;
    }
    
    // Determine which rings to query
    final ringIndex = node.ringManager.calculateRingIndex(myRtt);
    final peersToQuery = <RingMember>[];
    
    final ringsToCheck = [ringIndex];
    if (ringIndex > 0) ringsToCheck.add(ringIndex - 1);
    if (ringIndex < node.config.ringsPerNode - 1) ringsToCheck.add(ringIndex + 1);
    
    for (final ri in ringsToCheck) {
      for (final member in node.rings[ri].primaryMembers) {
        if (member.rttMs >= myRtt / 2 && member.rttMs <= myRtt * 2) {
          peersToQuery.add(member);
        }
      }
    }
    
    if (peersToQuery.isEmpty) {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        closestPeerId: node.peerId,
        closestRttMs: myRtt,
        hopCount: query.hopCount,
      ));
      return;
    }
    
    // Ask peers to measure RTT to target (in parallel)
    final results = await Future.wait(
      peersToQuery.map((m) => _askPeerToProbe(m, query, myRtt)),
      eagerError: false,
    );
    
    // Find closest
    String? closestPeerId;
    double closestRtt = myRtt;
    rtc.RTCDataChannel? closestDc;
    
    for (final result in results) {
      if (result != null && result.rttMs < closestRtt) {
        closestRtt = result.rttMs;
        closestPeerId = result.peerId;
        closestDc = result.dataChannel;
      }
    }
    
    // Check acceptance threshold (β)
    if (closestPeerId != null && closestRtt < myRtt * node.config.routeAcceptanceThreshold) {
      query.hopCount++;
      query.requesterDc = null; // Will be set by the forwarding peer
      closestDc?.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'query_forward',
        'query': _queryToMap(query),
      })));
    } else {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        closestPeerId: closestPeerId ?? node.peerId,
        closestRttMs: closestRtt,
        hopCount: query.hopCount,
      ));
    }
  }
  
  /// Ask a ring member to probe the target
  Future<_ProbeResult?> _askPeerToProbe(
    RingMember member, Query query, double myRtt
  ) async {
    final probeId = node._uuid.v4();
    final completer = Completer<_ProbeResult?>();
    
    final timeout = Timer(
      Duration(milliseconds: (node.config.probeTimeoutFactor * myRtt).toInt()),
      () {
        if (!completer.isCompleted) completer.complete(null);
      },
    );
    
    // Set up one-shot handler
    void handler(rtc.RTCDataChannelMessage event) {
      try {
        final msg = jsonDecode(event.text) as Map<String, dynamic>;
        if (msg['type'] == 'probe_result' && msg['probeId'] == probeId) {
          timeout.cancel();
          if (!completer.isCompleted) {
            completer.complete(_ProbeResult(
              peerId: member.peerId,
              rttMs: (msg['rttMs'] as num).toDouble(),
              dataChannel: member.dataChannel,
            ));
          }
        }
      } catch (_) {}
    }
    
    member.dataChannel?.onMessage = handler;
    member.dataChannel?.send(rtc.RTCDataChannelMessage(jsonEncode({
      'type': 'probe_request',
      'probeId': probeId,
      'target': query.target,
      'targetType': query.targetType,
      'queryId': query.queryId,
    })));
    
    return completer.future;
  }
  
  /// Find the central leader (minimizes avg latency to a set of peers)
  Future<QueryResult> findCentralLeader(List<String> peerIds) async {
    final queryId = node._uuid.v4();
    final completer = Completer<QueryResult>();
    
    final timeout = Timer(node.config.queryTimeout, () {
      if (!completer.isCompleted) {
        node.pendingQueries.remove(queryId);
        completer.complete(QueryResult(
          queryId: queryId,
          error: 'Leader election timeout',
        ));
      }
    });
    
    node.pendingQueries[queryId] = completer;
    
    _routeLeaderQuery(Query(
      queryId: queryId,
      type: QueryType.leaderElection,
      targets: peerIds,
      originator: node.peerId,
    ));
    
    return completer.future;
  }
  
  /// Route a leader election query
  Future<void> _routeLeaderQuery(Query query) async {
    if (query.hopCount > node.config.maxHops) {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        error: 'Max hops exceeded',
      ));
      return;
    }
    
    // Measure average RTT to all targets
    double myAvgRtt;
    try {
      final rtts = await Future.wait(
        query.targets!.map((t) => node.rttMeasurement.measureToTarget(t, 'peer')),
      );
      myAvgRtt = rtts.reduce((a, b) => a + b) / rtts.length;
    } catch (e) {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        error: 'Cannot measure targets',
      ));
      return;
    }
    
    // Same routing logic as closest-node but with avg RTT
    final ringIndex = node.ringManager.calculateRingIndex(myAvgRtt);
    final peersToQuery = <RingMember>[];
    
    final ringsToCheck = [ringIndex];
    if (ringIndex > 0) ringsToCheck.add(ringIndex - 1);
    if (ringIndex < node.config.ringsPerNode - 1) ringsToCheck.add(ringIndex + 1);
    
    for (final ri in ringsToCheck) {
      for (final member in node.rings[ri].primaryMembers) {
        if (member.rttMs >= myAvgRtt / 2 && member.rttMs <= myAvgRtt * 2) {
          peersToQuery.add(member);
        }
      }
    }
    
    if (peersToQuery.isEmpty) {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        leaderId: node.peerId,
        avgRttMs: myAvgRtt,
        hopCount: query.hopCount,
      ));
      return;
    }
    
    // Ask peers to measure average RTT
    final results = await Future.wait(
      peersToQuery.map((m) => _askPeerToProbeAverage(m, query)),
      eagerError: false,
    );
    
    String? bestPeerId;
    double bestAvgRtt = myAvgRtt;
    rtc.RTCDataChannel? bestDc;
    
    for (final result in results) {
      if (result != null && result.avgRttMs < bestAvgRtt) {
        bestAvgRtt = result.avgRttMs;
        bestPeerId = result.peerId;
        bestDc = result.dataChannel;
      }
    }
    
    if (bestPeerId != null && bestAvgRtt < myAvgRtt * node.config.routeAcceptanceThreshold) {
      query.hopCount++;
      bestDc?.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'leader_query_forward',
        'query': _queryToMap(query),
      })));
    } else {
      _respondToQuery(query, QueryResult(
        queryId: query.queryId,
        leaderId: bestPeerId ?? node.peerId,
        avgRttMs: bestAvgRtt,
        hopCount: query.hopCount,
      ));
    }
  }
  
  // ... (similar implementations for multi-constraint queries)
  
  void _respondToQuery(Query query, QueryResult result) {
    if (query.requesterDc != null) {
      // Forward response back through the chain
      query.requesterDc!.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'query_result',
        'queryId': result.queryId,
        'closestPeerId': result.closestPeerId,
        'closestRttMs': result.closestRttMs,
        'leaderId': result.leaderId,
        'avgRttMs': result.avgRttMs,
        'satisfyingPeers': result.satisfyingPeers,
        'hopCount': result.hopCount,
        'error': result.error,
      })));
    } else {
      // We're the originator
      final completer = node.pendingQueries.remove(result.queryId);
      if (completer != null && !completer.isCompleted) {
        completer.complete(result);
      }
    }
  }
  
  Map<String, dynamic> _queryToMap(Query query) {
    return {
      'queryId': query.queryId,
      'type': query.type.index,
      'target': query.target,
      'targetType': query.targetType,
      'targets': query.targets,
      'hopCount': query.hopCount,
      'originator': query.originator,
      'timestamp': query.timestamp.millisecondsSinceEpoch,
    };
  }
}

class _ProbeResult {
  final String peerId;
  final double rttMs;
  final rtc.RTCDataChannel? dataChannel;
  
  _ProbeResult({
    required this.peerId,
    required this.rttMs,
    this.dataChannel,
  });
}
```

---

## 6. WebRTC Layer (webrtc/)

### 6.1 peer_connection_manager.dart

```dart
import 'dart:convert';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import '../overlay/meridian_node.dart';

class PeerConnectionManager {
  final MeridianNode node;
  final Map<String, rtc.RTCPeerConnection> _connections = {};
  
  PeerConnectionManager(this.node);
  
  /// Create a new RTCPeerConnection with standard config
  Future<rtc.RTCPeerConnection> createConnection({
    Map<String, dynamic>? iceServers,
  }) async {
    final pc = await rtc.createPeerConnection({
      'iceServers': iceServers ?? [
        {'urls': node.config.stunServers},
        ...node.config.turnServers.map((t) => {
          'urls': t.url,
          'username': t.username,
          'credential': t.credential,
        }),
      ],
    });
    return pc;
  }
  
  /// Create an ephemeral connection for probing
  Future<rtc.RTCPeerConnection> createEphemeralConnection() async {
    return createConnection();
  }
  
  /// Establish a full connection to a discovered peer
  Future<void> establishConnection(String peerId) async {
    final pc = await createConnection();
    _connections[peerId] = pc;
    
    final dc = await pc.createDataChannel('meridian-${node._uuid.v4()}');
    
    final completer = Completer<void>();
    final timeout = Timer(const Duration(seconds: 10), () {
      if (!completer.isCompleted) {
        pc.close();
        _connections.remove(peerId);
        completer.completeError(Exception('Connection timeout'));
      }
    });
    
    dc.onOpen = () async {
      timeout.cancel();
      try {
        final rtt = await node.rttMeasurement.measureOverDataChannel(dc);
        await node.ringManager.addPeerToRing(peerId, dc, rtt);
        completer.complete();
      } catch (e) {
        pc.close();
        _connections.remove(peerId);
        completer.completeError(e);
      }
    };
    
    // Send offer via signaling
    pc.onIceCandidate = (candidate) {
      node._signalClient?.send({
        'type': 'ice_candidate',
        'target': peerId,
        'senderId': node.peerId,
        'candidate': candidate.toMap(),
      });
    };
    
    final offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    
    node._signalClient?.send({
      'type': 'connect_offer',
      'target': peerId,
      'senderId': node.peerId,
      'sdp': pc.localDescription?.toMap(),
    });
    
    return completer.future;
  }
  
  /// Handle incoming connection offer
  Future<void> handleOffer(Map<String, dynamic> msg) async {
    final senderId = msg['senderId'] as String;
    final pc = await createConnection();
    _connections[senderId] = pc;
    
    pc.onIceCandidate = (candidate) {
      node._signalClient?.send({
        'type': 'ice_candidate',
        'target': senderId,
        'senderId': node.peerId,
        'candidate': candidate.toMap(),
      });
    };
    
    rtc.RTCDataChannel? dc;
    pc.onDataChannel = (channel) {
      dc = channel;
      dc!.onOpen = () async {
        final rtt = await node.rttMeasurement.measureOverDataChannel(dc!);
        await node.ringManager.addPeerToRing(senderId, dc!, rtt);
      };
    };
    
    await pc.setRemoteDescription(rtc.RTCSessionDescription(
      msg['sdp']['type'],
      msg['sdp']['sdp'],
    ));
    
    final answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    
    node._signalClient?.send({
      'type': 'connect_answer',
      'target': senderId,
      'senderId': node.peerId,
      'sdp': pc.localDescription?.toMap(),
    });
  }
  
  void dispose() {
    for (final pc in _connections.values) {
      pc.close();
    }
    _connections.clear();
  }
}
```

### 6.2 data_channel_handler.dart — Message Protocol

```dart
import 'dart:convert';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import '../overlay/meridian_node.dart';

class DataChannelHandler {
  final MeridianNode node;
  
  DataChannelHandler(this.node);
  
  /// Set up all message handlers for a DataChannel
  void setupHandlers(rtc.RTCDataChannel dc, String peerId) {
    dc.onMessage = (message) {
      try {
        final msg = jsonDecode(message.text) as Map<String, dynamic>;
        _dispatch(msg, dc, peerId);
      } catch (e) {
        // Ignore malformed messages
      }
    };
    
    dc.onClose = () {
      node._handlePeerFailure(peerId);
    };
  }
  
  void _dispatch(Map<String, dynamic> msg, rtc.RTCDataChannel dc, String peerId) {
    switch (msg['type'] as String) {
      // RTT measurement
      case 'ping':
        node.rttMeasurement.handlePing(dc, msg);
        break;
      case 'pong':
        node.rttMeasurement.handlePong(msg);
        break;
      
      // Gossip
      case 'gossip':
        node.gossipProtocol.handleGossip(msg);
        break;
      
      // Query routing
      case 'query_forward':
        node.queryRouting._routeQuery(_mapToQuery(msg['query']));
        break;
      case 'leader_query_forward':
        node.queryRouting._routeLeaderQuery(_mapToQuery(msg['query']));
        break;
      case 'probe_request':
        _handleProbeRequest(msg, dc);
        break;
      case 'probe_request_avg':
        _handleProbeRequestAvg(msg, dc);
        break;
      case 'query_result':
        _handleQueryResult(msg);
        break;
      
      // Media
      case 'media_offer':
        node.mediaManager.handleMediaOffer(msg);
        break;
      case 'media_answer':
        node.mediaManager.handleMediaAnswer(msg);
        break;
      case 'forwarded_stream':
        node.mediaManager.handleForwardedStream(msg);
        break;
      
      // Supernode / Raft
      case 'supernode_elected':
        _handleSupernodeElected(msg);
        break;
      case 'raft_append_entries':
        node.raftConsensus?.handleAppendEntries(msg);
        break;
      case 'raft_append_entries_response':
        node.raftConsensus?.handleAppendEntriesResponse(msg);
        break;
      case 'raft_request_vote':
        node.raftConsensus?.handleRequestVote(msg, dc);
        break;
      case 'raft_request_vote_response':
        node.raftConsensus?.handleRequestVoteResponse(msg);
        break;
      
      // Peer management
      case 'peer_leaving':
        node._handlePeerFailure(msg['senderId'] as String);
        break;
    }
  }
  
  Future<void> _handleProbeRequest(Map<String, dynamic> msg, rtc.RTCDataChannel dc) async {
    try {
      final rtt = await node.rttMeasurement.measureToTarget(
        msg['target'] as String,
        msg['targetType'] as String? ?? 'peer',
      );
      dc.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'probe_result',
        'probeId': msg['probeId'],
        'rttMs': rtt,
        'queryId': msg['queryId'],
      })));
    } catch (e) {
      // Don't respond — timeout will handle it
    }
  }
  
  Future<void> _handleProbeRequestAvg(Map<String, dynamic> msg, rtc.RTCDataChannel dc) async {
    try {
      final targets = (msg['targets'] as List).cast<String>();
      final rtts = await Future.wait(
        targets.map((t) => node.rttMeasurement.measureToTarget(t, 'peer')),
      );
      final avgRtt = rtts.reduce((a, b) => a + b) / rtts.length;
      
      dc.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'probe_result_avg',
        'probeId': msg['probeId'],
        'avgRttMs': avgRtt,
      })));
    } catch (e) {}
  }
  
  void _handleQueryResult(Map<String, dynamic> msg) {
    final queryId = msg['queryId'] as String;
    final completer = node.pendingQueries.remove(queryId);
    if (completer != null && !completer.isCompleted) {
      completer.complete(QueryResult(
        queryId: queryId,
        closestPeerId: msg['closestPeerId'] as String?,
        closestRttMs: (msg['closestRttMs'] as num?)?.toDouble(),
        leaderId: msg['leaderId'] as String?,
        avgRttMs: (msg['avgRttMs'] as num?)?.toDouble(),
        satisfyingPeers: (msg['satisfyingPeers'] as List?)?.cast<String>(),
        hopCount: (msg['hopCount'] as num?)?.toInt() ?? 0,
        error: msg['error'] as String?,
      ));
    }
  }
  
  void _handleSupernodeElected(Map<String, dynamic> msg) {
    final supernodeId = msg['supernodeId'] as String;
    node.clusterLeader = supernodeId;
    node.isSupernode = supernodeId == node.peerId;
    
    if (node.isSupernode) {
      final clusterPeers = (msg['clusterPeers'] as List).cast<String>();
      node.raftConsensus?.initState(clusterPeers);
    }
    
    node.onSupernodeElected?.call(supernodeId);
  }
  
  Query _mapToQuery(Map<String, dynamic>? map) {
    if (map == null) throw Exception('Invalid query map');
    return Query(
      queryId: map['queryId'] as String,
      type: QueryType.values[map['type'] as int],
      target: map['target'] as String?,
      targetType: map['targetType'] as String?,
      targets: (map['targets'] as List?)?.cast<String>(),
      hopCount: (map['hopCount'] as num?)?.toInt() ?? 0,
      originator: map['originator'] as String,
      timestamp: DateTime.fromMillisecondsSinceEpoch(
        (map['timestamp'] as num?)?.toInt() ?? DateTime.now().millisecondsSinceEpoch
      ),
    );
  }
}
```

### 6.3 media_manager.dart — Media Stream Management

```dart
import 'dart:convert';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import '../overlay/meridian_node.dart';

class MediaManager {
  final MeridianNode node;
  final Map<String, rtc.RTCPeerConnection> _mediaConnections = {};
  
  MediaManager(this.node);
  
  /// Establish a media stream to a peer discovered via Meridian query
  Future<rtc.RTCPeerConnection> establishMediaStream(String targetPeerId) async {
    final known = node.knownPeers[targetPeerId];
    if (known?.dataChannel == null) {
      throw Exception('Target peer not connected');
    }
    
    final pc = await node.pcManager.createConnection();
    _mediaConnections[targetPeerId] = pc;
    
    // Add local media
    if (node.localStream != null) {
      for (final track in node.localStream!.getTracks()) {
        await pc.addTrack(track, node.localStream!);
      }
    }
    
    // Handle incoming media
    pc.onTrack = (event) {
      if (event.streams.isNotEmpty) {
        node.remoteStreams[targetPeerId] = event.streams[0];
        node.onRemoteStreamAdded?.call(targetPeerId, event.streams[0]);
      }
    };
    
    // Create offer and send via existing DataChannel
    final offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    
    known.dataChannel!.send(rtc.RTCDataChannelMessage(jsonEncode({
      'type': 'media_offer',
      'senderId': node.peerId,
      'sdp': pc.localDescription?.toMap(),
    })));
    
    return pc;
  }
  
  /// Handle incoming media offer
  Future<void> handleMediaOffer(Map<String, dynamic> msg) async {
    final senderId = msg['senderId'] as String;
    final pc = await node.pcManager.createConnection();
    _mediaConnections[senderId] = pc;
    
    // Add local media
    if (node.localStream != null) {
      for (final track in node.localStream!.getTracks()) {
        await pc.addTrack(track, node.localStream!);
      }
    }
    
    pc.onTrack = (event) {
      if (event.streams.isNotEmpty) {
        node.remoteStreams[senderId] = event.streams[0];
        node.onRemoteStreamAdded?.call(senderId, event.streams[0]);
      }
    };
    
    await pc.setRemoteDescription(rtc.RTCSessionDescription(
      msg['sdp']['type'],
      msg['sdp']['sdp'],
    ));
    
    final answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    
    // Send answer via existing DataChannel
    final known = node.knownPeers[senderId];
    if (known?.dataChannel != null) {
      known!.dataChannel!.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'media_answer',
        'senderId': node.peerId,
        'sdp': pc.localDescription?.toMap(),
      })));
    }
  }
  
  void handleMediaAnswer(Map<String, dynamic> msg) {
    // The offer initiator handles this via the pending connection
  }
  
  void handleForwardedStream(Map<String, dynamic> msg) {
    // Supernode is forwarding a stream from another peer
    final sourcePeerId = msg['sourcePeerId'] as String;
    // The actual media will arrive via the media connection to the supernode
  }
  
  void dispose() {
    for (final pc in _mediaConnections.values) {
      pc.close();
    }
    _mediaConnections.clear();
  }
}
```

---

## 7. Signaling Layer (signaling/)

### 7.1 signaling_client.dart

```dart
import 'dart:convert';
import 'package:web_socket_channel/web_socket_channel.dart';
import '../overlay/meridian_node.dart';

class SignalingClient {
  final String url;
  final MeridianNode node;
  WebSocketChannel? _channel;
  
  SignalingClient({required this.url, required this.node});
  
  Future<void> connect() async {
    final wsUrl = Uri.parse(url);
    _channel = WebSocketChannel.connect(wsUrl);
    
    // Register with signaling server
    send({'type': 'register', 'peerId': node.peerId});
    
    // Request initial peer list
    send({'type': 'get_peers', 'senderId': node.peerId});
    
    // Listen for messages
    _channel!.stream.listen(
      (data) {
        final msg = jsonDecode(data as String) as Map<String, dynamic>;
        _handleMessage(msg);
      },
      onError: (error) {
        // Reconnect logic would go here
      },
      onDone: () {
        // Reconnect logic would go here
      },
    );
  }
  
  void send(Map<String, dynamic> message) {
    _channel?.sink.add(jsonEncode(message));
  }
  
  void _handleMessage(Map<String, dynamic> msg) {
    switch (msg['type'] as String) {
      case 'peers_list':
        final peers = (msg['peers'] as List).cast<String>();
        for (final peerId in peers) {
          if (!node.knownPeers.containsKey(peerId) &&
              !node.pendingConnections.contains(peerId)) {
            node.pendingConnections.add(peerId);
            node.pcManager.establishConnection(peerId).catchError((_) {}).whenComplete(() {
              node.pendingConnections.remove(peerId);
            });
          }
        }
        break;
        
      case 'connect_offer':
        if (msg['target'] == node.peerId) {
          node.pcManager.handleOffer(msg);
        }
        break;
        
      case 'connect_answer':
        // Handled by the connection initiator
        break;
        
      case 'probe_offer':
        if (msg['target'] == node.peerId) {
          _handleProbeOffer(msg);
        }
        break;
        
      case 'probe_answer':
        // Handled by the probe initiator
        break;
        
      case 'ice_candidate':
        if (msg['target'] == node.peerId) {
          _handleIceCandidate(msg);
        }
        break;
    }
  }
  
  Future<void> _handleProbeOffer(Map<String, dynamic> msg) async {
    final senderId = msg['senderId'] as String;
    final pc = await node.pcManager.createEphemeralConnection();
    
    rtc.RTCDataChannel? dc;
    pc.onDataChannel = (channel) {
      dc = channel;
    };
    
    await pc.setRemoteDescription(rtc.RTCSessionDescription(
      msg['sdp']['type'],
      msg['sdp']['sdp'],
    ));
    
    final answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    
    send({
      'type': 'probe_answer',
      'target': senderId,
      'senderId': node.peerId,
      'sdp': pc.localDescription?.toMap(),
    });
  }
  
  void _handleIceCandidate(Map<String, dynamic> msg) {
    // Forward to the appropriate RTCPeerConnection
    // This requires mapping candidate messages to connections
    // Implementation depends on how connections are tracked
  }
  
  Future<void> disconnect() async {
    await _channel?.sink.close();
  }
}
```

---

## 8. Consensus Layer (consensus/)

### 8.1 raft_consensus.dart

```dart
import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import '../models/raft_state.dart';
import '../overlay/meridian_node.dart';

class RaftConsensus {
  final MeridianNode node;
  SupernodeCluster? _cluster;
  Timer? _electionTimer;
  Timer? _heartbeatTimer;
  
  RaftConsensus(this.node);
  
  void initState(List<String> clusterPeers) {
    _cluster = SupernodeCluster(
      clusterId: node._uuid.v4(),
      members: [node.peerId, ...clusterPeers.where((id) => id != node.peerId)],
      leaderId: node.peerId,
      raftState: RaftState.leader,
    );
    
    // Initialize leader state
    for (final member in _cluster!.members) {
      if (member != node.peerId) {
        _cluster!.nextIndex[member] = _cluster!.log.length;
        _cluster!.matchIndex[member] = 0;
      }
    }
    
    _startHeartbeats();
  }
  
  void _startHeartbeats() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = Timer.periodic(
      _cluster!.heartbeatInterval,
      (_) => _sendAppendEntries(),
    );
  }
  
  void _sendAppendEntries() {
    if (_cluster == null || _cluster!.raftState != RaftState.leader) return;
    
    for (final member in _cluster!.members) {
      if (member == node.peerId) continue;
      
      final nextIdx = _cluster!.nextIndex[member] ?? 0;
      final entries = _cluster!.log.length > nextIdx
          ? _cluster!.log.sublist(nextIdx)
          : <RaftLogEntry>[];
      
      final prevLogIndex = nextIdx - 1;
      final prevLogTerm = prevLogIndex >= 0 && prevLogIndex < _cluster!.log.length
          ? _cluster!.log[prevLogIndex].term
          : 0;
      
      final known = node.knownPeers[member];
      if (known?.dataChannel == null) continue;
      
      try {
        known!.dataChannel!.send(rtc.RTCDataChannelMessage(jsonEncode({
          'type': 'raft_append_entries',
          'term': _cluster!.currentTerm,
          'leaderId': node.peerId,
          'prevLogIndex': prevLogIndex,
          'prevLogTerm': prevLogTerm,
          'entries': entries.map((e) => {
            'term': e.term,
            'index': e.index,
            'command': e.command,
          }).toList(),
          'leaderCommit': _cluster!.commitIndex,
        })));
      } catch (_) {}
    }
  }
  
  void handleAppendEntries(Map<String, dynamic> msg) {
    if (_cluster == null) return;
    
    // Reply false if term < currentTerm
    final term = msg['term'] as int;
    if (term < _cluster!.currentTerm) {
      _sendRaftResponse(msg['leaderId'] as String, {
        'type': 'raft_append_entries_response',
        'term': _cluster!.currentTerm,
        'success': false,
        'lastLogIndex': _cluster!.log.length - 1,
      });
      return;
    }
    
    // Update term if necessary
    if (term > _cluster!.currentTerm) {
      _cluster!.currentTerm = term;
      _cluster!.raftState = RaftState.follower;
      _cluster!.votedFor = null;
    }
    
    // Reset election timeout
    _resetElectionTimeout();
    
    // Recognize leader
    _cluster!.leaderId = msg['leaderId'] as String;
    
    // Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
    final prevLogIndex = msg['prevLogIndex'] as int;
    if (prevLogIndex >= 0) {
      if (prevLogIndex >= _cluster!.log.length) {
        _sendRaftResponse(msg['leaderId'] as String, {
          'type': 'raft_append_entries_response',
          'term': _cluster!.currentTerm,
          'success': false,
          'lastLogIndex': _cluster!.log.length - 1,
        });
        return;
      }
      
      if (_cluster!.log[prevLogIndex].term != (msg['prevLogTerm'] as int)) {
        _cluster!.log = _cluster!.log.sublist(0, prevLogIndex);
        _sendRaftResponse(msg['leaderId'] as String, {
          'type': 'raft_append_entries_response',
          'term': _cluster!.currentTerm,
          'success': false,
          'lastLogIndex': _cluster!.log.length - 1,
        });
        return;
      }
    }
    
    // Append new entries
    final entries = (msg['entries'] as List).cast<Map<String, dynamic>>();
    for (final entryMap in entries) {
      final entry = RaftLogEntry(
        term: entryMap['term'] as int,
        index: entryMap['index'] as int,
        command: entryMap['command'] as Map<String, dynamic>,
      );
      
      if (entry.index < _cluster!.log.length) {
        if (_cluster!.log[entry.index].term != entry.term) {
          _cluster!.log = _cluster!.log.sublist(0, entry.index);
          _cluster!.log.add(entry);
        }
      } else {
        _cluster!.log.add(entry);
      }
    }
    
    // Update commitIndex
    final leaderCommit = msg['leaderCommit'] as int;
    if (leaderCommit > _cluster!.commitIndex) {
      _cluster!.commitIndex = leaderCommit < _cluster!.log.length
          ? leaderCommit
          : _cluster!.log.length - 1;
    }
    
    // Apply committed entries
    _applyCommittedEntries();
    
    // Reply success
    _sendRaftResponse(msg['leaderId'] as String, {
      'type': 'raft_append_entries_response',
      'term': _cluster!.currentTerm,
      'success': true,
      'lastLogIndex': _cluster!.log.length - 1,
    });
  }
  
  void handleAppendEntriesResponse(Map<String, dynamic> msg) {
    if (_cluster == null || _cluster!.raftState != RaftState.leader) return;
    
    final followerId = msg['senderId'] as String? ?? '';
    if (followerId.isEmpty) return;
    
    if (msg['success'] as bool) {
      final lastLogIndex = msg['lastLogIndex'] as int;
      _cluster!.nextIndex[followerId] = lastLogIndex + 1;
      _cluster!.matchIndex[followerId] = lastLogIndex;
      
      // Update commitIndex
      final majority = (_cluster!.members.length / 2).ceil();
      for (int i = _cluster!.commitIndex + 1; i < _cluster!.log.length; i++) {
        int matchCount = 1; // Self
        for (final member in _cluster!.members) {
          if (member != node.peerId &&
              (_cluster!.matchIndex[member] ?? 0) >= i) {
            matchCount++;
          }
        }
        if (matchCount >= majority && _cluster!.log[i].term == _cluster!.currentTerm) {
          _cluster!.commitIndex = i;
        }
      }
      
      _applyCommittedEntries();
    } else {
      // Decrement nextIndex and retry
      final nextIdx = _cluster!.nextIndex[followerId] ?? 0;
      if (nextIdx > 0) {
        _cluster!.nextIndex[followerId] = nextIdx - 1;
      }
    }
  }
  
  void handleRequestVote(Map<String, dynamic> msg, rtc.RTCDataChannel dc) {
    if (_cluster == null) return;
    
    final term = msg['term'] as int;
    final candidateId = msg['candidateId'] as String;
    
    // Reply false if term < currentTerm
    if (term < _cluster!.currentTerm) {
      dc.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'raft_request_vote_response',
        'term': _cluster!.currentTerm,
        'voteGranted': false,
      })));
      return;
    }
    
    // Update term if necessary
    if (term > _cluster!.currentTerm) {
      _cluster!.currentTerm = term;
      _cluster!.raftState = RaftState.follower;
      _cluster!.votedFor = null;
    }
    
    // Check if already voted
    if (_cluster!.votedFor != null && _cluster!.votedFor != candidateId) {
      dc.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'raft_request_vote_response',
        'term': _cluster!.currentTerm,
        'voteGranted': false,
      })));
      return;
    }
    
    // Check log freshness
    final lastLogIndex = _cluster!.log.length - 1;
    final lastLogTerm = lastLogIndex >= 0 ? _cluster!.log[lastLogIndex].term : 0;
    final candidateLastLogIndex = msg['lastLogIndex'] as int;
    final candidateLastLogTerm = msg['lastLogTerm'] as int;
    
    final logOk = candidateLastLogTerm > lastLogTerm ||
        (candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex);
    
    if (logOk) {
      _cluster!.votedFor = candidateId;
      _resetElectionTimeout();
      
      dc.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'raft_request_vote_response',
        'term': _cluster!.currentTerm,
        'voteGranted': true,
      })));
    } else {
      dc.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'raft_request_vote_response',
        'term': _cluster!.currentTerm,
        'voteGranted': false,
      })));
    }
  }
  
  void handleRequestVoteResponse(Map<String, dynamic> msg) {
    // Handled by the election initiator
  }
  
  void _resetElectionTimeout() {
    _electionTimer?.cancel();
    final timeout = Duration(
      milliseconds: 150 + Random().nextInt(150),
    );
    _electionTimer = Timer(timeout, _startElection);
  }
  
  void _startElection() {
    if (_cluster == null) return;
    
    _cluster!.raftState = RaftState.candidate;
    _cluster!.currentTerm++;
    _cluster!.votedFor = node.peerId;
    
    int votesReceived = 1;
    final majority = (_cluster!.members.length / 2).ceil();
    
    final lastLogIndex = _cluster!.log.length - 1;
    final lastLogTerm = lastLogIndex >= 0 ? _cluster!.log[lastLogIndex].term : 0;
    
    for (final member in _cluster!.members) {
      if (member == node.peerId) continue;
      
      final known = node.knownPeers[member];
      if (known?.dataChannel == null) continue;
      
      known!.dataChannel!.send(rtc.RTCDataChannelMessage(jsonEncode({
        'type': 'raft_request_vote',
        'term': _cluster!.currentTerm,
        'candidateId': node.peerId,
        'lastLogIndex': lastLogIndex,
        'lastLogTerm': lastLogTerm,
      })));
    }
    
    // Set up vote response handler
    // (In practice, responses come through the DataChannel handler)
  }
  
  void _applyCommittedEntries() {
    if (_cluster == null) return;
    
    while (_cluster!.lastApplied < _cluster!.commitIndex) {
      _cluster!.lastApplied++;
      final entry = _cluster!.log[_cluster!.lastApplied];
      
      switch (entry.command['type'] as String) {
        case 'cluster_membership':
          final action = entry.command['action'] as String;
          final peerId = entry.command['peerId'] as String;
          if (action == 'join' && !_cluster!.members.contains(peerId)) {
            _cluster!.members.add(peerId);
          } else if (action == 'leave') {
            _cluster!.members.remove(peerId);
          }
          break;
          
        case 'stream_metadata':
          // Update stream metadata
          break;
          
        case 'topology_change':
          // Handle topology change
          break;
      }
    }
  }
  
  void _sendRaftResponse(String targetId, Map<String, dynamic> message) {
    final known = node.knownPeers[targetId];
    if (known?.dataChannel != null) {
      known!.dataChannel!.send(rtc.RTCDataChannelMessage(jsonEncode(message)));
    }
  }
  
  void dispose() {
    _heartbeatTimer?.cancel();
    _electionTimer?.cancel();
  }
}
```

---

## 9. Platform-Specific Handling

### 9.1 Web Platform Considerations

On the **web target**, `flutter_webrtc` wraps browser WebRTC APIs. The following constraints apply:

1. **No raw UDP sockets**: All probing must use DataChannel ping-pong or ephemeral WebRTC connections
2. **No dart:io**: Use `package:http` instead of `HttpClient` for HTTP probing
3. **No isolates for networking**: Use `Future` concurrency

Create a platform utility:

```dart
import 'dart:io' show Platform;
import 'package:flutter/foundation.dart' show kIsWeb;

class PlatformUtils {
  static bool get isWeb => kIsWeb;
  static bool get isMobile => !kIsWeb && (Platform.isAndroid || Platform.isIOS);
  static bool get isDesktop => !kIsWeb && (Platform.isMacOS || Platform.isWindows || Platform.isLinux);
  
  /// HTTP probing — uses different implementations per platform
  static Future<double> probeHttpTarget(String url) async {
    if (isWeb) {
      // Web: use http package
      final stopwatch = Stopwatch()..start();
      try {
        final response = await http.head(Uri.parse(url));
        return stopwatch.elapsedMilliseconds.toDouble();
      } catch (e) {
        return double.infinity;
      }
    } else {
      // Native: use dart:io
      final stopwatch = Stopwatch()..start();
      try {
        final client = HttpClient();
        await client.headUrl(Uri.parse(url));
        client.close();
        return stopwatch.elapsedMilliseconds.toDouble();
      } catch (e) {
        return double.infinity;
      }
    }
  }
}
```

### 9.2 Connection Limit Management

Browsers limit connections per origin (~256 in Chrome). The Meridian overlay uses ~72 DataChannels + ~5 media connections, which is well within limits. On mobile/desktop, there are no such limits.

```dart
class ConnectionLimiter {
  static const int maxDataChannels = 200; // Safety margin
  static const int maxMediaConnections = 10;
  
  static bool canOpenDataChannel(MeridianNode node) {
    int count = 0;
    for (final ring in node.rings) {
      count += ring.primaryMembers.length;
      count += ring.secondaryMembers.length;
    }
    return count < maxDataChannels;
  }
}
```

---

## 10. UI Layer (ui/)

### 10.1 home_screen.dart (Minimal)

```dart
import 'package:flutter/material.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:provider/provider.dart';
import '../overlay/meridian_node.dart';
import 'video_grid.dart';
import 'peer_list.dart';
import 'controls.dart';

class HomeScreen extends StatefulWidget {
  final MeridianNode node;
  
  const HomeScreen({super.key, required this.node});
  
  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final rtc.RTCVideoRenderer _localRenderer = rtc.RTCVideoRenderer();
  
  @override
  void initState() {
    super.initState();
    _initRenderer();
    
    widget.node.onRemoteStreamAdded = (peerId, stream) {
      setState(() {});
    };
    
    widget.node.onSupernodeElected = (supernodeId) {
      setState(() {});
    };
  }
  
  Future<void> _initRenderer() async {
    await _localRenderer.initialize();
    if (widget.node.localStream != null) {
      _localRenderer.srcObject = widget.node.localStream;
    }
  }
  
  @override
  void dispose() {
    _localRenderer.dispose();
    super.dispose();
  }
  
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Meridian P2P — ${widget.node.peerId.substring(0, 8)}...'),
        actions: [
          if (widget.node.isSupernode)
            const Chip(
              label: Text('SUPERNODE'),
              backgroundColor: Colors.amber,
            ),
        ],
      ),
      body: Column(
        children: [
          // Local video
          Expanded(
            flex: 1,
            child: rtc.RTCVideoView(_localRenderer),
          ),
          // Remote videos
          Expanded(
            flex: 2,
            child: VideoGrid(
              streams: widget.node.remoteStreams,
            ),
          ),
          // Peer list
          Expanded(
            flex: 1,
            child: PeerList(node: widget.node),
          ),
          // Controls
          Controls(node: widget.node),
        ],
      ),
    );
  }
}
```

---

## 11. Initialization and Lifecycle (main.dart)

```dart
import 'package:flutter/material.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'overlay/meridian_node.dart';
import 'config/meridian_config.dart';
import 'ui/home_screen.dart';

void main() {
  runApp(const MeridianApp());
}

class MeridianApp extends StatelessWidget {
  const MeridianApp({super.key});
  
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Meridian P2P Streaming',
      theme: ThemeData.dark(),
      home: const SetupScreen(),
    );
  }
}

class SetupScreen extends StatefulWidget {
  const SetupScreen({super.key});
  
  @override
  State<SetupScreen> createState() => _SetupScreenState();
}

class _SetupScreenState extends State<SetupScreen> {
  final MeridianNode _node = MeridianNode();
  bool _initialized = false;
  String? _error;
  
  @override
  void initState() {
    super.initState();
    _initialize();
  }
  
  Future<void> _initialize() async {
    try {
      // Get user media
      final mediaStream = await rtc.navigator.mediaDevices.getUserMedia({
        'video': true,
        'audio': true,
      });
      
      // Initialize Meridian node
      await _node.initialize(
        'wss://signaling.example.com',
        mediaStream: mediaStream,
      );
      
      setState(() => _initialized = true);
    } catch (e) {
      setState(() => _error = e.toString());
    }
  }
  
  @override
  void dispose() {
    _node.dispose();
    super.dispose();
  }
  
  @override
  Widget build(BuildContext context) {
    if (_error != null) {
      return Scaffold(
        body: Center(
          child: Text('Error: $_error'),
        ),
      );
    }
    
    if (!_initialized) {
      return const Scaffold(
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              CircularProgressIndicator(),
              SizedBox(height: 16),
              Text('Initializing Meridian overlay...'),
              Text('Discovering peers...'),
            ],
          ),
        ),
      );
    }
    
    return HomeScreen(node: _node);
  }
}
```

---

## 12. Implementation Order

### Phase 1 — Core Overlay (Week 1)
1. `meridian_config.dart` — System constants
2. All model classes (`ring.dart`, `peer_state.dart`, `connection_pool.dart`, `query_types.dart`, `raft_state.dart`)
3. `meridian_node.dart` — Main class with ring initialization
4. `ring_manager.dart` — Ring index calculation, add/remove peers
5. `rtt_measurement.dart` — DataChannel ping/pong

### Phase 2 — Discovery & Gossip (Week 2)
1. `signaling_client.dart` — WebSocket connection, message relay
2. `peer_connection_manager.dart` — RTCPeerConnection lifecycle
3. `gossip_protocol.dart` — Gossip cycle, handler
4. `data_channel_handler.dart` — Message dispatch

### Phase 3 — Query Routing (Week 3)
1. `query_routing.dart` — Closest node discovery
2. Leader election extension
3. Multi-constraint extension
4. `ring_manager.dart` — Hypervolume optimization

### Phase 4 — Supernode & Raft (Week 4)
1. `raft_consensus.dart` — Full Raft implementation
2. Supernode election integration
3. Cluster membership management

### Phase 5 — Streaming (Week 5)
1. `media_manager.dart` — MediaTrack establishment
2. `sfu_forwarder.dart` — Supernode media forwarding
3. Failure recovery and re-streaming

### Phase 6 — UI & Polish (Week 6)
1. `home_screen.dart`, `video_grid.dart`, `peer_list.dart`, `controls.dart`
2. Platform-specific handling (web vs native)
3. Connection limit management
4. Error handling and reconnection

---

## 13. Testing Strategy

```dart
// Unit tests for ring logic
test('calculateRingIndex returns correct ring for RTT', () {
  final manager = RingManager(node);
  expect(manager.calculateRingIndex(1), 0);
  expect(manager.calculateRingIndex(2), 1);
  expect(manager.calculateRingIndex(4), 2);
  expect(manager.calculateRingIndex(1000), 9); // Clamped to max
});

// Integration test for gossip
test('gossip cycle discovers new peers', () async {
  // Set up two nodes with signaling server
  // Run gossip cycle
  // Verify peers appear in each other's rings
});

// End-to-end test for closest node
test('findClosestNode returns peer with lowest RTT', () async {
  // Set up 3-node network
  // Query from node A for target T
  // Verify result is the closest node
});
```

---

## 14. Key Properties (From Meridian Paper)

Property	Guarantee	How Achieved
**Closest node accuracy**	Median error ~2ms	Direct measurement, no coordinate error
**Query latency**	~300ms, constant with system size	Logarithmic hop count
**Scalability**	O(log N) hops	Exponentially increasing ring radii
**Load balance**	In-degree ratio < 2 for 90% of nodes	Stochastic ring independence + hypervolume
**Failure recovery**	< 1 gossip period (~30s)	DataChannel close detection + secondary promotion
**Supernode election**	Minimizes avg latency to group	Meridian central leader election
**Cross-platform**	Android, iOS, Web, macOS, Windows, Linux	Single Flutter codebase + flutter_webrtc

---

## 15. Notes for the AI

- All code must be **pure Dart** with no platform-specific conditionals unless explicitly noted
- Use `flutter_webrtc`'s `rtc` prefix for all WebRTC types
- The signaling server is a **separate process** (Node.js or Dart shelf) — only the client is implemented here
- For the web target, `dart:io` is unavailable — use `package:http` and conditional imports where needed
- The `uuid` package is used for generating unique IDs
- All asynchronous operations should use `Future` and `async/await`
- Error handling should be comprehensive — network operations fail frequently in P2P systems
- The system should gracefully handle the case where a peer disappears without sending a `peer_leaving` message

## PROMPT END

---

This prompt gives Claude Code everything it needs to implement the full system. It covers:

- **All data structures** with exact field names and types
- **All algorithms** with pseudocode-level detail
- **All protocol messages** with exact JSON formats
- **Platform-specific handling** for web vs native
- **Implementation order** broken into 6 phases
- **Testing strategy** with example test cases
- **Flutter-specific patterns** (Provider, widget lifecycle, etc.)

The total implementation is approximately 3,000-4,000 lines of Dart across ~20 files.