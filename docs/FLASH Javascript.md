# FLASH: Meridian-WebRTC -- Browser-Based P2P Streaming Overlay
## Full Implementation Specification (JavaScript)

---

## 1. System Overview

A fully browser-based peer-to-peer video/audio streaming system where **every node is a first-class citizen** running in the browser. The system uses **Meridian's overlay methodology** — multi-resolution rings, gossip-based discovery, query routing via direct measurements — adapted entirely to **WebRTC primitives**. No raw UDP sockets, no native code, no plugins.

### 1.1 Key Design Decisions

- **All communication over WebRTC**: DataChannels for control plane, MediaTracks for streaming
- **Every node is equal**: Any browser can be a supernode via Meridian's central leader election
- **No raw sockets**: Probing uses DataChannel ping-pong (existing connections) and ephemeral WebRTC (new connections)
- **Minimal signaling server**: Only for ICE handshake relay and initial bootstrap
- **Raft for supernode control plane**: Consistent state across supernode clusters

### 1.2 Architecture Layers

```
┌──────────────────────────────────────────────────────────────┐
│                    Application Layer                          │
│  Video/Audio streaming, peer management, room management     │
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

---

## 2. Data Structures

### 2.1 MeridianConfig

```javascript
// System-wide constants
const MERIDIAN_CONFIG = {
  ringsPerNode: 9,
  nodesPerRing: 8,
  secondaryCandidates: 4,
  innermostRingRadius: 1,       // ms
  ringMultiplicativeFactor: 2,  // r
  routeAcceptanceThreshold: 0.5, // β
  probeTimeoutFactor: 2,        // ε
  gossipPeriodMs: 30000,        // 30 seconds
  ringReplacementPeriodMs: 60000, // 60 seconds
  maxEphemeralConnections: 10,
  stunServers: ['stun:stun.l.google.com:19302'],
  turnServers: [],
  maxHops: 32,
  ephemeralProbeTimeoutMs: 5000
};
```

### 2.2 MeridianNode (Main State Object)

```javascript
class MeridianNode {
  constructor(peerId, signalChannel, config = MERIDIAN_CONFIG) {
    this.peerId = peerId;
    this.signalChannel = signalChannel;  // WebSocket or similar
    this.config = config;
    
    // Rings: array of Ring objects
    this.rings = [];
    for (let i = 0; i < config.ringsPerNode; i++) {
      this.rings.push(this._createRing(i));
    }
    
    // All known peers (includes ring members + extras)
    this.knownPeers = new Map();  // peerId -> KnownPeer
    
    // Connection pool for ephemeral probes
    this.connectionPool = {
      entries: [],
      maxSize: config.maxEphemeralConnections
    };
    
    // Supernode state
    this.isSupernode = false;
    this.supernodeCluster = null;
    this.clusterLeader = null;
    
    // Streaming state
    this.activeStreams = new Map();   // peerId -> MediaStream
    this.uplinkStream = null;
    this.supernodeDc = null;
    
    // Pending operations
    this.pendingProbes = new Map();   // queryId -> { resolve, reject, timer }
    this.pendingConnections = new Set(); // peerIds being connected to
    
    // Gossip state
    this.lastGossipTime = 0;
    this.gossipInterval = null;
    
    // Ring replacement timer
    this.ringReplacementInterval = null;
    
    // Event handlers
    this.handlers = {
      onStreamRequest: null,
      onStreamOffer: null,
      onSupernodeElected: null,
      onPeerDisconnected: null
    };
  }
  
  _createRing(index) {
    const r = this.config.ringMultiplicativeFactor;
    return {
      index,
      innerRadius: index === 0 ? 1 : Math.pow(r, index - 1),
      outerRadius: index < this.config.ringsPerNode - 1 
        ? Math.pow(r, index) 
        : Infinity,
      primaryMembers: [],  // RingMember objects
      secondaryMembers: [] // RingMember objects (FIFO)
    };
  }
}
```

### 2.3 RingMember

```javascript
/*
{
  peerId: string,
  dataChannel: RTCDataChannel,
  rtt: number,              // Last measured RTT in ms
  lastProbed: number,       // Date.now() timestamp
  iceCandidateType: string, // 'host' | 'srflx' | 'relay' | 'prflx'
  natType: string,          // 'same_subnet' | 'cone_nat' | 'symmetric_nat' | ...
  isSupernode: boolean,
  isFirewalled: boolean,
  joinedAt: number
}
*/
```

### 2.4 KnownPeer

```javascript
/*
{
  peerId: string,
  dataChannel: RTCDataChannel | null,
  rtt: number | null,
  lastSeen: number,
  isSupernode: boolean,
  ringIndex: number | null,
  status: 'discovered' | 'connecting' | 'connected' | 'failed'
}
*/
```

### 2.5 ConnectionPoolEntry

```javascript
/*
{
  targetId: string,
  pc: RTCPeerConnection,
  dc: RTCDataChannel,
  lastUsed: number,
  createdAt: number
}
*/
```

### 2.6 Query Object

```javascript
/*
{
  queryId: string,
  type: 'closest_node' | 'leader_election' | 'multi_constraint',
  target: string,                    // For closest_node
  targets: string[],                 // For leader_election
  constraints: [                     // For multi_constraint
    { target: string, maxLatencyMs: number }
  ],
  hopCount: number,
  originator: string,
  requesterDc: RTCDataChannel,
  timestamp: number
}
*/
```

### 2.7 Raft State (for Supernodes)

```javascript
/*
{
  clusterId: string,
  members: string[],          // peerIds
  leaderId: string | null,
  
  // Persistent state
  currentTerm: number,
  votedFor: string | null,
  log: [
    { term: number, index: number, command: object }
  ],
  
  // Volatile state
  commitIndex: number,
  lastApplied: number,
  
  // Leader-only volatile state
  nextIndex: Map,   // peerId -> number
  matchIndex: Map,  // peerId -> number
  
  // Timers
  electionTimeoutMs: number,   // Random 150-300ms
  heartbeatIntervalMs: number, // 50ms
  
  // Internal
  raftState: 'follower' | 'candidate' | 'leader',
  electionTimer: null | timer,
  heartbeatTimer: null | timer
}
*/
```

---

## 3. Core Algorithms

### 3.1 Ring Index Calculation

```javascript
/**
 * Determines which ring a peer belongs to based on measured RTT.
 * Rings are exponentially increasing radii.
 */
function calculateRingIndex(rtt, config) {
  if (rtt <= config.innermostRingRadius) return 0;
  
  const ringIndex = Math.ceil(
    Math.log(rtt / config.innermostRingRadius) /
    Math.log(config.ringMultiplicativeFactor)
  );
  
  return Math.min(ringIndex, config.ringsPerNode - 1);
}

/**
 * Returns the ring bounds for a given ring index.
 */
function getRingBounds(index, config) {
  const r = config.ringMultiplicativeFactor;
  return {
    inner: index === 0 ? 1 : Math.pow(r, index - 1),
    outer: index < config.ringsPerNode - 1 
      ? Math.pow(r, index) 
      : Infinity
  };
}
```

### 3.2 RTT Measurement

```javascript
/**
 * Fast path: measure RTT over an existing DataChannel.
 * Uses ping-pong with timing.
 */
async function measureRttOverDataChannel(dataChannel) {
  return new Promise((resolve, reject) => {
    const id = crypto.randomUUID();
    const start = performance.now();
    
    const timeout = setTimeout(() => {
      dataChannel.removeEventListener('message', handler);
      reject(new Error('RTT probe timeout'));
    }, 10000);
    
    function handler(event) {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'pong' && msg.id === id) {
          clearTimeout(timeout);
          dataChannel.removeEventListener('message', handler);
          resolve(performance.now() - start);
        }
      } catch (e) {
        // Ignore malformed messages
      }
    }
    
    dataChannel.addEventListener('message', handler);
    dataChannel.send(JSON.stringify({ type: 'ping', id, t: start }));
  });
}

/**
 * Medium path: establish an ephemeral WebRTC connection to probe
 * a peer we don't yet have a DataChannel with.
 */
async function probePeerViaEphemeralConnection(peerId, signalChannel, config) {
  // Check connection pool first
  const poolEntry = findInConnectionPool(peerId);
  if (poolEntry) {
    poolEntry.lastUsed = Date.now();
    return await measureRttOverDataChannel(poolEntry.dc);
  }
  
  // Create ephemeral connection
  const pc = new RTCPeerConnection({
    iceServers: [{ urls: config.stunServers }]
  });
  
  const dc = pc.createDataChannel('probe-' + crypto.randomUUID());
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pc.close();
      reject(new Error('Ephemeral probe timeout'));
    }, config.ephemeralProbeTimeoutMs);
    
    dc.onopen = async () => {
      try {
        const rtt = await measureRttOverDataChannel(dc);
        
        // Add to connection pool
        addToConnectionPool({
          targetId: peerId,
          pc,
          dc,
          lastUsed: Date.now(),
          createdAt: Date.now()
        });
        
        clearTimeout(timeout);
        resolve(rtt);
      } catch (err) {
        pc.close();
        clearTimeout(timeout);
        reject(err);
      }
    };
    
    // Create offer and send via signaling
    pc.createOffer()
      .then(offer => pc.setLocalDescription(offer))
      .then(() => {
        signalChannel.send(JSON.stringify({
          type: 'probe_offer',
          target: peerId,
          sdp: pc.localDescription,
          senderId: this.peerId
        }));
      })
      .catch(err => {
        pc.close();
        clearTimeout(timeout);
        reject(err);
      });
    
    // Listen for answer on signal channel
    signalChannel.addEventListener('message', function handler(event) {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'probe_answer' && msg.senderId === peerId) {
          signalChannel.removeEventListener('message', handler);
          pc.setRemoteDescription(new RTCSessionDescription(msg.sdp))
            .catch(err => {
              clearTimeout(timeout);
              reject(err);
            });
        }
      } catch (e) { /* ignore */ }
    });
  });
}

/**
 * Slow path: measure RTT to a web server (HTTP target).
 * Uses Image fetch timing as a CORS-friendly approach.
 */
async function probeHttpTarget(url) {
  const start = performance.now();
  
  return new Promise((resolve) => {
    const img = new Image();
    img.onload = () => resolve(performance.now() - start);
    img.onerror = () => resolve(performance.now() - start); // 404 still gives timing
    img.src = url + '/favicon.ico?t=' + start;
  });
}
```

### 3.3 Connection Pool Management

```javascript
function findInConnectionPool(targetId) {
  // Not using 'this' — operates on a pool object
  return this.connectionPool.entries.find(e => e.targetId === targetId) || null;
}

function addToConnectionPool(entry) {
  const pool = this.connectionPool;
  
  // Evict oldest if at capacity
  if (pool.entries.length >= pool.maxSize) {
    pool.entries.sort((a, b) => a.lastUsed - b.lastUsed);
    const oldest = pool.entries.shift();
    oldest.pc.close();
  }
  
  pool.entries.push(entry);
}

function cleanupConnectionPool() {
  const now = Date.now();
  const maxAge = 120000; // 2 minutes
  
  this.connectionPool.entries = this.connectionPool.entries.filter(entry => {
    if (now - entry.lastUsed > maxAge) {
      entry.pc.close();
      return false;
    }
    return true;
  });
}
```

### 3.4 Ring Membership Management

```javascript
/**
 * Adds a peer to the appropriate ring based on measured RTT.
 * If the ring is full, the peer becomes a secondary candidate.
 */
async function addPeerToRing(peerId, dataChannel, rtt) {
  const ringIndex = calculateRingIndex(rtt, this.config);
  const ring = this.rings[ringIndex];
  
  const member = {
    peerId,
    dataChannel,
    rtt,
    lastProbed: Date.now(),
    iceCandidateType: 'host', // Will be updated from ICE stats
    natType: 'unknown',
    isSupernode: false,
    isFirewalled: false,
    joinedAt: Date.now()
  };
  
  if (ring.primaryMembers.length < this.config.nodesPerRing) {
    // Space available in primary
    ring.primaryMembers.push(member);
  } else {
    // Add to secondary pool (FIFO)
    ring.secondaryMembers.push(member);
    if (ring.secondaryMembers.length > this.config.secondaryCandidates) {
      ring.secondaryMembers.shift(); // Remove oldest
    }
    
    // Run hypervolume replacement to see if this peer should be promoted
    this._optimizeRing(ringIndex);
  }
  
  // Store in known peers
  this.knownPeers.set(peerId, {
    peerId,
    dataChannel,
    rtt,
    lastSeen: Date.now(),
    isSupernode: false,
    ringIndex,
    status: 'connected'
  });
  
  // Set up DataChannel message handler
  this._setupDataChannelHandlers(dataChannel, peerId);
}

/**
 * Hypervolume-based ring optimization.
 * Selects the most diverse set of primary members from the
 * union of primary + secondary members.
 * 
 * Uses a simple greedy approach: compute a local coordinate space
 * from RTTs to other ring members, then select the subset that
 * maximizes hypervolume (diversity).
 */
async function _optimizeRing(ringIndex) {
  const ring = this.rings[ringIndex];
  const allCandidates = [...ring.primaryMembers, ...ring.secondaryMembers];
  
  if (allCandidates.length <= this.config.nodesPerRing) {
    // Not enough candidates to optimize
    return;
  }
  
  // Build local coordinate space: for each candidate, create a vector
  // of RTTs to all other candidates
  const coordinates = new Map(); // peerId -> number[]
  
  for (const a of allCandidates) {
    const vec = [];
    for (const b of allCandidates) {
      if (a.peerId === b.peerId) {
        vec.push(0);
      } else {
        vec.push(b.rtt); // Use RTT as coordinate dimension
      }
    }
    coordinates.set(a.peerId, vec);
  }
  
  // Greedy selection: start with all candidates, iteratively drop
  // the one whose removal reduces hypervolume the least
  let selected = allCandidates.map(c => c.peerId);
  
  while (selected.length > this.config.nodesPerRing) {
    let worstPeer = null;
    let smallestReduction = Infinity;
    
    for (const peerId of selected) {
      const without = selected.filter(id => id !== peerId);
      const volWith = this._computeHypervolume(selected, coordinates);
      const volWithout = this._computeHypervolume(without, coordinates);
      const reduction = volWith - volWithout;
      
      if (reduction < smallestReduction) {
        smallestReduction = reduction;
        worstPeer = peerId;
      }
    }
    
    if (worstPeer) {
      selected = selected.filter(id => id !== worstPeer);
    }
  }
  
  // Promote selected peers to primary, demote others to secondary
  const newPrimary = [];
  const newSecondary = [];
  
  for (const candidate of allCandidates) {
    if (selected.includes(candidate.peerId)) {
      newPrimary.push(candidate);
    } else {
      newSecondary.push(candidate);
    }
  }
  
  ring.primaryMembers = newPrimary;
  ring.secondaryMembers = newSecondary.slice(0, this.config.secondaryCandidates);
}

/**
 * Computes the hypervolume of a set of points in the local coordinate space.
 * Uses the product of coordinate ranges as a proxy for hypervolume.
 */
function _computeHypervolume(peerIds, coordinates) {
  if (peerIds.length === 0) return 0;
  if (peerIds.length === 1) return 1;
  
  let volume = 1;
  const dims = coordinates.get(peerIds[0]).length;
  
  for (let d = 0; d < dims; d++) {
    let minVal = Infinity;
    let maxVal = -Infinity;
    
    for (const peerId of peerIds) {
      const val = coordinates.get(peerId)[d];
      if (val < minVal) minVal = val;
      if (val > maxVal) maxVal = val;
    }
    
    volume *= (maxVal - minVal + 1); // +1 to avoid zero
  }
  
  return volume;
}

/**
 * Periodically called to refresh ring membership.
 * Re-measures RTTs and re-evaluates ring placement.
 */
async function refreshRings() {
  for (const ring of this.rings) {
    for (const member of ring.primaryMembers) {
      try {
        const newRtt = await measureRttOverDataChannel(member.dataChannel);
        member.rtt = newRtt;
        member.lastProbed = Date.now();
        
        // Check if peer should move to a different ring
        const correctRing = calculateRingIndex(newRtt, this.config);
        if (correctRing !== ring.index) {
          // Remove from current ring, add to correct ring
          this._moveMember(member, ring.index, correctRing);
        }
      } catch (err) {
        // Peer may be disconnected
        this._handlePeerFailure(member.peerId, ring.index);
      }
    }
    
    // Run ring optimization
    this._optimizeRing(ring.index);
  }
}

function _moveMember(member, fromRingIndex, toRingIndex) {
  const fromRing = this.rings[fromRingIndex];
  const toRing = this.rings[toRingIndex];
  
  // Remove from source ring
  fromRing.primaryMembers = fromRing.primaryMembers.filter(
    m => m.peerId !== member.peerId
  );
  
  // Promote a secondary if available
  if (fromRing.secondaryMembers.length > 0) {
    const promoted = fromRing.secondaryMembers.shift();
    fromRing.primaryMembers.push(promoted);
  }
  
  // Add to target ring
  if (toRing.primaryMembers.length < this.config.nodesPerRing) {
    toRing.primaryMembers.push(member);
  } else {
    toRing.secondaryMembers.push(member);
    this._optimizeRing(toRingIndex);
  }
  
  // Update known peer
  const known = this.knownPeers.get(member.peerId);
  if (known) {
    known.ringIndex = toRingIndex;
  }
}
```

### 3.5 Gossip Protocol

```javascript
/**
 * Anti-entropy push gossip protocol.
 * Each node periodically sends a random sample of its ring members
 * to one random peer from each ring.
 */
async function runGossipCycle() {
  const now = Date.now();
  
  // For each ring, pick one random primary member and send gossip
  for (const ring of this.rings) {
    if (ring.primaryMembers.length === 0) continue;
    
    const target = ring.primaryMembers[
      Math.floor(Math.random() * ring.primaryMembers.length)
    ];
    
    // Build gossip payload: one random peer from each ring
    const ringSamples = {};
    for (const r of this.rings) {
      if (r.primaryMembers.length > 0) {
        const sample = r.primaryMembers[
          Math.floor(Math.random() * r.primaryMembers.length)
        ];
        ringSamples[r.index] = sample.peerId;
      }
    }
    
    try {
      target.dataChannel.send(JSON.stringify({
        type: 'gossip',
        senderId: this.peerId,
        timestamp: now,
        ringSamples
      }));
    } catch (err) {
      // DataChannel may be dead
      this._handlePeerFailure(target.peerId, ring.index);
    }
  }
  
  this.lastGossipTime = now;
}

/**
 * Handles an incoming gossip message.
 * Measures RTT to the sender and to each node in the gossip payload,
 * then adds them to rings as appropriate.
 */
async function handleGossip(message) {
  const senderId = message.senderId;
  
  // Measure RTT to the gossip sender (already have DC)
  try {
    const senderRtt = await measureRttOverDataChannel(
      this.knownPeers.get(senderId).dataChannel
    );
    
    const known = this.knownPeers.get(senderId);
    if (known) {
      known.rtt = senderRtt;
      known.lastSeen = Date.now();
    }
  } catch (err) {
    // Will be handled by ring refresh
  }
  
  // Process each peer in the gossip sample
  for (const [ringIndex, peerId] of Object.entries(message.ringSamples)) {
    if (peerId === this.peerId) continue; // Skip self
    if (this.knownPeers.has(peerId)) {
      // Already known — update last seen
      this.knownPeers.get(peerId).lastSeen = Date.now();
      continue;
    }
    
    // New peer discovered — initiate connection
    if (!this.pendingConnections.has(peerId)) {
      this.pendingConnections.add(peerId);
      this._establishConnectionToPeer(peerId).catch(err => {
        console.warn('Failed to connect to discovered peer:', peerId, err);
      }).finally(() => {
        this.pendingConnections.delete(peerId);
      });
    }
  }
}

/**
 * Establishes a WebRTC connection to a newly discovered peer.
 * Uses the signaling channel for ICE handshake.
 */
async function _establishConnectionToPeer(peerId) {
  const pc = new RTCPeerConnection({
    iceServers: [{ urls: this.config.stunServers }]
  });
  
  const dc = pc.createDataChannel('meridian-' + crypto.randomUUID());
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pc.close();
      reject(new Error('Connection establishment timeout'));
    }, 10000);
    
    dc.onopen = async () => {
      clearTimeout(timeout);
      
      // Measure RTT
      try {
        const rtt = await measureRttOverDataChannel(dc);
        await this.addPeerToRing(peerId, dc, rtt);
        resolve();
      } catch (err) {
        pc.close();
        reject(err);
      }
    };
    
    // Handle ICE candidate gathering
    pc.onicecandidate = (event) => {
      if (event.candidate === null) {
        // All candidates gathered — send offer via signaling
        this.signalChannel.send(JSON.stringify({
          type: 'connect_offer',
          target: peerId,
          senderId: this.peerId,
          sdp: pc.localDescription
        }));
      }
    };
    
    pc.createOffer()
      .then(offer => pc.setLocalDescription(offer))
      .catch(reject);
    
    // Listen for answer
    const answerHandler = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'connect_answer' && msg.senderId === peerId) {
          this.signalChannel.removeEventListener('message', answerHandler);
          pc.setRemoteDescription(new RTCSessionDescription(msg.sdp))
            .catch(reject);
        }
      } catch (e) { /* ignore */ }
    };
    
    this.signalChannel.addEventListener('message', answerHandler);
  });
}
```

### 3.6 Query Routing — Closest Node Discovery

```javascript
/**
 * Entry point: find the closest node to a target.
 * Initiates a multi-hop search through the Meridian overlay.
 */
async function findClosestNode(target, targetType) {
  const queryId = crypto.randomUUID();
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      this.pendingProbes.delete(queryId);
      reject(new Error('Query timeout'));
    }, 30000);
    
    this.pendingProbes.set(queryId, { resolve, reject, timer: timeout });
    
    // Start the query at this node
    this._routeQuery({
      queryId,
      type: 'closest_node',
      target,
      targetType: targetType || 'peer',
      hopCount: 0,
      originator: this.peerId,
      requesterDc: null, // Will be set when we need to respond
      timestamp: Date.now()
    });
  });
}

/**
 * Routes a closest-node query one hop.
 * Measures RTT to target, queries relevant ring members,
 * and forwards to the closest one if progress meets threshold.
 */
async function _routeQuery(query) {
  if (query.hopCount > this.config.maxHops) {
    this._respondToQuery(query, { error: 'Max hops exceeded' });
    return;
  }
  
  // Measure this node's RTT to the target
  let myRtt;
  try {
    myRtt = await this._measureRttToTarget(query.target, query.targetType);
  } catch (err) {
    this._respondToQuery(query, { error: 'Cannot measure target' });
    return;
  }
  
  // Determine which ring to query
  const ringIndex = calculateRingIndex(myRtt, this.config);
  const ring = this.rings[ringIndex];
  
  // Also query adjacent rings (i-1 and i+1) for nodes within range
  const peersToQuery = [];
  
  const ringsToCheck = [ringIndex];
  if (ringIndex > 0) ringsToCheck.push(ringIndex - 1);
  if (ringIndex < this.config.ringsPerNode - 1) ringsToCheck.push(ringIndex + 1);
  
  for (const ri of ringsToCheck) {
    const r = this.rings[ri];
    for (const member of r.primaryMembers) {
      // Only query peers whose distance from us is within
      // [myRtt / 2, myRtt * 2] — they could be closer to the target
      if (member.rtt >= myRtt / 2 && member.rtt <= myRtt * 2) {
        peersToQuery.push(member);
      }
    }
  }
  
  if (peersToQuery.length === 0) {
    // No peers to query — we're the closest known
    this._respondToQuery(query, {
      closestPeerId: this.peerId,
      closestRtt: myRtt,
      hopCount: query.hopCount
    });
    return;
  }
  
  // Ask peers to measure their RTT to the target (in parallel)
  const probeResults = await Promise.allSettled(
    peersToQuery.map(member => this._askPeerToProbe(member, query))
  );
  
  // Find the closest peer from results
  let closestPeer = null;
  let closestRtt = myRtt;
  
  for (const result of probeResults) {
    if (result.status === 'fulfilled' && result.value.rtt < closestRtt) {
      closestRtt = result.value.rtt;
      closestPeer = result.value;
    }
  }
  
  // Check acceptance threshold (β)
  if (closestPeer && closestRtt < myRtt * this.config.routeAcceptanceThreshold) {
    // Forward query to the closer peer
    query.hopCount++;
    closestPeer.dataChannel.send(JSON.stringify({
      type: 'query_forward',
      query
    }));
  } else {
    // No peer made enough progress — we're the closest
    this._respondToQuery(query, {
      closestPeerId: closestPeer ? closestPeer.peerId : this.peerId,
      closestRtt: closestRtt,
      hopCount: query.hopCount
    });
  }
}

/**
 * Asks a ring member to measure RTT to the query target.
 * Sets a timeout: if the peer takes longer than ε * myRtt,
 * the result is discarded (they can't be closer).
 */
async function _askPeerToProbe(member, query) {
  const probeId = crypto.randomUUID();
  
  return new Promise((resolve, reject) => {
    // Timeout: if probe takes > ε * myRtt, peer can't be closer
    const timeout = setTimeout(() => {
      reject(new Error('Probe timeout'));
    }, this.config.probeTimeoutFactor * query._myRtt || 5000);
    
    const handler = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'probe_result' && msg.probeId === probeId) {
          clearTimeout(timeout);
          member.dataChannel.removeEventListener('message', handler);
          resolve({
            peerId: member.peerId,
            rtt: msg.rttMs,
            dataChannel: member.dataChannel
          });
        }
      } catch (e) { /* ignore */ }
    };
    
    member.dataChannel.addEventListener('message', handler);
    member.dataChannel.send(JSON.stringify({
      type: 'probe_request',
      probeId,
      target: query.target,
      targetType: query.targetType,
      queryId: query.queryId
    }));
  });
}

/**
 * Measures RTT to an arbitrary target.
 * Dispatches to the appropriate method based on target type.
 */
async function _measureRttToTarget(target, targetType) {
  switch (targetType) {
    case 'peer': {
      // Check if we have a DC to this peer
      const known = this.knownPeers.get(target);
      if (known && known.dataChannel) {
        return await measureRttOverDataChannel(known.dataChannel);
      }
      // Ephemeral connection
      return await probePeerViaEphemeralConnection(
        target, this.signalChannel, this.config
      );
    }
    case 'http':
    case 'https':
      return await probeHttpTarget(target);
    default:
      throw new Error('Unknown target type: ' + targetType);
  }
}

/**
 * Responds to a query originator with the result.
 */
function _respondToQuery(query, result) {
  result.queryId = query.queryId;
  
  if (query.requesterDc) {
    // Forward response back through the chain
    query.requesterDc.send(JSON.stringify({
      type: 'query_result',
      ...result
    }));
  } else {
    // We're the originator — resolve the pending promise
    const pending = this.pendingProbes.get(query.queryId);
    if (pending) {
      clearTimeout(pending.timer);
      this.pendingProbes.delete(query.queryId);
      pending.resolve(result);
    }
  }
}
```

### 3.7 Query Routing — Central Leader Election

```javascript
/**
 * Finds the node that minimizes average latency to a set of peers.
 * Extends closest-node discovery by using average RTT as the metric.
 */
async function findCentralLeader(peerIds) {
  const queryId = crypto.randomUUID();
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      this.pendingProbes.delete(queryId);
      reject(new Error('Leader election timeout'));
    }, 30000);
    
    this.pendingProbes.set(queryId, { resolve, reject, timer: timeout });
    
    this._routeLeaderQuery({
      queryId,
      type: 'leader_election',
      targets: peerIds,
      hopCount: 0,
      originator: this.peerId,
      requesterDc: null,
      timestamp: Date.now()
    });
  });
}

/**
 * Routes a leader election query.
 * Uses average RTT to all targets as the distance metric.
 */
async function _routeLeaderQuery(query) {
  if (query.hopCount > this.config.maxHops) {
    this._respondToQuery(query, { error: 'Max hops exceeded' });
    return;
  }
  
  // Measure average RTT to all targets
  let myAvgRtt;
  try {
    const rtts = await Promise.all(
      query.targets.map(t => this._measureRttToTarget(t, 'peer'))
    );
    myAvgRtt = rtts.reduce((a, b) => a + b, 0) / rtts.length;
  } catch (err) {
    this._respondToQuery(query, { error: 'Cannot measure targets' });
    return;
  }
  
  // Use average RTT for ring selection
  const ringIndex = calculateRingIndex(myAvgRtt, this.config);
  const ring = this.rings[ringIndex];
  
  // Query ring members (same as closest-node, but with avg RTT)
  const peersToQuery = [];
  const ringsToCheck = [ringIndex];
  if (ringIndex > 0) ringsToCheck.push(ringIndex - 1);
  if (ringIndex < this.config.ringsPerNode - 1) ringsToCheck.push(ringIndex + 1);
  
  for (const ri of ringsToCheck) {
    for (const member of this.rings[ri].primaryMembers) {
      if (member.rtt >= myAvgRtt / 2 && member.rtt <= myAvgRtt * 2) {
        peersToQuery.push(member);
      }
    }
  }
  
  if (peersToQuery.length === 0) {
    this._respondToQuery(query, {
      leaderId: this.peerId,
      avgRtt: myAvgRtt,
      hopCount: query.hopCount
    });
    return;
  }
  
  // Ask peers to measure average RTT to all targets
  const probeResults = await Promise.allSettled(
    peersToQuery.map(member => this._askPeerToProbeAverage(member, query))
  );
  
  let closestPeer = null;
  let closestAvgRtt = myAvgRtt;
  
  for (const result of probeResults) {
    if (result.status === 'fulfilled' && result.value.avgRtt < closestAvgRtt) {
      closestAvgRtt = result.value.avgRtt;
      closestPeer = result.value;
    }
  }
  
  if (closestPeer && closestAvgRtt < myAvgRtt * this.config.routeAcceptanceThreshold) {
    query.hopCount++;
    closestPeer.dataChannel.send(JSON.stringify({
      type: 'leader_query_forward',
      query
    }));
  } else {
    this._respondToQuery(query, {
      leaderId: closestPeer ? closestPeer.peerId : this.peerId,
      avgRtt: closestAvgRtt,
      hopCount: query.hopCount
    });
  }
}

/**
 * Asks a peer to measure average RTT to a set of targets.
 */
async function _askPeerToProbeAverage(member, query) {
  const probeId = crypto.randomUUID();
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Probe timeout')), 5000);
    
    const handler = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'probe_result_avg' && msg.probeId === probeId) {
          clearTimeout(timeout);
          member.dataChannel.removeEventListener('message', handler);
          resolve({
            peerId: member.peerId,
            avgRtt: msg.avgRttMs,
            dataChannel: member.dataChannel
          });
        }
      } catch (e) { /* ignore */ }
    };
    
    member.dataChannel.addEventListener('message', handler);
    member.dataChannel.send(JSON.stringify({
      type: 'probe_request_avg',
      probeId,
      targets: query.targets,
      queryId: query.queryId
    }));
  });
}
```

### 3.8 Query Routing — Multi-Constraint Queries

```javascript
/**
 * Finds nodes that satisfy multiple latency constraints.
 * Each constraint specifies a target and a maximum latency.
 */
async function findNodesSatisfyingConstraints(constraints) {
  const queryId = crypto.randomUUID();
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      this.pendingProbes.delete(queryId);
      reject(new Error('Multi-constraint query timeout'));
    }, 30000);
    
    this.pendingProbes.set(queryId, { resolve, reject, timer: timeout });
    
    this._routeConstraintQuery({
      queryId,
      type: 'multi_constraint',
      constraints,
      hopCount: 0,
      originator: this.peerId,
      requesterDc: null,
      timestamp: Date.now(),
      satisfyingPeers: []
    });
  });
}

/**
 * Routes a multi-constraint query.
 * Distance to solution space = max(0, measured_rtt - max_latency) for each constraint.
 */
async function _routeConstraintQuery(query) {
  if (query.hopCount > this.config.maxHops) {
    this._respondToQuery(query, { error: 'Max hops exceeded' });
    return;
  }
  
  // Measure RTT to each constraint target
  const measurements = [];
  for (const constraint of query.constraints) {
    try {
      const rtt = await this._measureRttToTarget(constraint.target, 'peer');
      measurements.push({ target: constraint.target, rtt, maxLatency: constraint.maxLatencyMs });
    } catch (err) {
      measurements.push({ target: constraint.target, rtt: Infinity, maxLatency: constraint.maxLatencyMs });
    }
  }
  
  // Calculate distance to solution space
  // distance = max(0, rtt - maxLatency) for each constraint
  // totalDistance = sum of individual distances
  let totalDistance = 0;
  let satisfiesAll = true;
  
  for (const m of measurements) {
    const distance = Math.max(0, m.rtt - m.maxLatency);
    totalDistance += distance;
    if (distance > 0) satisfiesAll = false;
  }
  
  // If this node satisfies all constraints, add to results
  if (satisfiesAll) {
    query.satisfyingPeers.push(this.peerId);
    
    // If we have enough results, respond early
    if (query.satisfyingPeers.length >= 5) {
      this._respondToQuery(query, {
        satisfyingPeers: query.satisfyingPeers,
        hopCount: query.hopCount
      });
      return;
    }
  }
  
  // Find peers close to the solution space
  const peersToQuery = [];
  for (const ring of this.rings) {
    for (const member of ring.primaryMembers) {
      // Check if this peer is within range of at least one constraint
      for (const m of measurements) {
        const range = m.maxLatency;
        if (member.rtt >= range / 2 && member.rtt <= range * 2) {
          peersToQuery.push(member);
          break;
        }
      }
    }
  }
  
  if (peersToQuery.length === 0) {
    this._respondToQuery(query, {
      satisfyingPeers: query.satisfyingPeers,
      hopCount: query.hopCount
    });
    return;
  }
  
  // Ask peers to check constraints
  const probeResults = await Promise.allSettled(
    peersToQuery.map(member => this._askPeerToCheckConstraints(member, query))
  );
  
  // Collect satisfying peers from results
  for (const result of probeResults) {
    if (result.status === 'fulfilled' && result.value.satisfiesAll) {
      if (!query.satisfyingPeers.includes(result.value.peerId)) {
        query.satisfyingPeers.push(result.value.peerId);
      }
    }
  }
  
  // Find the peer closest to the solution space for forwarding
  let bestPeer = null;
  let bestDistance = totalDistance;
  
  for (const result of probeResults) {
    if (result.status === 'fulfilled' && result.value.totalDistance < bestDistance) {
      bestDistance = result.value.totalDistance;
      bestPeer = result.value;
    }
  }
  
  if (bestPeer && bestDistance < totalDistance * this.config.routeAcceptanceThreshold) {
    query.hopCount++;
    bestPeer.dataChannel.send(JSON.stringify({
      type: 'constraint_query_forward',
      query
    }));
  } else {
    this._respondToQuery(query, {
      satisfyingPeers: query.satisfyingPeers,
      hopCount: query.hopCount
    });
  }
}
```

---

## 4. WebRTC Signaling Protocol

### 4.1 Signaling Server (Minimal)

```javascript
/**
 * WebSocket-based signaling server.
 * Only handles: ICE handshake relay + bootstrap peer list.
 * 
 * Server endpoints (WebSocket messages):
 * - register: { type: 'register', peerId: string }
 * - connect_offer: { type: 'connect_offer', target: string, senderId: string, sdp: object }
 * - connect_answer: { type: 'connect_answer', target: string, senderId: string, sdp: object }
 * - probe_offer: { type: 'probe_offer', target: string, senderId: string, sdp: object }
 * - probe_answer: { type: 'probe_answer', target: string, senderId: string, sdp: object }
 * - get_peers: { type: 'get_peers', senderId: string }
 * - peers_list: { type: 'peers_list', peers: string[] }
 * - ice_candidate: { type: 'ice_candidate', target: string, senderId: string, candidate: object }
 */

// Server-side logic (Node.js example)
class SignalingServer {
  constructor() {
    this.peers = new Map(); // peerId -> WebSocket
  }
  
  handleMessage(ws, message) {
    switch (message.type) {
      case 'register':
        this.peers.set(message.peerId, ws);
        ws.peerId = message.peerId;
        break;
        
      case 'connect_offer':
      case 'probe_offer':
        // Forward to target
        const targetWs = this.peers.get(message.target);
        if (targetWs) {
          targetWs.send(JSON.stringify(message));
        }
        break;
        
      case 'connect_answer':
      case 'probe_answer':
        const originWs = this.peers.get(message.target);
        if (originWs) {
          originWs.send(JSON.stringify(message));
        }
        break;
        
      case 'ice_candidate':
        const candTarget = this.peers.get(message.target);
        if (candTarget) {
          candTarget.send(JSON.stringify(message));
        }
        break;
        
      case 'get_peers':
        const peerList = Array.from(this.peers.keys())
          .filter(id => id !== message.senderId);
        ws.send(JSON.stringify({
          type: 'peers_list',
          peers: peerList.slice(0, 20) // Return up to 20 peers
        }));
        break;
        
      case 'disconnect':
        this.peers.delete(message.peerId);
        break;
    }
  }
  
  handleDisconnect(ws) {
    if (ws.peerId) {
      this.peers.delete(ws.peerId);
    }
  }
}
```

### 4.2 Client-Side Signaling Integration

```javascript
/**
 * Initializes the signaling channel and bootstraps into the Meridian overlay.
 */
async function bootstrap(signalServerUrl) {
  const ws = new WebSocket(signalServerUrl);
  
  return new Promise((resolve, reject) => {
    ws.onopen = () => {
      // Register with signaling server
      ws.send(JSON.stringify({
        type: 'register',
        peerId: this.peerId
      }));
      
      // Request initial peer list
      ws.send(JSON.stringify({
        type: 'get_peers',
        senderId: this.peerId
      }));
    };
    
    ws.onmessage = async (event) => {
      const msg = JSON.parse(event.data);
      
      switch (msg.type) {
        case 'peers_list':
          // Bootstrap: connect to discovered peers
          for (const peerId of msg.peers) {
            if (!this.knownPeers.has(peerId) && !this.pendingConnections.has(peerId)) {
              this.pendingConnections.add(peerId);
              this._establishConnectionToPeer(peerId).catch(() => {});
            }
          }
          resolve();
          break;
          
        case 'connect_offer':
          if (msg.target === this.peerId) {
            await this._handleIncomingConnection(msg);
          }
          break;
          
        case 'connect_answer':
          if (msg.target === this.peerId) {
            await this._handleConnectionAnswer(msg);
          }
          break;
          
        case 'probe_offer':
          if (msg.target === this.peerId) {
            await this._handleProbeOffer(msg);
          }
          break;
          
        case 'probe_answer':
          if (msg.target === this.peerId) {
            await this._handleProbeAnswer(msg);
          }
          break;
          
        case 'ice_candidate':
          if (msg.target === this.peerId) {
            await this._handleIceCandidate(msg);
          }
          break;
      }
    };
    
    ws.onerror = reject;
    
    // Store signal channel
    this.signalChannel = ws;
  });
}

/**
 * Handles an incoming connection offer from another peer.
 */
async function _handleIncomingConnection(msg) {
  const pc = new RTCPeerConnection({
    iceServers: [{ urls: this.config.stunServers }]
  });
  
  // Store for later use
  this._pendingPeerConnections = this._pendingPeerConnections || new Map();
  this._pendingPeerConnections.set(msg.senderId, pc);
  
  pc.ondatachannel = (event) => {
    const dc = event.channel;
    
    dc.onopen = async () => {
      const rtt = await measureRttOverDataChannel(dc);
      await this.addPeerToRing(msg.senderId, dc, rtt);
      this._pendingPeerConnections.delete(msg.senderId);
    };
  };
  
  pc.onicecandidate = (event) => {
    if (event.candidate === null) {
      this.signalChannel.send(JSON.stringify({
        type: 'connect_answer',
        target: msg.senderId,
        senderId: this.peerId,
        sdp: pc.localDescription
      }));
    } else if (event.candidate) {
      this.signalChannel.send(JSON.stringify({
        type: 'ice_candidate',
        target: msg.senderId,
        senderId: this.peerId,
        candidate: event.candidate
      }));
    }
  };
  
  await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
  await pc.createAnswer();
  await pc.setLocalDescription(pc.localDescription);
}
```

---

## 5. Supernode Election and Raft Consensus

### 5.1 Supernode Election via Meridian

```javascript
/**
 * Elects a supernode for a cluster of peers using Meridian's
 * central leader election protocol.
 * 
 * The elected supernode is the peer that minimizes average
 * latency to all peers in the cluster.
 */
async function electSupernode(clusterPeers) {
  // Use Meridian's leader election to find the central node
  const result = await this.findCentralLeader(clusterPeers);
  
  if (result.leaderId === this.peerId) {
    // We are the elected supernode
    this.isSupernode = true;
    this.clusterLeader = this.peerId;
    
    // Initialize Raft state
    this._initRaftState(clusterPeers);
    
    // Announce to cluster
    this._broadcastToCluster({
      type: 'supernode_elected',
      supernodeId: this.peerId,
      clusterPeers
    });
    
    if (this.handlers.onSupernodeElected) {
      this.handlers.onSupernodeElected(this.peerId);
    }
  } else {
    // Another peer was elected — connect to them as our supernode
    this.clusterLeader = result.leaderId;
    this.isSupernode = false;
    
    // The supernode will establish a DataChannel to us
  }
  
  return result;
}

/**
 * Broadcasts a message to all peers in the cluster via the overlay.
 */
function _broadcastToCluster(message) {
  for (const ring of this.rings) {
    for (const member of ring.primaryMembers) {
      try {
        member.dataChannel.send(JSON.stringify(message));
      } catch (e) {
        // Ignore failed sends
      }
    }
  }
}
```

### 5.2 Raft Consensus Implementation

```javascript
/**
 * Initializes Raft state for a supernode cluster.
 * Called when this node is elected as a supernode.
 */
function _initRaftState(clusterMembers) {
  this.supernodeCluster = {
    clusterId: crypto.randomUUID(),
    members: clusterMembers,
    leaderId: this.peerId, // We start as leader (just elected)
    
    // Persistent state
    currentTerm: 0,
    votedFor: null,
    log: [],
    
    // Volatile state
    commitIndex: 0,
    lastApplied: 0,
    
    // Leader state
    nextIndex: new Map(),
    matchIndex: new Map(),
    
    // Timers
    electionTimeoutMs: 150 + Math.floor(Math.random() * 150),
    heartbeatIntervalMs: 50,
    
    // Internal
    raftState: 'leader',
    electionTimer: null,
    heartbeatTimer: null
  };
  
  // Initialize nextIndex and matchIndex for each follower
  for (const member of clusterMembers) {
    if (member !== this.peerId) {
      this.supernodeCluster.nextIndex.set(member, 0);
      this.supernodeCluster.matchIndex.set(member, 0);
    }
  }
  
  // Start heartbeats
  this._startHeartbeats();
}

/**
 * Starts sending heartbeat AppendEntries RPCs to followers.
 */
function _startHeartbeats() {
  const cluster = this.supernodeCluster;
  if (!cluster) return;
  
  cluster.heartbeatTimer = setInterval(() => {
    if (cluster.raftState !== 'leader') {
      clearInterval(cluster.heartbeatTimer);
      return;
    }
    
    for (const member of cluster.members) {
      if (member === this.peerId) continue;
      
      const nextIdx = cluster.nextIndex.get(member) || 0;
      const entries = cluster.log.slice(nextIdx);
      
      const knownPeer = this.knownPeers.get(member);
      if (!knownPeer || !knownPeer.dataChannel) continue;
      
      try {
        knownPeer.dataChannel.send(JSON.stringify({
          type: 'raft_append_entries',
          term: cluster.currentTerm,
          leaderId: this.peerId,
          prevLogIndex: nextIdx - 1,
          prevLogTerm: nextIdx > 0 ? cluster.log[nextIdx - 1].term : 0,
          entries,
          leaderCommit: cluster.commitIndex
        }));
      } catch (e) {
        // Connection may be dead
      }
    }
  }, cluster.heartbeatIntervalMs);
}

/**
 * Handles an incoming AppendEntries RPC (heartbeat or log replication).
 */
function _handleAppendEntries(msg) {
  const cluster = this.supernodeCluster;
  if (!cluster) return;
  
  // Reply false if term < currentTerm
  if (msg.term < cluster.currentTerm) {
    this._sendRaftResponse(msg.leaderId, {
      type: 'raft_append_entries_response',
      term: cluster.currentTerm,
      success: false,
      lastLogIndex: cluster.log.length - 1
    });
    return;
  }
  
  // Update term if necessary
  if (msg.term > cluster.currentTerm) {
    cluster.currentTerm = msg.term;
    cluster.raftState = 'follower';
    cluster.votedFor = null;
  }
  
  // Reset election timeout
  this._resetElectionTimeout();
  
  // Recognize leader
  cluster.leaderId = msg.leaderId;
  
  // Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
  if (msg.prevLogIndex >= 0) {
    if (msg.prevLogIndex >= cluster.log.length) {
      this._sendRaftResponse(msg.leaderId, {
        type: 'raft_append_entries_response',
        term: cluster.currentTerm,
        success: false,
        lastLogIndex: cluster.log.length - 1
      });
      return;
    }
    
    if (cluster.log[msg.prevLogIndex].term !== msg.prevLogTerm) {
      // Delete conflicting entry and all that follow
      cluster.log = cluster.log.slice(0, msg.prevLogIndex);
      this._sendRaftResponse(msg.leaderId, {
        type: 'raft_append_entries_response',
        term: cluster.currentTerm,
        success: false,
        lastLogIndex: cluster.log.length - 1
      });
      return;
    }
  }
  
  // Append any new entries not already in the log
  for (const entry of msg.entries) {
    const idx = entry.index;
    if (idx < cluster.log.length) {
      if (cluster.log[idx].term !== entry.term) {
        cluster.log = cluster.log.slice(0, idx);
        cluster.log.push(entry);
      }
    } else {
      cluster.log.push(entry);
    }
  }
  
  // Update commitIndex
  if (msg.leaderCommit > cluster.commitIndex) {
    cluster.commitIndex = Math.min(msg.leaderCommit, cluster.log.length - 1);
  }
  
  // Apply committed entries
  this._applyCommittedEntries();
  
  // Reply success
  this._sendRaftResponse(msg.leaderId, {
    type: 'raft_append_entries_response',
    term: cluster.currentTerm,
    success: true,
    lastLogIndex: cluster.log.length - 1
  });
}

/**
 * Starts an election when the election timeout fires.
 */
function _startElection() {
  const cluster = this.supernodeCluster;
  if (!cluster) return;
  
  cluster.raftState = 'candidate';
  cluster.currentTerm++;
  cluster.votedFor = this.peerId;
  
  let votesReceived = 1; // Vote for self
  const majority = Math.floor(cluster.members.length / 2) + 1;
  
  // Request votes from all other members
  for (const member of cluster.members) {
    if (member === this.peerId) continue;
    
    const lastLogIndex = cluster.log.length - 1;
    const lastLogTerm = lastLogIndex >= 0 ? cluster.log[lastLogIndex].term : 0;
    
    const knownPeer = this.knownPeers.get(member);
    if (!knownPeer || !knownPeer.dataChannel) continue;
    
    knownPeer.dataChannel.send(JSON.stringify({
      type: 'raft_request_vote',
      term: cluster.currentTerm,
      candidateId: this.peerId,
      lastLogIndex,
      lastLogTerm
    }));
  }
  
  // Set up response handler
  const voteHandler = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'raft_request_vote_response' && msg.term === cluster.currentTerm) {
        if (msg.voteGranted) {
          votesReceived++;
          if (votesReceived >= majority) {
            // We won the election
            cluster.raftState = 'leader';
            cluster.leaderId = this.peerId;
            
            // Initialize leader state
            for (const m of cluster.members) {
              if (m !== this.peerId) {
                cluster.nextIndex.set(m, cluster.log.length);
                cluster.matchIndex.set(m, 0);
              }
            }
            
            // Start heartbeats
            this._startHeartbeats();
            
            // Notify application
            if (this.handlers.onSupernodeElected) {
              this.handlers.onSupernodeElected(this.peerId);
            }
          }
        }
      }
    } catch (e) { /* ignore */ }
  };
  
  // Listen for vote responses (clean up after timeout)
  setTimeout(() => {
    // Remove handler logic would go here
  }, cluster.electionTimeoutMs);
}

/**
 * Applies committed log entries to the state machine.
 */
function _applyCommittedEntries() {
  const cluster = this.supernodeCluster;
  if (!cluster) return;
  
  while (cluster.lastApplied < cluster.commitIndex) {
    cluster.lastApplied++;
    const entry = cluster.log[cluster.lastApplied];
    
    switch (entry.command.type) {
      case 'cluster_membership':
        if (entry.command.action === 'join') {
          if (!cluster.members.includes(entry.command.peerId)) {
            cluster.members.push(entry.command.peerId);
          }
        } else if (entry.command.action === 'leave') {
          cluster.members = cluster.members.filter(
            id => id !== entry.command.peerId
          );
        }
        break;
        
      case 'stream_metadata':
        // Update stream metadata in local state
        this._updateStreamMetadata(entry.command.streamId, entry.command.metadata);
        break;
        
      case 'topology_change':
        // Handle topology change
        this._handleTopologyChange(entry.command.change);
        break;
    }
  }
}
```

---

## 6. Streaming Integration

### 6.1 Media Connection Establishment

```javascript
/**
 * Establishes a media stream to a peer discovered via Meridian query.
 * Called after findClosestNode() returns a result.
 */
async function establishMediaStream(targetPeerId) {
  const knownPeer = this.knownPeers.get(targetPeerId);
  if (!knownPeer || !knownPeer.dataChannel) {
    throw new Error('Target peer not connected');
  }
  
  const pc = new RTCPeerConnection({
    iceServers: [
      { urls: this.config.stunServers },
      ...this.config.turnServers
    ]
  });
  
  // Add local media stream
  if (this.uplinkStream) {
    for (const track of this.uplinkStream.getTracks()) {
      pc.addTrack(track, this.uplinkStream);
    }
  }
  
  // Handle incoming media
  pc.ontrack = (event) => {
    this.activeStreams.set(targetPeerId, event.streams[0]);
  };
  
  // Create offer and send via existing DataChannel
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  
  knownPeer.dataChannel.send(JSON.stringify({
    type: 'media_offer',
    senderId: this.peerId,
    sdp: pc.localDescription
  }));
  
  // Listen for answer on the DataChannel
  return new Promise((resolve) => {
    const handler = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'media_answer' && msg.senderId === targetPeerId) {
          knownPeer.dataChannel.removeEventListener('message', handler);
          pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
          resolve(pc);
        }
      } catch (e) { /* ignore */ }
    };
    
    knownPeer.dataChannel.addEventListener('message', handler);
  });
}

/**
 * Handles an incoming media offer from another peer.
 */
async function _handleMediaOffer(msg) {
  const pc = new RTCPeerConnection({
    iceServers: [
      { urls: this.config.stunServers },
      ...this.config.turnServers
    ]
  });
  
  // Add local media stream
  if (this.uplinkStream) {
    for (const track of this.uplinkStream.getTracks()) {
      pc.addTrack(track, this.uplinkStream);
    }
  }
  
  // Handle incoming media
  pc.ontrack = (event) => {
    this.activeStreams.set(msg.senderId, event.streams[0]);
  };
  
  await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  
  // Send answer via existing DataChannel
  const knownPeer = this.knownPeers.get(msg.senderId);
  if (knownPeer && knownPeer.dataChannel) {
    knownPeer.dataChannel.send(JSON.stringify({
      type: 'media_answer',
      senderId: this.peerId,
      sdp: pc.localDescription
    }));
  }
}
```

### 6.2 Supernode Media Forwarding

```javascript
/**
 * Supernode: forwards media from one peer to others in the cluster.
 * Acts as an SFU (Selective Forwarding Unit).
 */
function _setupMediaForwarding() {
  if (!this.isSupernode) return;
  
  // When we receive a new media stream, forward to all other cluster members
  const originalOntrack = this._ontrack;
  this._ontrack = (peerId, stream) => {
    if (originalOntrack) originalOntrack(peerId, stream);
    
    // Forward to all other cluster members
    for (const member of this.supernodeCluster.members) {
      if (member === peerId || member === this.peerId) continue;
      
      const knownPeer = this.knownPeers.get(member);
      if (!knownPeer || !knownPeer.dataChannel) continue;
      
      // Signal the peer to expect a forwarded stream
      knownPeer.dataChannel.send(JSON.stringify({
        type: 'forwarded_stream',
        sourcePeerId: peerId,
        streamId: stream.id
      }));
    }
  };
}
```

---

## 7. DataChannel Message Protocol

### 7.1 Complete Message Types

```javascript
const MESSAGE_TYPES = {
  // RTT measurement
  PING: 'ping',
  PONG: 'pong',
  
  // Gossip
  GOSSIP: 'gossip',
  
  // Query routing
  QUERY_FORWARD: 'query_forward',
  LEADER_QUERY_FORWARD: 'leader_query_forward',
  CONSTRAINT_QUERY_FORWARD: 'constraint_query_forward',
  PROBE_REQUEST: 'probe_request',
  PROBE_RESULT: 'probe_result',
  PROBE_REQUEST_AVG: 'probe_request_avg',
  PROBE_RESULT_AVG: 'probe_result_avg',
  PROBE_REQUEST_CONSTRAINTS: 'probe_request_constraints',
  PROBE_RESULT_CONSTRAINTS: 'probe_result_constraints',
  QUERY_RESULT: 'query_result',
  
  // Media
  MEDIA_OFFER: 'media_offer',
  MEDIA_ANSWER: 'media_answer',
  FORWARDED_STREAM: 'forwarded_stream',
  MEDIA_CLOSE: 'media_close',
  
  // Supernode / Raft
  SUPERNODE_ELECTED: 'supernode_elected',
  RAFT_APPEND_ENTRIES: 'raft_append_entries',
  RAFT_APPEND_ENTRIES_RESPONSE: 'raft_append_entries_response',
  RAFT_REQUEST_VOTE: 'raft_request_vote',
  RAFT_REQUEST_VOTE_RESPONSE: 'raft_request_vote_response',
  
  // Peer management
  PEER_LEAVING: 'peer_leaving',
  PEER_STATUS: 'peer_status'
};
```

### 7.2 DataChannel Message Handler Setup

```javascript
function _setupDataChannelHandlers(dataChannel, peerId) {
  dataChannel.addEventListener('message', (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch (e) {
      return; // Ignore malformed messages
    }
    
    switch (msg.type) {
      // RTT measurement
      case 'ping':
        dataChannel.send(JSON.stringify({
          type: 'pong',
          id: msg.id,
          t: msg.t
        }));
        break;
        
      case 'pong':
        // Handled by the caller that initiated the ping
        break;
      
      // Gossip
      case 'gossip':
        this.handleGossip(msg);
        break;
      
      // Query routing
      case 'query_forward':
        this._routeQuery(msg.query);
        break;
        
      case 'leader_query_forward':
        this._routeLeaderQuery(msg.query);
        break;
        
      case 'constraint_query_forward':
        this._routeConstraintQuery(msg.query);
        break;
        
      case 'probe_request':
        this._handleProbeRequest(msg, dataChannel);
        break;
        
      case 'probe_request_avg':
        this._handleProbeRequestAvg(msg, dataChannel);
        break;
        
      case 'probe_request_constraints':
        this._handleProbeRequestConstraints(msg, dataChannel);
        break;
        
      case 'query_result':
        this._handleQueryResult(msg);
        break;
      
      // Media
      case 'media_offer':
        this._handleMediaOffer(msg);
        break;
        
      case 'media_answer':
        // Handled by the caller that initiated the offer
        break;
        
      case 'forwarded_stream':
        this._handleForwardedStream(msg);
        break;
        
      case 'media_close':
        this._handleMediaClose(msg);
        break;
      
      // Supernode / Raft
      case 'supernode_elected':
        this._handleSupernodeElected(msg);
        break;
        
      case 'raft_append_entries':
        this._handleAppendEntries(msg);
        break;
        
      case 'raft_append_entries_response':
        this._handleAppendEntriesResponse(msg);
        break;
        
      case 'raft_request_vote':
        this._handleRequestVote(msg, dataChannel);
        break;
        
      case 'raft_request_vote_response':
        // Handled by election initiator
        break;
      
      // Peer management
      case 'peer_leaving':
        this._handlePeerLeaving(msg.senderId);
        break;
        
      case 'peer_status':
        this._handlePeerStatus(msg);
        break;
    }
  });
  
  dataChannel.addEventListener('close', () => {
    this._handlePeerFailure(peerId);
  });
}
```

---

## 8. Failure Detection and Recovery

```javascript
/**
 * Handles a peer failure detected via DataChannel close or probe timeout.
 */
function _handlePeerFailure(peerId) {
  // Remove from all rings
  for (const ring of this.rings) {
    ring.primaryMembers = ring.primaryMembers.filter(
      m => m.peerId !== peerId
    );
    ring.secondaryMembers = ring.secondaryMembers.filter(
      m => m.peerId !== peerId
    );
  }
  
  // Promote secondary candidates
  for (const ring of this.rings) {
    while (ring.primaryMembers.length < this.config.nodesPerRing 
           && ring.secondaryMembers.length > 0) {
      const promoted = ring.secondaryMembers.shift();
      ring.primaryMembers.push(promoted);
    }
  }
  
  // Update known peers
  const known = this.knownPeers.get(peerId);
  if (known) {
    known.status = 'failed';
    known.dataChannel = null;
  }
  
  // If this was our supernode, trigger re-election
  if (peerId === this.clusterLeader && !this.isSupernode) {
    this._triggerSupernodeReelection();
  }
  
  // If this was a streaming partner, find a replacement
  if (this.activeStreams.has(peerId)) {
    this._replaceStreamingPartner(peerId);
  }
  
  // If this was a supernode cluster member, update Raft
  if (this.isSupernode && this.supernodeCluster) {
    this.supernodeCluster.members = this.supernodeCluster.members.filter(
      id => id !== peerId
    );
    
    // Replicate membership change via Raft
    this._replicateCommand({
      type: 'cluster_membership',
      action: 'leave',
      peerId
    });
  }
  
  // Notify application
  if (this.handlers.onPeerDisconnected) {
    this.handlers.onPeerDisconnected(peerId);
  }
}

/**
 * Triggers a new supernode election when the current supernode fails.
 */
async function _triggerSupernodeReelection() {
  // Get the cluster members from the last known state
  const clusterPeers = this.supernodeCluster 
    ? this.supernodeCluster.members 
    : Array.from(this.knownPeers.keys()).slice(0, 20);
  
  // Remove the failed leader
  const filteredPeers = clusterPeers.filter(id => id !== this.clusterLeader);
  
  // Run Meridian leader election
  const result = await this.findCentralLeader(filteredPeers);
  
  if (result.leaderId === this.peerId) {
    this.isSupernode = true;
    this.clusterLeader = this.peerId;
    this._initRaftState(filteredPeers);
  } else {
    this.clusterLeader = result.leaderId;
  }
}

/**
 * Replaces a failed streaming partner with the next closest peer.
 */
async function _replaceStreamingPartner(failedPeerId) {
  // Find the closest peer (excluding the failed one)
  const result = await this.findClosestNode(failedPeerId, 'peer');
  
  if (result.closestPeerId && result.closestPeerId !== failedPeerId) {
    // Establish new media stream
    await this.establishMediaStream(result.closestPeerId);
    
    // Close old stream
    const oldStream = this.activeStreams.get(failedPeerId);
    if (oldStream) {
      oldStream.getTracks().forEach(t => t.stop());
      this.activeStreams.delete(failedPeerId);
    }
  }
}
```

---

## 9. Initialization and Lifecycle

```javascript
/**
 * Complete initialization sequence for a Meridian-WebRTC node.
 */
async function initialize(signalServerUrl, mediaStream) {
  // 1. Generate peer identity
  this.peerId = crypto.randomUUID();
  this.uplinkStream = mediaStream;
  
  // 2. Connect to signaling server
  await this.bootstrap(signalServerUrl);
  
  // 3. Start gossip protocol
  this.gossipInterval = setInterval(
    () => this.runGossipCycle(),
    this.config.gossipPeriodMs
  );
  
  // 4. Start ring maintenance
  this.ringReplacementInterval = setInterval(
    () => this.refreshRings(),
    this.config.ringReplacementPeriodMs
  );
  
  // 5. Start connection pool cleanup
  setInterval(
    () => this.cleanupConnectionPool(),
    60000 // Every minute
  );
  
  // 6. After initial ring population, attempt supernode election
  setTimeout(async () => {
    if (this.knownPeers.size >= 5) {
      const clusterPeers = Array.from(this.knownPeers.keys()).slice(0, 20);
      await this.electSupernode(clusterPeers);
    }
  }, 10000); // Wait 10s for initial connections
  
  console.log(`Meridian node initialized: ${this.peerId}`);
  return this.peerId;
}

/**
 * Graceful shutdown.
 */
function shutdown() {
  // Broadcast departure
  this._broadcastToCluster({
    type: 'peer_leaving',
    senderId: this.peerId
  });
  
  // Close all DataChannels
  for (const ring of this.rings) {
    for (const member of ring.primaryMembers) {
      try { member.dataChannel.close(); } catch (e) { /* ignore */ }
    }
    for (const member of ring.secondaryMembers) {
      try { member.dataChannel.close(); } catch (e) { /* ignore */ }
    }
  }
  
  // Close connection pool
  for (const entry of this.connectionPool.entries) {
    try { entry.pc.close(); } catch (e) { /* ignore */ }
  }
  
  // Close media streams
  for (const stream of this.activeStreams.values()) {
    stream.getTracks().forEach(t => t.stop());
  }
  
  // Clear timers
  if (this.gossipInterval) clearInterval(this.gossipInterval);
  if (this.ringReplacementInterval) clearInterval(this.ringReplacementInterval);
  if (this.supernodeCluster) {
    if (this.supernodeCluster.heartbeatTimer) {
      clearInterval(this.supernodeCluster.heartbeatTimer);
    }
    if (this.supernodeCluster.electionTimer) {
      clearTimeout(this.supernodeCluster.electionTimer);
    }
  }
  
  // Close signaling channel
  if (this.signalChannel) {
    this.signalChannel.close();
  }
  
  console.log(`Meridian node shutdown: ${this.peerId}`);
}
```

---

## 10. Usage Example

```javascript
// Application entry point
async function main() {
  // Get user media
  const mediaStream = await navigator.mediaDevices.getUserMedia({
    video: true,
    audio: true
  });
  
  // Create Meridian node
  const node = new MeridianNode(
    crypto.randomUUID(),
    null, // signalChannel will be set during bootstrap
    MERIDIAN_CONFIG
  );
  
  // Set up event handlers
  node.handlers.onSupernodeElected = (supernodeId) => {
    console.log('Supernode elected:', supernodeId);
    if (supernodeId === node.peerId) {
      document.getElementById('status').textContent = 'I am the supernode!';
    }
  };
  
  node.handlers.onPeerDisconnected = (peerId) => {
    console.log('Peer disconnected:', peerId);
  };
  
  // Initialize
  await node.initialize('wss://signaling.example.com', mediaStream);
  
  // Find closest peer and stream to them
  const result = await node.findClosestNode('some-peer-id', 'peer');
  if (result.closestPeerId) {
    await node.establishMediaStream(result.closestPeerId);
  }
  
  // Find a central leader for a group
  const leaderResult = await node.findCentralLeader([
    'peer-a', 'peer-b', 'peer-c', 'peer-d'
  ]);
  console.log('Central leader:', leaderResult.leaderId);
  
  // Find nodes satisfying constraints
  const constraintResult = await node.findNodesSatisfyingConstraints([
    { target: 'media-server-1', maxLatencyMs: 50 },
    { target: 'media-server-2', maxLatencyMs: 30 }
  ]);
  console.log('Satisfying peers:', constraintResult.satisfyingPeers);
}
```

---

## 11. Key Properties (From Meridian Paper)

Property	Guarantee	How Achieved
**Closest node accuracy**	Median error ~2ms (vs 12-18ms for embedding)	Direct measurement, no coordinate error
**Query latency**	~300ms, constant with system size	Logarithmic hop count
**Scalability**	O(log N) hops	Exponentially increasing ring radii
**Load balance**	In-degree ratio < 2 for 90% of nodes	Stochastic ring independence + hypervolume optimization
**Failure recovery**	< 1 gossip period (~30s)	DataChannel close detection + secondary promotion
**Supernode election**	Minimizes avg latency to group	Meridian central leader election
**Browser compatibility**	Full	All probing via WebRTC (DC ping + ephemeral ICE)

---

## 12. Implementation Order (Recommended)

1. **Phase 1 — Core Overlay**
   - DataChannel connection establishment via signaling
   - RTT measurement (ping/pong)
   - Ring data structure and peer placement
   - Basic gossip protocol

2. **Phase 2 — Query Routing**
   - Closest node discovery
   - Leader election
   - Multi-constraint queries

3. **Phase 3 — Ring Optimization**
   - Hypervolume-based ring replacement
   - Periodic ring refresh
   - Connection pool management

4. **Phase 4 — Supernode / Raft**
   - Supernode election via Meridian
   - Raft consensus for control plane
   - Cluster membership management

5. **Phase 5 — Streaming**
   - MediaTrack establishment over WebRTC
   - Supernode media forwarding (SFU)
   - Failure recovery and re-streaming

6. **Phase 6 — Production Hardening**
   - NAT type classification
   - TURN server discovery via multi-constraint queries
   - ICE restart handling
   - Connection limits and backpressure
