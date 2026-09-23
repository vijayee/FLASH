import 'dart:async';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:logging/logging.dart';

import '../config/meridian_config.dart';
import '../overlay/meridian_node.dart';
import '../utils/crypto_utils.dart';
import 'rtc_utils.dart';

/// A WebRTC handshake in flight to one remote peer, holding ICE candidates
/// that trickled in before the remote description was set.
class PendingConnection {
  final rtc.RTCPeerConnection pc;
  final List<rtc.RTCIceCandidate> pendingCandidates = [];
  bool remoteDescriptionSet = false;
  Timer? timeout;

  PendingConnection(this.pc);
}

/// RTCPeerConnection lifecycle (spec §6.1): assembly with ICE servers from
/// config, the offerer's full connect handshake (with glare resolution on
/// the answering side), and answering of connect and probe offers.
class PeerConnectionManager {
  static final Logger _logger = Logger('flash_webrtc.pc');

  final MeridianNode node;

  PeerConnectionManager(this.node);

  /// Creates a PeerConnection with ICE servers from config (STUN collapsed
  /// into one entry, TURN appended).
  Future<rtc.RTCPeerConnection> createConnection() {
    return rtc
        .createPeerConnection({'iceServers': buildIceServers(node.config)});
  }

  /// Creates a short-lived connection for probing (same ICE configuration).
  Future<rtc.RTCPeerConnection> createEphemeralConnection() =>
      createConnection();

  /// Offerer side of a full connect handshake: create the connection,
  /// trickle our ICE candidates, send the offer at once, and integrate the
  /// peer once the channel opens.
  ///
  /// One live connection per peer: refuses to dial over an existing open or
  /// in-flight connection (glare is resolved on the answering side).
  Future<void> establishConnection(String peerId) async {
    if (node.pendingPeerConnections.containsKey(peerId) ||
        node.peerConnections.containsKey(peerId)) {
      return;
    }

    final pc = await createConnection();
    final dc = await pc.createDataChannel(
      'meridian-${uuidV4()}',
      rtc.RTCDataChannelInit(),
    );
    final entry = PendingConnection(pc);
    node.pendingPeerConnections[peerId] = entry;
    node.peerConnections[peerId] = pc;

    final completer = Completer<void>();
    var settled = false;
    late final Timer timeout;

    void settleError(Object err) {
      if (settled) return;
      settled = true;
      timeout.cancel();
      // A glare rollback may already have replaced this entry; only tear down
      // while ours is still the tracked connection.
      if (node.pendingPeerConnections[peerId] == entry) {
        node.failConnection(peerId, err);
      }
      if (!completer.isCompleted) completer.completeError(err);
    }

    void settleOk() {
      if (settled) return;
      settled = true;
      timeout.cancel();
      if (node.pendingPeerConnections[peerId] == entry) {
        node.pendingPeerConnections.remove(peerId);
      }
      if (!completer.isCompleted) completer.complete();
    }

    timeout = Timer(connectionEstablishmentTimeout, () {
      settleError(TimeoutException('Connection establishment timeout'));
    });

    pc.onConnectionState = (state) {
      if (state == rtc.RTCPeerConnectionState.RTCPeerConnectionStateFailed ||
          state == rtc.RTCPeerConnectionState.RTCPeerConnectionStateClosed) {
        settleError(StateError('PeerConnection ${state.name}'));
      }
    };

    pc.onIceCandidate = (candidate) {
      // Trickle ICE: send every candidate as it is gathered.
      node.sendSignaling({
        'type': 'ice_candidate',
        'target': peerId,
        'senderId': node.peerId,
        'candidate': candidate.toMap(),
      });
    };

    Future<void> integrate() async {
      if (settled) return;
      try {
        // The protocol responder must be live before measuring RTT: the
        // remote only answers our PING once our PONG responder exists.
        node.dcHandler.setupHandlers(dc, peerId);
        final rtt = await node.rttMeasurement.measureOverDataChannel(dc);
        await node.ringManager.addPeerToRing(peerId, dc, rtt);
        settleOk();
      } catch (err) {
        settleError(err);
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

    try {
      final offer = await pc.createOffer(const {});
      await pc.setLocalDescription(offer);
      final local = await pc.getLocalDescription();
      node.sendSignaling({
        'type': 'connect_offer',
        'target': peerId,
        'senderId': node.peerId,
        'sdp': sessionDescriptionToMap(local),
      });
    } catch (err) {
      settleError(err);
    }

    return completer.future;
  }

  /// Handles an incoming connect_offer (answers it).
  Future<void> handleOffer(Map<String, dynamic> msg) {
    return _answerOffer(msg, 'connect_answer');
  }

  /// Handles an incoming probe_offer (answers it).
  Future<void> handleProbeOffer(Map<String, dynamic> msg) {
    return _answerOffer(msg, 'probe_answer');
  }

  /// We received an offer: answer it and SEND the answer immediately, with
  /// ICE candidates trickling separately.
  Future<void> _answerOffer(Map<String, dynamic> msg, String answerType) async {
    final peerId = msg['senderId'];
    final sdp = msg['sdp'];
    if (peerId is! String || sdp is! Map || !isWellFormedSdp(sdp)) {
      _logger.warning('invalid offer sdp from ${msg['senderId']}');
      return;
    }

    // Glare (every peer dials every peer from the same peers_list, so both
    // sides may offer at once): resolve deterministically. The
    // lexicographically greater peerId is impolite and ignores the incoming
    // offer (its own offer wins); the lesser rolls back its own offer and
    // answers, so exactly one connection survives per pair.
    if (node.pendingPeerConnections.containsKey(peerId) ||
        node.peerConnections.containsKey(peerId)) {
      if (node.peerId.compareTo(peerId) > 0) return;
      final stale = node.pendingPeerConnections[peerId];
      node.failConnection(peerId);
      if (stale != null && stale.pendingCandidates.isNotEmpty) {
        // The remote's trickled candidates belong to the ICE session we are
        // now answering; keep them for the replacement pc.
        node.earlyCandidates[peerId] = [...stale.pendingCandidates];
      }
    }

    final pc = await createConnection();
    final entry = PendingConnection(pc);
    node.pendingPeerConnections[peerId] = entry;
    node.peerConnections[peerId] = pc;

    // A channel that never opens must not leak the pc until shutdown.
    entry.timeout = Timer(connectionEstablishmentTimeout, () {
      if (node.pendingPeerConnections[peerId] == entry) {
        node.handlePeerFailure(peerId);
      }
    });

    pc.onConnectionState = (state) {
      if (state == rtc.RTCPeerConnectionState.RTCPeerConnectionStateFailed ||
          state == rtc.RTCPeerConnectionState.RTCPeerConnectionStateClosed) {
        node.handlePeerFailure(peerId);
      }
    };
    pc.onDataChannel = (channel) {
      unawaited(node.integrateIncomingDataChannel(peerId, channel));
    };
    pc.onIceCandidate = (candidate) {
      node.sendSignaling({
        'type': 'ice_candidate',
        'target': peerId,
        'senderId': node.peerId,
        'candidate': candidate.toMap(),
      });
    };

    try {
      await pc.setRemoteDescription(
        rtc.RTCSessionDescription(sdp['sdp'] as String, sdp['type'] as String),
      );
      entry.remoteDescriptionSet = true;
      node.flushPendingCandidates(peerId, entry);

      final answer = await pc.createAnswer(const {});
      await pc.setLocalDescription(answer);
      final local = await pc.getLocalDescription();

      final answerMsg = <String, dynamic>{
        'type': answerType,
        'target': peerId,
        'senderId': node.peerId,
        'sdp': sessionDescriptionToMap(local),
      };
      // Correlation id echoed for the offerer's probe matching; the
      // signaling server relays payloads verbatim.
      final probeId = msg['probeId'];
      if (probeId != null) answerMsg['probeId'] = probeId;
      node.sendSignaling(answerMsg);
    } catch (err) {
      node.failConnection(peerId, err);
      rethrow;
    }
  }

  /// Applies a connect_answer to our in-flight handshake for its sender,
  /// flushing any ICE candidates buffered before the remote description was
  /// set.
  void handleConnectAnswer(Map<String, dynamic> msg) {
    final senderId = msg['senderId'];
    final sdp = msg['sdp'];
    if (sdp is! Map || senderId is! String || !isWellFormedSdp(sdp)) {
      _logger.warning('invalid connect_answer sdp from $senderId');
      return;
    }

    final entry = node.pendingPeerConnections[senderId];
    if (entry == null) return;

    entry.pc
        .setRemoteDescription(
      rtc.RTCSessionDescription(sdp['sdp'] as String, sdp['type'] as String),
    )
        .then((_) {
      entry.remoteDescriptionSet = true;
      node.flushPendingCandidates(senderId, entry);
    }).catchError((Object err) {
      _logger.warning('connect_answer rejected from $senderId: $err');
    });
  }
}
