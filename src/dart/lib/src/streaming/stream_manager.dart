import 'dart:async';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:logging/logging.dart';

import '../data_channel_handler.dart';
import '../message_types.dart';
import '../overlay/meridian_node.dart';
import '../webrtc/rtc_utils.dart';
import 'sfu_forwarder.dart';

// Cap on waiting for ICE gathering before sending the SDP anyway
// (non-trickle media handshake: candidates ride inside the SDP over the
// existing DataChannel, since the spec defines no media ICE message).
const Duration _iceGatherTimeout = Duration(seconds: 3);

/// One outstanding `media_answer` we are waiting for (single assignable
/// `onMessage` on flutter_webrtc channels: answers correlate through the
/// node's dispatch rather than per-call listeners).
class _PendingMediaAnswer {
  final String targetPeerId;
  final rtc.RTCPeerConnection pc;
  final Completer<rtc.RTCPeerConnection> completer;
  late final Timer timer;
  bool settled = false;

  _PendingMediaAnswer({
    required this.targetPeerId,
    required this.pc,
    required this.completer,
  });

  void settle(rtc.RTCPeerConnection connection) {
    if (settled) return;
    settled = true;
    timer.cancel();
    if (!completer.isCompleted) completer.complete(connection);
  }

  void fail(Object error) {
    if (settled) return;
    settled = true;
    timer.cancel();
    if (!completer.isCompleted) completer.completeError(error);
  }
}

/// Direct peer-to-peer media (spec §6.1): the offer/answer exchange
/// travels over the EXISTING DataChannel; media flows over a dedicated
/// PeerConnection built through the shared rtc-utils factory (stun +
/// turn).
class StreamManager {
  static final Logger _logger = Logger('meridian_webrtc.media');

  final MeridianNode node;

  // targetPeerId -> pending media answer.
  final Map<String, _PendingMediaAnswer> _pendingAnswers = {};

  StreamManager(this.node);

  /// Establishes a media stream to a connected peer (spec §6.1).
  /// Resolves with our own PeerConnection once the answer is applied.
  Future<rtc.RTCPeerConnection> establishMediaStream(
      String targetPeerId) async {
    final known = node.knownPeers[targetPeerId];
    final channel = known?.dataChannel;
    if (channel == null) {
      throw StateError('Target peer not connected');
    }

    // Re-establishment replaces any previous media connection to this
    // peer.
    final previous = node.mediaConnections.remove(targetPeerId);
    if (previous != null) {
      try {
        unawaited(previous.close());
      } catch (_) {
        // Already closed.
      }
    }

    final pc = await createMediaConnection();
    node.mediaConnections[targetPeerId] = pc;
    _wireMediaPeer(targetPeerId, pc);

    _addUplinkTracks(pc);

    try {
      final offer = await pc.createOffer(const {});
      await pc.setLocalDescription(offer);
      await _iceGatheringComplete(pc);
      final local = await pc.getLocalDescription();

      sendChannelMessage(channel, {
        'type': MeridianMessageTypes.mediaOffer,
        'senderId': node.peerId,
        'sdp': sessionDescriptionToMap(local),
      });
    } catch (error) {
      closeMediaTo(targetPeerId);
      rethrow;
    }

    // Single assignable onMessage: the answer is correlated by senderId
    // from the node's dispatch (see handleMediaAnswer).
    final completer = Completer<rtc.RTCPeerConnection>();
    final pending = _PendingMediaAnswer(
      targetPeerId: targetPeerId,
      pc: pc,
      completer: completer,
    );
    pending.timer = Timer(node.config.queryTimeout, () {
      if (_pendingAnswers.remove(targetPeerId) == null) return;
      pending.fail(TimeoutException('media_answer timeout'));
      closeMediaTo(targetPeerId);
    });
    _pendingAnswers.remove(targetPeerId)?.fail(
          StateError('Replaced by a newer media handshake'),
        );
    _pendingAnswers[targetPeerId] = pending;

    return completer.future;
  }

  /// Answers an incoming media offer (spec §6.1). When we have no uplink
  /// of our own, the [MeridianNode.onStreamRequest] hook fires first so
  /// the embedder can decide (it may synchronously set
  /// [MeridianNode.localStream] to serve the stream).
  Future<void> handleMediaOffer(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dataChannel,
  ) async {
    final peerId = msg['senderId'];
    final sdp = msg['sdp'];
    if (peerId is! String || sdp is! Map || !isWellFormedSdp(sdp)) return;

    if (node.localStream == null) {
      // Consumer decides whether to serve this stream request.
      try {
        node.onStreamRequest?.call(peerId);
      } catch (_) {
        // An application handler error must not break the answer.
      }
    }

    final pc = await createMediaConnection();
    node.mediaConnections[peerId] = pc;
    _wireMediaPeer(peerId, pc);

    _addUplinkTracks(pc);

    try {
      await pc.setRemoteDescription(
        rtc.RTCSessionDescription(sdp['sdp'] as String, sdp['type'] as String),
      );
      final answer = await pc.createAnswer(const {});
      await pc.setLocalDescription(answer);
      await _iceGatheringComplete(pc);
      final local = await pc.getLocalDescription();

      sendChannelMessage(dataChannel, {
        'type': MeridianMessageTypes.mediaAnswer,
        'senderId': node.peerId,
        'sdp': sessionDescriptionToMap(local),
      });
    } catch (error) {
      _logger.fine('media offer from $peerId failed: $error');
      // Channel may have just closed; the offerer times out.
    }
  }

  /// Applies an incoming media_answer to the pending handshake for its
  /// sender.
  void handleMediaAnswer(Map<String, dynamic> msg) {
    final senderId = msg['senderId'];
    if (senderId is! String) return;
    final pending = _pendingAnswers[senderId];
    if (pending == null) return;

    final sdp = msg['sdp'];
    if (sdp is! Map || !isWellFormedSdp(sdp)) {
      _failAnswer(senderId, pending, StateError('invalid media_answer sdp'));
      return;
    }

    _pendingAnswers.remove(senderId);
    pending.pc
        .setRemoteDescription(
      rtc.RTCSessionDescription(sdp['sdp'] as String, sdp['type'] as String),
    )
        .then((_) {
      pending.settle(pending.pc);
    }).catchError((Object error) {
      _failAnswer(senderId, pending, error);
    });
  }

  /// Closes our inbound stream from a peer (spec §7.1 media_close).
  void handleMediaClose(Map<String, dynamic> msg) {
    final senderId = msg['senderId'];
    if (senderId is! String) return;
    closeMediaTo(senderId);
  }

  /// Politely ends our outbound stream to a peer and tears the media
  /// connection down on both sides.
  void closeStream(String peerId) {
    final channel = node.knownPeers[peerId]?.dataChannel;
    if (channel != null) {
      sendChannelMessage(channel, {
        'type': MeridianMessageTypes.mediaClose,
        'senderId': node.peerId,
      });
    }
    closeMediaTo(peerId);
  }

  /// Stops a peer's stream locally and closes the media PeerConnection
  /// we hold for it.
  void closeMediaTo(String peerId) {
    final stream = node.activeStreams.remove(peerId);
    if (stream != null) {
      for (final track in stream.getTracks()) {
        try {
          unawaited(track.stop());
        } catch (_) {
          // Tracks may already be stopped.
        }
      }
      node.onRemoteStreamRemoved?.call(peerId);
    }
    final pc = node.mediaConnections.remove(peerId);
    if (pc != null) {
      try {
        unawaited(pc.close());
      } catch (_) {
        // Already closed.
      }
    }
  }

  /// Drains pending answers and closes every media PeerConnection; called
  /// from [MeridianNode.dispose] (active stream tracks are stopped by the
  /// node).
  void dispose() {
    for (final pending in _pendingAnswers.values) {
      pending.fail(StateError('Node shutting down'));
    }
    _pendingAnswers.clear();
    for (final pc in node.mediaConnections.values) {
      try {
        unawaited(pc.close());
      } catch (_) {
        // Already closed.
      }
    }
    node.mediaConnections.clear();
  }

  // --- Internals ---

  /// Creates the media PeerConnection. Tests inject
  /// [MeridianNode.mediaConnectionFactory]; production uses the shared
  /// [PeerConnectionManager] factory (stun + turn).
  Future<rtc.RTCPeerConnection> createMediaConnection() async {
    final factory = node.mediaConnectionFactory;
    if (factory != null) return factory();
    return node.pcManager.createConnection();
  }

  void _wireMediaPeer(String peerId, rtc.RTCPeerConnection pc) {
    pc.onTrack = (event) {
      final stream = event.streams.isNotEmpty ? event.streams.first : null;
      if (stream == null) return;
      node.activeStreams[peerId] = stream;
      SfuForwarder.forwardStreamToCluster(node, peerId, stream);
      node.onRemoteStreamAdded?.call(peerId, stream);
    };
    pc.onConnectionState = (state) {
      if (!node.shuttingDown &&
          (state == rtc.RTCPeerConnectionState.RTCPeerConnectionStateFailed ||
              state ==
                  rtc.RTCPeerConnectionState.RTCPeerConnectionStateClosed)) {
        closeMediaTo(peerId);
      }
    };
  }

  void _addUplinkTracks(rtc.RTCPeerConnection pc) {
    final uplink = node.localStream;
    if (uplink == null) return;
    for (final track in uplink.getTracks()) {
      unawaited(
          pc.addTrack(track, uplink).then((_) {}, onError: (Object error) {
        // A dead media connection surfaces on its connection state.
        _logger.fine('addTrack failed: $error');
      }));
    }
  }

  /// Waits (capped) for ICE gathering to complete; a slow TURN path is
  /// better than a handshake that never completes.
  Future<void> _iceGatheringComplete(rtc.RTCPeerConnection pc) async {
    if (pc.iceGatheringState ==
        rtc.RTCIceGatheringState.RTCIceGatheringStateComplete) {
      return;
    }
    final completer = Completer<void>();
    late final Timer timer;
    void done() {
      timer.cancel();
      if (!completer.isCompleted) completer.complete();
    }

    timer = Timer(_iceGatherTimeout, done);
    pc.onIceGatheringState = (state) {
      if (state == rtc.RTCIceGatheringState.RTCIceGatheringStateComplete) {
        done();
      }
    };
    return completer.future;
  }

  void _failAnswer(
    String peerId,
    _PendingMediaAnswer pending,
    Object error,
  ) {
    _pendingAnswers.remove(peerId);
    closeMediaTo(peerId);
    pending.fail(error);
  }
}
