import 'dart:async';
import 'dart:convert';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import '../data_channel_handler.dart';
import '../message_types.dart';
import '../models/connection_pool.dart';
import '../utils/crypto_utils.dart';
import '../utils/rtt_http.dart' as rtt_http;
import '../webrtc/rtc_utils.dart';
import 'meridian_node.dart';

const _rttProbeTimeout = Duration(seconds: 10);

/// One in-flight ping, awaiting its pong.
class _PendingPong {
  final Completer<double> completer;
  final Timer timer;
  _PendingPong(this.completer, this.timer);
}

/// An in-flight ephemeral probe connection to one peer.
class _EphemeralProbe {
  final rtc.RTCPeerConnection pc;
  final rtc.RTCDataChannel dc;
  final String probeId;
  final String peerId;
  final List<rtc.RTCIceCandidate> pendingRemoteCandidates = [];
  bool remoteDescriptionSet = false;
  bool offerSent = false;
  bool settled = false;
  late final Timer timeout;
  final Completer<double> completer = Completer<double>();

  _EphemeralProbe({
    required this.pc,
    required this.dc,
    required this.probeId,
    required this.peerId,
  });

  void fail(Object err) {
    if (settled) return;
    settled = true;
    timeout.cancel();
    try {
      pc.close();
    } catch (_) {
      // Already closed.
    }
    if (!completer.isCompleted) completer.completeError(err);
  }

  void succeed(double rtt) {
    if (settled) return;
    settled = true;
    timeout.cancel();
    if (!completer.isCompleted) completer.complete(rtt);
  }
}

/// RTT measurement paths (spec §3.2): DataChannel ping/pong against ring
/// channels, ephemeral WebRTC connections for peers we are not connected to
/// yet, and HTTP HEAD requests for web targets.
class RttMeasurement {
  final MeridianNode node;

  // ping id -> (completer, timeout). flutter_webrtc DataChannels expose a
  // single assignable onMessage, so pongs are routed here by the channel's
  // installed responder rather than via per-measure listeners; cleanup means
  // dropping the pending entry and cancelling its timer.
  final Map<String, _PendingPong> _pendingPongs = {};

  // peerId -> in-flight ephemeral probe (guards cross-applied answers).
  final Map<String, _EphemeralProbe> _activeProbes = {};

  // Channels with the PING responder already installed (pooled channels are
  // reused across probes).
  final Set<Object> _probeWired = {};

  RttMeasurement(this.node);

  /// Fast path: measure RTT over an existing DataChannel with a correlated
  /// ping/pong exchange, timing out after 10s. The channel's protocol
  /// responder must be wired ([DataChannelHandler.setupHandlers] or
  /// [_installProbeResponder]) or the pong never arrives.
  Future<double> measureOverDataChannel(rtc.RTCDataChannel dc) {
    if (dc.state == rtc.RTCDataChannelState.RTCDataChannelClosed) {
      return Future.error(StateError('DataChannel is closed'));
    }

    final completer = Completer<double>();
    final id = uuidV4();
    final start = node.clock();
    final timer = Timer(_rttProbeTimeout, () {
      if (_pendingPongs.remove(id) != null && !completer.isCompleted) {
        completer.completeError(TimeoutException('RTT probe timeout'));
      }
    });
    _pendingPongs[id] = _PendingPong(completer, timer);

    // A send failure cleans up its own probe; the completer surfaces it.
    dc
        .send(rtc.RTCDataChannelMessage(
      jsonEncode({'type': MeridianMessageTypes.ping, 'id': id, 't': start}),
    ))
        .then((_) {}, onError: (Object err) {
      if (_pendingPongs.remove(id) != null && !completer.isCompleted) {
        timer.cancel();
        completer.completeError(err);
      }
    });

    return completer.future;
  }

  /// Answers an incoming ping with a pong echoing id and t verbatim.
  void handlePing(rtc.RTCDataChannel dc, Map<String, dynamic> msg) {
    sendChannelMessage(dc, {
      'type': MeridianMessageTypes.pong,
      'id': msg['id'],
      't': msg['t'],
    });
  }

  /// Resolves the pending measurement a pong correlates to.
  void handlePong(Map<String, dynamic> msg) {
    final id = msg['id'];
    if (id is! String) return;
    final pending = _pendingPongs.remove(id);
    if (pending == null) return;
    pending.timer.cancel();
    if (pending.completer.isCompleted) return;

    final start = msg['t'];
    if (start is! num) {
      pending.completer.completeError(
        const FormatException('pong missing timestamp'),
      );
      return;
    }
    pending.completer.complete(node.clock() - start.toDouble());
  }

  /// Installs the minimal PING responder for channels we only probe with
  /// (the full message dispatch lives on the node's ring channels; without a
  /// responder the remote's own RTT measure would time out waiting for a
  /// pong). Close events on probe-only channels must not read as peer
  /// failures, hence not the full dispatch.
  void _installProbeResponder(rtc.RTCDataChannel dc) {
    if (_probeWired.contains(dc)) return;
    _probeWired.add(dc);

    dc.onMessage = (message) {
      Map<String, dynamic>? msg;
      try {
        final decoded = jsonDecode(message.text);
        if (decoded is! Map<String, dynamic>) return;
        msg = decoded;
      } catch (_) {
        return;
      }
      if (msg['type'] == MeridianMessageTypes.ping) {
        try {
          dc
              .send(rtc.RTCDataChannelMessage(jsonEncode({
                'type': MeridianMessageTypes.pong,
                'id': msg['id'],
                't': msg['t'],
              })))
              .catchError((_) {});
        } catch (_) {
          // Channel may have just closed.
        }
      } else if (msg['type'] == MeridianMessageTypes.pong) {
        // A pooled channel's single onMessage serves both directions:
        // our own outstanding probe must still correlate its pong.
        handlePong(msg);
      }
    };
  }

  /// Medium path: probe a peer we have no DataChannel with yet by
  /// establishing an ephemeral WebRTC connection. Reuses a pooled connection
  /// when available; pools the new connection on success.
  Future<double> probePeerViaEphemeral(String peerId) async {
    final pooled = node.connectionPool.find(peerId);
    if (pooled != null) {
      pooled.lastUsed = DateTime.now();
      _installProbeResponder(pooled.dc);
      return measureOverDataChannel(pooled.dc);
    }

    // Concurrent probes to the same peer would cross-apply each other's
    // probe_answer.
    if (_activeProbes.containsKey(peerId)) {
      throw StateError('Probe already in flight for $peerId');
    }
    final probeId = uuidV4();
    final pc = await node.pcManager.createEphemeralConnection();
    final dc =
        await pc.createDataChannel('probe-$probeId', rtc.RTCDataChannelInit());
    _installProbeResponder(dc);
    final probe =
        _EphemeralProbe(pc: pc, dc: dc, probeId: probeId, peerId: peerId);
    _activeProbes[peerId] = probe;

    probe.timeout = Timer(node.config.ephemeralProbeTimeout, () {
      probe.fail(TimeoutException('Ephemeral probe timeout'));
    });

    Future<void> whenOpen() async {
      if (probe.settled) return;
      try {
        final rtt = await measureOverDataChannel(dc);
        probe.succeed(rtt);
        node.connectionPool
            .add(ConnectionPoolEntry(targetId: peerId, pc: pc, dc: dc));
      } catch (err) {
        probe.fail(err);
      }
    }

    if (dc.state == rtc.RTCDataChannelState.RTCDataChannelOpen) {
      unawaited(whenOpen());
    } else {
      dc.onDataChannelState = (state) {
        if (state == rtc.RTCDataChannelState.RTCDataChannelOpen &&
            !probe.settled) {
          unawaited(whenOpen());
        }
      };
    }

    // Trickle ICE: forward every candidate as it is gathered.
    pc.onIceCandidate = (candidate) {
      node.sendSignaling({
        'type': 'ice_candidate',
        'target': peerId,
        'senderId': node.peerId,
        'candidate': candidate.toMap(),
      });
    };

    try {
      final offer = await pc.createOffer(const {});
      await pc.setLocalDescription(offer);
      final local = await pc.getLocalDescription();
      probe.offerSent = true;
      node.sendSignaling({
        'type': 'probe_offer',
        'target': peerId,
        'senderId': node.peerId,
        'probeId': probeId,
        'sdp': sessionDescriptionToMap(local),
      });
    } catch (err) {
      probe.fail(err);
    }

    return probe.completer.future;
  }

  /// Applies a probe_answer to the in-flight ephemeral probe for
  /// [msg['senderId']]. Tolerant when the echoed probeId is absent: peers
  /// that drop the field fall back to senderId matching plus the requirement
  /// that our own offer is in flight and not yet answered.
  void handleProbeAnswer(Map<String, dynamic> msg) {
    final senderId = msg['senderId'];
    if (senderId is! String) return;
    final probe = _activeProbes[senderId];
    if (probe == null) return;

    final echoed = msg['probeId'];
    if (echoed is String && echoed != probe.probeId) return;
    if (!probe.offerSent || probe.remoteDescriptionSet) return;

    final sdp = msg['sdp'];
    if (sdp is! Map || !isWellFormedSdp(sdp)) {
      probe.fail(StateError('invalid probe_answer sdp'));
      return;
    }

    probe.pc
        .setRemoteDescription(
      rtc.RTCSessionDescription(sdp['sdp'] as String, sdp['type'] as String),
    )
        .then((_) {
      probe.remoteDescriptionSet = true;
      final candidates = probe.pendingRemoteCandidates.toList();
      probe.pendingRemoteCandidates.clear();
      for (final candidate in candidates) {
        probe.pc.addCandidate(candidate).catchError((_) {});
      }
    }).catchError((Object err) {
      probe.fail(err);
    });
  }

  /// Buffers or applies an ICE candidate belonging to an in-flight ephemeral
  /// probe. Returns true when consumed.
  bool handleEphemeralIceCandidate(
      String senderId, rtc.RTCIceCandidate candidate) {
    final probe = _activeProbes[senderId];
    if (probe == null) return false;
    if (probe.remoteDescriptionSet) {
      probe.pc.addCandidate(candidate).catchError((_) {});
    } else {
      probe.pendingRemoteCandidates.add(candidate);
    }
    return true;
  }

  /// Slow path: measure RTT to a web server via an HTTP HEAD request
  /// (platform-split, see utils/rtt_http.dart). Returns infinity on failure.
  Future<double> probeHttpTarget(String url) {
    return rtt_http.probeHttpTarget(url,
        timeout: node.config.ephemeralProbeTimeout);
  }

  /// Dispatches a measurement to the appropriate method for [targetType]
  /// ('peer', 'http' or 'https').
  Future<double> measureToTarget(String target, String targetType) {
    switch (targetType) {
      case 'peer':
        final known = node.knownPeers[target];
        final dc = known?.dataChannel;
        if (dc != null) return measureOverDataChannel(dc);
        return probePeerViaEphemeral(target);
      case 'http':
      case 'https':
        return probeHttpTarget(target);
      default:
        throw ArgumentError('Unknown target type: $targetType');
    }
  }
}
