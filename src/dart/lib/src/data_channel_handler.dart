import 'dart:async';
import 'dart:convert';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import 'message_types.dart';
import 'overlay/meridian_node.dart';
import 'streaming/sfu_forwarder.dart';

/// Sends a JSON-encoded protocol message over [dc], swallowing send errors
/// from channels that may have just closed.
void sendChannelMessage(rtc.RTCDataChannel dc, Map<String, dynamic> payload) {
  try {
    dc.send(rtc.RTCDataChannelMessage(jsonEncode(payload))).catchError((_) {});
  } catch (_) {
    // Channel may be dead; ring refresh / RTT timeouts surface it.
  }
}

/// Message protocol dispatch (spec §6.2). Ping/pong, gossip and peer-leaving
/// are implemented here; the query, Raft and media message families surface
/// on [MeridianNode.onUnhandledMessage] until their task wires them.
class DataChannelHandler {
  final MeridianNode node;

  // Channels with the protocol responder already installed (guards
  // double-wiring: addPeerToRing re-runs [setupHandlers]).
  final Set<Object> _wiredChannels = {};

  DataChannelHandler(this.node);

  /// Installs the message pump and the close handler for a DataChannel. The
  /// responder must be live before any RTT measurement: the remote only
  /// answers our PING once our PONG responder exists.
  void setupHandlers(rtc.RTCDataChannel dc, String peerId) {
    if (_wiredChannels.contains(dc)) return;
    _wiredChannels.add(dc);

    dc.onMessage = (message) {
      Map<String, dynamic>? msg;
      try {
        final decoded = jsonDecode(message.text);
        if (decoded is! Map<String, dynamic>) return;
        msg = decoded;
      } catch (_) {
        return; // Ignore malformed messages.
      }
      final type = msg['type'];
      if (type is! String) return;

      // Any traffic proves the peer alive (feeds lastSeen-based pruning).
      final known = node.knownPeers[peerId];
      if (known != null) known.lastSeen = DateTime.now();

      dispatch(msg, dc, peerId);
    };

    dc.onDataChannelState = (state) {
      if (state == rtc.RTCDataChannelState.RTCDataChannelClosed) {
        node.handlePeerFailure(peerId);
      }
    };
  }

  /// Routes one decoded message. Wire vocabulary: [MeridianMessageTypes].
  void dispatch(
    Map<String, dynamic> msg,
    rtc.RTCDataChannel dc,
    String peerId,
  ) {
    switch (msg['type'] as String) {
      case MeridianMessageTypes.ping:
        node.rttMeasurement.handlePing(dc, msg);
      case MeridianMessageTypes.pong:
        node.rttMeasurement.handlePong(msg);
      case MeridianMessageTypes.gossip:
        unawaited(
          node.gossipProtocol.handleGossip(msg).catchError((_) {}),
        );
      case MeridianMessageTypes.peerLeaving:
        final senderId = msg['senderId'];
        if (senderId is String) node.handlePeerFailure(senderId);

      // --- Query routing (spec §3.6-§3.8) ---

      case MeridianMessageTypes.queryForward:
        node.queryRouting.handleQueryForward(msg, dc);
      case MeridianMessageTypes.leaderQueryForward:
        node.queryRouting.handleLeaderQueryForward(msg, dc);
      case MeridianMessageTypes.constraintQueryForward:
        node.queryRouting.handleConstraintQueryForward(msg, dc);
      case MeridianMessageTypes.probeRequest:
        unawaited(
          node.queryRouting.handleProbeRequest(dc, msg).catchError((_) {}),
        );
      case MeridianMessageTypes.probeRequestAvg:
        unawaited(
          node.queryRouting.handleProbeRequestAvg(dc, msg).catchError((_) {}),
        );
      case MeridianMessageTypes.probeRequestConstraints:
        unawaited(
          node.queryRouting
              .handleProbeRequestConstraints(dc, msg)
              .catchError((_) {}),
        );
      case MeridianMessageTypes.queryResult:
        node.queryRouting.handleQueryResult(msg);

      // probe_result / probe_result_avg / probe_result_constraints /
      // media_answer are consumed by query routing's pending-probe
      // correlation and the media answer registry respectively.
      case MeridianMessageTypes.probeResult:
      case MeridianMessageTypes.probeResultAvg:
      case MeridianMessageTypes.probeResultConstraints:
        node.queryRouting.handleProbeResult(msg, peerId);

      // --- Media (spec §6) ---

      case MeridianMessageTypes.mediaOffer:
        unawaited(
          node.streamManager.handleMediaOffer(msg, dc).catchError((_) {}),
        );
      case MeridianMessageTypes.mediaAnswer:
        node.streamManager.handleMediaAnswer(msg);
      case MeridianMessageTypes.forwardedStream:
        SfuForwarder.handleForwardedStream(node, msg);
      case MeridianMessageTypes.mediaClose:
        node.streamManager.handleMediaClose(msg);

      // --- Supernode / Raft (spec §5) ---

      case MeridianMessageTypes.supernodeElected:
        node.raftConsensus.handleSupernodeElected(msg);
      case MeridianMessageTypes.raftAppendEntries:
        node.raftConsensus.handleAppendEntries(msg, dc);
      case MeridianMessageTypes.raftAppendEntriesResponse:
        node.raftConsensus.handleAppendEntriesResponse(msg, peerId);
      case MeridianMessageTypes.raftRequestVote:
        node.raftConsensus.handleRequestVote(msg, dc);
      case MeridianMessageTypes.raftRequestVoteResponse:
        node.raftConsensus.handleRequestVoteResponse(msg, peerId);

      default:
        // Runtime message types an embedder adds surface here too.
        node.onUnhandledMessage?.call(msg, dc, peerId);
    }
  }
}
