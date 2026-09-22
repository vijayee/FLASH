import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import '../data_channel_handler.dart';
import '../message_types.dart';
import '../overlay/meridian_node.dart';

/// Supernode SFU forwarding (spec §6.2): every stream a supernode
/// receives is signalled to the other cluster members so they expect the
/// relayed media.
class SfuForwarder {
  SfuForwarder._();

  /// Activates SFU mode (spec §6.2). Called on EVERY supernode win path:
  /// the primary election, a `supernode_elected` self-init echo, and a
  /// re-election win.
  static void setupMediaForwarding(MeridianNode node) {
    if (!node.isSupernode) return;
    node.mediaForwarding = true;
  }

  /// Announces a freshly received remote stream to the other cluster
  /// members so they expect the forwarded media.
  static void forwardStreamToCluster(
    MeridianNode node,
    String sourcePeerId,
    rtc.MediaStream? stream,
  ) {
    if (!node.isSupernode || !node.mediaForwarding) return;
    final cluster = node.supernodeCluster;
    if (cluster == null) return;

    for (final member in cluster.members) {
      if (member == sourcePeerId || member == node.peerId) continue;
      final channel = node.knownPeers[member]?.dataChannel;
      if (channel == null) continue;
      sendChannelMessage(channel, {
        'type': MeridianMessageTypes.forwardedStream,
        'sourcePeerId': sourcePeerId,
        'streamId': stream?.id,
      });
    }
  }

  /// Handles a supernode's `forwarded_stream` signal: a stream originated
  /// by `sourcePeerId` will arrive relayed through the cluster leader.
  static void handleForwardedStream(
      MeridianNode node, Map<String, dynamic> msg) {
    final sourcePeerId = msg['sourcePeerId'];
    if (sourcePeerId is! String) return;
    node.forwardedStreams.add(sourcePeerId);
    try {
      node.onStreamOffer?.call(sourcePeerId, msg['streamId'] as String?);
    } catch (_) {
      // Application handler errors must not break the dispatch.
    }
  }
}
