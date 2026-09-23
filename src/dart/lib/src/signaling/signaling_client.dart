import 'dart:async';
import 'dart:convert';

import 'package:logging/logging.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import '../config/meridian_config.dart';
import '../overlay/meridian_node.dart';

/// A sink that can deliver signaling messages to the server. Published on
/// [MeridianNode.signalChannel] by [SignalingClient.connect] before register,
/// so bootstrap-triggered offers and ICE reach the server immediately.
abstract interface class SignalSink {
  void send(Map<String, dynamic> message);
}

/// WebSocket bootstrap per spec §4.2. Owns the signaling socket, performs
/// registration plus initial peer discovery, and relays every signaling
/// message into the owning [MeridianNode]'s handler methods.
class SignalingClient implements SignalSink {
  static final Logger _logger = Logger('flash_webrtc.signaling');

  final MeridianNode node;
  WebSocketChannel? _channel;

  SignalingClient({required this.node});

  /// Connects, registers, and requests the initial peer list. Resolves once
  /// the first peers_list reply is processed; fails on a bootstrap timeout
  /// ([MeridianConfig.queryTimeout]) or socket errors before that.
  Future<void> connect(String signalingUrl) async {
    final channel = WebSocketChannel.connect(Uri.parse(signalingUrl));
    _channel = channel;

    final bootstrapped = Completer<void>();
    late final StreamSubscription<dynamic> subscription;
    subscription = channel.stream.listen(
      (data) => _handleData(data, bootstrapped),
      onError: (Object error) {
        if (!bootstrapped.isCompleted) bootstrapped.completeError(error);
      },
      onDone: () {
        if (!bootstrapped.isCompleted) {
          bootstrapped.completeError(
            StateError(
                'Signaling connection closed before bootstrap completed'),
          );
        }
      },
    );
    final timer = Timer(node.config.queryTimeout, () {
      if (!bootstrapped.isCompleted) {
        bootstrapped
            .completeError(TimeoutException('Signaling bootstrap timeout'));
      }
    });

    try {
      await channel.ready;
      // Publish the live socket before registering: peers_list processing
      // may trigger offers/ICE immediately, which must reach the server.
      node.signalChannel = this;
      send({'type': 'register', 'peerId': node.peerId});
      send({'type': 'get_peers', 'senderId': node.peerId});
      await bootstrapped.future;
    } catch (err) {
      timer.cancel();
      unawaited(subscription.cancel());
      try {
        await channel.sink.close();
      } catch (_) {
        // Already closed.
      }
      if (_channel == channel) _channel = null;
      rethrow;
    }
    timer.cancel();
  }

  @override
  void send(Map<String, dynamic> message) {
    final channel = _channel;
    if (channel == null) return;
    try {
      channel.sink.add(jsonEncode(message));
    } catch (_) {
      // Channel may be closed.
    }
  }

  /// Closes the signaling socket.
  void close() {
    final channel = _channel;
    _channel = null;
    if (channel == null) return;
    try {
      channel.sink.close();
    } catch (_) {
      // Already closed.
    }
  }

  void _handleData(dynamic data, Completer<void> bootstrapped) {
    String text;
    if (data is String) {
      text = data;
    } else if (data is List<int>) {
      try {
        text = utf8.decode(data);
      } catch (_) {
        return;
      }
    } else {
      return;
    }

    Map<String, dynamic> msg;
    try {
      final decoded = jsonDecode(text);
      if (decoded is! Map<String, dynamic>) return;
      msg = decoded;
    } catch (_) {
      return;
    }
    final type = msg['type'];
    if (type is! String) return;

    switch (type) {
      case 'peers_list':
        node.handlePeersList(msg);
        if (!bootstrapped.isCompleted) bootstrapped.complete();
      case 'connect_offer':
        if (msg['target'] == node.peerId) {
          unawaited(node.pcManager.handleOffer(msg).catchError((_) {}));
        }
      case 'connect_answer':
        if (msg['target'] == node.peerId) {
          node.pcManager.handleConnectAnswer(msg);
        }
      case 'probe_offer':
        if (msg['target'] == node.peerId) {
          unawaited(node.pcManager.handleProbeOffer(msg).catchError((_) {}));
        }
      case 'probe_answer':
        if (msg['target'] == node.peerId) {
          node.handleProbeAnswer(msg);
        }
      case 'ice_candidate':
        if (msg['target'] == node.peerId) {
          node.handleIceCandidate(msg);
        }
      default:
        _logger.fine('ignoring signaling message of type $type');
    }
  }
}
