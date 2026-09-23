/// P2P video/audio streaming using the Meridian overlay over WebRTC.
///
/// Exposes the overlay node and its supporting models. Wire formats are
/// byte-compatible with the JavaScript implementation in `src/js`, so
/// a Dart peer interoperates with a JS peer.
///
/// No UI is included; applications embed [MeridianNode] directly.
library;

export 'src/config/meridian_config.dart';
export 'src/data_channel_handler.dart'
    show DataChannelHandler, sendChannelMessage;
export 'src/message_types.dart';
export 'src/models/connection_pool.dart';
export 'src/models/peer_state.dart';
export 'src/models/query_types.dart';
export 'src/models/raft_state.dart';
export 'src/models/ring.dart';
export 'src/overlay/gossip_protocol.dart';
export 'src/overlay/meridian_node.dart';
export 'src/overlay/ring_manager.dart'
    show RingManager, RingBounds, calculateRingIndex, getRingBounds;
export 'src/overlay/rtt_measurement.dart';
export 'src/signaling/signaling_client.dart';
export 'src/webrtc/peer_connection_manager.dart';
export 'src/webrtc/rtc_utils.dart'
    show
        buildIceServers,
        candidateFromMap,
        isWellFormedSdp,
        sessionDescriptionToMap;
