// Test doubles for the flash_webrtc package: in-memory
// RTCPeerConnection / RTCDataChannel / MediaStream stand-ins (the real
// flutter_webrtc bindings need platform channels unavailable under
// flutter test) plus wiring helpers connecting two nodes' channels.

import 'dart:async';
import 'dart:convert';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

import 'package:flash_webrtc/src/overlay/meridian_node.dart';

/// Minimal in-memory RTCPeerConnection. Every member is a no-op; media
/// and data-channel creation throw unless a test overrides them.
class FakeRTCPeerConnection implements rtc.RTCPeerConnection {
  @override
  Future<void> dispose() async {}
  @override
  Future<void> close() async {}
  @override
  Map<String, dynamic> get getConfiguration => {};
  @override
  Future<void> setConfiguration(Map<String, dynamic> configuration) async {}
  @override
  Future<rtc.RTCSessionDescription> createOffer(
      [Map<String, dynamic> constraints = const {}]) async {
    return rtc.RTCSessionDescription('sdp', 'offer');
  }

  @override
  Future<rtc.RTCSessionDescription> createAnswer(
      [Map<String, dynamic> constraints = const {}]) async {
    return rtc.RTCSessionDescription('sdp', 'answer');
  }

  @override
  Future<void> setLocalDescription(
      rtc.RTCSessionDescription description) async {}
  @override
  Future<void> setRemoteDescription(
      rtc.RTCSessionDescription description) async {}
  @override
  Future<rtc.RTCSessionDescription?> getLocalDescription() async => null;
  @override
  Future<rtc.RTCSessionDescription?> getRemoteDescription() async => null;
  @override
  Future<void> addCandidate(rtc.RTCIceCandidate candidate) async {}
  @override
  Future<List<rtc.StatsReport>> getStats([rtc.MediaStreamTrack? track]) async =>
      [];
  @override
  Future<rtc.RTCDataChannel> createDataChannel(
      String label, rtc.RTCDataChannelInit dataChannelDict) async {
    throw UnimplementedError();
  }

  @override
  Future<void> restartIce() async {}
  @override
  Future<List<rtc.RTCRtpSender>> getSenders() async => [];
  @override
  Future<List<rtc.RTCRtpReceiver>> getReceivers() async => [];
  @override
  Future<List<rtc.RTCRtpTransceiver>> getTransceivers() async => [];
  @override
  Future<rtc.RTCRtpSender> addTrack(rtc.MediaStreamTrack track,
      [rtc.MediaStream? stream]) async {
    throw UnimplementedError();
  }

  @override
  Future<bool> removeTrack(rtc.RTCRtpSender sender) async => true;
  @override
  Future<rtc.RTCRtpTransceiver> addTransceiver(
      {rtc.MediaStreamTrack? track,
      rtc.RTCRtpMediaType? kind,
      rtc.RTCRtpTransceiverInit? init}) async {
    throw UnimplementedError();
  }

  @override
  Future<void> addStream(rtc.MediaStream stream) async {}
  @override
  Future<void> removeStream(rtc.MediaStream stream) async {}
  @override
  rtc.RTCDTMFSender createDtmfSender(rtc.MediaStreamTrack track) {
    throw UnimplementedError();
  }

  @override
  List<rtc.MediaStream?> getLocalStreams() => [];
  @override
  List<rtc.MediaStream?> getRemoteStreams() => [];
  @override
  rtc.RTCPeerConnectionState? get connectionState => null;
  @override
  rtc.RTCIceConnectionState? get iceConnectionState => null;
  @override
  rtc.RTCIceGatheringState? get iceGatheringState => null;
  @override
  rtc.RTCSignalingState? get signalingState => null;
  @override
  Future<rtc.RTCSignalingState?> getSignalingState() async => signalingState;
  @override
  Future<rtc.RTCIceGatheringState?> getIceGatheringState() async =>
      iceGatheringState;
  @override
  Future<rtc.RTCIceConnectionState?> getIceConnectionState() async =>
      iceConnectionState;
  @override
  Future<rtc.RTCPeerConnectionState?> getConnectionState() async =>
      connectionState;
  @override
  Future<List<rtc.RTCRtpSender>> get senders => getSenders();
  @override
  Future<List<rtc.RTCRtpReceiver>> get receivers => getReceivers();
  @override
  Future<List<rtc.RTCRtpTransceiver>> get transceivers => getTransceivers();
  @override
  Function(rtc.RTCSignalingState state)? onSignalingState;
  @override
  Function(rtc.RTCPeerConnectionState state)? onConnectionState;
  @override
  Function(rtc.RTCIceGatheringState state)? onIceGatheringState;
  @override
  Function(rtc.RTCIceConnectionState state)? onIceConnectionState;
  @override
  Function(rtc.RTCIceCandidate candidate)? onIceCandidate;
  @override
  Function(rtc.MediaStream stream)? onAddStream;
  @override
  Function(rtc.MediaStream stream)? onRemoveStream;
  @override
  Function(rtc.MediaStream stream, rtc.MediaStreamTrack track)? onAddTrack;
  @override
  Function(rtc.MediaStream stream, rtc.MediaStreamTrack track)? onRemoveTrack;
  @override
  Function(rtc.RTCDataChannel channel)? onDataChannel;
  @override
  Function()? onRenegotiationNeeded;
  @override
  Function(rtc.RTCTrackEvent event)? onTrack;
}

/// Minimal data channel: settable send sink + handlers, and a record of
/// every message sent (for wire-shape assertions).
class FakeRTCDataChannel implements rtc.RTCDataChannel {
  /// Every message passed to [send], in order.
  final sent = <rtc.RTCDataChannelMessage>[];

  /// Injectable send sink — delivering to the remote side.
  Future<void> Function(rtc.RTCDataChannelMessage message)? onSend;

  @override
  Future<void> send(rtc.RTCDataChannelMessage message) async {
    sent.add(message);
    await (onSend?.call(message) ?? Future<void>.value());
  }

  /// Feeds [message] into the locally installed onMessage handler
  /// (simulating an inbound channel message).
  void receive(rtc.RTCDataChannelMessage message) {
    onMessage?.call(message);
  }

  @override
  Future<void> close() async {}
  @override
  rtc.RTCDataChannelState? get state =>
      rtc.RTCDataChannelState.RTCDataChannelOpen;
  @override
  int? get id => null;
  @override
  String? get label => null;
  @override
  int? get bufferedAmount => null;
  @override
  Future<int> getBufferedAmount() async => 0;
  @override
  int? bufferedAmountLowThreshold;
  @override
  Function(rtc.RTCDataChannelState state)? onDataChannelState;
  @override
  Function(rtc.RTCDataChannelMessage data)? onMessage;
  @override
  Function(int currentAmount, int changedAmount)? onBufferedAmountChange;
  @override
  Function(int currentAmount)? onBufferedAmountLow;
  @override
  Stream<rtc.RTCDataChannelState> get stateChangeStream => const Stream.empty();
  @override
  set stateChangeStream(Stream<rtc.RTCDataChannelState> stream) {}
  @override
  Stream<rtc.RTCDataChannelMessage> get messageStream => const Stream.empty();
  @override
  set messageStream(Stream<rtc.RTCDataChannelMessage> stream) {}
}

/// A media-capable peer connection: SDP round-trips locally, ICE is
/// already gathered and added tracks are recorded.
class FakeMediaPeerConnection extends FakeRTCPeerConnection {
  final addedTracks = <(rtc.MediaStreamTrack, rtc.MediaStream?)>[];
  bool closed = false;
  rtc.RTCSessionDescription? _local;

  @override
  Future<void> setLocalDescription(
      rtc.RTCSessionDescription description) async {
    _local = description;
  }

  @override
  Future<rtc.RTCSessionDescription?> getLocalDescription() async => _local;

  @override
  Future<rtc.RTCRtpSender> addTrack(rtc.MediaStreamTrack track,
      [rtc.MediaStream? stream]) async {
    addedTracks.add((track, stream));
    return _FakeRtpSender();
  }

  @override
  rtc.RTCIceGatheringState? get iceGatheringState =>
      rtc.RTCIceGatheringState.RTCIceGatheringStateComplete;

  @override
  Future<void> close() async {
    closed = true;
  }
}

class _FakeRtpSender implements rtc.RTCRtpSender {
  @override
  Future<bool> replaceTrack(rtc.MediaStreamTrack? track) async => true;

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class FakeMediaStreamTrack extends rtc.MediaStreamTrack {
  bool stopped = false;

  @override
  String? get id => 'track-${kind ?? 'audio'}';
  @override
  String? get label => 'fake';
  String? _kind;

  @override
  String? get kind => _kind ?? 'audio';

  set kind(String? value) => _kind = value;
  @override
  bool get enabled => _enabled;
  bool _enabled = true;
  @override
  set enabled(bool b) => _enabled = b;
  @override
  bool? get muted => false;

  @override
  Future<void> stop() async {
    stopped = true;
  }

  @override
  Future<void> dispose() async {}
}

class FakeMediaStream extends rtc.MediaStream {
  final tracks = <rtc.MediaStreamTrack>[];

  FakeMediaStream([String id = 'stream-a']) : super(id, 'test');

  @override
  bool? get active => tracks.isNotEmpty;
  @override
  Future<void> getMediaTracks() async {}
  @override
  Future<void> addTrack(rtc.MediaStreamTrack track,
      {bool addToNative = true}) async {
    tracks.add(track);
  }

  @override
  Future<void> removeTrack(rtc.MediaStreamTrack track,
      {bool removeFromNative = true}) async {
    tracks.remove(track);
  }

  @override
  List<rtc.MediaStreamTrack> getTracks() => tracks;
  @override
  List<rtc.MediaStreamTrack> getAudioTracks() =>
      tracks.where((t) => t.kind == 'audio').toList();
  @override
  List<rtc.MediaStreamTrack> getVideoTracks() =>
      tracks.where((t) => t.kind == 'video').toList();
}

/// Deterministic millisecond clock for scripting RTTs: every read advances
/// by [stepMs], and a DataChannel ping->pong exchange reads the measurer's
/// clock exactly twice (start, pong), so a live measurement lands on
/// exactly [stepMs] regardless of event-loop load.
class FakeClock {
  FakeClock(this.stepMs);

  final double stepMs;
  double _now = 0;

  double read() => _now += stepMs;
}

/// Decodes a recorded wire message.
Map<String, dynamic> decoded(rtc.RTCDataChannelMessage message) =>
    jsonDecode(message.text) as Map<String, dynamic>;

/// Delivers a sent message to the remote channel's installed handler
/// after [delay] (scripted RTT for query-routing tests).
Future<void> deliver(
  FakeRTCDataChannel to,
  rtc.RTCDataChannelMessage message,
  Duration delay,
) async {
  if (delay > Duration.zero) await Future<void>.delayed(delay);
  to.receive(message);
}

/// Wires a piped DataChannel pair between [a] and [b], installs the
/// protocol responders on both sides and enrolls each into the other's
/// ring at [rttMs]. Messages take [delay] each way, which makes live
/// RTT measurements land near [delay] while enrolled RTTs stay fixed.
void connectPeers(
  MeridianNode a,
  MeridianNode b, {
  double rttMs = 0.5,
  Duration delay = Duration.zero,
}) {
  final aDc = FakeRTCDataChannel();
  final bDc = FakeRTCDataChannel();
  aDc.onSend = (message) => deliver(bDc, message, delay);
  bDc.onSend = (message) => deliver(aDc, message, delay);
  a.dcHandler.setupHandlers(aDc, b.peerId);
  b.dcHandler.setupHandlers(bDc, a.peerId);
  unawaited(a.ringManager.addPeerToRing(b.peerId, aDc, rttMs));
  unawaited(b.ringManager.addPeerToRing(a.peerId, bDc, rttMs));
}
