import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:meridian_webrtc/meridian_webrtc.dart';
import 'package:meridian_webrtc/src/streaming/sfu_forwarder.dart';

import 'fakes.dart';

void main() {
  test(
      'establishMediaStream sends media_offer over the existing channel '
      'and resolves on the correlated media_answer', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12);

    final mediaPc = FakeMediaPeerConnection();
    a.mediaConnectionFactory = () async => mediaPc;

    final track = FakeMediaStreamTrack()..kind = 'video';
    a.localStream = FakeMediaStream('uplink')..tracks.add(track);

    final establish = a.establishMediaStream('b');
    await Future<void>.delayed(const Duration(milliseconds: 10));

    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final offers = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'media_offer') decoded(message),
    ];
    expect(offers, hasLength(1));
    expect(offers.single['senderId'], 'a');
    expect(offers.single['sdp'], {'type': 'offer', 'sdp': 'sdp'},
        reason: 'media rides inside the SDP over the existing DataChannel');
    expect(mediaPc.addedTracks, hasLength(1));
    expect(mediaPc.addedTracks.single.$1, same(track));
    expect(mediaPc.addedTracks.single.$2, same(a.localStream));

    // The answer is correlated by senderId through the node's dispatch
    // (single assignable onMessage — no per-call listeners).
    a.dcHandler.dispatch({
      'type': 'media_answer',
      'senderId': 'b',
      'sdp': {'type': 'answer', 'sdp': 'sdp'},
    }, aDc, 'b');

    final pc = await establish;
    expect(pc, same(mediaPc));
  });

  test(
      'establishMediaStream times out without an answer and tears the '
      'media connection down', () async {
    final a = MeridianNode(
      peerId: 'a',
      config: const MeridianConfig(
        queryTimeout: Duration(milliseconds: 100),
      ),
    );
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12, delay: Duration.zero);
    // Black-hole b's replies.
    (b.ringMember('a')!.dataChannel as FakeRTCDataChannel).onSend = null;

    final mediaPc = FakeMediaPeerConnection();
    a.mediaConnectionFactory = () async => mediaPc;

    await expectLater(
      a.establishMediaStream('b'),
      throwsA(isA<TimeoutException>()),
    );
    expect(mediaPc.closed, isTrue,
        reason: 'the answer timeout tears the pc down');
    expect(a.mediaConnections, isEmpty);
  }, timeout: const Timeout(Duration(seconds: 5)));

  test(
      'handleMediaOffer fires onStreamRequest without an uplink and '
      'answers with media_answer', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12, delay: const Duration(milliseconds: 10));

    final streamRequests = <String>[];
    b.onStreamRequest = (peerId) => streamRequests.add(peerId);

    final mediaPc = FakeMediaPeerConnection();
    b.mediaConnectionFactory = () async => mediaPc;

    final bDc = b.ringMember('a')!.dataChannel! as FakeRTCDataChannel;
    b.dcHandler.dispatch({
      'type': 'media_offer',
      'senderId': 'a',
      'sdp': {'type': 'offer', 'sdp': 'sdp'},
    }, bDc, 'a');
    await Future<void>.delayed(const Duration(milliseconds: 30));

    expect(streamRequests, ['a'],
        reason: 'the embedder decides whether to serve the stream');
    final answers = [
      for (final message in bDc.sent)
        if (decoded(message)['type'] == 'media_answer') decoded(message),
    ];
    expect(answers, hasLength(1));
    expect(answers.single['senderId'], 'b');
    expect(answers.single['sdp'], {'type': 'answer', 'sdp': 'sdp'});
  });

  test('an uplink adds its tracks to the answering connection', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12, delay: const Duration(milliseconds: 10));

    final track = FakeMediaStreamTrack()..kind = 'audio';
    b.localStream = FakeMediaStream('uplink')..tracks.add(track);
    final mediaPc = FakeMediaPeerConnection();
    b.mediaConnectionFactory = () async => mediaPc;

    final bDc = b.ringMember('a')!.dataChannel! as FakeRTCDataChannel;
    b.dcHandler.dispatch({
      'type': 'media_offer',
      'senderId': 'a',
      'sdp': {'type': 'offer', 'sdp': 'sdp'},
    }, bDc, 'a');
    await Future<void>.delayed(const Duration(milliseconds: 10));

    expect(mediaPc.addedTracks, hasLength(1));
    expect(mediaPc.addedTracks.single.$1, same(track));
  });

  test(
      'an onTrack event activates the stream and forwards it to the '
      'cluster (supernode SFU, spec §6.2)', () async {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    final c = MeridianNode(peerId: 'c');
    connectPeers(a, b, rttMs: 12);
    connectPeers(a, c, rttMs: 12);

    a.isSupernode = true;
    a.raftConsensus.initRaftState(['b', 'c']);
    addTearDown(a.raftConsensus.shutdown);
    // setupMediaForwarding is a no-op without the supernode flag...
    expect(a.mediaForwarding, isFalse);
    SfuForwarder.setupMediaForwarding(a);
    expect(a.mediaForwarding, isTrue);

    final stream = FakeMediaStream('from-b');
    final track = FakeMediaStreamTrack()..kind = 'video';
    stream.tracks.add(track);
    final mediaPc = FakeMediaPeerConnection();
    a.mediaConnectionFactory = () async => mediaPc;

    final remoteAdded = <String>[];
    a.onRemoteStreamAdded = (peerId, _) => remoteAdded.add(peerId);

    // Production wiring runs on the answering side of a media_offer; the
    // onTrack event then arrives on the (now wired) media connection.
    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    a.dcHandler.dispatch({
      'type': 'media_offer',
      'senderId': 'b',
      'sdp': {'type': 'offer', 'sdp': 'sdp'},
    }, aDc, 'b');
    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(a.mediaConnections['b'], same(mediaPc));
    expect(mediaPc.onTrack, isNotNull);

    mediaPc.onTrack!(rtc.RTCTrackEvent(
      streams: [stream],
      track: track,
    ));

    expect(a.activeStreams['b'], same(stream));
    expect(remoteAdded, ['b']);

    // Other cluster members are told to expect the relayed stream.
    final cDc = a.ringMember('c')!.dataChannel! as FakeRTCDataChannel;
    final forwards = [
      for (final message in cDc.sent)
        if (decoded(message)['type'] == 'forwarded_stream') decoded(message),
    ];
    expect(forwards, hasLength(1));
    expect(forwards.single['sourcePeerId'], 'b');
    expect(forwards.single['streamId'], 'from-b');

    // The origin is not re-notified.
    final bDc = b.ringMember('a')!.dataChannel! as FakeRTCDataChannel;
    expect(
      [
        for (final message in bDc.sent)
          if (decoded(message)['type'] == 'forwarded_stream') 1,
      ],
      isEmpty,
    );
  });

  test('setupMediaForwarding is inert without the supernode flag', () {
    final node = MeridianNode(peerId: 'a');
    SfuForwarder.setupMediaForwarding(node);
    expect(node.mediaForwarding, isFalse);

    node.isSupernode = true;
    SfuForwarder.setupMediaForwarding(node);
    expect(node.mediaForwarding, isTrue);
  });

  test('handleForwardedStream tracks the expected source and offers it', () {
    final node = MeridianNode(peerId: 'f');
    final offered = <(String, String?)>[];
    node.onStreamOffer =
        (sourcePeerId, streamId) => offered.add((sourcePeerId, streamId));

    SfuForwarder.handleForwardedStream(node, {
      'type': 'forwarded_stream',
      'sourcePeerId': 'leader',
      'streamId': 's1',
    });
    SfuForwarder.handleForwardedStream(node, {});
    // Malformed input: nothing tracked.
    SfuForwarder.handleForwardedStream(node, {'sourcePeerId': 5});

    expect(node.forwardedStreams, ['leader']);
    expect(offered, hasLength(1));
    expect(offered.single.$1, 'leader');
    expect(offered.single.$2, 's1');
  });

  test('media_close stops the remote tracks and closes the media pc', () {
    final node = MeridianNode(peerId: 'a');
    final stream = FakeMediaStream('from-b');
    stream.tracks.add(FakeMediaStreamTrack());
    node.activeStreams['b'] = stream;
    final mediaPc = FakeMediaPeerConnection();
    node.mediaConnections['b'] = mediaPc;

    final removed = <String>[];
    node.onRemoteStreamRemoved = (peerId) => removed.add(peerId);

    final dc = FakeRTCDataChannel();
    node.dcHandler.setupHandlers(dc, 'b');
    node.dcHandler.dispatch({
      'type': 'media_close',
      'senderId': 'b',
    }, dc, 'b');

    expect((stream.tracks.single as FakeMediaStreamTrack).stopped, isTrue);
    expect(mediaPc.closed, isTrue);
    expect(node.activeStreams.containsKey('b'), isFalse);
    expect(node.mediaConnections.containsKey('b'), isFalse);
    expect(removed, ['b']);
  });

  test('closeStream politely signals the peer before tearing down', () {
    final a = MeridianNode(peerId: 'a');
    final b = MeridianNode(peerId: 'b');
    connectPeers(a, b, rttMs: 12);

    final stream = FakeMediaStream('to-b');
    stream.tracks.add(FakeMediaStreamTrack());
    a.activeStreams['b'] = stream;
    final mediaPc = FakeMediaPeerConnection();
    a.mediaConnections['b'] = mediaPc;

    a.closeStream('b');

    final aDc = a.ringMember('b')!.dataChannel! as FakeRTCDataChannel;
    final closes = [
      for (final message in aDc.sent)
        if (decoded(message)['type'] == 'media_close') decoded(message),
    ];
    expect(closes.single['senderId'], 'a');
    expect((stream.tracks.single as FakeMediaStreamTrack).stopped, isTrue);
    expect(mediaPc.closed, isTrue);
    expect(a.activeStreams.containsKey('b'), isFalse);
  });
}
