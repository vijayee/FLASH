import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:meridian_webrtc/meridian_webrtc.dart';
import 'package:uuid/uuid.dart';

void main() => runApp(const MeridianExampleApp());

class MeridianExampleApp extends StatelessWidget {
  const MeridianExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return const MaterialApp(
      title: 'Meridian example',
      home: MeridianDemoPage(),
    );
  }
}

class MeridianDemoPage extends StatefulWidget {
  const MeridianDemoPage({super.key});

  @override
  State<MeridianDemoPage> createState() => _MeridianDemoPageState();
}

class _MeridianDemoPageState extends State<MeridianDemoPage> {
  static const _signalingUrl = 'ws://localhost:8080';
  static const _maxLogEntries = 200;

  final _findTarget = TextEditingController();
  final _streamTarget = TextEditingController();
  final _logEntries = <String>[];
  final _remoteRenderers = <String, rtc.RTCVideoRenderer>{};

  MeridianNode? _node;
  Timer? _statusTimer;
  String? _error;

  @override
  void initState() {
    super.initState();
    _start();
  }

  Future<void> _start() async {
    try {
      rtc.MediaStream? stream;
      try {
        stream = await rtc.navigator.mediaDevices.getUserMedia({
          'video': true,
          'audio': true,
        });
      } catch (err) {
        _log('no local media ($err); joining without an uplink');
      }
      final node = MeridianNode(peerId: const Uuid().v4());
      node.onSupernodeElected = (peerId) => _log('supernode elected: $peerId');
      node.onPeerDisconnected = (peerId) => _log('peer disconnected: $peerId');
      node.onRemoteStreamRemoved = (peerId) {
        _log('stream from $peerId closed');
        _remoteRenderers.remove(peerId)?.dispose();
        setState(() {});
      };
      node.onRemoteStreamAdded = (peerId, stream) {
        _log('stream from $peerId');
        _renderRemoteStream(peerId, stream);
      };
      await node.initialize(_signalingUrl, mediaStream: stream);
      setState(() => _node = node);
      _log('connected as ${node.peerId} via $_signalingUrl');
      _statusTimer = Timer.periodic(
        const Duration(seconds: 1),
        (_) => setState(() {}),
      );
    } catch (err) {
      setState(() => _error = err.toString());
    }
  }

  Future<void> _renderRemoteStream(
    String peerId,
    rtc.MediaStream stream,
  ) async {
    final renderer = rtc.RTCVideoRenderer();
    await renderer.initialize();
    renderer.srcObject = stream;
    if (!mounted) {
      await renderer.dispose();
      return;
    }
    setState(() => _remoteRenderers[peerId] = renderer);
  }

  Future<void> _findClosest() async {
    final node = _node;
    final target = _findTarget.text.trim();
    if (node == null || target.isEmpty) return;
    _log('finding closest node to $target...');
    try {
      final result = await node.findClosestNode(target);
      final rtt = result.closestRttMs?.toStringAsFixed(0);
      _log('closest to $target: ${result.closestPeerId} (${rtt ?? '?'} ms)');
    } catch (err) {
      _log('closest-node query failed: $err');
    }
  }

  Future<void> _streamToPeer() async {
    final node = _node;
    final target = _streamTarget.text.trim();
    if (node == null || target.isEmpty) return;
    try {
      await node.establishMediaStream(target);
      _log('streaming to $target');
    } catch (err) {
      _log('streaming to $target failed: $err');
    }
  }

  void _log(String message) {
    if (!mounted) return;
    setState(() {
      _logEntries.insert(0, '${DateTime.now().toIso8601String()}  $message');
      if (_logEntries.length > _maxLogEntries) {
        _logEntries.removeLast();
      }
    });
  }

  @override
  void dispose() {
    _statusTimer?.cancel();
    _findTarget.dispose();
    _streamTarget.dispose();
    for (final renderer in _remoteRenderers.values) {
      renderer.dispose();
    }
    _remoteRenderers.clear();
    unawaited(_node?.dispose());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final node = _node;
    return Scaffold(
      appBar: AppBar(title: const Text('Meridian P2P example')),
      body: _error != null
          ? Center(child: Text('Error: $_error'))
          : node == null
              ? const Center(child: CircularProgressIndicator())
              : Padding(
                  padding: const EdgeInsets.all(12),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      Text(
                        'peer: ${node.peerId.substring(0, 8)} · '
                        'knownPeers: ${node.knownPeers.length} · '
                        'ring slots: '
                        '${node.rings.fold<int>(
                          0,
                          (total, ring) => total + ring.primaryMembers.length,
                        )} · '
                        'supernode: ${node.isSupernode ? 'yes' : 'no'}',
                      ),
                      const SizedBox(height: 12),
                      Row(
                        children: [
                          Expanded(
                            child: TextField(
                              controller: _findTarget,
                              decoration: const InputDecoration(
                                labelText: 'Target peer id',
                              ),
                            ),
                          ),
                          TextButton(
                            onPressed: _findClosest,
                            child: const Text('Find closest node'),
                          ),
                        ],
                      ),
                      Row(
                        children: [
                          Expanded(
                            child: TextField(
                              controller: _streamTarget,
                              decoration: const InputDecoration(
                                labelText: 'Peer id',
                              ),
                            ),
                          ),
                          TextButton(
                            onPressed: _streamToPeer,
                            child: const Text('Stream to peer'),
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      SizedBox(
                        height: 160,
                        child: Wrap(
                          children: [
                            for (final renderer in _remoteRenderers.values)
                              SizedBox(
                                width: 240,
                                height: 160,
                                child: rtc.RTCVideoView(renderer),
                              ),
                          ],
                        ),
                      ),
                      const SizedBox(height: 12),
                      const Text('Events'),
                      Expanded(
                        child: ListView.builder(
                          itemCount: _logEntries.length,
                          itemBuilder: (context, index) =>
                              Text(_logEntries[index]),
                        ),
                      ),
                    ],
                  ),
                ),
    );
  }
}
