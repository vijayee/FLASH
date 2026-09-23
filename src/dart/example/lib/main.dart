import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:flash_webrtc/flash_webrtc.dart';
import 'package:uuid/uuid.dart';

// e2e read seam (Task 1): `window.__meridianState()` on web builds; native
// targets (desktop peer, Task 7) get a no-op stub.
import 'web_hook_stub.dart' if (dart.library.js_interop) 'web_hook_web.dart';
// e2e drive seam + `?mediaSrc=` uplink (Task 6): `window.__meridianAction()`
// and the looping-file uplink on web builds; native targets get stubs.
import 'uplink_stub.dart' if (dart.library.js_interop) 'uplink_web.dart';
// Task 7 (desktop peer): native runtime env overrides (`MRD_SIGNALING`,
// `MRD_STATUS_FILE`) + the appendable status file; web builds get no-ops.
import 'desktop_env_stub.dart' if (dart.library.io) 'desktop_env_io.dart';

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
  static const _defaultSignalingUrl = 'ws://localhost:8080';
  static const _maxLogEntries = 200;
  static const _maxWireLogEntries = 2000;

  final _findTarget = TextEditingController();
  final _streamTarget = TextEditingController();
  final _logEntries = <String>[];
  final _remoteRenderers = <String, rtc.RTCVideoRenderer>{};

  // --- e2e wire log (?wirelog=1) -----------------------------------------
  // Bounded record of observed wire traffic (signaling sends + DataChannel
  // receives — the only directions reachable from the example without a
  // library seam), surfaced through window.__meridianState().wireLog.
  // Wirelog parity: the JS demo's wirelog covers both DataChannel
  // directions; the Dart side's send path needs a library seam (Task 6).
  final _wireLogEntries = <Map<String, dynamic>>[];
  final _wrappedChannels = <rtc.RTCDataChannel>{};
  late final bool _wireLogEnabled = _e2eParams['wirelog'] == '1';

  // --- e2e URL-param overrides (Task 6) -----------------------------------
  // The Dart twins of the JS demo's parsing (examples/browser/main.js):
  //   ?signaling=<url> bootstrap signaling URL (the Dart example has no
  //     Connect flow to drive — it auto-connects — so e2e points it at the
  //     rig's server through this param),
  //   ?stun=a,b   overrides MeridianConfig.stunServers,
  //   ?gossipMs=<n> (n > 0) overrides MeridianConfig.gossipPeriod,
  //   ?mediaSrc=<url> swaps the getUserMedia uplink for a looping <video>
  //     playing the file (captureStream) — see uplink_web.dart.
  // Task 2 (comprehensive config): the remaining MERIDIAN_CONFIG knobs are
  // reachable two ways —
  //   ?config=<json> a JSON blob of overrides keyed by the published JS
  //     package's MERIDIAN_CONFIG names (e.g. {"gossipPeriodMs": 2000,
  //     "ringsPerNode": 5}), the simplest cross-language way to hand one
  //     config to both demo flavors; unknown keys and malformed values are
  //     ignored, so the library defaults survive,
  //   ?turn=url,username,credential appends one long-term-credential TURN
  //     relay, exactly like the JS demo's affordance.
  // Explicit params (stun/gossipMs/turn) win over the blob; everything is
  // applied BEFORE MeridianNode construction.
  // Parsed only on http(s) bases: Uri.base on native targets is not a
  // browser URL, and the desktop peer (Task 7) must keep its defaults.
  static Map<String, String> get _e2eParams {
    final base = Uri.base;
    return (base.scheme == 'http' || base.scheme == 'https')
        ? base.queryParameters
        : const <String, String>{};
  }

  // Task 7 precedence: the web `?signaling=` param wins (e2e Dart-web tabs),
  // then the native `MRD_SIGNALING` env override (desktop peer), then the
  // default. envOverride is a browser no-op, so web behavior is unchanged.
  late final String _signalingUrl = _e2eParams['signaling'] ??
      envOverride('MRD_SIGNALING') ??
      _defaultSignalingUrl;
  late final String? _mediaSrc = _e2eParams['mediaSrc'];
  // Task 7 (desktop peer): when MRD_STATUS_FILE is set, one JSON status
  // line per second (the _stateJson() snapshot — same fields as the web
  // hook) is appended there, so an agent/test reads the native peer's
  // state without any display or JS seam.
  late final String? _statusFilePath = envOverride('MRD_STATUS_FILE');
  late final MeridianConfig _config = _configFromParams();

  MeridianConfig _configFromParams() {
    var config = _configFromBlob();
    final stun = _e2eParams['stun'];
    final gossipMs = int.tryParse(_e2eParams['gossipMs'] ?? '');
    final turn = _e2eParams['turn'];
    if (stun == null && gossipMs == null && turn == null) return config;
    return MeridianConfig(
      ringsPerNode: config.ringsPerNode,
      nodesPerRing: config.nodesPerRing,
      secondaryCandidates: config.secondaryCandidates,
      innermostRingRadiusMs: config.innermostRingRadiusMs,
      ringMultiplicativeFactor: config.ringMultiplicativeFactor,
      routeAcceptanceThreshold: config.routeAcceptanceThreshold,
      probeTimeoutFactor: config.probeTimeoutFactor,
      gossipPeriod: (gossipMs == null || gossipMs <= 0)
          ? config.gossipPeriod
          : Duration(milliseconds: gossipMs),
      ringReplacementPeriod: config.ringReplacementPeriod,
      maxEphemeralConnections: config.maxEphemeralConnections,
      stunServers: stun == null
          ? config.stunServers
          : stun
              .split(',')
              .map((s) => s.trim())
              .where((s) => s.isNotEmpty)
              .toList(),
      turnServers: turn == null
          ? config.turnServers
          : _turnEntry(turn) ?? config.turnServers,
      maxHops: config.maxHops,
      ephemeralProbeTimeout: config.ephemeralProbeTimeout,
      queryTimeout: config.queryTimeout,
    );
  }

  /// The `?config=` JSON blob (see the params comment above), mapped onto
  /// [MeridianConfig]. Anything missing or malformed keeps the library
  /// default, so a partial blob is a valid override set.
  MeridianConfig _configFromBlob() {
    const base = MeridianConfig();
    final blob = _e2eParams['config'];
    if (blob == null) return base;
    final Object? decoded;
    try {
      decoded = jsonDecode(blob);
    } catch (_) {
      return base;
    }
    if (decoded is! Map) return base;
    // A typed local: closures (intField etc.) below don't inherit the `is
    // Map` promotion of `decoded`.
    final overrides = Map<Object?, Object?>.from(decoded);
    int intField(String key, int fallback) {
      final value = overrides[key];
      return value is int ? value : fallback;
    }

    double doubleField(String key, double fallback) {
      final value = overrides[key];
      return value is num ? value.toDouble() : fallback;
    }

    int msField(String key, Duration fallback) {
      final value = overrides[key];
      return value is int && value > 0 ? value : fallback.inMilliseconds;
    }

    final stun = overrides['stunServers'];
    return MeridianConfig(
      ringsPerNode: intField('ringsPerNode', base.ringsPerNode),
      nodesPerRing: intField('nodesPerRing', base.nodesPerRing),
      secondaryCandidates:
          intField('secondaryCandidates', base.secondaryCandidates),
      // Both the JS demo's key (innermostRingRadius) and the Dart field
      // name (innermostRingRadiusMs) are accepted.
      innermostRingRadiusMs: doubleField(
        'innermostRingRadiusMs',
        doubleField('innermostRingRadius', base.innermostRingRadiusMs),
      ),
      ringMultiplicativeFactor: doubleField(
        'ringMultiplicativeFactor',
        base.ringMultiplicativeFactor,
      ),
      routeAcceptanceThreshold: doubleField(
        'routeAcceptanceThreshold',
        base.routeAcceptanceThreshold,
      ),
      probeTimeoutFactor:
          doubleField('probeTimeoutFactor', base.probeTimeoutFactor),
      gossipPeriod: Duration(
        milliseconds: msField('gossipPeriodMs', base.gossipPeriod),
      ),
      ringReplacementPeriod: Duration(
        milliseconds:
            msField('ringReplacementPeriodMs', base.ringReplacementPeriod),
      ),
      maxEphemeralConnections:
          intField('maxEphemeralConnections', base.maxEphemeralConnections),
      stunServers: stun is List
          ? [
              for (final entry in stun)
                if (entry is String) entry
            ]
          : base.stunServers,
      turnServers: _turnList(overrides['turnServers'], base.turnServers),
      maxHops: intField('maxHops', base.maxHops),
      ephemeralProbeTimeout: Duration(
        milliseconds:
            msField('ephemeralProbeTimeoutMs', base.ephemeralProbeTimeout),
      ),
      queryTimeout: Duration(
        milliseconds: msField('queryTimeoutMs', base.queryTimeout),
      ),
    );
  }

  /// One TURN entry from the `?turn=url,username,credential` param (the
  /// shape the JS demo's affordance parses; TURN URLs contain no commas).
  /// Returns null when the param is malformed, so defaults survive.
  List<TurnServerConfig>? _turnEntry(String param) {
    final fields = param.split(',').map((s) => s.trim()).toList();
    if (fields.length < 3 ||
        fields[0].isEmpty ||
        fields[1].isEmpty ||
        fields[2].isEmpty) {
      return null;
    }
    return [
      TurnServerConfig(
        url: fields[0],
        username: fields[1],
        credential: fields[2],
      ),
    ];
  }

  /// `turnServers` entries from the `?config=` blob: either
  /// `{"url": ..., "username": ..., "credential": ...}` maps or
  /// "url,username,credential" strings; invalid entries are skipped.
  List<TurnServerConfig> _turnList(
    Object? raw,
    List<TurnServerConfig> fallback,
  ) {
    if (raw is! List) return fallback;
    final parsed = <TurnServerConfig>[];
    for (final entry in raw) {
      if (entry is Map) {
        final url = entry['url'];
        final username = entry['username'];
        final credential = entry['credential'];
        if (url is String && username is String && credential is String) {
          parsed.add(
            TurnServerConfig(
                url: url, username: username, credential: credential),
          );
        }
      } else if (entry is String) {
        final single = _turnEntry(entry);
        if (single != null) parsed.addAll(single);
      }
    }
    return parsed;
  }

  // --- e2e action results (Task 6) -----------------------------------------
  // The last find/stream outcome, surfaced through window.__meridianState()
  // so Playwright can await the cross-language query/media handshakes the
  // __meridianAction seam started (the Dart side renders no DOM log).
  Map<String, Object?>? _lastFindResult;
  Map<String, Object?>? _lastStreamResult;

  MeridianNode? _node;
  Timer? _statusTimer;
  Timer? _statusFileTimer;
  String? _error;

  @override
  void initState() {
    super.initState();
    // Installed at startup — before/parallel to the connect attempt — so
    // the global is defined (reporting initialized:false, plus any error)
    // while connecting and after a failed connect too.
    installStateHook(_stateJson);
    // Task 6: the drive seam (`window.__meridianAction(action, arg)`).
    installActionHook(_runE2eAction);
    // Task 7: desktop-peer status file — one snapshot line now (so a poller
    // sees the file and initialized:false immediately) and one per second.
    final statusFile = _statusFilePath;
    if (statusFile != null) {
      void writeStatusLine() => appendStatusLine(statusFile, _stateJson());
      writeStatusLine();
      _statusFileTimer = Timer.periodic(
        const Duration(seconds: 1),
        (_) => writeStatusLine(),
      );
    }
    _start();
  }

  /// e2e drive seam (Task 6): invokes the same handlers the demo's buttons
  /// call — Flutter web (CanvasKit) renders no DOM widgets for Playwright
  /// to click, so the hook drives `find`/`stream` directly.
  void _runE2eAction(String action, String arg) {
    switch (action) {
      case 'find':
        unawaited(_findClosest(arg));
      case 'stream':
        unawaited(_streamToPeer(arg));
    }
  }

  Future<void> _start() async {
    try {
      rtc.MediaStream? stream;
      // Local copy: a `late final` field cannot be type-promoted, and the
      // conditional-imported fileUplink takes the non-null param.
      final mediaSrc = _mediaSrc;
      if (mediaSrc != null) {
        try {
          stream = await fileUplink(mediaSrc);
          _log('uplink: looping $mediaSrc');
        } catch (err) {
          _log('mediaSrc uplink failed ($err); falling back');
        }
      }
      if (stream == null) {
        try {
          stream = await rtc.navigator.mediaDevices.getUserMedia({
            'video': true,
            'audio': true,
          });
        } catch (err) {
          _log('no local media ($err); joining without an uplink');
        }
      }
      final node = MeridianNode(peerId: const Uuid().v4(), config: _config);
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
      _installE2eHooks(node);
      setState(() => _node = node);
      _log('connected as ${node.peerId} via $_signalingUrl');
      _statusTimer = Timer.periodic(const Duration(seconds: 1), (_) {
        // Newly integrated peers' DataChannels get their receive path
        // recorded lazily here (onMessage is a single slot, so the wrap
        // must ride on top of the library's own handler).
        _wrapDataChannels();
        setState(() {});
      });
    } catch (err) {
      setState(() => _error = err.toString());
    }
  }

  /// e2e seam (Task 1): records subsequent signaling sends by wrapping the
  /// node's [SignalSink] (the state hook itself is installed at startup).
  void _installE2eHooks(MeridianNode node) {
    final sink = node.signalChannel;
    if (_wireLogEnabled && sink != null) {
      node.signalChannel = _RecordingSignalSink(sink, node.peerId, _recordWire);
    }
  }

  void _wrapDataChannels() {
    if (!_wireLogEnabled) return;
    final node = _node;
    if (node == null) return;
    for (final peer in node.knownPeers.values) {
      final dc = peer.dataChannel;
      if (dc == null || _wrappedChannels.contains(dc)) continue;
      _wrappedChannels.add(dc);
      final original = dc.onMessage;
      dc.onMessage = (message) {
        _recordWire('recv', peer.peerId, message.text);
        if (original != null) original(message);
      };
    }
  }

  void _recordWire(String dir, String peerId, String raw) {
    var type = 'unknown';
    Object? payload = raw;
    try {
      final decoded = jsonDecode(raw);
      if (decoded is Map<String, dynamic>) {
        payload = decoded;
        final decodedType = decoded['type'];
        if (decodedType is String) type = decodedType;
      }
    } catch (_) {
      // Not JSON; keep the raw payload with the placeholder type.
    }
    if (_wireLogEntries.length >= _maxWireLogEntries) {
      _wireLogEntries.removeAt(0);
    }
    _wireLogEntries.add({
      'dir': dir,
      'type': type,
      'ts': DateTime.now().millisecondsSinceEpoch,
      'peerId': peerId,
      'payload': payload,
    });
  }

  /// Snapshot over the node's public fields, mirroring the JS demo's
  /// `window.__meridian.state()` shape (ring 0 is the closest ring). Unlike
  /// the JS hook — which returns null until connected — this always
  /// resolves, flagging `initialized` so e2e can tell the states apart.
  String _stateJson() {
    final node = _node;
    return jsonEncode({
      'initialized': node != null,
      'error': _error,
      'peerId': node?.peerId,
      'knownPeers': [
        for (final peer in node?.knownPeers.values ?? const <KnownPeer>[])
          {
            'id': peer.peerId,
            'rtt': peer.rttMs,
            'status': peer.status.name,
            'ringIndex': peer.ringIndex,
          },
      ],
      'rings': [
        for (final ring in node?.rings ?? const <Ring>[])
          {
            'index': ring.index,
            'primary': [
              for (final member in ring.primaryMembers) member.peerId,
            ],
          },
      ],
      'isSupernode': node?.isSupernode ?? false,
      'clusterLeader': node?.clusterLeader,
      'activeStreams': [...?node?.activeStreams.keys],
      'lastFindResult': _lastFindResult,
      'lastStreamResult': _lastStreamResult,
      'wireLog': _wireLogEnabled ? _wireLogEntries : const <dynamic>[],
    });
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

  Future<void> _findClosest(String target) async {
    final node = _node;
    if (node == null || target.isEmpty) return;
    _log('finding closest node to $target...');
    try {
      final result = await node.findClosestNode(target);
      final rtt = result.closestRttMs?.toStringAsFixed(0);
      _log('closest to $target: ${result.closestPeerId} (${rtt ?? '?'} ms)');
      _lastFindResult = {
        'target': target,
        'closestPeerId': result.closestPeerId,
        if (result.closestRttMs != null) 'closestRttMs': result.closestRttMs,
      };
    } catch (err) {
      _log('closest-node query failed: $err');
      _lastFindResult = {'target': target, 'error': err.toString()};
    }
  }

  Future<void> _streamToPeer(String target) async {
    final node = _node;
    if (node == null || target.isEmpty) return;
    try {
      await node.establishMediaStream(target);
      _log('streaming to $target');
      _lastStreamResult = {'peerId': target, 'ok': true};
    } catch (err) {
      _log('streaming to $target failed: $err');
      _lastStreamResult = {
        'peerId': target,
        'ok': false,
        'error': err.toString()
      };
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
    _statusFileTimer?.cancel();
    _findTarget.dispose();
    _streamTarget.dispose();
    for (final renderer in _remoteRenderers.values) {
      renderer.dispose();
    }
    _remoteRenderers.clear();
    unawaited(_node?.dispose());
    super.dispose();
  }

  /// In-app guidance (mirrors the JS demo's one-liners): short, user-facing
  /// explanations under each control.
  Widget _guidance(String text) => Text(
        text,
        style: const TextStyle(fontSize: 12, color: Colors.black54),
      );

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
                      _guidance(
                        'Peers meet through the signaling server, then all '
                        'media and data flow peer-to-peer; the rings sort '
                        'them by RTT (ring 0 = closest).',
                      ),
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
                            onPressed: () => unawaited(
                                _findClosest(_findTarget.text.trim())),
                            child: const Text('Find closest node'),
                          ),
                        ],
                      ),
                      _guidance(
                        'Floods a routed query over the rings and returns the '
                        'lowest-RTT peer that answers (up to maxHops hops, '
                        'accepted once routeAcceptanceThreshold of the '
                        'queried rings replied).',
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
                            onPressed: () => unawaited(
                                _streamToPeer(_streamTarget.text.trim())),
                            child: const Text('Stream to peer'),
                          ),
                        ],
                      ),
                      _guidance(
                        'Asks that peer (a known peer id) for its media and '
                        'renders the result below.',
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

/// e2e affordance: records outgoing signaling messages while delegating to
/// the client the node bootstrapped with (the bootstrap register/get_peers
/// sends happen before this wrapper is installed and are not recorded).
class _RecordingSignalSink implements SignalSink {
  _RecordingSignalSink(this._inner, this._peerId, this._record);

  final SignalSink _inner;
  final String _peerId;
  final void Function(String dir, String peerId, String raw) _record;

  @override
  void send(Map<String, dynamic> message) {
    _record('send', _peerId, jsonEncode(message));
    _inner.send(message);
  }
}
