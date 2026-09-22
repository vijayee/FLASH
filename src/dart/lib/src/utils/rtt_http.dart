/// Platform-split HTTP RTT probing (spec §9.1): dart:io HttpClient on native
/// targets, package:http on the web where dart:io is unavailable.
library;

export 'rtt_http_stub.dart'
    if (dart.library.io) 'rtt_http_io.dart'
    if (dart.library.html) 'rtt_http_web.dart';
