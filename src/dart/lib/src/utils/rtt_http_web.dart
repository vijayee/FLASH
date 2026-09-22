import 'package:http/http.dart' as http;

/// Web (package:http) implementation of HTTP RTT probing. Returns infinity
/// when the target cannot be reached (CORS and network failures included).
Future<double> probeHttpTarget(
  String url, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final stopwatch = Stopwatch()..start();
  try {
    await http.head(Uri.parse(url)).timeout(timeout);
    return stopwatch.elapsedMilliseconds.toDouble();
  } catch (_) {
    return double.infinity;
  }
}
