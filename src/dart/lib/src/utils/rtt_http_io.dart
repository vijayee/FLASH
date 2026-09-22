import 'dart:io';

/// Native (dart:io) implementation: HTTP HEAD round-trip timing. Returns
/// infinity when the target cannot be reached.
Future<double> probeHttpTarget(
  String url, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final stopwatch = Stopwatch()..start();
  HttpClient? client;
  try {
    client = HttpClient();
    client.connectionTimeout = timeout;
    final request = await client.headUrl(Uri.parse(url));
    final response = await request.close();
    await response.drain<void>();
    return stopwatch.elapsedMilliseconds.toDouble();
  } catch (_) {
    return double.infinity;
  } finally {
    client?.close(force: true);
  }
}
