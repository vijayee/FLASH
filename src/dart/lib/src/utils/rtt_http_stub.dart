/// Fallback for platforms with neither dart:io nor the web DOM: HTTP probing
/// is unsupported and always reports infinite latency.
Future<double> probeHttpTarget(
  String url, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  return double.infinity;
}
