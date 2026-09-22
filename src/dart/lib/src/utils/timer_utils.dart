/// Millisecond clock backed by microseconds, so sub-millisecond RTT
/// measurements are observable (the stand-in for `performance.now()`).
double nowMs() => DateTime.now().microsecondsSinceEpoch / 1000;
