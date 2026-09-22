import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;

/// A pooled ephemeral probe connection (spec §2.5, §3.3).
class ConnectionPoolEntry {
  final String targetId;
  final rtc.RTCPeerConnection pc;
  final rtc.RTCDataChannel dc;
  DateTime lastUsed;
  final DateTime createdAt;

  ConnectionPoolEntry({
    required this.targetId,
    required this.pc,
    required this.dc,
    DateTime? lastUsed,
    DateTime? createdAt,
  })  : lastUsed = lastUsed ?? DateTime.now(),
        createdAt = createdAt ?? DateTime.now();
}

/// Pool of established probe connections, keyed by target peer. Evicts (and
/// closes) the least-recently-used entry once [maxSize] is exceeded.
class ConnectionPool {
  final List<ConnectionPoolEntry> entries = [];
  final int maxSize;

  ConnectionPool({this.maxSize = 10});

  /// Returns the pooled connection for [targetId], or null.
  ConnectionPoolEntry? find(String targetId) {
    for (final entry in entries) {
      if (entry.targetId == targetId) return entry;
    }
    return null;
  }

  /// Adds [entry], evicting the least-recently-used entry (closing its
  /// PeerConnection) when the pool is at capacity.
  void add(ConnectionPoolEntry entry) {
    if (entries.length >= maxSize) {
      var oldestIndex = 0;
      for (var i = 1; i < entries.length; i++) {
        if (entries[i].lastUsed.isBefore(entries[oldestIndex].lastUsed)) {
          oldestIndex = i;
        }
      }
      final oldest = entries.removeAt(oldestIndex);
      try {
        oldest.pc.close();
      } catch (_) {
        // Already closed.
      }
    }
    entries.add(entry);
  }

  /// Closes and drops entries unused for longer than [maxAge].
  void cleanup({Duration maxAge = const Duration(minutes: 2)}) {
    final now = DateTime.now();
    final stale = entries
        .where((entry) => now.difference(entry.lastUsed) > maxAge)
        .toList();
    entries.removeWhere(stale.contains);
    for (final entry in stale) {
      try {
        entry.pc.close();
      } catch (_) {
        // Already closed.
      }
    }
  }

  /// Closes every pooled connection.
  void dispose() {
    for (final entry in entries) {
      try {
        entry.pc.close();
      } catch (_) {
        // Already closed.
      }
    }
    entries.clear();
  }
}
