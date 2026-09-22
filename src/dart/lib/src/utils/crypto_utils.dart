import 'package:uuid/uuid.dart';

const Uuid _uuid = Uuid();

/// Returns a random UUID v4 string.
String uuidV4() => _uuid.v4();

/// FNV-1a 32-bit hash — a cheap, stable, non-cryptographic hash for identity
/// checks. NOT for security-sensitive uses.
int fnv1a32(String input) {
  var hash = 0x811c9dc5;
  for (final codeUnit in input.codeUnits) {
    hash ^= codeUnit;
    hash = (hash * 0x01000193) & 0xFFFFFFFF;
  }
  return hash;
}
