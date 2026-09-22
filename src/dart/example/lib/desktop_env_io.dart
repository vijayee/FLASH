/// Native-only runtime environment seam (Task 7): the desktop peer reads
/// `MRD_SIGNALING` / `MRD_STATUS_FILE` from the process environment.
/// Selected by the conditional import in main.dart when `dart:io` exists.
library;

import 'dart:io';

/// One process-environment override, or null when unset.
String? envOverride(String name) => Platform.environment[name];

/// Appends one pre-encoded status line (+ newline) to the status file.
/// Synchronous appends keep the writer trivial and ordered; one line per
/// second is far below any sane filesystem throughput bound.
void appendStatusLine(String path, String json) {
  File(path).writeAsStringSync('$json\n', mode: FileMode.append);
}
