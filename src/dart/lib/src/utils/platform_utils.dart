import 'package:flutter/foundation.dart' show kIsWeb;

import 'platform_check_io.dart' if (dart.library.html) 'platform_check_web.dart'
    as check;

/// Platform checks (spec §9.1). dart:io is unavailable on the web target, so
/// the native checks are conditionally imported.
class PlatformUtils {
  static bool get isWeb => kIsWeb;
  static bool get isMobile =>
      !kIsWeb && (check.platformIsAndroid || check.platformIsIOS);
  static bool get isDesktop =>
      !kIsWeb &&
      (check.platformIsMacOS ||
          check.platformIsWindows ||
          check.platformIsLinux);
}
