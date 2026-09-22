import 'dart:io' show Platform;

/// dart:io-backed platform checks (compiled only on native targets).
bool get platformIsAndroid => Platform.isAndroid;
bool get platformIsIOS => Platform.isIOS;
bool get platformIsMacOS => Platform.isMacOS;
bool get platformIsWindows => Platform.isWindows;
bool get platformIsLinux => Platform.isLinux;
