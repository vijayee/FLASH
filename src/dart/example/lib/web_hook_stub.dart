/// No-op on non-web platforms: the `window.__meridianState` hook is a
/// browser e2e affordance (Playwright reads it via
/// `page.evaluate('window.__meridianState()')`), so native targets get a
/// stub. Selected by the conditional import in main.dart.
void installStateHook(String Function() stateJson) {}

/// No-op on non-web platforms: the `window.__meridianAction` drive seam is
/// a browser e2e affordance (Task 6); native targets get a stub.
void installActionHook(void Function(String action, String arg) action) {}
