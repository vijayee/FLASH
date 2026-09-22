import 'dart:js_interop';

/// Installs `window.__meridianState()` — the e2e read seam (Task 1):
/// Playwright polls the example's state snapshot via
/// `page.evaluate('window.__meridianState()')`, which returns the JSON
/// string built by [stateJson] at call time (including `initialized` and
/// `error`, so the global is meaningful before the node exists too).
@JS('__meridianState')
external set _meridianStateHook(JSFunction hook);

void installStateHook(String Function() stateJson) {
  JSString hook() => stateJson().toJS;
  _meridianStateHook = hook.toJS;
}
