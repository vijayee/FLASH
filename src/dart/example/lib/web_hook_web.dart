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

/// Installs `window.__meridianAction(action, arg)` — the e2e drive seam
/// (Task 6). Flutter web (CanvasKit) renders no DOM widgets, so Playwright
/// cannot click the demo's buttons or type into its fields; the hook runs
/// the same handlers the buttons call (`find` -> the closest-node query,
/// `stream` -> establishMediaStream). Results are observable through the
/// state hook's `lastFindResult`/`lastStreamResult`.
@JS('__meridianAction')
external set _meridianActionHook(JSFunction hook);

void installActionHook(void Function(String action, String arg) action) {
  void hook(JSString jsAction, JSString jsArg) =>
      action(jsAction.toDart, jsArg.toDart);
  _meridianActionHook = hook.toJS;
}
