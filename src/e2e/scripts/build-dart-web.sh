#!/usr/bin/env bash
# build-dart-web.sh — Task 6: builds the Dart example's Flutter web bundle
# and stages it where the e2e rig serves it (the DART_ROOT mode of
# scripts/serve.mjs). dart-interop.spec.ts does NOT rebuild the bundle on
# every run — run this once (and again after any src/dart change) before
# the spec:
#
#   src/e2e/scripts/build-dart-web.sh
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../../.." && pwd)"
example="$repo_root/src/dart/example"
staged="$repo_root/src/e2e/build/dart-web"

cd "$example"
flutter build web --release

rm -rf "$staged"
mkdir -p "$(dirname "$staged")"
cp -R "$example/build/web" "$staged"
echo "staged dart-web bundle: $staged"