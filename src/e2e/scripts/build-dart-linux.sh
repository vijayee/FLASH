#!/usr/bin/env bash
# build-dart-linux.sh — Task 8: builds the Dart example's Flutter Linux
# desktop bundle (x64 release) and stages it where the Azure provisioning
# picks it up (scped to the lab VM's /opt/flash/dart-linux by
# azure/provision.sh; Task 10 runs the binary there under xvfb). Mirrors
# build-dart-web.sh: run once, and again after any src/dart change:
#
#   src/e2e/scripts/build-dart-linux.sh
#
# Linux is a build-on-target platform (no cross-compilation), so this runs
# on the orchestrator machine (x64 Linux) and again on the lab VM if the
# binary ever needs to be rebuilt there (cloud-init installs the toolchain).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../../.." && pwd)"
example="$repo_root/src/dart/example"
staged="$repo_root/src/e2e/build/dart-linux"

cd "$example"
flutter build linux --release

rm -rf "$staged"
mkdir -p "$(dirname "$staged")"
cp -R "$example/build/linux/x64/release/bundle" "$staged"
echo "staged dart-linux bundle: $staged"