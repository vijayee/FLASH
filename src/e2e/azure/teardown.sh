#!/usr/bin/env bash
# teardown.sh — delete the whole geo e2e estate (Task 8 / plan Task 10
# Step 5). Everything lives in one resource group, so this is one call:
# --no-wait returns immediately; Azure keeps deleting in the background
# (check with `az group show -n <rg>`).
#
# B2s x4 ≈ $120/month running 24/7 — tear down when idle.
#
# Usage:  src/e2e/azure/teardown.sh [--dry-run]
# Env:    AZ_RESOURCE_GROUP (default flash-e2e-rg)
set -euo pipefail

AZ_RESOURCE_GROUP=${AZ_RESOURCE_GROUP:-flash-e2e-rg}

DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    *) echo "teardown: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

die() { echo "teardown: $*" >&2; exit 1; }
command -v az >/dev/null || die "az CLI not found (https://aka.ms/azure-cli)"
if ! az account show >/dev/null 2>&1; then
  die "az is not logged in — run 'az login' first"
fi

echo "+ az group delete --name $AZ_RESOURCE_GROUP --yes --no-wait"
if [ "$DRY_RUN" = 1 ]; then
  echo "(dry-run: nothing deleted)"
else
  az group delete --name "$AZ_RESOURCE_GROUP" --yes --no-wait
  echo "deletion of $AZ_RESOURCE_GROUP started (async); verify with:"
  echo "  az group show -n $AZ_RESOURCE_GROUP   # expect ResourceNotFound when done"
fi