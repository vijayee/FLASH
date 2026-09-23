#!/usr/bin/env bash
# provision.sh — Azure geo e2e rig (Task 8). Creates the 4-VM estate the
# geo suite runs against:
#
#   flash-e2e-<region>  (peer VMs, one per AZ_REGIONS entry)
#   flash-e2e-lab       (signaling + Flutter desktop peer + later coturn)
#
# Each VM ends up with (see cloud-init.yaml + the scp layout below):
#   :80    JS demo     (serve.mjs, flash-demo.service)
#   :8090  dart-web    (serve.mjs DART_ROOT, flash-demo-dart.service)
#   :8080  signaling   (lab VM only, flash-signaling.service)
#   :9223  CDP relay   (agent.sh: 0.0.0.0:9223 -> Chromium's 127.0.0.1:9222)
#
# Chromium instances themselves are NOT provisioned artifacts: the
# orchestrator starts/stops each VM's agent (azure/agent.sh) over SSH at
# run time (Task 9).
#
# Usage:
#   src/e2e/azure/provision.sh [--dry-run]
#
# Parameters (env):
#   AZ_RESOURCE_GROUP  flash-e2e-rg
#   AZ_REGIONS         "eastus westeurope southeastasia"   (peer VMs)
#   AZ_VM_SIZE         Standard_B2s
#   AZ_LAB_LOCATION    default: the FIRST entry of AZ_REGIONS
#   ORCHESTRATOR_IP    default: this machine's egress IP (curl ifconfig.me);
#                      SSH + CDP NSG rules are restricted to it
#
# Image URN: verify before changing —
#   az vm image list --publisher Canonical --offer ubuntu-24_04-lts \
#     --sku server --all --output table
#
# No Azure resource is touched until the `az account show` preflight
# passes; --dry-run executes nothing at all (it still queries az account
# to catch the not-logged-in case early, with the same clear error).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"

AZ_RESOURCE_GROUP=${AZ_RESOURCE_GROUP:-flash-e2e-rg}
AZ_REGIONS=${AZ_REGIONS:-"eastus westeurope southeastasia"}
AZ_VM_SIZE=${AZ_VM_SIZE:-Standard_B2s}
AZ_LAB_LOCATION=${AZ_LAB_LOCATION:-}
# NSG source restriction for SSH (22) and CDP (9222-9225). Everything else
# (80, 8080-8091, ICE UDP range) stays open — peers must reach each other's
# public endpoints, and their public IPs are not known at provision time.
ORCHESTRATOR_IP=${ORCHESTRATOR_IP:-}
ADMIN_USER=azureuser
# Canonical Ubuntu 24.04 LTS server (see the URN verification note above).
IMAGE_URN="Canonical:ubuntu-24_04-lts:server:latest"
OS_DISK_GB=64   # the lab role clones the Flutter SDK (~10GB) onto the image

DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    *) echo "provision: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

# --- helpers -----------------------------------------------------------------
# Echo (and, outside --dry-run, execute) every mutating command so the
# transcript doubles as the audit log.
run() {
  echo "+ $*"
  if [ "$DRY_RUN" = 0 ]; then "$@"; fi
}
run_ssh() { # <ip> <shell-command>
  echo "+ ssh $ADMIN_USER@$1 -- $2"
  if [ "$DRY_RUN" = 0 ]; then ssh "$ADMIN_USER@$1" "$2"; fi
}
run_scp() { # <ip> <src...> : <dst>
  local ip=$1; shift
  local dst=${*: -1}   # last argument is the remote destination
  local srcs=("${@:1:$#-1}")
  echo "+ scp -r ${srcs[*]} $ADMIN_USER@$ip:$dst"
  if [ "$DRY_RUN" = 0 ]; then scp -r "${srcs[@]}" "$ADMIN_USER@$ip:$dst"; fi
}

die() { echo "provision: $*" >&2; exit 1; }

# --- preflight ---------------------------------------------------------------
command -v az >/dev/null || die "az CLI not found (https://aka.ms/azure-cli)"
if ! az account show >/dev/null 2>&1; then
  die "az is not logged in — run 'az login' (and 'az account set --subscription <id>') first"
fi
[ "$DRY_RUN" = 1 ] || az account show --output table

if [ -z "$ORCHESTRATOR_IP" ]; then
  echo "ORCHESTRATOR_IP unset — detecting this machine's egress IP (ifconfig.me)"
  ORCHESTRATOR_IP=$(curl -fsS --max-time 10 ifconfig.me || true)
fi
[ -n "$ORCHESTRATOR_IP" ] ||
  die "could not detect ORCHESTRATOR_IP (no ifconfig.me reachability) — set it explicitly"

for required in \
  "$here/cloud-init.yaml" "$here/agent.sh" "$here/signaling.service" \
  "$repo_root/src/e2e/build/dart-web/index.html" \
  "$repo_root/src/e2e/build/dart-linux/data/flutter_assets" \
  "$repo_root/src/signaling-server/server.js"; do
  [ -e "$required" ] ||
    die "missing build artifact: $required
(run src/e2e/scripts/build-dart-web.sh and build-dart-linux.sh first)"
done

# shellcheck disable=SC2206 # word splitting IS the multi-value parse
regions=($AZ_REGIONS)
[ ${#regions[@]} -gt 0 ] || die "AZ_REGIONS is empty"
lab_location=${AZ_LAB_LOCATION:-${regions[0]}}
echo "resource group : $AZ_RESOURCE_GROUP"
echo "peer regions   : ${regions[*]}"
echo "lab location   : $lab_location"
echo "vm size        : $AZ_VM_SIZE"
echo "orchestrator IP: $ORCHESTRATOR_IP"
[ "$DRY_RUN" = 1 ] && echo "(dry-run: nothing will be executed)"

# --- resource group ----------------------------------------------------------
# Created in the lab location; the group holds every VM regardless of region.
run az group create --name "$AZ_RESOURCE_GROUP" --location "$lab_location"

# --- per-VM NSG --------------------------------------------------------------
# az vm create leaves a default-allow-ssh (0.0.0.0/0) rule behind; delete it
# and re-add SSH restricted to the orchestrator. NSG rule names are stable,
# so re-running provision.sh re-applies (idempotent) rather than duplicating.
apply_nsg() { # <nsg-name>
  local nsg=$1
  # The default rule only exists right after az vm create; tolerate absence.
  if [ "$DRY_RUN" = 0 ]; then
    az network nsg rule delete -g "$AZ_RESOURCE_GROUP" --nsg-name "$nsg" \
      --name default-allow-ssh >/dev/null 2>&1 || true
  else
    echo "+ az network nsg rule delete -g $AZ_RESOURCE_GROUP --nsg-name $nsg --name default-allow-ssh (if present)"
  fi
  # SSH: orchestrator only.
  run az network nsg rule create -g "$AZ_RESOURCE_GROUP" --nsg-name "$nsg" \
    --name allow-ssh-orchestrator --priority 1000 --direction Inbound \
    --access Allow --protocol Tcp \
    --source-address-prefixes "$ORCHESTRATOR_IP" \
    --destination-port-ranges 22
  # Demo/static on 80; signaling + demo/dart tabs on 8080-8091 (open — the
  # peers dial the lab VM's public signaling endpoint from their own public
  # IPs, which are not known at provision time).
  run az network nsg rule create -g "$AZ_RESOURCE_GROUP" --nsg-name "$nsg" \
    --name allow-demo-http --priority 1010 --direction Inbound \
    --access Allow --protocol Tcp \
    --source-address-prefixes '*' \
    --destination-port-ranges 80
  run az network nsg rule create -g "$AZ_RESOURCE_GROUP" --nsg-name "$nsg" \
    --name allow-signaling-demos --priority 1020 --direction Inbound \
    --access Allow --protocol Tcp \
    --source-address-prefixes '*' \
    --destination-port-ranges 8080-8091
  # CDP: orchestrator only. 9222 is Chromium's loopback-only DevTools port
  # (reached through the agent's relay on 9223); 9224-9225 spare.
  run az network nsg rule create -g "$AZ_RESOURCE_GROUP" --nsg-name "$nsg" \
    --name allow-cdp-orchestrator --priority 1030 --direction Inbound \
    --access Allow --protocol Tcp \
    --source-address-prefixes "$ORCHESTRATOR_IP" \
    --destination-port-ranges 9222-9225
  # ICE: Chromium cannot restrict its ICE port range, so host/srflx
  # candidates land on ephemeral UDP ports (plan known-risks).
  run az network nsg rule create -g "$AZ_RESOURCE_GROUP" --nsg-name "$nsg" \
    --name allow-ice-udp --priority 1040 --direction Inbound \
    --access Allow --protocol Udp \
    --source-address-prefixes '*' \
    --destination-port-ranges 1024-65535
}

# --- VM creation -------------------------------------------------------------
create_vm() { # <name> <location> <role>
  local name=$1 location=$2 role=$3
  local custom_data="/tmp/flash-cloudinit-${name}.yaml"
  echo "--- VM $name ($location, role=$role)"
  sed "s/__FLASH_ROLE__/$role/g" "$here/cloud-init.yaml" >"$custom_data" ||
    die "could not render cloud-init for $name"
  echo "+ (cloud-init for $name -> $custom_data, role=$role)"
  # Idempotent re-run: skip creation, re-apply the NSG rules.
  if [ "$DRY_RUN" = 0 ] && az vm show -g "$AZ_RESOURCE_GROUP" -n "$name" >/dev/null 2>&1; then
    echo "VM $name already exists — skipping creation (NSG rules re-applied)"
    local nic_id nsg
    nic_id=$(az vm show -g "$AZ_RESOURCE_GROUP" -n "$name" \
      --query "networkProfile.networkInterfaces[0].id" -o tsv)
    nsg=$(az network nic show --ids "$nic_id" \
      --query "networkSecurityGroup.id" -o tsv | xargs basename)
    apply_nsg "$nsg"
    return 0
  fi
  run az vm create \
    --resource-group "$AZ_RESOURCE_GROUP" \
    --name "$name" \
    --image "$IMAGE_URN" \
    --size "$AZ_VM_SIZE" \
    --location "$location" \
    --admin-username "$ADMIN_USER" \
    --generate-ssh-keys \
    --public-ip-sku Standard \
    --os-disk-size-gb "$OS_DISK_GB" \
    --custom-data "$custom_data" || {
      # Capacity restrictions (SkuNotAvailable) and region-eligibility
      # (RequestDisallowedByAzure) only surface at create time —
      # list-skus reports no restriction for both. Skip the region and
      # keep provisioning the rest.
      echo "provision: VM $name in $location could not be created — skipping region" >&2
      return 1
    }
  # az vm create's default NSG is named after the VM.
  apply_nsg "$name"
  if [ "$DRY_RUN" = 0 ]; then
    az vm wait -g "$AZ_RESOURCE_GROUP" -n "$name" --updated >/dev/null
    echo "VM $name provisioned"
  else
    echo "+ az vm wait -g $AZ_RESOURCE_GROUP -n $name --updated"
  fi
}

created_regions=()
for region in "${regions[@]}"; do
  if create_vm "flash-e2e-$region" "$region" peer; then
    created_regions+=("$region")
  fi
done
[ ${#created_regions[@]} -gt 0 ] ||
  die "no peer VM could be created in any AZ_REGIONS entry (capacity or region eligibility)"
regions=("${created_regions[@]}")
echo "peer regions provisioned: ${regions[*]}"
if ! create_vm "flash-e2e-lab" "$lab_location" lab; then
  # The lab VM hosts signaling — mandatory. Fall back to a provisioned
  # peer region before giving up.
  for fallback in "${regions[@]}"; do
    echo "lab VM could not be created in $lab_location — trying $fallback"
    if create_vm "flash-e2e-lab" "$fallback" lab; then lab_location=$fallback; break; fi
  done
  [ "$lab_location" != "${regions[0]}" ] || die "the lab VM could not be created in any provisioned region"
fi

# --- public IPs --------------------------------------------------------------
declare -A IP
vm_ip() { # <name> -> public IP ("<dry-run>" in dry-run mode)
  if [ "$DRY_RUN" = 1 ]; then echo "<dry-run>"; return 0; fi
  az vm list-ip-addresses -g "$AZ_RESOURCE_GROUP" -n "$1" \
    --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv
}
for region in "${regions[@]}"; do
  IP["flash-e2e-$region"]=$(vm_ip "flash-e2e-$region")
done
IP["flash-e2e-lab"]=$(vm_ip "flash-e2e-lab")
echo "--- public IPs"
for vm in "${!IP[@]}"; do
  echo "  $vm: ${IP[$vm]}"
done

# --- post-boot: wait for ssh + cloud-init, upload the /opt/flash tree -------
ssh_ready() { # <ip> [timeoutSec] — cloud-init status --wait blocks until
  #             --custom-data is fully applied.
  local ip=$1 deadline=$((SECONDS + ${2:-900}))
  while [ $SECONDS -lt $deadline ]; do
    if ssh -o ConnectTimeout=8 -o BatchMode=yes "$ADMIN_USER@$ip" \
      'cloud-init status --wait >/dev/null && echo CLOUD_INIT_DONE' 2>/dev/null |
      grep -q CLOUD_INIT; then
      return 0
    fi
    sleep 10
  done
  return 1
}

# The scp target paths mirror the repo's relative layout so
# scripts/serve.mjs's repo-root-relative bases (src/js/examples/browser,
# src/js, src/e2e/fixtures, src/e2e/build/dart-web) resolve verbatim under
# /opt/flash on the VM — serve.mjs needs zero modifications.
upload_vm() { # <ip>  (all source paths are fixed below, per role)
  local ip=$1
  run_ssh "$ip" 'sudo mkdir -p /opt/flash && sudo chown -R azureuser:azureuser /opt/flash'
  run_ssh "$ip" 'mkdir -p /opt/flash/src/js/examples/browser /opt/flash/src/js/src \
    /opt/flash/src/e2e/fixtures /opt/flash/src/e2e/scripts /opt/flash/src/e2e/build \
    /opt/flash/agent'
  run_scp "$ip" \
    "$repo_root/src/js/examples/browser/index.html" \
    "$repo_root/src/js/examples/browser/main.js" \
    :/opt/flash/src/js/examples/browser/
  run_scp "$ip" "$repo_root/src/js/src" :/opt/flash/src/js/
  run_scp "$ip" "$repo_root/src/e2e/fixtures" :/opt/flash/src/e2e/
  run_scp "$ip" \
    "$repo_root/src/e2e/scripts/serve.mjs" \
    "$repo_root/src/e2e/scripts/cdp-relay.mjs" \
    :/opt/flash/src/e2e/scripts/
  # Staged dart-web bundle (both demo servers run on every VM; agent.sh
  # picks the page per its role argument).
  run_scp "$ip" "$repo_root/src/e2e/build/dart-web" :/opt/flash/src/e2e/build/
  run_scp "$ip" "$here/agent.sh" :/opt/flash/agent/
  run_ssh "$ip" 'chmod +x /opt/flash/agent/agent.sh'
}

upload_lab() { # <ip> — lab-only extras: signaling + desktop peer bundle
  local ip=$1
  run_ssh "$ip" 'mkdir -p /opt/flash/signaling-server'
  run_scp "$ip" \
    "$repo_root/src/signaling-server/server.js" \
    "$repo_root/src/signaling-server/package.json" \
    "$repo_root/src/signaling-server/package-lock.json" \
    :/opt/flash/signaling-server/
  run_scp "$ip" "$here/signaling.service" :/tmp/flash-signaling.service
  run_ssh "$ip" 'sudo mv /tmp/flash-signaling.service /etc/systemd/system/flash-signaling.service'
  # Linux desktop peer bundle (built locally by build-dart-linux.sh; Task 10
  # runs it under xvfb on this VM).
  run_scp "$ip" "$repo_root/src/e2e/build/dart-linux" :/opt/flash/
}

start_services() { # <ip> <isLab:0|1>
  local ip=$1 is_lab=$2
  # The demo units were enabled by cloud-init before /opt/flash existed —
  # restart them now that the tree is in place.
  run_ssh "$ip" 'sudo systemctl daemon-reload && sudo systemctl restart flash-demo flash-demo-dart'
  if [ "$is_lab" = 1 ]; then
    run_ssh "$ip" 'cd /opt/flash/signaling-server && npm ci --omit=dev'
    run_ssh "$ip" 'sudo systemctl enable --now flash-signaling'
  fi
}

for vm in "${!IP[@]}"; do
  ip=${IP[$vm]}
  if [ "$ip" = "<dry-run>" ]; then
    echo "(dry-run: would wait for ssh+cloud-init on $vm, upload the demo stack$([ "$vm" = flash-e2e-lab ] && echo " + signaling + dart-linux bundle"), start services)"
    continue
  fi
  echo "--- post-boot: $vm ($ip)"
  ssh_ready "$ip" || die "$vm: ssh/cloud-init never became ready within 900s"
  upload_vm "$ip"
  if [ "$vm" = flash-e2e-lab ]; then
    upload_lab "$ip"
    start_services "$ip" 1
  else
    start_services "$ip" 0
  fi
done

# --- verification (from this machine = the orchestrator) ----------------------
echo "--- verification"
for vm in "${!IP[@]}"; do
  ip=${IP[$vm]}
  [ "$ip" != "<dry-run>" ] || continue
  demo_code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://$ip/" || true)
  dart_code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://$ip:8090/" || true)
  echo "  $vm ($ip): js demo HTTP $demo_code, dart demo HTTP $dart_code"
  if [ "$vm" = flash-e2e-lab ]; then
    # The signaling endpoint answers plain HTTP with 426 Upgrade Required.
    sig_code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://$ip:8080/" || true)
    echo "  $vm ($ip): signaling HTTP $sig_code (expect 426)"
  fi
done

echo
echo "provision complete: resource group $AZ_RESOURCE_GROUP"
echo "next: run the geo suite from this machine (Task 9), or tear down with"
echo "  AZ_RESOURCE_GROUP=$AZ_RESOURCE_GROUP $here/teardown.sh"