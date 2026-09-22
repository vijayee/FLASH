# FLASH e2e + network testing

Playwright suite for the Meridian-WebRTC overlay: local two-peer discovery,
a 100% unprivileged netns latency rig, media/frame-flow, failure + recovery,
Dart web interop, and a native Flutter desktop peer. Run everything with
`npm test` from this directory (`src/e2e`).

## Suites

| Spec | What it proves |
| --- | --- |
| `test/*.spec.ts` | discovery, latency-ordered queries, media frame flow, failure/re-election, Dart interop, native desktop peer (see the per-file headers) |
| `src/netns-launch.ts` + `scripts/netns-up.sh` | per-region latency rig: one user+net namespace per region, `tc netem` egress delays, slirp4netns host-forwarded CDP — no root anywhere |
| `scripts/build-dart-web.sh` | stages the Dart example's Flutter web bundle to `build/dart-web` (serve.mjs `DART_ROOT` mode) |
| `scripts/build-dart-linux.sh` | stages the Dart example's Linux desktop bundle to `build/dart-linux` (the native peer binary) |

## Azure geo suite (Task 8+)

Real-geography variant of the netns rig: 3 regional peer VMs (eastus,
westeurope, southeastasia) + 1 lab VM (signaling, Flutter desktop peer,
later coturn), each a `Standard_B2s` Ubuntu 24.04 VM. Instead of netem
delays, the latency is *real* — the peers are on different continents.

### Prereqs

- `az` CLI installed and logged in: `az login` (then
  `az account set --subscription <id>` if needed). `provision.sh` fails
  fast with instructions otherwise.
- Build artifacts present: `scripts/build-dart-web.sh` and
  `scripts/build-dart-linux.sh` must have run (provision.sh checks).
- This machine is the **orchestrator**: its egress IP (auto-detected via
  `ifconfig.me`, override with `ORCHESTRATOR_IP`) is the only source
  allowed to SSH (22) and reach CDP (9222-9225) on any VM.

### Provision flow

```
src/e2e/azure/provision.sh            # add --dry-run to print the plan only
```

Per VM it: creates the resource group + B2s VM (`Canonical:ubuntu-24_04-lts:server:latest`,
64GB os disk), applies NSG rules (SSH/CDP orchestrator-only; 80 +
8080-8091 open; UDP 1024-65535 inbound for ICE — Chromium can't restrict
its ICE port range), passes a role-templated `cloud-init.yaml` via
`--custom-data` (Node 20, Playwright Chromium, xvfb; Flutter SDK + GTK
toolchain on the lab VM only), then scps the demo stack into
`/opt/flash/` (mirroring the repo layout so `serve.mjs`'s path bases
resolve unmodified) and starts the services:

- `:80` JS demo (`flash-demo.service`), `:8090` dart-web
  (`flash-demo-dart.service`) — every VM
- `:8080` signaling (`flash-signaling.service`) — lab VM only

`cloud-init.yaml` and `agent.sh` are parameterized (the `__FLASH_ROLE__`
placeholder; the agent's `js|dart` argument). Provision ends by curling
each VM's demo roots (expect HTTP 200) and the lab's signaling port
(expect 426).

### How the orchestrator reaches agents

Chromium instances are **not** provisioned artifacts: at run time (Task 9)
the orchestrator ssh'es each VM and runs `agent.sh <js|dart>`, which
launches one headless Chromium (fake media, no-sandbox, `--unsafely-treat-
insecure-origin-as-secure=http://<vm-ip>` so the plain-http demo origin is
a secure context) plus a CDP relay (`scripts/cdp-relay.mjs`,
`0.0.0.0:9223 -> 127.0.0.1:9222`, because new headless ignores
`--remote-debugging-address`), then Playwright attaches with
`chromium.connectOverCDP('http://<vm-ip>:9223')`. Closing the SSH session
tears Chromium and the relay down with it.

### Teardown + cost

```
src/e2e/azure/teardown.sh             # az group delete --yes --no-wait
```

B2s x4 ≈ **$120/month** running 24/7 (plus disk/IP churn) — tear the
group down whenever the rig is idle; re-provisioning is one command
(`provision.sh` is idempotent: existing VMs are skipped, NSG re-applied).