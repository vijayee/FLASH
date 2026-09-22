/**
 * Task 3 latency rig: each region is one unprivileged Linux network
 * namespace with a per-region `tc netem` egress delay (raise architecture
 * in scripts/netns-up.sh; launcher in src/netns-launch.ts).
 */
export type NetnsRegion = 'eu' | 'us' | 'asia';