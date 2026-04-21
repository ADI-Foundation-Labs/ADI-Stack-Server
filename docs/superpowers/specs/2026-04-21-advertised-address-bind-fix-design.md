# Fix `advertised_address` path to avoid non-local UDP bind

## Problem

When `network_advertised_address` is set to an IP not present on any local interface (typical in K8s: LB public IP, pods only have cluster IP), the node crashes on startup:

```
failed to create network service:
  Discv5Error(Io(Os { code: 99, kind: AddrNotAvailable,
                      message: "Cannot assign requested address" }))
```

Root cause in `lib/network/src/service.rs` (current behavior):

- `enr_address = SocketAddrV4(advertised_ip, port)` is threaded into **both**
  - `discv5::ListenConfig::from_ip(enr_address.ip(), port)` — the UDP bind address, and
  - `reth_discv5::Config::builder(enr_address)` — whose `tcp_socket.ip()` is used by `amend_listen_config_wrt_rlpx` to overwrite the listen IP.
- The resulting discv5 listen config binds UDP to `advertised_ip:port`. That IP doesn't exist on any pod interface and `/proc/sys/net/ipv4/ip_nonlocal_bind = 0`, so `bind()` returns `EADDRNOTAVAIL`.

The in-code comment at `service.rs:109-111` already documents this as a known prerequisite (`net.ipv4.ip_nonlocal_bind=1` or equivalent). The fix below eliminates that prerequisite entirely by decoupling bind from advertised IP.

## Scope

Minimum fix, option (a) from brainstorming:

- **In scope:** Only the `advertised_address = Some(_)` path. Bind UDP to the configured bind address; advertise the pinned IP in the ENR.
- **Out of scope:** Any change to the `advertised_address = None` path — its behavior is assumed identical to upstream reth and must stay byte-for-byte the same. The known small-deployment quorum issue (`enr_peer_update_min(2)` never satisfied with <3 nodes) is not addressed here.

## Design

### Mechanism

`discv5` exposes `Discv5::update_local_enr_socket(socket_addr, is_tcp)` which sets `ip4 + udp4` (when `is_tcp=false`) or `ip4 + tcp4` (when `is_tcp=true`) on the local ENR and re-signs it. `reth_discv5::Discv5::with_discv5(|d| ...)` exposes the inner discv5 handle. This lets us rewrite the ENR after construction without touching the bind.

### Behavioral changes

In `NetworkService::new` (`lib/network/src/service.rs`):

1. Always build the discv5 listen config from the **bind address**:
   ```rust
   discv5::ListenConfig::from_ip(config.address, config.port)
   ```
   This is equivalent to the pre-advertised-address code and never attempts a non-local bind.

2. Always pass `rlpx_address` (not `enr_address`) to `reth_discv5::Config::builder(...)`. This prevents reth's `amend_listen_config_wrt_rlpx` from overriding the listen IP to the advertised one.

3. When `advertised_address` is set, keep `enr_peer_update_min(usize::MAX)` so PONG-quorum votes cannot overwrite the pinned ENR IP. Unchanged.

4. After `NetworkManager::builder(net_cfg).await?.split()` returns, before spawning the manager task or returning from `NetworkService::new`:
   - If `advertised_address` is `Some(ip)`, acquire the inner discv5 handle through the `NetworkManager` and call:
     ```rust
     let sock = SocketAddr::V4(SocketAddrV4::new(ip, config.port));
     discv5.update_local_enr_socket(sock, /*is_tcp=*/ false);
     discv5.update_local_enr_socket(sock, /*is_tcp=*/ true);
     ```
   - If the handle cannot be obtained synchronously (e.g., reth only exposes it through `NetworkHandle` which requires the manager to be running), do the rewrite at the earliest moment the handle is available and ensure it runs before any discovery traffic can depend on the advertised IP being correct. The implementation plan will pin this down exactly.

### Post-fix behavior (as observed from outside)

- `/proc/net/udp` inside the pod: UDP listener on `0.0.0.0:<port>`, regardless of `advertised_address`.
- Startup never crashes on a pod without `ip_nonlocal_bind=1`.
- Local ENR published to peers contains `ip4 = advertised_address`, `udp4 = port`, `tcp4 = port`.
- Incoming UDP to `advertised_ip:port` hairpin-NATs to the pod's `0.0.0.0` listener. Relies on the same hairpin that already works for TCP/RLPx (confirmed during debugging: an ESTABLISHED TCP session existed between main and EN over port 3060 via the LB IP).
- `advertised_address = None`: no change.

### Non-goals

- Fixing the `advertised_address = None` small-deployment quorum issue.
- Supporting IPv6 (code is Ipv4-only today).
- Resolving misnamed env keys (e.g., vault secret `advertised_address` without `network_` prefix) — that's an operator-side concern and already surfaces as `advertised_address: None` in the startup config dump.
- Runtime changes to the advertised IP (pinned means pinned).

## Risks and open items

1. **Timing race.** If discv5 sends any outbound traffic (e.g., initial bootnode PING) carrying its ENR before the post-split rewrite lands, peers briefly see a wrong ENR. Mitigations:
   - Call `update_local_enr_socket` synchronously after `split()` and before spawning the manager task, if the API allows it.
   - If the handle is only reachable after spawn, any peer that learned the stale ENR will re-learn the updated ENR on the next contact (ENR sequence bumps on each call to `set_udp_socket` / `set_tcp_socket`). This is acceptable; the race window is bounded to the first few hundred milliseconds of startup.
   The implementation plan picks the tightest available ordering and documents the chosen approach.

2. **reth handle exposure.** The precise call path from `NetworkManager::split()` return to an object that implements `with_discv5` needs to be confirmed against reth `v1.11.1`. If the path is not trivially available, the implementation plan will either (a) use `NetworkHandle` after spawn or (b) request a small reth patch upstream. Preference: (a), since it keeps the change local.

3. **Hairpin NAT for UDP.** We confirmed TCP hairpin works in this cluster (an ESTABLISHED main↔EN session was present). UDP hairpin is usually configured the same way but isn't guaranteed. If UDP hairpin turns out to be broken, this fix won't be sufficient — but that would be an infra issue, not a code issue, and surfaces clearly (no UDP traffic counters climbing on either side). Out of scope for this fix.

## Testing

- No unit test: `NetworkService::new` needs a live reth/discv5 stack; fine-grained unit coverage has low value.
- **Integration test (in repo):** if feasible, extend an existing P2P integration test in `zksync_os_integration_tests` with two nodes where one sets `advertised_address` to an IP that exists but differs from the bind (e.g., a secondary loopback alias `127.0.0.2`), then assert that the peer's received ENR from the test node reports `ip4 = 127.0.0.2`. Implementation plan confirms test feasibility.
- **Manual K8s verification** on the existing test deployment:
  1. No crash loop on the main node.
  2. `/proc/net/udp` on both pods shows UDP bound to `0.0.0.0:3060`.
  3. UDP datagram counters (`/proc/net/snmp`) climb on both pods over a 30s window.
  4. `No known_closest_peers` WARN stops after the first successful PING/PONG populates the kbuckets.
  5. Existing RLPx session continues to work.

## Files touched

- `lib/network/src/service.rs` — the only file expected to change. Modifications are localized to `NetworkService::new`.

## Rollback

Revert the commit. The `advertised_address = None` path is untouched, so any deployment not using the feature is unaffected either way.
