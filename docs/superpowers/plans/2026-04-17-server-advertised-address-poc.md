# Server POC: `advertised_address` for P2P ENR

**Target repo:** `/Users/max/git/adi/ADI-Stack-Server`
**Base tag:** `v0.17.1-b1`
**POC tag:** `v0.17.1-a1` (throwaway test build)
**Promotion tag:** `v0.17.1-b2` (after EN validation passes)

## Goal

Add one optional config field, `advertised_address: Option<Ipv4Addr>`, that pins the node's discv5 ENR to a caller-supplied public IP. Keep the K8s service-lookup out of the server entirely — env-init (or any other infra layer) will populate this field. Backward compatible: if unset, server behaves exactly as today.

## Why

In the current v0.17 code, `config.network.address` is used for both the kernel socket bind and the initial ENR advertisement. In K8s with a LoadBalancer:
- bind must be `0.0.0.0` (or the pod IP) — `EADDRNOTAVAIL` otherwise.
- advertise must be the LB's external IP — unreachable if it's `0.0.0.0`, and the ENR auto-update via discv5 PONG quorum learns the *egress* IP (cluster NAT), not the LB's ingress IP.

No upstream config knob exists today (verified on matter-labs main at commit `fa29459f`, 2026-04-09). Besu shipped and later abandoned a K8s-aware variant of this same feature (PR [#7534](https://github.com/hyperledger/besu/pull/7534)) — we're deliberately picking the *minimal* shape (one config field) that Besu's post-deprecation guidance pointed users toward.

## Design contract

**`advertised_address`:**
- Type: `Option<Ipv4Addr>`
- Default: `None` (exactly today's behavior preserved — discv5 PONG quorum auto-updates the ENR)
- When `Some(ip)`: the node's ENR is initialized with this IP and discv5 auto-updates are disabled
- Env var (smart-config mapping): `network_advertised_address`
- Read once at startup; changing it requires a restart

**Explicitly NOT in this patch:**
- No DNS name support (take an `Ipv4Addr`, not a `String`). If the operator has a hostname, resolve it upstream.
- No K8s API client in the server. Use env-init / a separate init-container / a downward-API sidecar for that.
- No periodic re-query or watch. K8s Service LB IPs are stable in practice; if it changes, restart the pod.
- No IPv6 (consistent with current `address: Ipv4Addr`).
- No port override (tcp/udp advertise port = bind port, same as today).

## Tagging workflow

```
current:       v0.17.1-b1          (production baseline)
POC:           v0.17.1-a1          (this POC — validation build)
  ↓ if validation passes
promotion:     v0.17.1-b2          (replaces -b1 in production)
```

The `-a1` / `-b2` scheme matches existing ADI convention: lowercase letter = stream (a=alpha, b=build/blessed), digit = iteration. `a1` sorts before `b1` which sorts before `b2` — anyone using semver-compare ordering sees `b2 > b1 > a1`, so the POC is clearly distinguishable from production.

## Code changes — three files, ~40 LOC

> **Verify before committing:** the reth 1.11.1 builder method to pin an external IP for discv5 may be exposed as `.set_nat(NatResolver)`, `.external_ip(Option<IpAddr>)`, or similar. Quickest check: `rustdoc` on the `NetworkConfigBuilder` type (`cargo doc -p reth-network --open`) or grep the crate:
>
> ```bash
> find ~/.cargo/registry/src -name 'reth-network-*' -type d | head -1 | \
>   xargs -I{} grep -rn 'pub fn.*nat\|pub fn.*external' {}/src/builder.rs
> ```
>
> If reth doesn't expose a clean path to pin the ENR IP on `NetworkConfigBuilder`, do it via the inner `discv5::ConfigBuilder` instead — that crate definitely supports it via `.enr_ip4(Ipv4Addr)` / `.enr_peer_update_min(u16::MAX)` (or equivalent), which is the raw mechanism anyway. Fall back to that if the outer builder doesn't have an ergonomic hook.

### File 1: `lib/network/src/config.rs`

```rust
use reth_network::config::SecretKey;
use reth_network_peers::NodeRecord;
use std::net::Ipv4Addr;

#[derive(Debug)]
pub struct NetworkConfig {
    pub secret_key: SecretKey,
    pub address: Ipv4Addr,
    pub port: u16,
    pub boot_nodes: Vec<NodeRecord>,
    /// If set, the node's ENR is pinned to this IPv4 address and discv5
    /// auto-update is disabled. If None, behavior is unchanged: the ENR is
    /// initialized from `address` and updated dynamically via PONG quorum.
    pub advertised_address: Option<Ipv4Addr>,
}
```

### File 2: `node/bin/src/config/mod.rs`

Around line 251 (the `NetworkConfig` struct in the node crate):

```rust
#[derive(Clone, Debug, DescribeConfig, DeserializeConfig)]
#[config(derive(Default))]
pub struct NetworkConfig {
    #[config(default_t = false)]
    pub enabled: bool,

    #[config(secret)]
    #[config(default, with = SecretKeyDeserializer)]
    pub secret_key: Option<SecretKey>,

    /// IPv4 address to bind TCP (RLPx) and UDP (discv5) sockets to.
    /// Must be an address assigned to a local interface, or 0.0.0.0 to bind all.
    #[config(default_t = Ipv4Addr::UNSPECIFIED, with = Serde![str])]
    pub address: Ipv4Addr,

    /// Optional IPv4 address to advertise in the node's ENR. Set this when the
    /// bind address is not reachable from peers (e.g., behind a Kubernetes
    /// LoadBalancer where the bind is 0.0.0.0 or a pod IP but peers must reach
    /// the LB's external IP). If unset, the ENR IP is learned via discv5 PONG
    /// quorum — which may be wrong in NATed environments.
    #[config(default, with = Serde![str])]
    pub advertised_address: Option<Ipv4Addr>,

    #[config(default_t = 3060)]
    pub port: u16,

    #[config(default, with = Delimited::repeat(Serde![str], ","))]
    pub boot_nodes: Vec<String>,
}
```

Update the `From` impl around line 986:

```rust
impl From<NetworkConfig> for zksync_os_network::config::NetworkConfig {
    fn from(value: NetworkConfig) -> Self {
        Self {
            secret_key: value
                .secret_key
                .expect("`network.secret_key` is required for running p2p networking stack"),
            address: value.address,
            port: value.port,
            boot_nodes: value
                .boot_nodes
                .iter()
                .map(|s| resolve_enode(s)
                    .unwrap_or_else(|e| panic!("failed to resolve boot node `{s}`: {e}")))
                .collect(),
            advertised_address: value.advertised_address,
        }
    }
}
```

### File 3: `lib/network/src/service.rs`

Around line 79, replace the hardcoded `.disable_nat()` and adjust the discv5 block so the ENR is pinned when `advertised_address` is set. The structure looks like:

```rust
let rlpx_address = SocketAddr::V4(SocketAddrV4::new(config.address, config.port));

// Decide the ENR address — either the pinned advertise IP, or the bind IP (today's behavior)
let enr_ip: Ipv4Addr = config.advertised_address.unwrap_or(config.address);
let enr_pinned = config.advertised_address.is_some();

let discv5_config = {
    let mut b = discv5::ConfigBuilder::new(discv5::ListenConfig::from_ip(
        rlpx_address.ip(),
        config.port,
    ))
    .vote_duration(Duration::from_secs(3600))
    .ban_duration(Some(Duration::from_secs(1)));

    if enr_pinned {
        // Stop peer-driven ENR updates; trust the operator-provided IP
        b = b.enr_peer_update_min(u16::MAX);
        // discv5 crate exposes .enr_ip4 / .enr_tcp4 / .enr_udp4; use whichever
        // matches the 0.x version we're pulling. If not present, build an Enr
        // manually and pass it via reth_discv5::Config::builder(...).bootstrap_enr(...)
    } else {
        b = b.enr_peer_update_min(2);
    }
    b.build()
};

let cfg_builder = RethNetworkConfig::builder(config.secret_key)
    .boot_nodes(config.boot_nodes.clone())
    .apply(|builder| {
        let peer_id = builder.get_peer_id();
        builder.hello_message(
            HelloMessageWithProtocols::builder(peer_id)
                .client_version(NODE_CLIENT_VERSION)
                .build(),
        )
    })
    .disable_discv4_discovery()
    .disable_dns_discovery()
    // Keep NAT disabled at the reth layer; we pin the ENR manually in discv5_config
    .disable_nat()
    .discovery_v5(
        reth_discv5::Config::builder(SocketAddr::V4(SocketAddrV4::new(enr_ip, config.port)))
            .discv5_config(discv5_config),
    )
    .peer_config(/* unchanged */)
    .listener_addr(rlpx_address)   // bind stays on config.address
    .discovery_addr(rlpx_address)  // bind stays on config.address
    .disable_tx_gossip(true)
    .required_block_hashes(vec![])
    .network_id(Some(client.chain_spec().chain_id()));
```

The key moves:
1. **Bind is still `rlpx_address` (= `config.address:config.port`)** — operators keep setting `network_address=0.0.0.0` in K8s. Unchanged.
2. **`reth_discv5::Config::builder()` is given a socket address with the *advertise* IP** — this is what the ENR is seeded from when it first signs itself.
3. **If `advertised_address` is `Some`, `enr_peer_update_min(u16::MAX)`** — effectively disables PONG-driven updates, so the ENR stays at the pinned IP even as peers report observed source IPs (which will be the cluster egress IP in K8s, i.e. wrong).

> **Verification step while coding:** after changes, run the node locally (or in a sandbox) and dump the ENR. The node logs its own ENR at startup — confirm `ip` matches the advertise value, not the bind value. If discv5 crate version doesn't expose `.enr_ip4` directly, the fallback is to build an `Enr` record manually with `EnrBuilder::new("v4").ip4(enr_ip).tcp4(port).udp4(port).build(&secret_key)?` and pass it to reth_discv5 via whatever ENR-injection method it exposes.

### Test sketch

The existing integration test file is `lib/network/tests/e2e.rs` and already uses `reth_network::test_utils::Testnet` + `reth_provider::test_utils::MockEthProvider`. Add a test along these lines:

```rust
#[tokio::test]
async fn advertised_address_pins_enr_ip() {
    use std::net::Ipv4Addr;

    let bind_ip = Ipv4Addr::LOCALHOST;               // 127.0.0.1
    let advertise_ip = Ipv4Addr::new(198, 51, 100, 42);  // TEST-NET-2 (unused public range)

    let cfg = NetworkConfig {
        secret_key: random_secret_key(),
        address: bind_ip,
        port: 0,                                      // ephemeral; discv5 will pick one
        boot_nodes: vec![],
        advertised_address: Some(advertise_ip),
    };

    let service = NetworkService::new(cfg, /* ...zks_config, replay, client... */).await.unwrap();

    // Reach into the service to get the local ENR and assert the ip field.
    // Exact accessor depends on what NetworkService exposes — may require adding
    // a `local_enr()` helper during the patch, which is fine (small, test-only).
    let enr = service.local_enr();
    assert_eq!(enr.ip4(), Some(advertise_ip), "ENR must advertise the pinned IP");
    assert_ne!(enr.ip4(), Some(bind_ip),       "ENR must not advertise the bind IP");
}

#[tokio::test]
async fn advertised_address_unset_preserves_current_behavior() {
    // Same as above but advertised_address = None; assert ENR.ip4 is the bind IP
    // (today's behavior — the regression guard).
}
```

The first test catches the forward case; the second is the backward-compat guard so we can confidently roll `-a1` into prod without worrying about an untested code path.

## Build & tag workflow

```bash
cd /Users/max/git/adi/ADI-Stack-Server

# 1. Branch off v0.17.1-b1 for the POC
git checkout -b feat/advertised-address v0.17.1-b1

# 2. Make the three file edits above
# 3. Test compile
cargo check -p zksync-os-server
cargo test -p zksync-os-network   # at minimum, don't break existing tests

# 4. Commit with conventional-commits style (matches matterlabs' release-please)
git add -p
git commit -m "feat(network): add optional advertised_address for ENR pinning

Introduces Option<Ipv4Addr> on NetworkConfig. When set, the node's ENR
is initialized with this IP and discv5 PONG-driven auto-updates are
disabled. When unset, behavior is unchanged.

Motivation: in K8s with a LoadBalancer Service, the bind address
(typically 0.0.0.0 or a pod IP) is not reachable from peers, and the
ENR auto-update learns the cluster egress IP via PONG — also wrong.
Operators need a way to pin the ENR to the LB's external IP, populated
at the infra layer (env-init, downward API, etc.).

POC tag: v0.17.1-a1"

# 5. Tag as alpha
git tag v0.17.1-a1
git push origin feat/advertised-address
git push origin v0.17.1-a1
```

**Image publishing — verify before relying on it.** The in-repo CI at `.github/workflows/docker.yml` is inherited from upstream matterlabs: it triggers on any `v**` tag and publishes to `ghcr.io/<owner>/zksync-os-server` + `us-docker.pkg.dev/matterlabs-infra/matterlabs-docker/zksync-os-server` — **not** to `harbor.sde.adifoundation.ai/adi-public/chain/server`, which is what ADI production workloads pull. The ADI-specific pipeline that produces the harbor-hosted server image lives outside this repo (not in `infra/.gitlab-ci.yml`, which only builds `ci-toolkit` and `env-init`). Before pushing `v0.17.1-a1`, confirm the mechanism that produced `v0.17.1-b1` in harbor — likely a manual `docker build && docker push`, or a pipeline in a repo not yet located. The `-a1` tag must be published through that same mechanism, not by relying on the inherited GitHub Actions workflow.

## Deploy to a test environment

Pick a low-stakes env — devnet4 is the natural target (Vault secret already cleaned up, and the deployment is new enough that nothing breaks if we trash it).

### Service-type caveat

The infra chart `k8s-tenant/base/charts/zksync-rollup/templates/server.yaml` currently exposes a **single ClusterIP Service** with the p2p ports named `p2p-tcp` / `p2p-udp` on 3060. There is **no LoadBalancer-type Service** in the chart today, so by default the pod has no external IP to advertise. Before this POC can actually be validated against third-party ENs, one of the following must be in place in the devnet4 env:

- **Option A (recommended for POC):** add a LoadBalancer-type Service (or a second Service) that exposes port 3060 TCP+UDP. The `zkos-test-server-p2p` Service in `adi-testnet-test` is a working example (type `LoadBalancer`, external IP `20.233.32.181`). Either overlay one in the devnet4 values, or add a chart template gated on a values flag.
- **Option B (simpler, less realistic):** test from inside the cluster — spin up an EN pod in the same cluster and use the ClusterIP. Won't exercise the LB/NAT asymmetry but will prove the config field works end-to-end.

For (A), the service-name pattern is `{{ include "zksync-rollup.fullname" . }}-server` in our chart; add `-p2p` suffix if you split the Service. For the `zkos-test-server-p2p` pattern in `adi-testnet-test`, the Service is exactly `zkos-test-server-p2p` (no helm-fullname prefix).

### Deploy steps

1. **Confirm the LB has an external IP:**
   ```bash
   NS=<devnet4-namespace>
   SVC=<p2p-service-name>         # e.g. cinder-adi-devnet4-integration-zksync-rollup-server-p2p
   LB_IP=$(kubectl -n "$NS" get svc "$SVC" \
     -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
   [ -n "$LB_IP" ] || { echo "LB IP not populated yet — wait/retry"; exit 1; }
   echo "LB IP: $LB_IP"
   ```
   On AWS the field may be `.hostname` instead of `.ip` — resolve it with `dig +short` before writing to Vault.

2. **Update the Vault secret** at `Adi-chain/cinder/adi/devnet4/server`:
   ```bash
   vault kv patch -mount=Adi-chain cinder/adi/devnet4/server \
     network_advertised_address="$LB_IP"
   ```

3. **Bump the image tag** to `v0.17.1-a1` in the devnet4 ArgoCD workload / values file. Commit + sync.

4. **Watch the pod come up:**
   ```bash
   kubectl -n "$NS" logs -f <server-pod> | grep -iE 'network|enr|p2p|advertised|address'
   ```
   Success signals:
   - `Loaded config ... network_config: NetworkConfig { ..., advertised_address: Some(<LB_IP>), ... }`
   - discv5 module logs (`target=reth_discv5` or `target=discv5`) showing the ENR with `ip: <LB_IP>`
   - No panic at `lib.rs:425`
   - STUN log line is still present (informational) and now shows a *different* IP than `advertised_address` — that's the cluster egress IP, and it being different is the whole point.

## ENR inspection — how to actually see it

The ENR is logged at info/debug by both reth_discv5 and discv5 crates. If the log output isn't clear enough, two deeper options:

- **Raw discv5 probe from outside.** With a small script using `discv5` crate (or `enr` crate + `nc -u`), send a discv5 PING to `<LB_IP>:3060`, get back the node's ENR, decode, and check the `ip` field matches `<LB_IP>`. Example with `cast` is **not** sufficient — cast doesn't speak discv5. A short Rust or Go test program is the lightweight path.
- **Peer-side view.** Once an EN connects (next step below), the main node shows up in the EN's peer table with its ENR. The EN's logs will include the gossiped ENR; grep for the peer ID.

If both the config log line AND the EN's view of the ENR show the right IP, the pin is working.

## Validation — EN can connect

1. **Build the enode URL** from the node's peer ID + the LB IP:
   ```bash
   SK=$(vault kv get -mount=Adi-chain -field=network_secret_key cinder/adi/devnet4/server)
   PEER_ID=$(cast wallet public-key --private-key "0x$SK" | sed 's/^0x//')
   echo "enode://$PEER_ID@$LB_IP:3060"
   unset SK
   ```

2. **Configure one EN** (local test, or one of the existing devnet4 ENs if you have an idle one) with:
   ```bash
   network_enabled=true
   network_secret_key=<EN's own key, not the main node's>
   network_boot_nodes=enode://<peer-id>@<LB-IP>:3060
   ```

3. **Watch the EN log** for successful peer handshake:
   - `connected to peer` / `handshake complete` lines
   - No `dial failure` / `connection refused` / `timeout` errors

4. **Watch the main node log** from the other side — it should show an incoming peer with the EN's observed source IP.

5. **Sync check:** the EN should start downloading replays over RLPx. Block numbers on the EN should advance. This is the end-to-end success signal.

## Promotion criteria → `v0.17.1-b2`

Promote only if all of these hold on the POC build for at least 24h:

- [ ] Main node comes up without panics; ENR shows the LB IP
- [ ] At least one EN connects via the published enode URL
- [ ] EN stays connected across at least one main-node restart (so we know reconnect works)
- [ ] EN is syncing blocks (replay stream is functional, not just the handshake)
- [ ] Main node's logs show no new warnings from reth/discv5 related to the pinned ENR
- [ ] No regression in existing test suite (the MVP is additive; all current tests should pass unchanged)

Then:

```bash
git tag v0.17.1-b2 v0.17.1-a1   # tag the same commit
git push origin v0.17.1-b2
```

CI builds the `-b2` image. Update prod workloads to point at `v0.17.1-b2`. The `-a1` image can be garbage-collected from harbor.

## Rollback

The field is additive, so rollback has two orthogonal parts — do whichever is needed:

1. **Revert the image** — flip the env's values.yaml image tag back to `v0.17.1-b1`, commit, let ArgoCD sync. Old binary ignores `network_advertised_address` (smart-config warns on unknown fields but doesn't panic), so the extra env var is harmless.

2. **Revert the Vault secret** — rollback to the pre-patch version. The current version is `N` (bumped by `kv patch`), so the prior state is `N-1`:
   ```bash
   vault kv metadata get -mount=Adi-chain cinder/adi/devnet4/server  # confirm current version
   vault kv rollback -mount=Adi-chain -version=$((CURRENT-1)) cinder/adi/devnet4/server
   ```
   Or equivalently, re-patch to remove the field:
   ```bash
   vault kv patch -mount=Adi-chain cinder/adi/devnet4/server network_advertised_address=
   ```
   (Empty value — smart-config will parse it as absent for an `Option<Ipv4Addr>` field. Double-check this doesn't parse as `"0.0.0.0"` instead; if in doubt, use the rollback command.)

Neither step causes data loss or peer-table disruption beyond the normal restart churn.

## Open questions to resolve during implementation

- **Exact reth 1.11.1 / discv5 crate version's API surface** — flagged above. Biggest uncertainty in the patch; everything else is trivial plumbing. The specific crate tags to consult: `reth-network`, `reth-discv5`, `reth-net-nat` all pinned at `git tag v1.11.1` via `Cargo.toml` (confirmed). The transitive `discv5` crate version comes from reth's Cargo.lock — read it once before coding to pick the right builder calls.
- **Initial ENR sequence number** — when we disable peer-update, the first-gossiped ENR needs to already carry the right IP. If there's any lag where an unpinned ENR escapes to peers, they cache it. Verify by checking the ENR at t=0 immediately post-startup, not after any peer handshake.
- **Interaction with `network.boot_nodes` having our own enode URL** — main node shouldn't bootnode itself, but worth confirming no weird loop if someone misconfigures.
- **Harbor image build path** — flagged in "Build & tag workflow" above. Resolve this before `-a1` push or the validation loop won't have a deployable artifact.
- **Infra chart Service type** — flagged in "Service-type caveat" above. Our chart doesn't ship a LoadBalancer Service for p2p; add one in devnet4 values, or use the existing `zkos-test-server-p2p` deployment in `adi-testnet-test` as the validation target instead.

## Follow-ons (not blocking this POC)

- Infra-side: extend `env-init` to auto-populate `network_advertised_address` by querying `kubectl get svc` at job runtime, so operators don't copy/paste IPs. (Tracked as infra task.)
- Upstream: file issue + PR against `matter-labs/zksync-os-server:main` with exactly this field. Target 0.19-based main branch; 0.17.x is no longer patched upstream. (Tracked as upstream task.)
- Docs: update `docs/upgrades/server/0.13-0.17/v0.13.0_to_v0.17.0_main_node.md` with the new field once `-b2` ships.
