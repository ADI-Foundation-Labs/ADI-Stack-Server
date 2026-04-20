use reth_network::config::SecretKey;
use reth_network_peers::NodeRecord;
use std::net::Ipv4Addr;

#[derive(Debug)]
pub struct NetworkConfig {
    /// The node's secret key, from which the node's identity is derived. Used during initial RLPx
    /// handshake.
    pub secret_key: SecretKey,
    /// IPv4 address to use for Node Discovery Protocol v5 (discv5) and RLPx Transport Protocol (rlpx).
    pub address: Ipv4Addr,
    /// Port to use for Node Discovery Protocol v5 (discv5) and RLPx Transport Protocol (rlpx).
    pub port: u16,
    /// All boot nodes to start network discovery with. Expected format is
    /// `enode://<node ID>@<IP address or hostname>:<port>`.
    pub boot_nodes: Vec<NodeRecord>,
    /// If set, the node's ENR is pinned to this IPv4 address and discv5
    /// auto-update is disabled. If None, behavior is unchanged: the ENR is
    /// initialized from `address` and updated dynamically via PONG quorum.
    pub advertised_address: Option<Ipv4Addr>,
}
