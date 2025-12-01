use alloy::primitives::U256;
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::ServerBuilder;
use jsonrpsee::RpcModule;
use std::net::SocketAddr;
use tokio::sync::watch;

/// Configuration overrides that can be set via private API.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ConfigOverrides {
    pub base_fee: Option<U256>,
    pub pubdata_price: Option<U256>,
    pub native_price: Option<U256>,
}

/// Private RPC API for runtime configuration.
#[rpc(server, namespace = "config")]
pub trait ConfigApi {
    /// Set configuration overrides.
    #[method(name = "setOverrides")]
    async fn set_overrides(&self, overrides: ConfigOverrides) -> RpcResult<()>;

    /// Get the current configuration overrides.
    #[method(name = "getOverrides")]
    async fn get_overrides(&self) -> RpcResult<ConfigOverrides>;
}

pub struct ConfigNamespace {
    sender: watch::Sender<ConfigOverrides>,
    receiver: watch::Receiver<ConfigOverrides>,
}

impl ConfigNamespace {
    pub fn new(initial: ConfigOverrides) -> (Self, watch::Receiver<ConfigOverrides>) {
        let (sender, receiver) = watch::channel(initial);
        let namespace = Self {
            sender,
            receiver: receiver.clone(),
        };
        (namespace, receiver)
    }
}

#[async_trait]
impl ConfigApiServer for ConfigNamespace {
    async fn set_overrides(&self, overrides: ConfigOverrides) -> RpcResult<()> {
        self.sender.send_replace(overrides.clone());
        tracing::info!(?overrides, "config overrides updated via private API");
        Ok(())
    }

    async fn get_overrides(&self) -> RpcResult<ConfigOverrides> {
        Ok(self.receiver.borrow().clone())
    }
}

/// Run the private JSON-RPC server.
/// Returns a receiver that provides the current config overrides.
pub async fn run_private_rpc_server(
    address: SocketAddr,
    initial: ConfigOverrides,
) -> anyhow::Result<watch::Receiver<ConfigOverrides>> {
    tracing::info!(%address, ?initial, "Starting private API server");

    let (config_namespace, receiver) = ConfigNamespace::new(initial);

    let mut rpc = RpcModule::new(());
    rpc.merge(config_namespace.into_rpc())?;

    let server = ServerBuilder::default()
        .build(address)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build private RPC server: {}", e))?;

    let handle = server.start(rpc);

    tokio::spawn(async move {
        handle.stopped().await;
        tracing::warn!("Private RPC server stopped");
    });

    Ok(receiver)
}
