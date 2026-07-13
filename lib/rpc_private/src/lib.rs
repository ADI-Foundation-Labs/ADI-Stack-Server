use alloy::primitives::U256;
use async_trait::async_trait;
use jsonrpsee::RpcModule;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::ServerBuilder;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::watch;

#[cfg(test)]
mod tests;

/// Configuration overrides that can be set via private API.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ConfigOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_fee: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pubdata_price: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_price: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_sender_max_fee_per_gas_wei: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_sender_max_priority_fee_per_gas_wei: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_sender_max_fee_per_blob_gas_wei: Option<U256>,
}

impl ConfigOverrides {
    /// Load config overrides from file if it exists.
    fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let overrides: Self = serde_json::from_str(&contents)?;
        tracing::info!(?path, "Loaded config overrides from file");
        Ok(overrides)
    }

    /// Save config overrides to file.
    fn save_to_file(&self, path: &PathBuf) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        // Write atomically (tmp + rename) so a crash mid-write can't corrupt the file.
        let tmp_path = path.with_extension("json.tmp");
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Merge with another ConfigOverrides, preferring values from self.
    fn merge_with(self, fallback: Self) -> Self {
        Self {
            base_fee: self.base_fee.or(fallback.base_fee),
            pubdata_price: self.pubdata_price.or(fallback.pubdata_price),
            native_price: self.native_price.or(fallback.native_price),
            l1_sender_max_fee_per_gas_wei: self
                .l1_sender_max_fee_per_gas_wei
                .or(fallback.l1_sender_max_fee_per_gas_wei),
            l1_sender_max_priority_fee_per_gas_wei: self
                .l1_sender_max_priority_fee_per_gas_wei
                .or(fallback.l1_sender_max_priority_fee_per_gas_wei),
            l1_sender_max_fee_per_blob_gas_wei: self
                .l1_sender_max_fee_per_blob_gas_wei
                .or(fallback.l1_sender_max_fee_per_blob_gas_wei),
        }
    }

    /// Remove overrides by field name.
    fn remove_fields(mut self, fields: Vec<String>) -> Self {
        for field in fields {
            match field.as_str() {
                "base_fee" => self.base_fee = None,
                "pubdata_price" => self.pubdata_price = None,
                "native_price" => self.native_price = None,
                "l1_sender_max_fee_per_gas_wei" => self.l1_sender_max_fee_per_gas_wei = None,
                "l1_sender_max_priority_fee_per_gas_wei" => {
                    self.l1_sender_max_priority_fee_per_gas_wei = None
                }
                "l1_sender_max_fee_per_blob_gas_wei" => {
                    self.l1_sender_max_fee_per_blob_gas_wei = None
                }
                _ => tracing::warn!("Unknown field to remove: {}", field),
            }
        }
        self
    }
}

/// Private RPC API for runtime configuration.
#[rpc(server, namespace = "config")]
pub trait ConfigApi {
    /// Set or update configuration overrides.
    /// Only fields present in the request will be updated.
    /// Omitted fields remain unchanged.
    #[method(name = "setOverrides")]
    async fn set_overrides(&self, overrides: ConfigOverrides) -> RpcResult<()>;

    /// Remove configuration overrides by field name.
    #[method(name = "removeOverrides")]
    async fn remove_overrides(&self, fields: Vec<String>) -> RpcResult<()>;

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
        let current = self.receiver.borrow().clone();
        let updated = overrides.clone().merge_with(current);
        self.sender.send_replace(updated.clone());
        tracing::info!(?overrides, ?updated, "config overrides set via private API");
        Ok(())
    }

    async fn remove_overrides(&self, fields: Vec<String>) -> RpcResult<()> {
        let current = self.receiver.borrow().clone();
        let updated = current.remove_fields(fields.clone());
        self.sender.send_replace(updated.clone());
        tracing::info!(
            ?fields,
            ?updated,
            "config overrides removed via private API"
        );
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
    fallback: ConfigOverrides,
    persistence_path: PathBuf,
) -> anyhow::Result<watch::Receiver<ConfigOverrides>> {
    tracing::info!(
        ?fallback,
        ?persistence_path,
        "Initializing private API server"
    );

    // Load from file if exists, merge with fallback
    let loaded = ConfigOverrides::load_from_file(&persistence_path)
        .inspect_err(|e| tracing::warn!(?e, "Could not load config overrides from file"))
        .ok();

    let initial = loaded
        .clone()
        .unwrap_or_default()
        .merge_with(fallback.clone());

    tracing::info!(
        %address,
        ?loaded,
        ?fallback,
        ?initial,
        "Starting private API server with merged config"
    );

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

    // Spawn task to persist config overrides to file on changes
    let mut persistence_watcher = receiver.clone();
    tokio::spawn(async move {
        while persistence_watcher.changed().await.is_ok() {
            let overrides = persistence_watcher.borrow_and_update().clone();
            if let Err(e) = overrides.save_to_file(&persistence_path) {
                tracing::error!(?e, "Failed to persist config overrides");
            }
        }
    });

    Ok(receiver)
}
