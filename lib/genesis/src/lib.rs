use alloy::consensus::{EMPTY_OMMER_ROOT_HASH, Header};
use alloy::eips::eip1559::INITIAL_BASE_FEE;
use alloy::hex;
use alloy::network::Ethereum;
use alloy::primitives::{Address, B64, B256, Bloom, Bytes, U256};
use alloy::providers::{DynProvider, Provider};
use alloy::rlp::Encodable;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Context;
use blake2::{Blake2s256, Digest};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::OnceCell;
use zk_os_api::helpers::{set_properties_code, set_properties_nonce};
use zk_os_basic_system::system_implementation::flat_storage_model::{
    ACCOUNT_PROPERTIES_STORAGE_ADDRESS, AccountProperties,
};
use zksync_os_contract_interface::IL1GenesisUpgrade::GenesisUpgrade;
use zksync_os_contract_interface::ZkChain;
use zksync_os_interface::types::BlockContext;
use zksync_os_types::L1UpgradeEnvelope;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenesisInput {
    /// Initial contracts to deploy in genesis.
    /// Storage entries that set the contracts as deployed and preimages will be derived from this field.
    pub initial_contracts: Vec<(Address, alloy::primitives::Bytes)>,

    /// "Pretty" additional storage in address -> key -> value form.
    /// Keys and values must be 32 bytes (B256).
    /// Example:
    /// {
    ///   "0x...1000c": { "0x00..00": "0x...800f" },
    ///   "0x...800f": {
    ///     "0x3608...2bbc": "0x504c4a...f87",
    ///     "0xb531...6103": "0x0000...1000c"
    ///   }
    /// }
    #[serde(default)]
    pub additional_storage: BTreeMap<Address, BTreeMap<B256, B256>>,

    /// Raw (already flattened) additional storage, kept for backward compatibility.
    /// Same format as before.
    #[serde(default)]
    pub additional_storage_raw: Vec<(B256, B256)>,

    /// Execution version used for genesis.
    pub execution_version: u32,
    /// The expected root hash of the genesis state.
    pub genesis_root: B256,
}

impl GenesisInput {
    pub fn load_from_file(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path).context("Failed to open genesis input file")?;
        serde_json::from_reader(file).context("Failed to parse genesis input file")
    }
}

/// Info about genesis upgrade fetched from L1:
/// - genesis upgrade tx
/// - force deploy bytecode hashes and preimages, note that preimages are not padded and do not contain artifacts
#[derive(Debug, Clone)]
pub struct GenesisUpgradeTxInfo {
    pub tx: L1UpgradeEnvelope,
    pub force_deploy_preimages: Vec<(B256, Vec<u8>)>,
}

/// Struct that represents the genesis state of the system.
/// Lazy-initialized to avoid unnecessary computation at startup.
#[derive(Clone)]
pub struct Genesis {
    input_source: Arc<dyn GenesisInputSource>,
    zk_chain: ZkChain<DynProvider>,
    state: OnceCell<GenesisState>,
    genesis_upgrade_tx: OnceCell<GenesisUpgradeTxInfo>,
}

impl Debug for Genesis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Genesis")
            .field("input_source", &self.input_source)
            .field("zk_chain", &self.zk_chain.address())
            .field("state", &self.state.get())
            .field("genesis_upgrade_tx", &self.genesis_upgrade_tx.get())
            .finish()
    }
}

impl Genesis {
    pub fn new(input_source: Arc<dyn GenesisInputSource>, zk_chain: ZkChain<DynProvider>) -> Self {
        Self {
            input_source,
            zk_chain,
            state: OnceCell::new(),
            genesis_upgrade_tx: OnceCell::new(),
        }
    }

    pub async fn state(&self) -> &GenesisState {
        self.state
            .get_or_try_init(|| build_genesis(self.input_source.as_ref()))
            .await
            .expect("Failed to build genesis state")
    }

    pub async fn genesis_upgrade_tx(&self) -> GenesisUpgradeTxInfo {
        self.genesis_upgrade_tx
            .get_or_try_init(|| load_genesis_upgrade_tx(self.zk_chain.clone()))
            .await
            .expect("Failed to load genesis upgrade transaction")
            .clone()
    }
}

#[derive(Debug, Clone)]
pub struct GenesisState {
    /// Storage logs for the genesis block.
    pub storage_logs: Vec<(B256, B256)>,
    /// Preimages of the padded bytecodes with artifacts and hashes of account properties
    /// for the contracts deployed in the genesis block.
    /// Note: these preimages don't include `force_deploy_preimages` -
    /// see `genesis_upgrade_tx` method for details
    pub preimages: Vec<(B256, Vec<u8>)>,
    /// The header of the genesis block.
    pub header: Header,
    /// Context of the genesis block.
    pub context: BlockContext,
    /// Expected genesis root (state commitment).
    pub expected_genesis_root: B256,
}

fn flat_storage_key_for_contract(address: Address, key: B256) -> B256 {
    // Flat key = blake2s256( pad32(address) || key )
    let mut bytes = [0u8; 64];
    // first 32 bytes: address left-padded into the last 20 bytes
    bytes[12..32].copy_from_slice(address.as_slice());
    // second 32 bytes: the full storage slot key
    bytes[32..64].copy_from_slice(key.as_slice());
    B256::from_slice(Blake2s256::digest(bytes).as_slice())
}

fn account_properties_flat_key(address: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..32].copy_from_slice(&address.as_slice());

    flat_storage_key_for_contract(ACCOUNT_PROPERTIES_STORAGE_ADDRESS.to_be_bytes().into(), bytes.into())
}

// fn encode(&self, out: &mut dyn BufMut) {
//     let list_header =
//         alloy_rlp::Header { list: true, payload_length: self.header_payload_length() };
//     list_header.encode(out);
//     self.parent_hash.encode(out);
//     self.ommers_hash.encode(out);
//     self.beneficiary.encode(out);
//     self.state_root.encode(out);
//     self.transactions_root.encode(out);
//     self.receipts_root.encode(out);
//     self.logs_bloom.encode(out);
//     self.difficulty.encode(out);
//     U256::from(self.number).encode(out);
//     U256::from(self.gas_limit).encode(out);
//     U256::from(self.gas_used).encode(out);
//     self.timestamp.encode(out);
//     self.extra_data.encode(out);
//     self.mix_hash.encode(out);
//     self.nonce.encode(out);

//     // Encode all the fork specific fields
//     if let Some(ref base_fee) = self.base_fee_per_gas {
//         U256::from(*base_fee).encode(out);
//     }

//     if let Some(ref root) = self.withdrawals_root {
//         root.encode(out);
//     }

//     if let Some(ref blob_gas_used) = self.blob_gas_used {
//         U256::from(*blob_gas_used).encode(out);
//     }

//     if let Some(ref excess_blob_gas) = self.excess_blob_gas {
//         U256::from(*excess_blob_gas).encode(out);
//     }

//     if let Some(ref parent_beacon_block_root) = self.parent_beacon_block_root {
//         parent_beacon_block_root.encode(out);
//     }

//     if let Some(ref requests_hash) = self.requests_hash {
//         requests_hash.encode(out);
//     }
// }


// #[test]
// fn test_empty_block_hash() {
    
//     let header = Header {
//         parent_hash: B256::ZERO,
//         ommers_hash: EMPTY_OMMER_ROOT_HASH,
//         beneficiary: Address::ZERO,
//         // for now state root is zero
//         state_root: B256::ZERO,
//         transactions_root: B256::ZERO,
//         receipts_root: B256::ZERO,
//         logs_bloom: Bloom::ZERO,
//         difficulty: U256::ZERO,
//         number: 0,
//         gas_limit: 5_000,
//         gas_used: 0,
//         timestamp: 0,
//         extra_data: Default::default(),
//         mix_hash: B256::ZERO,
//         nonce: B64::ZERO,
//         base_fee_per_gas: Some(INITIAL_BASE_FEE),
//         withdrawals_root: None,
//         blob_gas_used: None,
//         excess_blob_gas: None,
//         parent_beacon_block_root: None,
//         requests_hash: None,
//     };

//     let mut out = Vec::new();
//     header.encode(&mut out);

//     println!("{:#?}", header.hash_slow());
//     println!("{}", hex::encode(&out));
// }


async fn build_genesis(genesis_input_source: &dyn GenesisInputSource) -> anyhow::Result<GenesisState> {
    let genesis_input = genesis_input_source.genesis_input().await?;

    // BTreeMap is used to ensure that the storage logs are sorted by key, so that the order is deterministic
    // which is important for tree.
    let mut storage_logs: BTreeMap<B256, B256> = BTreeMap::new();
    let mut preimages = vec![];

    for (address, deployed_code) in genesis_input.initial_contracts {
        let mut account_properties = AccountProperties::default();
        // When contracts are deployed, they have a nonce of 1.
        set_properties_nonce(&mut account_properties, 1);
        let bytecode_preimage = set_properties_code(&mut account_properties, &deployed_code);
        let bytecode_hash = account_properties.bytecode_hash;

        let flat_storage_key = account_properties_flat_key(address);

        println!("\n\n\n\n\n===== {:#?} ====", flat_storage_key);
        println!(
            "versioning_data: {}",
            hex::encode(account_properties.versioning_data.into_u64().to_be_bytes())
        );
        println!(
            "nonce: {}",
            hex::encode(account_properties.nonce.to_be_bytes())
        );
        println!(
            "balance: {}",
            hex::encode(account_properties.balance.to_be_bytes::<32>())
        );
        println!(
            "bytecode_hash: {}",
            hex::encode(account_properties.bytecode_hash.as_u8_ref())
        );
        println!(
            "unpadded_code_len: {}",
            hex::encode(account_properties.unpadded_code_len.to_be_bytes())
        );
        println!(
            "artifacts_len: {}",
            hex::encode(account_properties.artifacts_len.to_be_bytes())
        );
        println!(
            "observable_bytecode_hash: {}",
            hex::encode(account_properties.observable_bytecode_hash.as_u8_ref())
        );
        println!(
            "observable_bytecode_len: {}",
            hex::encode(account_properties.observable_bytecode_len.to_be_bytes())
        );

        let account_properties_hash = account_properties.compute_hash();
        storage_logs.insert(
            flat_storage_key,
            account_properties_hash.as_u8_array().into(),
        );

        preimages.push((bytecode_hash.as_u8_array().into(), bytecode_preimage));
        preimages.push((
            account_properties_hash.as_u8_array().into(),
            account_properties.encoding().to_vec(),
        ));
    }

    // 1) Insert RAW additional storage first
    for (key, value) in genesis_input.additional_storage_raw {
        let duplicate = storage_logs.insert(key, value).is_some();
        if duplicate {
            anyhow::bail!("Genesis input contains duplicate storage key in additional_storage_raw: {key:?}");
        }
    }

    // 2) Flatten and insert "pretty" additional storage (address -> key -> value).
    for (address, slots) in genesis_input.additional_storage {
        for (slot_key, value_b256) in slots {
            let flat_key = flat_storage_key_for_contract(address, slot_key);

            let duplicate = storage_logs.insert(flat_key, value_b256).is_some();
            if duplicate {
                anyhow::bail!(
                    "Genesis input contains duplicate flattened storage key derived from address {address:?}, slot {slot_key:?}. \
                     This likely conflicts with additional_storage_raw."
                );
            }
        }
    }

    let logs = storage_logs.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    println!("{:#?}", logs);

    let header = Header {
        parent_hash: B256::ZERO,
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: Address::ZERO,
        // for now state root is zero
        state_root: B256::ZERO,
        transactions_root: B256::ZERO,
        receipts_root: B256::ZERO,
        logs_bloom: Bloom::ZERO,
        difficulty: U256::ZERO,
        number: 0,
        gas_limit: 5_000,
        gas_used: 0,
        timestamp: 0,
        extra_data: Default::default(),
        mix_hash: B256::ZERO,
        nonce: B64::ZERO,
        base_fee_per_gas: Some(INITIAL_BASE_FEE),
        withdrawals_root: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        requests_hash: None,
    };

    let context = BlockContext {
        // todo: This shouldn't matter for genesis, right? maybe populate anyways
        chain_id: 0,
        block_number: 0,
        block_hashes: Default::default(),
        timestamp: 0,
        eip1559_basefee: U256::from(header.base_fee_per_gas.unwrap()),
        pubdata_price: U256::from(0),
        native_price: U256::from(1),
        coinbase: header.beneficiary,
        gas_limit: 100_000_000,
        pubdata_limit: 100_000_000,
        mix_hash: U256::ZERO,
        execution_version: genesis_input.execution_version,
    };

    Ok(GenesisState {
        storage_logs: storage_logs.into_iter().collect(),
        preimages,
        header,
        context,
        expected_genesis_root: genesis_input.genesis_root,
    })
}

async fn load_genesis_upgrade_tx(
    zk_chain: ZkChain<DynProvider>,
) -> anyhow::Result<GenesisUpgradeTxInfo> {
    const MAX_L1_BLOCKS_LOOKBEHIND: u64 = 100_000;

    let zk_chain_address = *zk_chain.address();
    let provider = zk_chain.provider().clone();
    let current_l1_block = zk_chain.provider().get_block_number().await?;
    // Find the block when the zk chain was deployed or fallback to [0; latest_block] in localhost case.
    let (from_block, to_block) = zksync_os_l1_watcher::util::find_l1_block_by_predicate(
            Arc::new(zk_chain),
            |_zk, _block| async { Ok(true) },
        )
        .await
        .map(|b| (b, b))
        .or_else(|err| {
            // This may error on Anvil with `--load-state` - as it doesn't support requests even for recent blocks.
            // We default to `[0; latest_block]` in this case - `eth_getLogs` are still supported.
            // Assert that we don't fallback on longer chains (e.g. Sepolia)
            if current_l1_block > MAX_L1_BLOCKS_LOOKBEHIND {
                anyhow::bail!(
                    "Binary search failed with {err}. Cannot default starting block to zero for a long chain. Current L1 block number: {current_l1_block}. Limit: {MAX_L1_BLOCKS_LOOKBEHIND}."
                )
            } else {
                Ok((0, current_l1_block))
            }
        })?;
    let event_sig = GenesisUpgrade::SIGNATURE_HASH;
    let filter = Filter::new()
        .from_block(from_block)
        .to_block(to_block)
        .event_signature(event_sig)
        .address(zk_chain_address);
    let logs = provider.get_logs(&filter).await?;
    anyhow::ensure!(
        logs.len() == 1,
        "Expected exactly one genesis upgrade tx log, found these {logs:?}"
    );
    let sol_event = GenesisUpgrade::decode_log(&logs[0].inner)?.data;
    let upgrade_tx = L1UpgradeEnvelope::try_from(sol_event._l2Transaction)?;
    let preimages = sol_event
        ._factoryDeps
        .into_iter()
        .map(|preimage| {
            let preimage = preimage.to_vec();
            let digest = Blake2s256::digest(&preimage);
            let mut digest_array = [0u8; 32];
            digest_array.copy_from_slice(digest.as_slice());
            (B256::new(digest_array), preimage)
        })
        .collect();

    Ok(GenesisUpgradeTxInfo {
        tx: upgrade_tx,
        force_deploy_preimages: preimages,
    })
}

#[async_trait::async_trait]
pub trait GenesisInputSource: Debug + Send + Sync + 'static {
    async fn genesis_input(&self) -> anyhow::Result<GenesisInput>;
}

#[derive(Debug)]
pub struct FileGenesisInputSource {
    path: PathBuf,
}

impl FileGenesisInputSource {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl GenesisInputSource for FileGenesisInputSource {
    async fn genesis_input(&self) -> anyhow::Result<GenesisInput> {
        GenesisInput::load_from_file(&self.path)
    }
}
