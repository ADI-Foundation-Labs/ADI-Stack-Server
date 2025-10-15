use std::{convert::TryFrom, fmt::Write as _};

use anyhow::{anyhow, Context, Result};
use bincode::config::standard;
use zksync_os_interface::types::BlockContext;
use zksync_os_types::{ZkTransaction, ZkTxType};

use super::utils::{decode_b256, decode_u64, format_address, format_b256, format_optional_address};
use super::{EntryField, EntryRecord, FieldCapabilities, FieldRole, FieldValue, Schema};

pub struct BlockReplaySchema;

const DB_NAME: &str = "block_replay_wal";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnFamily {
    Context,
    LastProcessedL1TxId,
    Txs,
    NodeVersion,
    BlockOutputHash,
    Latest,
}

impl ColumnFamily {
    const COUNT: usize = 6;

    const fn as_str(self) -> &'static str {
        match self {
            Self::Context => "context",
            Self::LastProcessedL1TxId => "last_processed_l1_tx_id",
            Self::Txs => "txs",
            Self::NodeVersion => "node_version",
            Self::BlockOutputHash => "block_output_hash",
            Self::Latest => "latest",
        }
    }

    fn parse(name: &str) -> Result<Self> {
        match name {
            name if name == Self::Context.as_str() => Ok(Self::Context),
            name if name == Self::LastProcessedL1TxId.as_str() => Ok(Self::LastProcessedL1TxId),
            name if name == Self::Txs.as_str() => Ok(Self::Txs),
            name if name == Self::NodeVersion.as_str() => Ok(Self::NodeVersion),
            name if name == Self::BlockOutputHash.as_str() => Ok(Self::BlockOutputHash),
            name if name == Self::Latest.as_str() => Ok(Self::Latest),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }

    fn matches(self, other: &str) -> bool {
        other == self.as_str()
    }
}

const COLUMN_FAMILY_NAMES: [&str; ColumnFamily::COUNT] = [
    ColumnFamily::Context.as_str(),
    ColumnFamily::LastProcessedL1TxId.as_str(),
    ColumnFamily::Txs.as_str(),
    ColumnFamily::NodeVersion.as_str(),
    ColumnFamily::BlockOutputHash.as_str(),
    ColumnFamily::Latest.as_str(),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Field {
    Block,
    LastL1TxId,
    Version,
    Hash,
    MetaKey,
}

impl Field {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::LastL1TxId => "last_l1_tx_id",
            Self::Version => "version",
            Self::Hash => "hash",
            Self::MetaKey => "meta_key",
        }
    }

    fn matches(self, other: &str) -> bool {
        other.eq_ignore_ascii_case(self.as_str())
    }
}

impl Schema for BlockReplaySchema {
    fn name(&self) -> &'static str {
        DB_NAME
    }

    fn db_path(&self, base: &std::path::Path) -> std::path::PathBuf {
        base.join(DB_NAME)
    }

    fn column_families(&self) -> &'static [&'static str] {
        &COLUMN_FAMILY_NAMES
    }

    fn decode_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
        match ColumnFamily::parse(cf)? {
            ColumnFamily::Context => format_context(cf, key, value),
            ColumnFamily::LastProcessedL1TxId => format_last_l1_id(cf, key, value),
            ColumnFamily::Txs => format_txs(cf, key, value),
            ColumnFamily::NodeVersion => format_node_version(cf, key, value),
            ColumnFamily::BlockOutputHash => format_block_output_hash(cf, key, value),
            ColumnFamily::Latest => format_latest(cf, key, value),
        }
    }

    fn update_value(
        &self,
        cf: &str,
        _entry: &EntryRecord,
        field_name: &str,
        new_value: &FieldValue,
    ) -> Result<Vec<u8>> {
        if ColumnFamily::Latest.matches(cf) && Field::Block.matches(field_name) {
            let block = match new_value {
                FieldValue::Unsigned(value) => *value,
                _ => return Err(anyhow!("Block number must be an unsigned integer")),
            };
            let block_u64 = u64::try_from(block)
                .map_err(|_| anyhow!("Block number {block} exceeds u64 range"))?;
            Ok(block_u64.to_be_bytes().to_vec())
        } else {
            Err(anyhow!("Editing not supported for column family `{cf}`"))
        }
    }
}

fn format_context(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let block = decode_u64(key)?;
    let (ctx, _) = bincode::serde::decode_from_slice::<BlockContext, _>(value, standard())?;
    let summary = format!(
        "block {block}: ts={} basefee={} pubdata_limit={}",
        ctx.timestamp, ctx.eip1559_basefee, ctx.pubdata_limit
    );
    let detail = format!(
        "Block #{block}\nTimestamp: {}\nChain ID: {}\nGas limit: {}\nPubdata limit: {}\nBase fee: {}\nFee recipient: {}\nExecution version: {}\nHashes[255]: {}\n",
        ctx.timestamp,
        ctx.chain_id,
        ctx.gas_limit,
        ctx.pubdata_limit,
        ctx.eip1559_basefee,
        format_address(ctx.coinbase),
        ctx.execution_version,
        ctx.block_hashes.0[255]
    );
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_field(EntryField::unsigned(
            Field::Block.as_str(),
            block,
            FieldRole::Key,
            FieldCapabilities::default()
                .sortable()
                .searchable()
                .key_part(),
        )),
    )
}

fn format_last_l1_id(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let block = decode_u64(key)?;
    let id = decode_u64(value)?;
    let summary = format!("block {block}: next L1 priority id {id}");
    let detail = format!("Block #{block}\nLast processed L1 tx id: {id}");
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::unsigned(
                Field::Block.as_str(),
                block,
                FieldRole::Key,
                FieldCapabilities::default()
                    .sortable()
                    .searchable()
                    .key_part(),
            ),
            EntryField::unsigned(
                Field::LastL1TxId.as_str(),
                id,
                FieldRole::Value,
                FieldCapabilities::default().searchable(),
            ),
        ]),
    )
}

fn format_txs(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let block = decode_u64(key)?;
    let (txs, _) = bincode::decode_from_slice::<Vec<ZkTransaction>, _>(value, standard())?;
    let counts = tx_counts(&txs);
    let summary = format!(
        "block {block}: {} txs (L1 {}, L2 {}, upgrade {})",
        txs.len(),
        counts.l1,
        counts.l2,
        counts.upgrade
    );
    let mut detail = format!("Block #{block} transactions ({} total):\n", txs.len());
    for (idx, tx) in txs.iter().enumerate() {
        let _ = writeln!(
            detail,
            "  #{idx:<3} {} | nonce {} | to {}",
            tx_summary(tx),
            tx.nonce(),
            format_optional_address(tx.to())
        );
    }
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_field(EntryField::unsigned(
            Field::Block.as_str(),
            block,
            FieldRole::Key,
            FieldCapabilities::default()
                .sortable()
                .searchable()
                .key_part(),
        )),
    )
}

fn format_node_version(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let block = decode_u64(key)?;
    let version = String::from_utf8(value.to_vec()).context("node_version entry is not UTF-8")?;
    let summary = format!("block {block}: node {version}");
    let detail = format!("Block #{block}\nNode version: {version}");
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::unsigned(
                Field::Block.as_str(),
                block,
                FieldRole::Key,
                FieldCapabilities::default()
                    .sortable()
                    .searchable()
                    .key_part(),
            ),
            EntryField::text(
                Field::Version.as_str(),
                version,
                FieldRole::Value,
                FieldCapabilities::default().searchable(),
            ),
        ]),
    )
}

fn format_block_output_hash(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let block = decode_u64(key)?;
    let hash = decode_b256(value, "block output hash")?;
    let summary = format!("block {block}: output {}", format_b256(hash, 12));
    let detail = format!(
        "Block #{block}\nBlock output hash: {}",
        format_b256(hash, 0)
    );
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::unsigned(
                Field::Block.as_str(),
                block,
                FieldRole::Key,
                FieldCapabilities::default()
                    .sortable()
                    .searchable()
                    .key_part(),
            ),
            EntryField::text(
                Field::Hash.as_str(),
                format_b256(hash, 0),
                FieldRole::Value,
                FieldCapabilities::default().searchable(),
            ),
        ]),
    )
}

fn format_latest(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let key_str = String::from_utf8_lossy(key);
    let block = decode_u64(value)?;
    let summary = format!("{key_str} → {block}");
    let detail = format!("Metadata key `{key_str}`\nValue: {block} (latest block number)");
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::MetaKey.as_str(),
                key_str.to_string(),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::unsigned(
                Field::Block.as_str(),
                block,
                FieldRole::Value,
                FieldCapabilities::default()
                    .sortable()
                    .searchable()
                    .editable(),
            ),
        ]),
    )
}

struct TxCounts {
    l1: usize,
    l2: usize,
    upgrade: usize,
}

fn tx_counts(txs: &[ZkTransaction]) -> TxCounts {
    let mut buckets = TxCounts {
        l1: 0,
        l2: 0,
        upgrade: 0,
    };
    for tx in txs {
        match tx.tx_type() {
            ZkTxType::L1 => buckets.l1 += 1,
            ZkTxType::Upgrade => buckets.upgrade += 1,
            ZkTxType::L2(_) => buckets.l2 += 1,
        }
    }
    buckets
}

fn tx_summary(tx: &ZkTransaction) -> String {
    format!("{} signer {}", tx.tx_type(), format_address(tx.signer()))
}
