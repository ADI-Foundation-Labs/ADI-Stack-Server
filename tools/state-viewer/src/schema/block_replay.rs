use std::fmt::Write as _;

use anyhow::{anyhow, Context, Result};
use bincode::config::standard;
use zksync_os_interface::types::BlockContext;
use zksync_os_types::{ZkTransaction, ZkTxType};

use super::utils::{decode_b256, decode_u64, format_address, format_b256, format_optional_address};
use super::{Entry, Schema};

pub struct BlockReplaySchema;

impl Schema for BlockReplaySchema {
    fn name(&self) -> &'static str {
        "block_replay_wal"
    }

    fn db_path(&self, base: &std::path::Path) -> std::path::PathBuf {
        base.join("block_replay_wal")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &[
            "context",
            "last_processed_l1_tx_id",
            "txs",
            "node_version",
            "block_output_hash",
            "latest",
        ]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "context" => format_context(key, value),
            "last_processed_l1_tx_id" => format_last_l1_id(key, value),
            "txs" => format_txs(key, value),
            "node_version" => format_node_version(key, value),
            "block_output_hash" => format_block_output_hash(key, value),
            "latest" => format_latest(key, value),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

fn format_context(key: &[u8], value: &[u8]) -> Result<Entry> {
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
    Ok(Entry::new(summary, detail))
}

fn format_last_l1_id(key: &[u8], value: &[u8]) -> Result<Entry> {
    let block = decode_u64(key)?;
    let id = decode_u64(value)?;
    let summary = format!("block {block}: next L1 priority id {id}");
    let detail = format!("Block #{block}\nLast processed L1 tx id: {id}");
    Ok(Entry::new(summary, detail))
}

fn format_txs(key: &[u8], value: &[u8]) -> Result<Entry> {
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
    Ok(Entry::new(summary, detail))
}

fn format_node_version(key: &[u8], value: &[u8]) -> Result<Entry> {
    let block = decode_u64(key)?;
    let version = String::from_utf8(value.to_vec()).context("node_version entry is not UTF-8")?;
    let summary = format!("block {block}: node {version}");
    let detail = format!("Block #{block}\nNode version: {version}");
    Ok(Entry::new(summary, detail))
}

fn format_block_output_hash(key: &[u8], value: &[u8]) -> Result<Entry> {
    let block = decode_u64(key)?;
    let hash = decode_b256(value, "block output hash")?;
    let summary = format!("block {block}: output {}", format_b256(hash, 12));
    let detail = format!(
        "Block #{block}\nBlock output hash: {}",
        format_b256(hash, 0)
    );
    Ok(Entry::new(summary, detail))
}

fn format_latest(key: &[u8], value: &[u8]) -> Result<Entry> {
    let key_str = String::from_utf8_lossy(key);
    let block = decode_u64(value)?;
    let summary = format!("{key_str} → {block}");
    let detail = format!("Metadata key `{key_str}`\nValue: {block} (latest block number)");
    Ok(Entry::new(summary, detail))
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
