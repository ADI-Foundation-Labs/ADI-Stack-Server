use std::fmt::Write as _;

use alloy::consensus::Block;
use alloy::eips::Decodable2718 as _;
use alloy::primitives::Address;
use alloy::rlp::Decodable;
use anyhow::{anyhow, Context, Result};
use zksync_os_storage_api::{RepositoryBlock, TxMeta};
use zksync_os_types::{ZkEnvelope, ZkReceiptEnvelope};

use super::utils::{
    decode_b256, decode_u64, ensure_len, format_address, format_b256, format_optional_address,
};
use super::{Entry, Schema};

pub struct RepositorySchema;

impl Schema for RepositorySchema {
    fn name(&self) -> &'static str {
        "repository"
    }

    fn db_path(&self, base: &std::path::Path) -> std::path::PathBuf {
        base.join("repository")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &[
            "block_data",
            "block_number_to_hash",
            "tx",
            "tx_receipt",
            "tx_meta",
            "initiator_and_nonce_to_hash",
            "meta",
        ]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "block_data" => format_block_data(key, value),
            "block_number_to_hash" => format_block_number_to_hash(key, value),
            "tx" => format_tx(key, value),
            "tx_receipt" => format_receipt(key, value),
            "tx_meta" => format_tx_meta(key, value),
            "initiator_and_nonce_to_hash" => format_initiator_nonce(key, value),
            "meta" => format_meta(key, value),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

fn format_block_data(key: &[u8], value: &[u8]) -> Result<Entry> {
    let hash = decode_b256(key, "block hash")?;
    let mut slice = value;
    let block = Block::decode(&mut slice).context("decoding block rlp")?;
    let sealed = RepositoryBlock::new_unchecked(block, hash);
    let header = &sealed.header;
    let tx_count = sealed.body.transactions.len();
    let summary = format!(
        "{} → block #{}, {} txs",
        format_b256(hash, 8),
        header.number,
        tx_count
    );
    let detail = format!(
        "Block hash: {}\nNumber: {}\nTimestamp: {}\nGas used: {}\nTx count: {}\nRaw size: {} bytes\n",
        format_b256(hash, 0),
        header.number,
        header.timestamp,
        header.gas_used,
        tx_count,
        value.len()
    );
    Ok(Entry::new(summary, detail))
}

fn format_block_number_to_hash(key: &[u8], value: &[u8]) -> Result<Entry> {
    let number = decode_u64(key)?;
    let hash = decode_b256(value, "block hash")?;
    let summary = format!("block #{number} → {}", format_b256(hash, 12));
    let detail = format!("Block #{number}\nHash: {}", format_b256(hash, 0));
    Ok(Entry::new(summary, detail))
}

fn format_tx(key: &[u8], value: &[u8]) -> Result<Entry> {
    let hash = decode_b256(key, "tx hash")?;
    let mut slice = value;
    let envelope = ZkEnvelope::decode_2718(&mut slice)?;
    let recovered = envelope.clone().try_into_recovered().ok();
    let summary = if let Some(ref tx) = recovered {
        format!(
            "{} → nonce {} signer {}",
            format_b256(hash, 12),
            tx.nonce(),
            format_address(tx.signer())
        )
    } else {
        format!("{} → type {:?}", format_b256(hash, 12), envelope.tx_type())
    };

    let mut detail = format!(
        "Transaction hash: {}\nType: {:?}\nEncoded length: {} bytes\n",
        format_b256(hash, 0),
        envelope.tx_type(),
        value.len()
    );

    if let Some(tx) = recovered {
        let (inner, signer) = tx.clone().into_parts();
        let _ = writeln!(detail, "Signer: {}", format_address(signer));
        let _ = writeln!(detail, "Nonce: {}", tx.nonce());
        let _ = writeln!(detail, "Gas limit: {}", tx.gas_limit());
        let _ = writeln!(detail, "To: {}", format_optional_address(tx.to()));
        if let ZkEnvelope::L2(inner_l2) = inner {
            let _ = writeln!(detail, "L2 tx type: {:?}", inner_l2.tx_type());
        } else {
            let _ = writeln!(detail, "Envelope: {:?}", inner.tx_type());
        }
    }

    Ok(Entry::new(summary, detail))
}

fn format_receipt(key: &[u8], value: &[u8]) -> Result<Entry> {
    let hash = decode_b256(key, "tx hash")?;
    let mut slice = value;
    let receipt = ZkReceiptEnvelope::decode_2718(&mut slice)?;
    let summary = format!(
        "{} → status {} logs {}",
        format_b256(hash, 12),
        receipt.status(),
        receipt.logs().len()
    );
    let detail = format!(
        "Transaction hash: {}\nType: {}\nStatus: {}\nGas used: {}\nLogs: {}\nRaw length: {} bytes\n",
        format_b256(hash, 0),
        receipt.tx_type(),
        receipt.status(),
        receipt.cumulative_gas_used(),
        receipt.logs().len(),
        value.len()
    );
    Ok(Entry::new(summary, detail))
}

fn format_tx_meta(key: &[u8], value: &[u8]) -> Result<Entry> {
    let hash = decode_b256(key, "tx hash")?;
    let mut slice = value;
    let meta = TxMeta::decode(&mut slice)?;
    let summary = format!(
        "{} → block {} (index {})",
        format_b256(hash, 12),
        meta.block_number,
        meta.tx_index_in_block
    );
    let detail = format!(
        "Transaction hash: {}\nBlock hash: {}\nBlock number: {}\nTimestamp: {}\nGas used: {}\nEffective gas price: {}\nIndex in block: {}\nLogs before this tx: {}\nContract address: {}\n",
        format_b256(hash, 0),
        format_b256(meta.block_hash, 0),
        meta.block_number,
        meta.block_timestamp,
        meta.gas_used,
        meta.effective_gas_price,
        meta.tx_index_in_block,
        meta.number_of_logs_before_this_tx,
        meta.contract_address.map_or_else(|| "none".into(), format_address)
    );
    Ok(Entry::new(summary, detail))
}

fn format_initiator_nonce(key: &[u8], value: &[u8]) -> Result<Entry> {
    ensure_len(key, 28, "initiator+nonce key")?;
    let (addr_bytes, nonce_bytes) = key.split_at(20);
    let address = Address::from_slice(addr_bytes);
    let nonce = decode_u64(nonce_bytes)?;
    let hash = decode_b256(value, "tx hash")?;
    let summary = format!(
        "{} nonce {} → {}",
        format_address(address),
        nonce,
        format_b256(hash, 12)
    );
    let detail = format!(
        "Initiator: {}\nNonce: {}\nTransaction hash: {}\n",
        format_address(address),
        nonce,
        format_b256(hash, 0)
    );
    Ok(Entry::new(summary, detail))
}

fn format_meta(key: &[u8], value: &[u8]) -> Result<Entry> {
    let key_str = String::from_utf8_lossy(key);
    let number = decode_u64(value)?;
    let summary = format!("{key_str} → {number}");
    let detail = format!("Metadata key `{key_str}`\nLatest block number: {number}");
    Ok(Entry::new(summary, detail))
}
