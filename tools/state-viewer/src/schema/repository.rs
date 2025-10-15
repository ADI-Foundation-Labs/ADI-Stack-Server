use std::{convert::TryFrom, fmt::Write as _};

use alloy::consensus::Block;
use alloy::eips::Decodable2718 as _;
use alloy::primitives::Address;
use alloy::rlp::{Decodable, Encodable};
use anyhow::{anyhow, Context, Result};
use zksync_os_storage_api::{RepositoryBlock, TxMeta};
use zksync_os_types::{ZkEnvelope, ZkReceiptEnvelope};

use super::utils::{
    decode_b256, decode_u64, ensure_len, format_address, format_b256, format_optional_address,
};
use super::{EntryField, EntryRecord, FieldCapabilities, FieldRole, FieldValue, Schema};

pub struct RepositorySchema;

const DB_NAME: &str = "repository";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnFamily {
    BlockData,
    BlockNumberToHash,
    Tx,
    TxReceipt,
    TxMeta,
    InitiatorAndNonceToHash,
    Meta,
}

impl ColumnFamily {
    const COUNT: usize = 7;

    const fn as_str(self) -> &'static str {
        match self {
            Self::BlockData => "block_data",
            Self::BlockNumberToHash => "block_number_to_hash",
            Self::Tx => "tx",
            Self::TxReceipt => "tx_receipt",
            Self::TxMeta => "tx_meta",
            Self::InitiatorAndNonceToHash => "initiator_and_nonce_to_hash",
            Self::Meta => "meta",
        }
    }

    fn parse(name: &str) -> Result<Self> {
        match name {
            name if name == Self::BlockData.as_str() => Ok(Self::BlockData),
            name if name == Self::BlockNumberToHash.as_str() => Ok(Self::BlockNumberToHash),
            name if name == Self::Tx.as_str() => Ok(Self::Tx),
            name if name == Self::TxReceipt.as_str() => Ok(Self::TxReceipt),
            name if name == Self::TxMeta.as_str() => Ok(Self::TxMeta),
            name if name == Self::InitiatorAndNonceToHash.as_str() => {
                Ok(Self::InitiatorAndNonceToHash)
            }
            name if name == Self::Meta.as_str() => Ok(Self::Meta),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

const COLUMN_FAMILY_NAMES: [&str; ColumnFamily::COUNT] = [
    ColumnFamily::BlockData.as_str(),
    ColumnFamily::BlockNumberToHash.as_str(),
    ColumnFamily::Tx.as_str(),
    ColumnFamily::TxReceipt.as_str(),
    ColumnFamily::TxMeta.as_str(),
    ColumnFamily::InitiatorAndNonceToHash.as_str(),
    ColumnFamily::Meta.as_str(),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Field {
    Hash,
    Block,
    Timestamp,
    TxCount,
    TxType,
    Nonce,
    Signer,
    Status,
    Logs,
    TxIndex,
    GasUsed,
    EffectiveGasPrice,
    LogsBefore,
    ContractAddress,
    MetaKey,
    Initiator,
}

impl Field {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Block => "block",
            Self::Timestamp => "timestamp",
            Self::TxCount => "tx_count",
            Self::TxType => "tx_type",
            Self::Nonce => "nonce",
            Self::Signer => "signer",
            Self::Status => "status",
            Self::Logs => "logs",
            Self::TxIndex => "tx_index",
            Self::GasUsed => "gas_used",
            Self::EffectiveGasPrice => "effective_gas_price",
            Self::LogsBefore => "logs_before",
            Self::ContractAddress => "contract_address",
            Self::MetaKey => "meta_key",
            Self::Initiator => "initiator",
        }
    }

    fn matches(self, other: &str) -> bool {
        other.eq_ignore_ascii_case(self.as_str())
    }

    fn parse(name: &str) -> Option<Self> {
        if Self::Hash.matches(name) {
            Some(Self::Hash)
        } else if Self::Block.matches(name) {
            Some(Self::Block)
        } else if Self::Timestamp.matches(name) {
            Some(Self::Timestamp)
        } else if Self::TxCount.matches(name) {
            Some(Self::TxCount)
        } else if Self::TxType.matches(name) {
            Some(Self::TxType)
        } else if Self::Nonce.matches(name) {
            Some(Self::Nonce)
        } else if Self::Signer.matches(name) {
            Some(Self::Signer)
        } else if Self::Status.matches(name) {
            Some(Self::Status)
        } else if Self::Logs.matches(name) {
            Some(Self::Logs)
        } else if Self::TxIndex.matches(name) {
            Some(Self::TxIndex)
        } else if Self::GasUsed.matches(name) {
            Some(Self::GasUsed)
        } else if Self::EffectiveGasPrice.matches(name) {
            Some(Self::EffectiveGasPrice)
        } else if Self::LogsBefore.matches(name) {
            Some(Self::LogsBefore)
        } else if Self::ContractAddress.matches(name) {
            Some(Self::ContractAddress)
        } else if Self::MetaKey.matches(name) {
            Some(Self::MetaKey)
        } else if Self::Initiator.matches(name) {
            Some(Self::Initiator)
        } else {
            None
        }
    }
}

impl Schema for RepositorySchema {
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
            ColumnFamily::BlockData => format_block_data(cf, key, value),
            ColumnFamily::BlockNumberToHash => format_block_number_to_hash(cf, key, value),
            ColumnFamily::Tx => format_tx(cf, key, value),
            ColumnFamily::TxReceipt => format_receipt(cf, key, value),
            ColumnFamily::TxMeta => format_tx_meta(cf, key, value),
            ColumnFamily::InitiatorAndNonceToHash => format_initiator_nonce(cf, key, value),
            ColumnFamily::Meta => format_meta(cf, key, value),
        }
    }

    fn update_value(
        &self,
        cf: &str,
        entry: &EntryRecord,
        field_name: &str,
        new_value: &FieldValue,
    ) -> Result<Vec<u8>> {
        match ColumnFamily::parse(cf)? {
            ColumnFamily::Meta => {
                if Field::Block.matches(field_name) {
                    let number = match new_value {
                        FieldValue::Unsigned(value) => *value,
                        _ => return Err(anyhow!("Metadata value must be an unsigned integer")),
                    };
                    let value_u64 = u64::try_from(number)
                        .map_err(|_| anyhow!("Value {number} exceeds u64 range"))?;
                    Ok(value_u64.to_be_bytes().to_vec())
                } else {
                    Err(anyhow!(
                        "Editing not supported for field `{field_name}` in `{cf}`"
                    ))
                }
            }
            ColumnFamily::TxMeta => {
                let mut slice = entry.value();
                let mut meta = TxMeta::decode(&mut slice)?;

                let field = Field::parse(field_name).ok_or_else(|| {
                    anyhow!("Editing not supported for field `{field_name}` in `{cf}`")
                })?;

                match field {
                    Field::Block => {
                        meta.block_number = convert_to_u64(field.as_str(), new_value)?;
                    }
                    Field::Timestamp => {
                        meta.block_timestamp = convert_to_u64(field.as_str(), new_value)?;
                    }
                    Field::TxIndex => {
                        meta.tx_index_in_block = convert_to_u64(field.as_str(), new_value)?;
                    }
                    Field::GasUsed => {
                        meta.gas_used = convert_to_u64(field.as_str(), new_value)?;
                    }
                    Field::LogsBefore => {
                        meta.number_of_logs_before_this_tx =
                            convert_to_u64(field.as_str(), new_value)?;
                    }
                    Field::EffectiveGasPrice => {
                        meta.effective_gas_price = convert_to_u128(field.as_str(), new_value)?;
                    }
                    Field::ContractAddress => {
                        meta.contract_address = convert_to_address(new_value)?;
                    }
                    _ => {
                        return Err(anyhow!(
                            "Editing not supported for field `{field_name}` in `{cf}`"
                        ));
                    }
                }

                let mut encoded = Vec::new();
                meta.encode(&mut encoded);
                Ok(encoded)
            }
            _ => Err(anyhow!("Editing not supported for column family `{cf}`")),
        }
    }
}

fn convert_to_u64(field: &str, value: &FieldValue) -> Result<u64> {
    let raw = match value {
        FieldValue::Unsigned(number) => *number,
        _ => return Err(anyhow!("`{field}` expects an unsigned integer value")),
    };
    u64::try_from(raw).map_err(|_| anyhow!("`{field}` value {raw} exceeds u64 range"))
}

fn convert_to_u128(field: &str, value: &FieldValue) -> Result<u128> {
    match value {
        FieldValue::Unsigned(number) => Ok(*number),
        _ => Err(anyhow!("`{field}` expects an unsigned integer value")),
    }
}

fn convert_to_address(value: &FieldValue) -> Result<Option<Address>> {
    match value {
        FieldValue::Text(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
                Ok(None)
            } else {
                let without_prefix = trimmed.strip_prefix("0x").unwrap_or(trimmed);
                let bytes = hex::decode(without_prefix)
                    .map_err(|err| anyhow!("Invalid address `{trimmed}`: {err}"))?;
                if bytes.len() != 20 {
                    return Err(anyhow!(
                        "Address `{trimmed}` must be exactly 20 bytes (40 hex characters)"
                    ));
                }
                Ok(Some(Address::from_slice(&bytes)))
            }
        }
        FieldValue::Bytes(bytes) => {
            if bytes.is_empty() {
                Ok(None)
            } else if bytes.len() == 20 {
                Ok(Some(Address::from_slice(bytes)))
            } else {
                Err(anyhow!(
                    "Contract address byte value must be 20 bytes, got {}",
                    bytes.len()
                ))
            }
        }
        _ => Err(anyhow!(
            "Contract address must be provided as a hex string or raw bytes"
        )),
    }
}

fn format_block_data(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::Hash.as_str(),
                format_b256(hash, 0),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::unsigned(
                Field::Block.as_str(),
                header.number as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().searchable(),
            ),
            EntryField::unsigned(
                Field::Timestamp.as_str(),
                header.timestamp as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().searchable(),
            ),
            EntryField::unsigned(
                Field::TxCount.as_str(),
                tx_count as u128,
                FieldRole::Derived,
                FieldCapabilities::default().sortable(),
            ),
        ]),
    )
}

fn format_block_number_to_hash(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let number = decode_u64(key)?;
    let hash = decode_b256(value, "block hash")?;
    let summary = format!("block #{number} → {}", format_b256(hash, 12));
    let detail = format!("Block #{number}\nHash: {}", format_b256(hash, 0));
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::unsigned(
                Field::Block.as_str(),
                number as u128,
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

fn format_tx(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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

    if let Some(tx) = recovered.as_ref() {
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

    let mut entry = EntryRecord::new(cf, key, value, summary, detail).with_field(EntryField::text(
        Field::Hash.as_str(),
        format_b256(hash, 0),
        FieldRole::Key,
        FieldCapabilities::default().searchable().key_part(),
    ));
    entry.add_field(EntryField::text(
        Field::TxType.as_str(),
        format!("{:?}", envelope.tx_type()),
        FieldRole::Derived,
        FieldCapabilities::default().searchable(),
    ));
    if let Some(tx) = recovered.as_ref() {
        let (_, signer) = tx.clone().into_parts();
        entry.add_field(EntryField::unsigned(
            Field::Nonce.as_str(),
            tx.nonce() as u128,
            FieldRole::Value,
            FieldCapabilities::default().sortable().searchable(),
        ));
        entry.add_field(EntryField::text(
            Field::Signer.as_str(),
            format_address(signer),
            FieldRole::Value,
            FieldCapabilities::default().searchable(),
        ));
    }
    Ok(entry)
}

fn format_receipt(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::Hash.as_str(),
                format_b256(hash, 0),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::boolean(
                Field::Status.as_str(),
                receipt.status(),
                FieldRole::Value,
                FieldCapabilities::default().searchable(),
            ),
            EntryField::unsigned(
                Field::Logs.as_str(),
                receipt.logs().len() as u128,
                FieldRole::Derived,
                FieldCapabilities::default().sortable(),
            ),
        ]),
    )
}

fn format_tx_meta(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::Hash.as_str(),
                format_b256(hash, 0),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::unsigned(
                Field::Block.as_str(),
                meta.block_number as u128,
                FieldRole::Value,
                FieldCapabilities::default()
                    .sortable()
                    .searchable()
                    .editable(),
            ),
            EntryField::unsigned(
                Field::Timestamp.as_str(),
                meta.block_timestamp as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().editable(),
            ),
            EntryField::unsigned(
                Field::TxIndex.as_str(),
                meta.tx_index_in_block as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().editable(),
            ),
            EntryField::unsigned(
                Field::GasUsed.as_str(),
                meta.gas_used as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().editable(),
            ),
            EntryField::unsigned(
                Field::EffectiveGasPrice.as_str(),
                meta.effective_gas_price,
                FieldRole::Value,
                FieldCapabilities::default().sortable().editable(),
            ),
            EntryField::unsigned(
                Field::LogsBefore.as_str(),
                meta.number_of_logs_before_this_tx as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().editable(),
            ),
            EntryField::text(
                Field::ContractAddress.as_str(),
                meta.contract_address
                    .map_or_else(|| "none".into(), format_address),
                FieldRole::Value,
                FieldCapabilities::default().searchable().editable(),
            ),
        ]),
    )
}

fn format_initiator_nonce(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::Initiator.as_str(),
                format_address(address),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::unsigned(
                Field::Nonce.as_str(),
                nonce as u128,
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

fn format_meta(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
    let key_str = String::from_utf8_lossy(key);
    let number = decode_u64(value)?;
    let summary = format!("{key_str} → {number}");
    let detail = format!("Metadata key `{key_str}`\nLatest block number: {number}");
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
                number as u128,
                FieldRole::Value,
                FieldCapabilities::default()
                    .sortable()
                    .searchable()
                    .editable(),
            ),
        ]),
    )
}
