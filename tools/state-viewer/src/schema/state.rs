use alloy::primitives::U256;
use std::convert::TryFrom;
use anyhow::{anyhow, Result};

use super::utils::{decode_b256, decode_u64, format_b256};
use super::{EntryField, EntryRecord, FieldCapabilities, FieldRole, FieldValue, Schema};

pub struct StateSchema;

impl Schema for StateSchema {
    fn name(&self) -> &'static str {
        "state_full_diffs"
    }

    fn db_path(&self, base: &std::path::Path) -> std::path::PathBuf {
        base.join("state_full_diffs")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &["data", "meta"]
    }

    fn decode_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
        match cf {
            "data" => {
                let key_b256 = decode_b256(&key[..32], "storage key")?;
                let block = decode_u64(&key[32..])?;
                let value_b256 = decode_b256(value, "storage value")?;
                let summary = format!(
                    "{} (block #{block}) → {}",
                    format_b256(key_b256, 12),
                    format_b256(value_b256, 12)
                );
                let detail = format!(
                    "Key: {}\nBlock: {}\nValue (B256): {}\nValue (U256): {}\n",
                    format_b256(key_b256, 0),
                    block,
                    format_b256(value_b256, 0),
                    U256::from_be_bytes(value_b256.0)
                );
                Ok(
                    EntryRecord::new(cf, key, value, summary, detail).with_fields([
                        EntryField::text(
                            "storage_key",
                            format_b256(key_b256, 0),
                            FieldRole::Key,
                            FieldCapabilities::default().searchable().key_part(),
                        ),
                        EntryField::unsigned(
                            "block",
                            block as u128,
                            FieldRole::Key,
                            FieldCapabilities::default()
                                .sortable()
                                .searchable()
                                .key_part(),
                        ),
                        EntryField::text(
                            "value",
                            format_b256(value_b256, 0),
                            FieldRole::Value,
                            FieldCapabilities::default().searchable(),
                        ),
                    ]),
                )
            }
            "meta" => {
                let key_str = String::from_utf8_lossy(key);
                let base_block = decode_u64(value)?;
                let summary = format!("{key_str} → {base_block}");
                let detail = format!("Metadata key `{key_str}`\nBase block number: {base_block}");
                Ok(
                    EntryRecord::new(cf, key, value, summary, detail).with_fields([
                        EntryField::text(
                            "meta_key",
                            key_str.to_string(),
                            FieldRole::Key,
                            FieldCapabilities::default().searchable().key_part(),
                        ),
                        EntryField::unsigned(
                            "base_block",
                            base_block as u128,
                            FieldRole::Value,
                            FieldCapabilities::default()
                                .sortable()
                                .searchable()
                                .editable(),
                        ),
                    ]),
                )
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }

    fn update_value(
        &self,
        cf: &str,
        _entry: &EntryRecord,
        field_name: &str,
        new_value: &FieldValue,
    ) -> Result<Vec<u8>> {
        if cf == "meta" && field_name.eq_ignore_ascii_case("base_block") {
            let number = match new_value {
                FieldValue::Unsigned(value) => *value,
                _ => return Err(anyhow!("Base block must be an unsigned integer")),
            };
            let base_block = u64::try_from(number)
                .map_err(|_| anyhow!("Base block {number} exceeds u64 range"))?;
            Ok(base_block.to_be_bytes().to_vec())
        } else {
            Err(anyhow!("Editing not supported for column family `{cf}`"))
        }
    }
}
