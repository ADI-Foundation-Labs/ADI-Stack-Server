use alloy::primitives::U256;
use anyhow::{anyhow, Result};

use super::utils::{decode_b256, decode_u64, format_b256};
use super::{Entry, Schema};

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

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
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
                Ok(Entry::new(summary, detail))
            }
            "meta" => {
                let key_str = String::from_utf8_lossy(key);
                let base_block = decode_u64(value)?;
                let summary = format!("{key_str} → {base_block}");
                let detail = format!("Metadata key `{key_str}`\nBase block number: {base_block}");
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}
