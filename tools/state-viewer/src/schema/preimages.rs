use anyhow::{anyhow, Result};

use super::utils::{ascii_preview, decode_b256, format_b256, truncate_hex};
use super::{Entry, Schema};

pub struct PreimagesSchema;

impl Schema for PreimagesSchema {
    fn name(&self) -> &'static str {
        "preimages_full_diffs"
    }

    fn db_path(&self, base: &std::path::Path) -> std::path::PathBuf {
        base.join("preimages_full_diffs")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &["storage"]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "storage" => {
                let hash = decode_b256(key, "preimage key")?;
                let summary = format!("{} → {} bytes", format_b256(hash, 12), value.len());
                let detail = format!(
                    "Hash: {}\nLength: {} bytes\nHex: {}\nASCII preview: {}\n",
                    format_b256(hash, 0),
                    value.len(),
                    truncate_hex(value, 256),
                    ascii_preview(value, 64)
                );
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}
