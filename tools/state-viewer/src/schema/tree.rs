use anyhow::{anyhow, Result};

use super::utils::{
    decode_b256, decode_u64_pair, ensure_len, format_b256, format_hex, short_hex, truncate_hex,
};
use super::{Entry, Schema};

pub struct TreeSchema;

impl Schema for TreeSchema {
    fn name(&self) -> &'static str {
        "tree"
    }

    fn db_path(&self, base: &std::path::Path) -> std::path::PathBuf {
        base.join("tree")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &["default", "key_indices"]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "default" => format_default_cf(key, value),
            "key_indices" => format_key_indices(key, value),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

fn format_default_cf(key: &[u8], value: &[u8]) -> Result<Entry> {
    let summary = if key == [0] {
        "manifest".to_string()
    } else {
        format!("node key {} ({} bytes)", short_hex(key, 12)?, value.len())
    };
    let detail = format!(
        "Key hex: {}\nValue length: {} bytes\nValue hex (truncated): {}\n",
        format_hex(key),
        value.len(),
        truncate_hex(value, 256)
    );
    Ok(Entry::new(summary, detail))
}

fn format_key_indices(key: &[u8], value: &[u8]) -> Result<Entry> {
    ensure_len(value, 16, "key index value")?;
    let hash = decode_b256(key, "key hash")?;
    let (index, version) = decode_u64_pair(value)?;
    let summary = format!(
        "{} → index {} (version {})",
        format_b256(hash, 12),
        index,
        version
    );
    let detail = format!(
        "Key hash: {}\nLeaf index: {}\nFirst stored at version: {}\n",
        format_b256(hash, 0),
        index,
        version
    );
    Ok(Entry::new(summary, detail))
}
