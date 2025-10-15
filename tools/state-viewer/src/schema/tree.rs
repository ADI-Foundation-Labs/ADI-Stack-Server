use anyhow::{Result, anyhow};

use super::utils::{
    decode_b256, decode_u64_pair, ensure_len, format_b256, format_hex, short_hex, truncate_hex,
};
use super::{EntryField, EntryRecord, FieldCapabilities, FieldRole, Schema};

pub struct TreeSchema;

const DB_NAME: &str = "tree";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnFamily {
    Default,
    KeyIndices,
}

impl ColumnFamily {
    const COUNT: usize = 2;

    const fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::KeyIndices => "key_indices",
        }
    }

    fn parse(name: &str) -> Result<Self> {
        match name {
            name if name == Self::Default.as_str() => Ok(Self::Default),
            name if name == Self::KeyIndices.as_str() => Ok(Self::KeyIndices),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

const COLUMN_FAMILY_NAMES: [&str; ColumnFamily::COUNT] = [
    ColumnFamily::Default.as_str(),
    ColumnFamily::KeyIndices.as_str(),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Field {
    KeyHex,
    ValueLen,
    Hash,
    Index,
    Version,
}

impl Field {
    const fn as_str(self) -> &'static str {
        match self {
            Self::KeyHex => "key_hex",
            Self::ValueLen => "value_len",
            Self::Hash => "hash",
            Self::Index => "index",
            Self::Version => "version",
        }
    }
}

impl Schema for TreeSchema {
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
            ColumnFamily::Default => format_default_cf(cf, key, value),
            ColumnFamily::KeyIndices => format_key_indices(cf, key, value),
        }
    }
}

fn format_default_cf(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::KeyHex.as_str(),
                format_hex(key),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::unsigned(
                Field::ValueLen.as_str(),
                value.len() as u128,
                FieldRole::Derived,
                FieldCapabilities::default().sortable(),
            ),
        ]),
    )
}

fn format_key_indices(cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord> {
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
    Ok(
        EntryRecord::new(cf, key, value, summary, detail).with_fields([
            EntryField::text(
                Field::Hash.as_str(),
                format_b256(hash, 0),
                FieldRole::Key,
                FieldCapabilities::default().searchable().key_part(),
            ),
            EntryField::unsigned(
                Field::Index.as_str(),
                index as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable().searchable(),
            ),
            EntryField::unsigned(
                Field::Version.as_str(),
                version as u128,
                FieldRole::Value,
                FieldCapabilities::default().sortable(),
            ),
        ]),
    )
}
