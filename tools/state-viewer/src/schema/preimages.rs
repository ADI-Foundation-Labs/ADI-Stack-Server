use anyhow::{Result, anyhow};

use super::utils::{ascii_preview, decode_b256, format_b256, truncate_hex};
use super::{EntryField, EntryRecord, FieldCapabilities, FieldRole, Schema};

pub struct PreimagesSchema;

const DB_NAME: &str = "preimages_full_diffs";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnFamily {
    Storage,
}

impl ColumnFamily {
    const COUNT: usize = 1;

    const fn as_str(self) -> &'static str {
        match self {
            Self::Storage => "storage",
        }
    }

    fn parse(name: &str) -> Result<Self> {
        match name {
            name if name == Self::Storage.as_str() => Ok(Self::Storage),
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

const COLUMN_FAMILY_NAMES: [&str; ColumnFamily::COUNT] = [ColumnFamily::Storage.as_str()];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Field {
    Hash,
    Length,
}

impl Field {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Length => "length",
        }
    }
}

impl Schema for PreimagesSchema {
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
            ColumnFamily::Storage => {
                let hash = decode_b256(key, "preimage key")?;
                let summary = format!("{} → {} bytes", format_b256(hash, 12), value.len());
                let detail = format!(
                    "Hash: {}\nLength: {} bytes\nHex: {}\nASCII preview: {}\n",
                    format_b256(hash, 0),
                    value.len(),
                    truncate_hex(value, 256),
                    ascii_preview(value, 64)
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
                            Field::Length.as_str(),
                            value.len() as u128,
                            FieldRole::Value,
                            FieldCapabilities::default().sortable().searchable(),
                        ),
                    ]),
                )
            }
        }
    }
}
