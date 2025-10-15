mod block_replay;
mod preimages;
mod repository;
mod state;
mod tree;
mod utils;

use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use clap::ValueEnum;

pub use block_replay::BlockReplaySchema;
pub use preimages::PreimagesSchema;
pub use repository::RepositorySchema;
pub use state::StateSchema;
pub use tree::TreeSchema;

use utils::short_hex;

/// Known database kinds the viewer can inspect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum DbKind {
    #[value(alias = "block-replay")]
    BlockReplayWal,
    Preimages,
    Repository,
    State,
    Tree,
}

pub const DB_KINDS: [DbKind; 5] = [
    DbKind::BlockReplayWal,
    DbKind::Preimages,
    DbKind::Repository,
    DbKind::State,
    DbKind::Tree,
];

pub trait Schema: Send + Sync {
    fn name(&self) -> &'static str;
    fn db_path(&self, base: &Path) -> PathBuf;
    fn column_families(&self) -> &'static [&'static str];
    fn decode_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<EntryRecord>;

    fn update_value(
        &self,
        cf: &str,
        _entry: &EntryRecord,
        _field_name: &str,
        _new_value: &FieldValue,
    ) -> Result<Vec<u8>> {
        Err(anyhow!("Editing not supported for column family `{cf}`"))
    }
}

#[derive(Clone, Debug)]
pub struct EntryRecord {
    _cf: String,
    key: Vec<u8>,
    value: Vec<u8>,
    summary: String,
    detail: String,
    fields: Vec<EntryField>,
    is_error: bool,
}

impl EntryRecord {
    pub fn new(
        cf: impl Into<String>,
        key: &[u8],
        value: &[u8],
        summary: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            _cf: cf.into(),
            key: key.to_vec(),
            value: value.to_vec(),
            summary: summary.into(),
            detail: detail.into(),
            fields: Vec::new(),
            is_error: false,
        }
    }

    pub fn from_error(cf: impl Into<String>, key: &[u8], err: anyhow::Error) -> Self {
        let key_repr = short_hex(key, 16).unwrap_or_else(|_| "<invalid>".into());
        let cf_name: String = cf.into();
        Self::new(
            cf_name.clone(),
            key,
            &[],
            format!("[{cf_name}] key={key_repr} (decode error)"),
            format!("Failed to decode entry: {err:?}"),
        )
        .with_error()
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn fields(&self) -> &[EntryField] {
        &self.fields
    }

    pub fn field(&self, name: &str) -> Option<&EntryField> {
        self.fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(name))
    }

    pub fn field_value(&self, name: &str) -> Option<&FieldValue> {
        self.field(name).map(|field| &field.value)
    }

    pub fn add_field(&mut self, field: EntryField) {
        self.fields.push(field);
    }

    pub fn with_field(mut self, field: EntryField) -> Self {
        self.add_field(field);
        self
    }

    pub fn with_fields(mut self, fields: impl IntoIterator<Item = EntryField>) -> Self {
        self.fields.extend(fields);
        self
    }

    fn with_error(mut self) -> Self {
        self.is_error = true;
        self
    }
}

#[derive(Clone, Debug)]
pub struct EntryField {
    pub name: String,
    pub kind: FieldKind,
    pub value: FieldValue,
    pub _role: FieldRole,
    pub capabilities: FieldCapabilities,
}

impl EntryField {
    pub fn new(
        name: impl Into<String>,
        kind: FieldKind,
        value: FieldValue,
        role: FieldRole,
        capabilities: FieldCapabilities,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            value,
            _role: role,
            capabilities,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> FieldKind {
        self.kind
    }

    pub fn value(&self) -> &FieldValue {
        &self.value
    }

    pub fn capabilities(&self) -> &FieldCapabilities {
        &self.capabilities
    }

    pub fn unsigned(
        name: impl Into<String>,
        value: impl Into<u128>,
        role: FieldRole,
        capabilities: FieldCapabilities,
    ) -> Self {
        Self::new(
            name,
            FieldKind::Unsigned,
            FieldValue::Unsigned(value.into()),
            role,
            capabilities,
        )
    }

    pub fn text(
        name: impl Into<String>,
        value: impl Into<String>,
        role: FieldRole,
        capabilities: FieldCapabilities,
    ) -> Self {
        Self::new(
            name,
            FieldKind::Text,
            FieldValue::Text(value.into()),
            role,
            capabilities,
        )
    }

    pub fn boolean(
        name: impl Into<String>,
        value: bool,
        role: FieldRole,
        capabilities: FieldCapabilities,
    ) -> Self {
        Self::new(
            name,
            FieldKind::Boolean,
            FieldValue::Boolean(value),
            role,
            capabilities,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    Unsigned,
    #[allow(dead_code)]
    Signed,
    Boolean,
    Text,
    #[allow(dead_code)]
    Bytes,
}

impl FieldKind {
    pub fn parse_value(&self, input: &str) -> Result<FieldValue> {
        match self {
            FieldKind::Unsigned => {
                let trimmed = input.trim();
                let value = trimmed
                    .parse::<u128>()
                    .map_err(|err| anyhow!("invalid unsigned integer `{trimmed}`: {err}"))?;
                Ok(FieldValue::Unsigned(value))
            }
            FieldKind::Signed => {
                let trimmed = input.trim();
                let value = trimmed
                    .parse::<i128>()
                    .map_err(|err| anyhow!("invalid signed integer `{trimmed}`: {err}"))?;
                Ok(FieldValue::Signed(value))
            }
            FieldKind::Boolean => {
                let trimmed = input.trim().to_ascii_lowercase();
                match trimmed.as_str() {
                    "true" | "1" => Ok(FieldValue::Boolean(true)),
                    "false" | "0" => Ok(FieldValue::Boolean(false)),
                    _ => Err(anyhow!("invalid boolean `{input}` (expected true/false)")),
                }
            }
            FieldKind::Text => Ok(FieldValue::Text(input.to_string())),
            FieldKind::Bytes => {
                let trimmed = input.trim();
                let without_prefix = trimmed.strip_prefix("0x").unwrap_or(trimmed);
                let bytes = hex::decode(without_prefix)
                    .map_err(|err| anyhow!("invalid hex string `{trimmed}`: {err}"))?;
                Ok(FieldValue::Bytes(bytes))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Unsigned(u128),
    Signed(i128),
    Boolean(bool),
    Text(String),
    Bytes(Vec<u8>),
}

impl FieldValue {
    pub fn cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (FieldValue::Unsigned(a), FieldValue::Unsigned(b)) => Some(a.cmp(b)),
            (FieldValue::Signed(a), FieldValue::Signed(b)) => Some(a.cmp(b)),
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => Some(a.cmp(b)),
            (FieldValue::Text(a), FieldValue::Text(b)) => Some(a.cmp(b)),
            (FieldValue::Bytes(a), FieldValue::Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    pub fn eq_value(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldValue::Unsigned(a), FieldValue::Unsigned(b)) => a == b,
            (FieldValue::Signed(a), FieldValue::Signed(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Text(a), FieldValue::Text(b)) => a == b,
            (FieldValue::Bytes(a), FieldValue::Bytes(b)) => a == b,
            _ => false,
        }
    }

    pub fn display(&self) -> String {
        match self {
            FieldValue::Unsigned(value) => value.to_string(),
            FieldValue::Signed(value) => value.to_string(),
            FieldValue::Boolean(value) => value.to_string(),
            FieldValue::Text(value) => value.clone(),
            FieldValue::Bytes(value) => format!("0x{}", hex::encode(value)),
        }
    }
}

impl std::fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::Unsigned(value) => write!(f, "{value}"),
            FieldValue::Signed(value) => write!(f, "{value}"),
            FieldValue::Boolean(value) => write!(f, "{value}"),
            FieldValue::Text(value) => f.write_str(value),
            FieldValue::Bytes(value) => write!(f, "0x{}", hex::encode(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FieldCapabilities {
    pub sortable: bool,
    pub searchable: bool,
    pub editable: bool,
    pub key_part: bool,
}

impl FieldCapabilities {
    pub const fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }

    pub const fn searchable(mut self) -> Self {
        self.searchable = true;
        self
    }

    pub const fn editable(mut self) -> Self {
        self.editable = true;
        self
    }

    pub const fn key_part(mut self) -> Self {
        self.key_part = true;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FieldRole {
    Key,
    Value,
    Derived,
}

pub fn schema_for_kind(kind: DbKind) -> Box<dyn Schema> {
    match kind {
        DbKind::BlockReplayWal => Box::new(BlockReplaySchema),
        DbKind::Preimages => Box::new(PreimagesSchema),
        DbKind::Repository => Box::new(RepositorySchema),
        DbKind::State => Box::new(StateSchema),
        DbKind::Tree => Box::new(TreeSchema),
    }
}
