mod block_replay;
mod preimages;
mod repository;
mod state;
mod tree;
mod utils;

use std::path::{Path, PathBuf};

use anyhow::Result;
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
    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry>;
}

#[derive(Clone, Debug)]
pub struct Entry {
    summary: String,
    detail: String,
}

impl Entry {
    pub fn new(summary: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            detail: detail.into(),
        }
    }

    pub fn from_error(cf: &str, key: &[u8], err: anyhow::Error) -> Self {
        let key_repr = short_hex(key, 16).unwrap_or_else(|_| "<invalid>".into());
        Self::new(
            format!("[{cf}] key={key_repr} (decode error)"),
            format!("Failed to decode entry: {err:?}"),
        )
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
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
