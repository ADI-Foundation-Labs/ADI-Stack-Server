use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use zksync_os_rocksdb::rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Options, SingleThreaded,
};

use crate::schema::{schema_for_kind, DbKind, Schema};

pub type RocksDb = DBWithThreadMode<SingleThreaded>;

/// Opens the RocksDB instance and materializes the schema metadata for the provided database kind.
pub fn open_components(
    base_path: &Path,
    kind: DbKind,
) -> Result<(Box<dyn Schema>, RocksDb, Vec<String>, PathBuf)> {
    let schema = schema_for_kind(kind);
    let db_path = schema.db_path(base_path);
    if !db_path.exists() {
        return Err(anyhow!(
            "Database directory `{}` does not exist",
            db_path.display()
        ));
    }

    let mut options = Options::default();
    options.create_if_missing(false);
    let cf_descriptors: Vec<_> = schema
        .column_families()
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect();

    let db = RocksDb::open_cf_descriptors(&options, &db_path, cf_descriptors)
        .with_context(|| format!("opening RocksDB at {}", db_path.display()))?;
    let cf_names = schema
        .column_families()
        .iter()
        .map(|name| name.to_string())
        .collect();

    Ok((schema, db, cf_names, db_path))
}
