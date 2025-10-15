use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use zksync_os_rocksdb::rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, Options, SingleThreaded,
};

use crate::schema::{schema_for_kind, DbKind, Schema};

pub type RocksDb = DBWithThreadMode<SingleThreaded>;

#[derive(Debug)]
pub struct DbStore {
    db: RocksDb,
}

impl DbStore {
    pub fn new(db: RocksDb) -> Self {
        Self { db }
    }

    pub fn scan(&self, cf_name: &str, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_internal(cf_name, Some(limit))
    }

    pub fn scan_all(&self, cf_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_internal(cf_name, None)
    }

    fn scan_internal(
        &self,
        cf_name: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = self.cf_handle(cf_name)?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        let mut entries = Vec::new();
        for (idx, item) in iter.enumerate() {
            if limit.is_some_and(|cap| idx >= cap) {
                break;
            }
            let (key, value) = item?;
            entries.push((key.to_vec(), value.to_vec()));
        }
        Ok(entries)
    }

    pub fn get(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self.cf_handle(cf_name)?;
        let result = self
            .db
            .get_cf(cf, key)
            .with_context(|| format!("fetching key from column family `{cf_name}`"))?;
        Ok(result.map(|value| value.to_vec()))
    }

    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.cf_handle(cf_name)?;
        self.db
            .put_cf(cf, key, value)
            .with_context(|| format!("updating column family `{cf_name}`"))?;
        Ok(())
    }

    pub fn delete(&self, cf_name: &str, key: &[u8]) -> Result<()> {
        let cf = self.cf_handle(cf_name)?;
        self.db
            .delete_cf(cf, key)
            .with_context(|| format!("removing key from column family `{cf_name}`"))?;
        Ok(())
    }

    pub fn inner(&self) -> &RocksDb {
        &self.db
    }

    pub fn into_inner(self) -> RocksDb {
        self.db
    }

    fn cf_handle(&self, cf_name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("missing column family `{cf_name}`"))
    }
}

/// Opens the RocksDB instance and materializes the schema metadata for the provided database kind.
pub fn open_components(
    base_path: &Path,
    kind: DbKind,
) -> Result<(Box<dyn Schema>, DbStore, Vec<String>, PathBuf)> {
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
    let store = DbStore::new(db);
    let cf_names = schema
        .column_families()
        .iter()
        .map(|name| name.to_string())
        .collect();

    Ok((schema, store, cf_names, db_path))
}
