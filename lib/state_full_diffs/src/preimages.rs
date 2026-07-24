use alloy::primitives::B256;
use std::path::Path;
use zksync_os_rocksdb::db::NamedColumnFamily;
use zksync_os_rocksdb::{RocksDB, RocksDBOptions};

/// Block cache for the preimages (bytecode) DB (also bounds SST index/filter memory).
const BLOCK_CACHE_CAPACITY_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub enum PreimagesCF {
    Storage,
}

impl NamedColumnFamily for PreimagesCF {
    const DB_NAME: &'static str = "preimages_full_diffs";
    const ALL: &'static [Self] = &[PreimagesCF::Storage];

    fn name(&self) -> &'static str {
        match self {
            PreimagesCF::Storage => "storage",
        }
    }
}

#[derive(Clone, Debug)]
pub struct FullDiffsPreimages {
    rocks: RocksDB<PreimagesCF>,
}

impl FullDiffsPreimages {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        let rocks = RocksDB::<PreimagesCF>::with_options(
            path,
            RocksDBOptions::read_optimized(BLOCK_CACHE_CAPACITY_BYTES),
        )?;
        Ok(Self { rocks })
    }

    pub fn get(&self, key: B256) -> Option<Vec<u8>> {
        self.rocks
            .get_cf(PreimagesCF::Storage, key.as_slice())
            .ok()
            .flatten()
    }

    pub fn add<'a, J>(&self, diffs: J) -> anyhow::Result<()>
    where
        J: IntoIterator<Item = (B256, &'a Vec<u8>)>,
    {
        let mut batch = self.rocks.new_write_batch();
        for (k, v) in diffs.into_iter() {
            batch.put_cf(PreimagesCF::Storage, k.as_slice(), v);
        }
        self.rocks.write(batch)?;
        Ok(())
    }
}
