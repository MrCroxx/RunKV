use std::path::PathBuf;

use runkv_common::sharded_hash_map::ShardedHashMap;

use super::error::{Error, Result};
use super::judge::Judge;

const MAGIC_FILENAME: &str = "RUNKV-FILE-CACHE";

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
struct IndexKey {
    sst: u64,
    idx: u32,
}

#[derive(Clone, Copy, Debug)]
struct Index {
    /// cache file id
    cache_file_id: u64,
    /// logical block offset after header and meta segment
    logical_block_offset: u32,
    /// length in bytes
    len: u32,
}

pub struct FileCacheOptions<J: Judge> {
    pub node: u64,
    pub path: String,
    pub capacity: usize,
    pub judge: J,
}

#[derive(Clone)]
pub struct FileCacheManager<J: Judge> {
    node: u64,
    path: String,
    capacity: usize,

    _judge: J,
    _indices: ShardedHashMap<IndexKey, Index>,
}

impl<J: Judge> std::fmt::Debug for FileCacheManager<J> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCacheManager")
            .field("node", &self.node)
            .finish()
    }
}

impl<J: Judge> FileCacheManager<J> {
    pub async fn open(options: FileCacheOptions<J>) -> Result<Self> {
        let exists = tokio::fs::metadata(&options.path).await.is_ok();
        let magic_file_path = PathBuf::from(&options.path).join(MAGIC_FILENAME);
        let indices = if exists {
            if tokio::fs::metadata(&magic_file_path).await.is_err() {
                return Err(Error::MagicFileNotFound);
            }
            ShardedHashMap::new(64)
        } else {
            tokio::fs::create_dir_all(&options.path).await?;
            tokio::fs::File::create(&magic_file_path).await?;
            let indices = ShardedHashMap::new(64);

            let mut r = tokio::fs::read_dir(&options.path).await?;
            while let Some(entry) = r.next_entry().await? {
                let raw_filename = entry.file_name();
                let _id: u64 = match raw_filename
                    .to_str()
                    .ok_or_else(|| Error::Other(format!("invalid file name: {:?}", raw_filename)))?
                    .parse()
                    .map_err(|_| Error::Other(format!("invalid file name: {:?}", raw_filename)))
                {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                // TODO: Restore indices.
            }

            indices
        };

        Ok(Self {
            node: options.node,
            path: options.path,
            capacity: options.capacity,
            _judge: options.judge,
            _indices: indices,
        })
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::*;
    use crate::file_cache::judge::DefaultJudge;

    fn is_send_sync_clone<T: Send + Sync + Clone>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCacheManager<DefaultJudge>>();
    }
}
