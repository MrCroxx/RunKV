use std::cmp::Ordering;

use async_trait::async_trait;

use super::{Iterator, Seek};
use crate::{BlockIterator, CachePolicy, Result, Sstable, SstableStoreRef};

pub struct SstableIterator {
    /// Used to fetch block data.
    sstable_store: SstableStoreRef,
    /// Sstable to iterate on.
    sstable: Sstable,
    /// Current block index.
    offset: usize,
    /// Current block iterator.
    iter: Option<BlockIterator>,
    /// Cache policy.
    cache_policy: CachePolicy,
}

impl SstableIterator {
    pub fn new(
        sstable_store: SstableStoreRef,
        sstable: Sstable,
        cache_policy: CachePolicy,
    ) -> Self {
        Self {
            sstable_store,
            sstable,
            offset: usize::MAX,
            iter: None,
            cache_policy,
        }
    }

    /// Invalidate current state after reaching a invalid state.
    fn invalid(&mut self) {
        self.offset = self.sstable.meta.block_metas.len();
        self.iter = None;
    }

    /// Note: Ensure that the current state is valid.
    async fn next_inner(&mut self) -> Result<()> {
        let iter = self.iter.as_mut().unwrap();
        iter.next().await?;
        if !iter.is_valid() {
            if self.offset + 1 < self.sstable.meta.block_metas.len() {
                self.offset += 1;
                let block = self
                    .sstable_store
                    .block(&self.sstable, self.offset, self.cache_policy)
                    .await?;
                self.iter = Some(BlockIterator::new(block));
                self.iter.as_mut().unwrap().seek(Seek::First).await?;
            } else {
                self.invalid();
            }
        }
        Ok(())
    }

    /// Note: Ensure that the current state is valid.
    async fn prev_inner(&mut self) -> Result<()> {
        let iter = self.iter.as_mut().unwrap();
        iter.prev().await?;
        if !iter.is_valid() {
            if self.offset > 0 {
                self.offset -= 1;
                let block = self
                    .sstable_store
                    .block(&self.sstable, self.offset, self.cache_policy)
                    .await?;
                self.iter = Some(BlockIterator::new(block));
                self.iter.as_mut().unwrap().seek(Seek::Last).await?;
            } else {
                self.invalid();
            }
        }
        Ok(())
    }

    /// Move backward until the position that the given `key` can be inserted in DESC order or EOF.
    async fn prev_until_key(&mut self, key: &[u8]) -> Result<()> {
        while self.is_valid() && self.key().cmp(key) == Ordering::Greater {
            self.prev_inner().await?;
        }
        Ok(())
    }

    async fn binary_seek_inner(&mut self, key: &[u8]) -> Result<usize> {
        let mut size = self.sstable.meta.block_metas.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            use std::cmp::Ordering::*;
            let mid = left + size / 2;
            let block = self
                .sstable_store
                .block(&self.sstable, mid, self.cache_policy)
                .await?;
            let mut iter = BlockIterator::new(block);
            iter.seek(Seek::RandomForward(key)).await?;
            let cmp = if iter.is_valid() {
                iter.key().cmp(key)
            } else {
                Less
            };
            match cmp {
                Less => left = mid + 1,
                Equal => return Ok(mid),
                Greater => right = mid,
            }
            size = right - left;
        }
        Ok(left.saturating_sub(1))
    }

    async fn binary_seek(&mut self, key: &[u8]) -> Result<()> {
        let offset = self.binary_seek_inner(key).await?;
        if offset >= self.sstable.meta.block_metas.len() {
            self.invalid();
            return Ok(());
        }
        let block = self
            .sstable_store
            .block(&self.sstable, offset, self.cache_policy)
            .await?;
        let mut iter = BlockIterator::new(block);
        iter.seek(Seek::RandomForward(key)).await?;
        if iter.is_valid() {
            self.offset = offset;
            self.iter = Some(iter)
        } else {
            // Move to the first entry of the next inner iter.
            self.offset = offset + 1;
            if self.offset < self.sstable.meta.block_metas.len() {
                let block = self
                    .sstable_store
                    .block(&self.sstable, self.offset, self.cache_policy)
                    .await?;
                let mut iter = BlockIterator::new(block);
                iter.seek(Seek::RandomForward(key)).await?;
                self.iter = Some(iter)
            } else {
                // No more valid entry, set invalid state.
                self.invalid()
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Iterator for SstableIterator {
    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.next_inner().await
    }

    async fn prev(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.prev_inner().await
    }

    fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        self.iter.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.iter.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.offset < self.sstable.meta.block_metas.len()
    }

    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<()> {
        match seek {
            Seek::First => {
                self.offset = 0;
                let block = self
                    .sstable_store
                    .block(&self.sstable, self.offset, self.cache_policy)
                    .await?;
                self.iter = Some(BlockIterator::new(block));
                self.iter.as_mut().unwrap().seek(Seek::First).await
            }
            Seek::Last => {
                self.offset = self.sstable.meta.block_metas.len() - 1;
                let block = self
                    .sstable_store
                    .block(&self.sstable, self.offset, self.cache_policy)
                    .await?;
                self.iter = Some(BlockIterator::new(block));
                self.iter.as_mut().unwrap().seek(Seek::Last).await
            }
            Seek::RandomForward(key) => self.binary_seek(key).await,
            Seek::RandomBackward(key) => {
                self.binary_seek(key).await?;
                if !self.is_valid() {
                    self.seek(Seek::Last).await?;
                }
                self.prev_until_key(key).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::lsm_tree::utils::{full_key, CompressionAlgorighm};
    use crate::lsm_tree::TEST_DEFAULT_RESTART_INTERVAL;
    use crate::{
        BlockCache, MemObjectStore, SstableBuilder, SstableBuilderOptions, SstableMeta,
        SstableStore, SstableStoreOptions,
    };

    fn build_sstable_for_test() -> (SstableMeta, Bytes) {
        let options = SstableBuilderOptions {
            capacity: 1024,
            block_capacity: 32,
            restart_interval: TEST_DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorighm::Lz4,
        };
        let mut builder = SstableBuilder::new(options);
        builder.add(b"k01", 1, Some(b"v01")).unwrap();
        builder.add(b"k02", 2, Some(b"v02")).unwrap();
        builder.add(b"k04", 4, Some(b"v04")).unwrap();
        builder.add(b"k05", 5, Some(b"v05")).unwrap();
        builder.add(b"k07", 7, Some(b"v07")).unwrap();
        builder.add(b"k08", 8, Some(b"v08")).unwrap();
        let (meta, data) = builder.build().unwrap();
        assert_eq!(3, meta.block_metas.len());
        (meta, data)
    }

    async fn build_iterator_for_test() -> SstableIterator {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(65536);
        let options = SstableStoreOptions {
            path: "test".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 1024,
        };
        let sstable_store = Arc::new(SstableStore::new(options));
        let (meta, data) = build_sstable_for_test();
        let sstable = Sstable { id: 1, meta };
        sstable_store
            .put(&sstable, data, CachePolicy::Fill)
            .await
            .unwrap();
        SstableIterator::new(sstable_store, sstable, CachePolicy::Fill)
    }

    #[tokio::test]
    async fn test_seek_first() {
        let mut it = build_iterator_for_test().await;
        it.seek(Seek::First).await.unwrap();
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
    }

    #[tokio::test]
    async fn test_seek_last() {
        let mut it = build_iterator_for_test().await;
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(&full_key(b"k08", 8)[..], it.key());
    }

    #[tokio::test]
    async fn test_seek_none_front() {
        let mut it = build_iterator_for_test().await;
        it.seek(Seek::RandomForward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k01", 1)[..], it.key());

        let mut it = build_iterator_for_test().await;
        it.seek(Seek::RandomBackward(&full_key(b"k00", 0)[..]))
            .await
            .unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_seek_none_back() {
        let mut it = build_iterator_for_test().await;
        it.seek(Seek::RandomForward(&full_key(b"k09", 9)[..]))
            .await
            .unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test().await;
        it.seek(Seek::RandomBackward(&full_key(b"k09", 9)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k08", 8)[..], it.key());
    }

    #[tokio::test]
    async fn bi_direction_seek() {
        let mut it = build_iterator_for_test().await;
        it.seek(Seek::RandomForward(&full_key(b"k03", 3)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k04", 4)[..], it.key());

        it.seek(Seek::RandomBackward(&full_key(b"k03", 3)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
    }

    #[tokio::test]
    async fn test_forward_iterate() {
        let mut it = build_iterator_for_test().await;

        it.seek(Seek::First).await.unwrap();
        for i in (1..=2).chain(4..=5).chain(7..=8) {
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                it.key()
            );
            it.next().await.unwrap();
        }
        assert!(!it.is_valid())
    }

    #[tokio::test]
    async fn test_backward_iterate() {
        let mut it = build_iterator_for_test().await;

        it.seek(Seek::Last).await.unwrap();
        for i in (1..=2).chain(4..=5).chain(7..=8).rev() {
            assert_eq!(
                &full_key(format!("k{:02}", i).as_bytes(), i as u64)[..],
                it.key()
            );
            it.prev().await.unwrap();
        }
        assert!(!it.is_valid())
    }

    #[tokio::test]
    async fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test().await;

        it.seek(Seek::RandomForward(&full_key(b"k03", 3)[..]))
            .await
            .unwrap();
        assert_eq!(&full_key(b"k04", 4)[..], it.key());

        it.prev().await.unwrap();
        assert_eq!(&full_key(b"k02", 2)[..], it.key());

        it.next().await.unwrap();
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
    }
}
