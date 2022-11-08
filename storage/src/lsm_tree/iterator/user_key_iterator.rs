use async_recursion::async_recursion;
use async_trait::async_trait;

use super::{BoxedIterator, Iterator, Seek};
use crate::utils::{full_key, sequence, user_key, value};
use crate::Result;

pub struct UserKeyIterator {
    /// Inner full key iterator.
    ///
    /// Note: `iter` is always valid when [`UserKeyIterator`] is valid.
    iter: BoxedIterator,
    // TODO: Should replaced with a `Snapshot` handler with epoch inside to pin the sst?
    /// Sequence for snapshot read.
    sequence: u64,
    /// Current user key.
    key: Vec<u8>,
}

impl UserKeyIterator {
    pub fn new(iter: BoxedIterator, sequence: u64) -> Self {
        Self {
            iter,
            sequence,
            key: Vec::default(),
        }
    }

    /// Note: Ensure that the current state is valid.
    async fn next_inner(&mut self, key: &[u8]) -> Result<bool> {
        let mut found = false;
        loop {
            if !self.iter.is_valid() {
                return Ok(found);
            }

            let uk = user_key(self.iter.key());
            let ts = sequence(self.iter.key());
            if key == uk && self.sequence >= ts {
                found = true;
            }
            if self.sequence >= ts && value(self.iter.value()).is_none() {
                // Get tombstone, skip the former versions of this user key.
                self.key = uk.to_vec();
            }
            if self.sequence >= ts && uk != self.key {
                self.key = uk.to_vec();
                return Ok(found);
            }
            // Call inner iter `next` later. It's useful to impl `Seel::First`.
            self.iter.next().await?;
        }
    }

    /// Note: Ensure that the current state is valid.
    #[async_recursion]
    async fn prev_inner(&mut self, key: &[u8]) -> Result<bool> {
        // Find the first visiable user key that not equals current user key based on sequence.
        let mut found = false;
        loop {
            if !self.iter.is_valid() {
                return Ok(found);
            }
            let uk = user_key(self.iter.key());
            let ts = sequence(self.iter.key());
            if key == uk && self.sequence >= ts {
                found = true;
            }
            if self.sequence >= ts && uk != self.key {
                self.key = uk.to_vec();
                self.seek_latest_visiable_current_user_key().await?;
                match value(self.iter.value()) {
                    Some(_) => return Ok(found),
                    // Current user key has been deleted. Keep finding.
                    None => return self.prev_inner(key).await,
                }
            }
            // Call inner iter `prev` later. It's useful to impl `Seel::Last`.
            self.iter.prev().await?;
        }
    }

    /// Move backward until reach the first visiable entry of the current user key.
    ///
    /// Note: Ensure that the current state is valid. And the current user key must have at least
    /// one visiable version based on sequence.
    async fn seek_latest_visiable_current_user_key(&mut self) -> Result<()> {
        loop {
            self.iter.prev().await?;
            if !self.iter.is_valid() {
                self.iter.seek(Seek::First).await?;
                return Ok(());
            }
            let user_key = user_key(self.iter.key());
            let sequence = sequence(self.iter.key());
            if self.key != user_key || self.sequence < sequence {
                return self.iter.next().await;
            }
        }
    }
}

#[async_trait]
impl Iterator for UserKeyIterator {
    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.next_inner(&[]).await?;
        Ok(())
    }

    async fn prev(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.prev_inner(&[]).await?;
        Ok(())
    }

    fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        value(self.iter.value()).unwrap()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<bool> {
        let found = match seek {
            Seek::First => {
                self.key.clear();
                self.iter.seek(Seek::First).await?;
                self.next_inner(&[]).await?;
                self.is_valid()
            }
            Seek::Last => {
                self.key.clear();
                self.iter.seek(Seek::Last).await?;
                self.prev_inner(&[]).await?;
                self.is_valid()
            }
            Seek::RandomForward(key) => {
                self.key.clear();
                self.iter
                    .seek(Seek::RandomForward(&full_key(key, u64::MAX)))
                    .await?;
                self.next_inner(key).await?
            }
            Seek::RandomBackward(key) => {
                self.key.clear();
                self.iter
                    .seek(Seek::RandomBackward(&full_key(key, 0)))
                    .await?;
                self.prev_inner(key).await?
            }
        };
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_log::test;

    use super::*;
    use crate::components::{
        BlockCache, CachePolicy, LsmTreeMetrics, Sstable, SstableBuilder, SstableBuilderOptions,
        SstableMeta, SstableStore, SstableStoreOptions,
    };
    use crate::iterator::SstableIterator;
    use crate::tiered_cache::TieredCache;
    use crate::MemObjectStore;

    async fn build_iterator_for_test(sequence: u64) -> UserKeyIterator {
        let object_store = Arc::new(MemObjectStore::default());
        let block_cache = BlockCache::new(65536, Arc::new(LsmTreeMetrics::new(0)));
        let options = SstableStoreOptions {
            path: "path".to_string(),
            object_store,
            block_cache,
            meta_cache_capacity: 1024,
            tiered_cache: TieredCache::none(),
        };
        let sstable_store = Arc::new(SstableStore::new(options));

        let (meta, data) = build_sstable_for_test();
        let sstable = Sstable::new(1, Arc::new(meta));
        sstable_store
            .put(&sstable, data, CachePolicy::Fill)
            .await
            .unwrap();

        let si = SstableIterator::new(sstable_store, sstable, CachePolicy::Fill);
        UserKeyIterator::new(Box::new(si), sequence)
    }

    fn build_sstable_for_test() -> (SstableMeta, Vec<u8>) {
        let options = SstableBuilderOptions::default();
        let mut builder = SstableBuilder::new(options);
        // Negative numbers stands for delete on the absolute number sequence.
        for (k, ts) in [
            (2, vec![-3, 2]),
            (3, vec![4, 3, 2]),
            (4, vec![-3, 2]),
            (5, vec![5, 4, 3, 2, 1]),
            (6, vec![-3, 2]),
            (7, vec![4, 3, 2]),
            (8, vec![-3, 2]),
            (9, vec![5, 4, 3, 2, 1]),
            (10, vec![-3, 2]),
            (11, vec![4, 3, 2]),
            (12, vec![-3, 2]),
        ]
        .into_iter()
        {
            for t in ts {
                if t >= 0 {
                    builder
                        .add(
                            format!("k{:02}", k).as_bytes(),
                            t as u64,
                            Some(format!("v{:02}-{:02}", k, t).as_bytes()),
                        )
                        .unwrap();
                } else {
                    builder
                        .add(format!("k{:02}", k).as_bytes(), -t as u64, None)
                        .unwrap();
                }
            }
        }
        builder.build().unwrap()
    }

    #[test(tokio::test)]
    async fn test_seek_first() {
        let mut it = build_iterator_for_test(u64::MAX).await;
        assert!(it.seek(Seek::First).await.unwrap());
        assert_eq!(b"v03-04", it.value());

        let mut it = build_iterator_for_test(3).await;
        assert!(it.seek(Seek::First).await.unwrap());
        assert_eq!(b"v03-03", it.value());

        let mut it = build_iterator_for_test(2).await;
        assert!(it.seek(Seek::First).await.unwrap());
        assert_eq!(b"v02-02", it.value());

        let mut it = build_iterator_for_test(1).await;
        assert!(it.seek(Seek::First).await.unwrap());
        assert_eq!(b"v05-01", it.value());

        let mut it = build_iterator_for_test(0).await;
        assert!(!it.seek(Seek::First).await.unwrap());
        assert!(!it.is_valid());
    }

    #[test(tokio::test)]
    async fn test_seek_last() {
        let mut it = build_iterator_for_test(u64::MAX).await;
        assert!(it.seek(Seek::Last).await.unwrap());
        assert_eq!(b"v11-04", it.value());

        let mut it = build_iterator_for_test(3).await;
        assert!(it.seek(Seek::Last).await.unwrap());
        assert_eq!(b"v11-03", it.value());

        let mut it = build_iterator_for_test(2).await;
        assert!(it.seek(Seek::Last).await.unwrap());
        assert_eq!(b"v12-02", it.value());

        let mut it = build_iterator_for_test(1).await;
        assert!(it.seek(Seek::Last).await.unwrap());
        assert_eq!(b"v09-01", it.value());

        let mut it = build_iterator_for_test(0).await;
        assert!(!it.seek(Seek::Last).await.unwrap());
        assert!(!it.is_valid());
    }

    #[test(tokio::test)]
    async fn test_bi_direction_seek() {
        // Forward.

        let mut it = build_iterator_for_test(u64::MAX).await;
        assert!(it.seek(Seek::RandomForward(b"k06")).await.unwrap());
        assert_eq!(b"v07-04", it.value());

        let mut it = build_iterator_for_test(3).await;
        assert!(it.seek(Seek::RandomForward(b"k06")).await.unwrap());
        assert_eq!(b"v07-03", it.value());

        let mut it = build_iterator_for_test(2).await;
        assert!(it.seek(Seek::RandomForward(b"k06")).await.unwrap());
        assert_eq!(b"v06-02", it.value());

        let mut it = build_iterator_for_test(1).await;
        assert!(!it.seek(Seek::RandomForward(b"k06")).await.unwrap());
        assert_eq!(b"v09-01", it.value());

        let mut it = build_iterator_for_test(0).await;
        assert!(!it.seek(Seek::RandomForward(b"k06")).await.unwrap());

        // Backward.

        let mut it = build_iterator_for_test(u64::MAX).await;
        assert!(it.seek(Seek::RandomBackward(b"k08")).await.unwrap());
        assert_eq!(b"v07-04", it.value());

        let mut it = build_iterator_for_test(3).await;
        assert!(it.seek(Seek::RandomBackward(b"k08")).await.unwrap());
        assert_eq!(b"v07-03", it.value());

        let mut it = build_iterator_for_test(2).await;
        assert!(it.seek(Seek::RandomBackward(b"k08")).await.unwrap());
        assert_eq!(b"v08-02", it.value());

        let mut it = build_iterator_for_test(1).await;
        assert!(!it.seek(Seek::RandomBackward(b"k08")).await.unwrap());
        assert_eq!(b"v05-01", it.value());

        let mut it = build_iterator_for_test(0).await;
        assert!(!it.seek(Seek::RandomBackward(b"k08")).await.unwrap());
        assert!(!it.is_valid());

        // Exsited forward & backward

        let mut it = build_iterator_for_test(u64::MAX).await;
        assert!(it.seek(Seek::RandomForward(b"k07")).await.unwrap());
        assert_eq!(b"v07-04", it.value());

        let mut it = build_iterator_for_test(4).await;
        assert!(it.seek(Seek::RandomForward(b"k07")).await.unwrap());
        assert_eq!(b"v07-04", it.value());

        let mut it = build_iterator_for_test(3).await;
        assert!(it.seek(Seek::RandomForward(b"k07")).await.unwrap());
        assert_eq!(b"v07-03", it.value());

        let mut it = build_iterator_for_test(2).await;
        assert!(it.seek(Seek::RandomForward(b"k07")).await.unwrap());
        assert_eq!(b"v07-02", it.value());

        let mut it = build_iterator_for_test(1).await;
        assert!(!it.seek(Seek::RandomForward(b"k07")).await.unwrap());
        assert_eq!(b"v09-01", it.value());

        let mut it = build_iterator_for_test(u64::MAX).await;
        assert!(it.seek(Seek::RandomBackward(b"k07")).await.unwrap());
        assert_eq!(b"v07-04", it.value());

        let mut it = build_iterator_for_test(4).await;
        assert!(it.seek(Seek::RandomBackward(b"k07")).await.unwrap());
        assert_eq!(b"v07-04", it.value());

        let mut it = build_iterator_for_test(3).await;
        assert!(it.seek(Seek::RandomBackward(b"k07")).await.unwrap());
        assert_eq!(b"v07-03", it.value());

        let mut it = build_iterator_for_test(2).await;
        assert!(it.seek(Seek::RandomBackward(b"k07")).await.unwrap());
        assert_eq!(b"v07-02", it.value());

        let mut it = build_iterator_for_test(1).await;
        assert!(!it.seek(Seek::RandomBackward(b"k07")).await.unwrap());
        assert_eq!(b"v05-01", it.value());
    }

    #[test(tokio::test)]
    async fn test_seek_none_front() {
        let mut it = build_iterator_for_test(u64::MAX).await;
        it.seek(Seek::RandomForward(b"k01")).await.unwrap();
        assert_eq!(b"v03-04", it.value());

        let mut it = build_iterator_for_test(u64::MAX).await;
        it.seek(Seek::RandomBackward(b"k01")).await.unwrap();
        assert!(!it.is_valid());
    }

    #[test(tokio::test)]
    async fn test_seek_none_back() {
        let mut it = build_iterator_for_test(u64::MAX).await;
        it.seek(Seek::RandomForward(b"k12")).await.unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(u64::MAX).await;
        it.seek(Seek::RandomBackward(b"k12")).await.unwrap();
        assert_eq!(b"v11-04", it.value());
    }

    #[test(tokio::test)]
    async fn test_forward_iterate() {
        let mut it = build_iterator_for_test(3).await;
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v03-03", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v05-03", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v07-03", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v09-03", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v11-03", it.value());

        it.next().await.unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(2).await;
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v02-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v03-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v04-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v05-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v06-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v07-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v08-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v09-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v10-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v11-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v12-02", it.value());

        it.next().await.unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(1).await;
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v05-01", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v09-01", it.value());

        it.next().await.unwrap();
        assert!(!it.is_valid());
    }

    #[test(tokio::test)]
    async fn test_backward_iterate() {
        let mut it = build_iterator_for_test(3).await;
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v11-03", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v09-03", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v07-03", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v05-03", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v03-03", it.value());

        it.prev().await.unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(2).await;
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v12-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v11-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v10-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v09-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v08-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v07-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v06-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v05-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v04-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v03-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v02-02", it.value());

        it.prev().await.unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(1).await;
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v09-01", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v05-01", it.value());

        it.prev().await.unwrap();
        assert!(!it.is_valid());
    }

    #[test(tokio::test)]
    async fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test(3).await;

        it.seek(Seek::RandomForward(b"k06")).await.unwrap();
        assert_eq!(b"v07-03", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v05-03", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v07-03", it.value());
    }
}
