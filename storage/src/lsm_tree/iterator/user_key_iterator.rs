use async_trait::async_trait;
use bytes::Bytes;

use super::{BoxedIterator, Iterator, Seek};
use crate::{timestamp, user_key, Result};

pub struct UserKeyIterator {
    /// Inner full key iterator.
    ///
    /// Note: `iter` is always valid when [`UserKeyIterator`] is valid.
    iter: BoxedIterator,
    // TODO: Should replaced with a `Snapshot` handler with epoch inside to pin the sst?
    /// Timestamp for snapshot read.
    timestamp: u64,
    /// Current user key.
    key: Bytes,
}

impl UserKeyIterator {
    pub fn new(iter: BoxedIterator, timestamp: u64) -> Self {
        Self {
            iter,
            timestamp,
            key: Bytes::default(),
        }
    }

    /// Note: Ensure that the current state is valid.
    async fn next_inner(&mut self) -> Result<()> {
        loop {
            if !self.iter.is_valid() {
                return Ok(());
            }
            let user_key = user_key(self.iter.key());
            let timestamp = timestamp(self.iter.key());
            if self.timestamp >= timestamp && user_key != self.key {
                self.key = Bytes::from(user_key.to_vec());
                return Ok(());
            }
            // Call inner iter `next` later. It's useful to impl `Seel::First`.
            self.iter.next().await?;
        }
    }

    /// Note: Ensure that the current state is valid.
    async fn prev_inner(&mut self) -> Result<()> {
        loop {
            if !self.iter.is_valid() {
                return Ok(());
            }
            let user_key = user_key(self.iter.key());
            let timestamp = timestamp(self.iter.key());
            if self.timestamp >= timestamp && user_key != self.key {
                self.key = Bytes::from(user_key.to_vec());
                return self.prev_until_key().await;
            }
            // Call inner iter `prev` later. It's useful to impl `Seel::Last`.
            self.iter.prev().await?;
        }
    }

    /// Move backward until reach the first visiable key of the current user key.
    ///
    /// Note: Ensure that the current state is valid. And the current user key must have at least
    /// one visiable version.
    async fn prev_until_key(&mut self) -> Result<()> {
        loop {
            self.iter.prev().await?;
            if !self.iter.is_valid() {
                return self.iter.seek(Seek::First).await;
            }
            let user_key = user_key(self.iter.key());
            let timestamp = timestamp(self.iter.key());
            if self.key != user_key || self.timestamp < timestamp {
                return self.iter.next().await;
            }
        }
    }
}

#[async_trait]
impl Iterator for UserKeyIterator {
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
        &self.key
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<()> {
        match seek {
            Seek::First => {
                self.key = Bytes::default();
                self.iter.seek(Seek::First).await?;
                self.next_inner().await
            }
            Seek::Last => {
                self.key = Bytes::default();
                self.iter.seek(Seek::Last).await?;
                self.prev_inner().await
            }
            Seek::RandomForward(key) => {
                self.key = Bytes::default();
                self.iter.seek(Seek::RandomForward(key)).await?;
                self.next_inner().await
            }
            Seek::RandomBackward(key) => {
                self.key = Bytes::default();
                self.iter.seek(Seek::RandomBackward(key)).await?;
                self.prev_inner().await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{full_key, Block, BlockBuilder, BlockBuilderOptions, BlockIterator};

    fn build_iterator_for_test(timestamp: u64) -> UserKeyIterator {
        let block = build_block_for_test();
        let bi = BlockIterator::new(block);
        UserKeyIterator::new(Box::new(bi), timestamp)
    }

    fn build_block_for_test() -> Arc<Block> {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        for (k, ts) in [
            (3, vec![3, 2]),
            (5, vec![5, 4, 3, 2, 1]),
            (7, vec![3, 2]),
            (9, vec![5, 4, 3, 2, 1]),
            (11, vec![3, 2]),
        ]
        .into_iter()
        {
            for t in ts {
                builder.add(
                    &full_key(format!("k{:02}", k).as_bytes(), t),
                    &Bytes::from(format!("v{:02}-{:02}", k, t)),
                );
            }
        }
        let buf = builder.build();
        Arc::new(Block::decode(buf).unwrap())
    }

    #[tokio::test]
    async fn test_seek_first() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v03-03", it.value());

        let mut it = build_iterator_for_test(2);
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v03-02", it.value());

        let mut it = build_iterator_for_test(1);
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v05-01", it.value());

        let mut it = build_iterator_for_test(0);
        it.seek(Seek::First).await.unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_seek_last() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v11-03", it.value());

        let mut it = build_iterator_for_test(2);
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v11-02", it.value());

        let mut it = build_iterator_for_test(1);
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v09-01", it.value());

        let mut it = build_iterator_for_test(0);
        it.seek(Seek::Last).await.unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_bi_direction_seek() {
        // Forward.

        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomForward(b"k06")).await.unwrap();
        assert_eq!(b"v07-03", it.value());

        let mut it = build_iterator_for_test(2);
        it.seek(Seek::RandomForward(b"k06")).await.unwrap();
        assert_eq!(b"v07-02", it.value());

        let mut it = build_iterator_for_test(1);
        it.seek(Seek::RandomForward(b"k06")).await.unwrap();
        assert_eq!(b"v09-01", it.value());

        let mut it = build_iterator_for_test(0);
        it.seek(Seek::RandomForward(b"k06")).await.unwrap();
        assert!(!it.is_valid());

        // Backward.

        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomBackward(b"k08")).await.unwrap();
        assert_eq!(b"v07-03", it.value());

        let mut it = build_iterator_for_test(2);
        it.seek(Seek::RandomBackward(b"k08")).await.unwrap();
        assert_eq!(b"v07-02", it.value());

        let mut it = build_iterator_for_test(1);
        it.seek(Seek::RandomBackward(b"k08")).await.unwrap();
        assert_eq!(b"v05-01", it.value());

        let mut it = build_iterator_for_test(0);
        it.seek(Seek::RandomBackward(b"k08")).await.unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_seek_none_front() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomForward(b"k01")).await.unwrap();
        assert_eq!(b"v03-03", it.value());

        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomBackward(b"k01")).await.unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_seek_none_back() {
        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomForward(b"k12")).await.unwrap();
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test(u64::MAX);
        it.seek(Seek::RandomBackward(b"k12")).await.unwrap();
        assert_eq!(b"v11-03", it.value());
    }

    #[tokio::test]
    async fn test_forward_iterate() {
        let mut it = build_iterator_for_test(2);
        it.seek(Seek::First).await.unwrap();
        assert_eq!(b"v03-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v05-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v07-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v09-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v11-02", it.value());

        it.next().await.unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_backward_iterate() {
        let mut it = build_iterator_for_test(2);
        it.seek(Seek::Last).await.unwrap();
        assert_eq!(b"v11-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v09-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v07-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v05-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v03-02", it.value());

        it.prev().await.unwrap();
        assert!(!it.is_valid());
    }

    #[tokio::test]
    async fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test(2);

        it.seek(Seek::RandomForward(b"k06")).await.unwrap();
        assert_eq!(b"v07-02", it.value());

        it.prev().await.unwrap();
        assert_eq!(b"v05-02", it.value());

        it.next().await.unwrap();
        assert_eq!(b"v07-02", it.value());
    }
}
