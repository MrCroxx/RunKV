mod block_iterator;
mod concat_iterator;
mod merge_iterator;
mod sstable_iterator;

use async_trait::async_trait;
pub use block_iterator::*;
pub use concat_iterator::*;
pub use merge_iterator::*;
pub use sstable_iterator::*;

use crate::Result;

pub enum Seek<'s> {
    /// Seek to the first valid position in order if exists.
    First,
    /// Seek to the last valid position in order if exists.
    Last,
    /// Seek to the position that the given key can be inserted into in order if exists.
    RandomForward(&'s [u8]),
    RandomBackward(&'s [u8]),
}

/// [`Iterator`] defines shared behaviours for all iterators.
///
/// NOTE:
///
/// [`Iterator`] must be initialized with `seek` before use.
#[async_trait]
pub trait Iterator: Send + Sync {
    /// Move a valid iterator to the next key.
    ///
    /// Note:
    ///
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - After calling this function, you may first check whether the iterator `is_valid` again,
    ///   then get the new data by calling `key` and `value`.
    /// - If the position after calling this is invalid, this function WON'T return an `Err`. You
    ///   should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    ///
    /// This function will panic if the iterator is invalid.
    async fn next(&mut self) -> Result<()>;

    /// Move a valid iterator to the next key.
    ///
    /// Note:
    ///
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - After calling this function, you may first check whether the iterator `is_valid` again,
    ///   then get the new data by calling `key` and `value`.
    /// - If the position after calling this is invalid, this function WON'T return an `Err`. You
    ///   should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    ///
    /// This function will panic if the iterator is invalid.
    async fn prev(&mut self) -> Result<()>;

    /// Retrieve the current key.
    ///
    /// Note:
    ///
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    ///
    /// This function will panic if the iterator is invalid.
    fn key(&self) -> &[u8];

    /// Retrieve the current value.
    ///
    /// Note:
    ///
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    ///
    /// This function will panic if the iterator is invalid.
    fn value(&self) -> &[u8];

    /// Indicate whether the iterator can be used.
    ///
    /// Note:
    ///
    /// - ONLY call `key`, `value`, and `next` if `is_valid` returns `true`.
    /// - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;

    /// Reset iterator and seek to the first position where the key >= provided key, or key <=
    /// provided key if this is a reverse iterator.
    ///
    /// Note:
    ///
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    async fn seek<'s>(&mut self, seek: Seek<'s>) -> Result<()>;
}

pub type BoxedIterator = Box<dyn Iterator>;

impl PartialEq for BoxedIterator {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for BoxedIterator {}

impl PartialOrd for BoxedIterator {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BoxedIterator {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(other.key())
    }
}
