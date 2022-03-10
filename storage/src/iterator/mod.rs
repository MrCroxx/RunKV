mod block_iterator;

use async_trait::async_trait;
pub use block_iterator::*;

use crate::Result;

pub const FORWARD: usize = 0;
pub const BACKWARD: usize = 1;

pub enum Seek<'s> {
    First,
    Last,
    Random(&'s [u8]),
}

/// [`Iterator`] defines shared behaviours for all iterators.
///
/// NOTE:
///
///     [`Iterator`] must be initialized with `seek` before use.
#[async_trait]
pub trait Iterator: Send + Sync {
    /// Move a valid iterator to the next key.
    ///
    /// Note:
    ///
    ///     - Before calling this function, make sure the iterator `is_valid`.
    ///     - After calling this function, you may first check whether the iterator `is_valid`
    ///       again, then get the new data by calling `key` and `value`.
    ///     - If the position after calling this is invalid, this function WON'T return an `Err`.
    ///       You should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    ///
    ///     This function will panic if the iterator is invalid.
    async fn next(&mut self) -> Result<()>;

    /// Move a valid iterator to the next key.
    ///
    /// Note:
    ///
    ///     - Before calling this function, make sure the iterator `is_valid`.
    ///     - After calling this function, you may first check whether the iterator `is_valid`
    ///       again, then get the new data by calling `key` and `value`.
    ///     - If the position after calling this is invalid, this function WON'T return an `Err`.
    ///       You should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    ///
    ///     This function will panic if the iterator is invalid.
    async fn prev(&mut self) -> Result<()>;

    /// Retrieve the current key.
    ///
    /// Note:
    ///
    ///     - Before calling this function, make sure the iterator `is_valid`.
    ///     - This function should be straightforward and return immediately.
    ///
    /// # Panics
    ///
    ///     This function will panic if the iterator is invalid.
    fn key(&self) -> &[u8];

    /// Retrieve the current value.
    ///
    /// Note:
    ///
    ///     - Before calling this function, make sure the iterator `is_valid`.
    ///     - This function should be straightforward and return immediately.
    ///
    /// # Panics
    ///
    ///     This function will panic if the iterator is invalid.
    fn value(&self) -> &[u8];

    /// Indicate whether the iterator can be used.
    ///
    /// Note:
    ///
    ///     - ONLY call `key`, `value`, and `next` if `is_valid` returns `true`.
    ///     - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;

    /// Reset iterator and seek to the first position where the key >= provided key, or key <=
    /// provided key if this is a reverse iterator.
    ///
    /// Note:
    ///
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    async fn seek<'s>(&mut self, position: Seek<'s>) -> Result<()>;
}
