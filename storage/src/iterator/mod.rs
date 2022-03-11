mod block_iterator;
mod concat_iterator;

use async_trait::async_trait;
pub use block_iterator::*;
pub use concat_iterator::*;

use crate::Result;

pub enum Seek<'s> {
    /// Seek to the first valid position in order if exists.
    First,
    /// Seek to the last valid position in order if exists.
    Last,
    /// Seek to the position that the given key can be inserted into in order if exists.
    Random(&'s [u8]),
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
    async fn seek<'s>(&mut self, position: Seek<'s>) -> Result<()>;
}

#[async_trait]
trait BinarySeekableIterator: Iterator {
    async fn binary_seek(&mut self, key: &[u8]) -> Result<()> {
        let res = self.binary_seek_inner(key).await?;
        println!("res: {:?}", res);
        let index = match res {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };
        self.set_offset(index);
        Ok(())
    }

    async fn binary_seek_inner(&mut self, key: &[u8]) -> Result<std::result::Result<usize, usize>> {
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            use std::cmp::Ordering::*;
            let mid = left + size / 2;
            println!("mid: {}", mid);
            let iter_mid = self.get_mut(mid);
            iter_mid.seek(Seek::Random(key)).await?;
            println!("cmp");
            let cmp = if iter_mid.is_valid() {
                println!("valid");
                iter_mid.key().cmp(key)
            } else {
                println!("invalid");
                Less
            };
            println!("cmp: {:?}", cmp);
            match cmp {
                Less => left = mid + 1,
                Equal => return Ok(Ok(mid)),
                Greater => right = mid,
            }

            size = right - left;
        }
        Ok(Err(left))
    }

    fn len(&self) -> usize;

    fn get_mut(&mut self, index: usize) -> &mut Box<dyn Iterator>;

    fn set_offset(&mut self, index: usize);
}
