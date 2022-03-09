//! Key categories:
//!
//! A full key value pair looks like:
//!
//! | user key | timestamp (8B) | value |
//!
//! |<------- full key ------->|

use bytes::Buf;

use super::utils::*;

/// Get user key in full key.
pub fn user_key(full_key: &[u8]) -> &[u8] {
    &full_key[..full_key.len() - 8]
}

/// Get timestamp in full key.
pub fn timestamp(full_key: &[u8]) -> u64 {
    !(&full_key[full_key.len() - 8..]).get_u64_le()
}

/// Calculate the difference between two keys.
pub fn key_diff<'a, 'b>(base: &'a [u8], target: &'b [u8]) -> &'b [u8] {
    bytes_diff(base, target)
}
