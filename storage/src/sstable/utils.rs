use std::{cmp, ptr};

use bytes::{Buf, BufMut};

use crate::error::{Error, Result};

unsafe fn u64(ptr: *const u8) -> u64 {
    ptr::read_unaligned(ptr as *const u64)
}

unsafe fn u32(ptr: *const u8) -> u32 {
    ptr::read_unaligned(ptr as *const u32)
}

#[inline]
pub fn bytes_diff<'a, 'b>(base: &'a [u8], target: &'b [u8]) -> &'b [u8] {
    let end = cmp::min(base.len(), target.len());
    let mut i = 0;
    unsafe {
        while i + 8 <= end {
            if u64(base.as_ptr().add(i)) != u64(target.as_ptr().add(i)) {
                break;
            }
            i += 8;
        }
        if i + 4 <= end && u32(base.as_ptr().add(i)) == u32(target.as_ptr().add(i)) {
            i += 4;
        }
        while i < end {
            if base.get_unchecked(i) != target.get_unchecked(i) {
                return target.get_unchecked(i..);
            }
            i += 1;
        }
        target.get_unchecked(end..)
    }
}

pub fn crc32sum(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub fn crc32check(data: &[u8], crc32sum: u32) -> bool {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() == crc32sum
}

pub enum CompressionAlgorighm {
    None,
    LZ4,
}

impl CompressionAlgorighm {
    pub fn encode(&self, buf: &mut impl BufMut) {
        let v = match self {
            Self::None => 0,
            Self::LZ4 => 1,
        };
        buf.put_u8(v);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(Self::None),
            1 => Ok(Self::LZ4),
            _ => Err(Error::DecodeError(
                "not valid compression algorithm".to_string(),
            )),
        }
    }
}
