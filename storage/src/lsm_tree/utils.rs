use std::{cmp, ptr};

use bytes::{Buf, BufMut};

use crate::error::{Error, Result};

const MASK: u32 = 128;

pub fn var_u32_len(n: u32) -> usize {
    if n < (1 << 7) {
        1
    } else if n < (1 << 14) {
        2
    } else if n < (1 << 21) {
        3
    } else if n < (1 << 28) {
        4
    } else {
        5
    }
}

pub trait BufMutExt: BufMut {
    fn put_var_u32(&mut self, n: u32) {
        if n < (1 << 7) {
            self.put_u8(n as u8);
        } else if n < (1 << 14) {
            self.put_u8((n | MASK) as u8);
            self.put_u8((n >> 7) as u8);
        } else if n < (1 << 21) {
            self.put_u8((n | MASK) as u8);
            self.put_u8(((n >> 7) | MASK) as u8);
            self.put_u8((n >> 14) as u8);
        } else if n < (1 << 28) {
            self.put_u8((n | MASK) as u8);
            self.put_u8(((n >> 7) | MASK) as u8);
            self.put_u8(((n >> 14) | MASK) as u8);
            self.put_u8((n >> 21) as u8);
        } else {
            self.put_u8((n | MASK) as u8);
            self.put_u8(((n >> 7) | MASK) as u8);
            self.put_u8(((n >> 14) | MASK) as u8);
            self.put_u8(((n >> 21) | MASK) as u8);
            self.put_u8((n >> 28) as u8);
        }
    }
}

pub trait BufExt: Buf {
    fn get_var_u32(&mut self) -> u32 {
        let mut n = 0u32;
        let mut shift = 0;
        loop {
            let v = self.get_u8() as u32;
            if v & MASK != 0 {
                n |= (v & (MASK - 1)) << shift;
            } else {
                n |= v << shift;
                break;
            }
            shift += 7;
        }
        n
    }
}

impl<T: BufMut + ?Sized> BufMutExt for &mut T {}

impl<T: Buf + ?Sized> BufExt for &mut T {}

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

#[derive(Clone, Debug)]
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_var_u32_enc_dec() {
        let mut buf = BytesMut::default();
        (&mut buf).put_var_u32((1 << 7) - 1);
        let mut buf = buf.freeze();
        assert_eq!(1, buf.len());
        assert_eq!((1 << 7) - 1, (&mut buf).get_var_u32());
        assert!(buf.is_empty());

        let mut buf = BytesMut::default();
        (&mut buf).put_var_u32((1 << 14) - 1);
        let mut buf = buf.freeze();
        assert_eq!(2, buf.len());
        assert_eq!((1 << 14) - 1, (&mut buf).get_var_u32());
        assert!(buf.is_empty());

        let mut buf = BytesMut::default();
        (&mut buf).put_var_u32((1 << 21) - 1);
        let mut buf = buf.freeze();
        assert_eq!(3, buf.len());
        assert_eq!((1 << 21) - 1, (&mut buf).get_var_u32());
        assert!(buf.is_empty());

        let mut buf = BytesMut::default();
        (&mut buf).put_var_u32((1 << 28) - 1);
        let mut buf = buf.freeze();
        assert_eq!(4, buf.len());
        assert_eq!((1 << 28) - 1, (&mut buf).get_var_u32());
        assert!(buf.is_empty());

        let mut buf = BytesMut::default();
        (&mut buf).put_var_u32(u32::MAX);
        let mut buf = buf.freeze();
        assert_eq!(5, buf.len());
        assert_eq!(u32::MAX, (&mut buf).get_var_u32());
        assert!(buf.is_empty());
    }
}
