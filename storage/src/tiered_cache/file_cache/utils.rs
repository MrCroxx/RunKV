// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Display};
use std::hash::{BuildHasher, Hasher};
use std::ops::{Add, BitAnd, Not, Sub};

pub trait UnsignedTrait = Add<Output = Self>
    + Sub<Output = Self>
    + BitAnd<Output = Self>
    + Not<Output = Self>
    + Sized
    + From<u8>
    + Eq
    + Debug
    + Display
    + Clone
    + Copy;

pub trait Unsigned: UnsignedTrait {}

impl<U: UnsignedTrait> Unsigned for U {}

#[inline(always)]
pub fn is_pow2<U: Unsigned>(v: U) -> bool {
    v & (v - U::from(1)) == U::from(0)
}

#[inline(always)]
pub fn assert_pow2<U: Unsigned>(v: U) {
    assert_eq!(v & (v - U::from(1)), U::from(0), "v: {}", v);
}

#[inline(always)]
pub fn debug_assert_pow2<U: Unsigned>(v: U) {
    debug_assert_eq!(v & (v - U::from(1)), U::from(0), "v: {}", v);
}

#[inline(always)]
pub fn is_aligned<U: Unsigned>(align: U, v: U) -> bool {
    debug_assert_pow2(align);
    v & (align - U::from(1)) == U::from(0)
}

#[inline(always)]
pub fn assert_aligned<U: Unsigned>(align: U, v: U) {
    debug_assert_pow2(align);
    assert!(is_aligned(align, v), "align: {}, v: {}", align, v);
}

#[inline(always)]
pub fn debug_assert_aligned<U: Unsigned>(align: U, v: U) {
    debug_assert_pow2(align);
    debug_assert!(is_aligned(align, v), "align: {}, v: {}", align, v);
}

#[inline(always)]
pub fn align_up<U: Unsigned>(align: U, v: U) -> U {
    debug_assert_pow2(align);
    (v + align - U::from(1)) & !(align - U::from(1))
}

#[inline(always)]
pub fn align_down<U: Unsigned>(align: U, v: U) -> U {
    debug_assert_pow2(align);
    v & !(align - U::from(1))
}

macro_rules! bpf_buffer_trace {
    ($buf:expr, $span:expr) => {
        #[cfg(feature = "bpf")]
        {
            if $buf.len() >= 16 && let Some(id) = $span.id() {
                use bytes::BufMut;

                const BPF_BUFFER_TRACE_MAGIC: u64 = 0xdeadbeefdeadbeef;

                $span.record("sid", id.into_u64());

                (&mut $buf[0..8]).put_u64_le(BPF_BUFFER_TRACE_MAGIC);
                (&mut $buf[8..16]).put_u64_le(id.into_u64());
            }
        }
    };
}

pub(crate) use bpf_buffer_trace;

pub struct ModuloHasher<const M: u8>(u8);

impl<const M: u8> Default for ModuloHasher<M> {
    fn default() -> Self {
        Self(0)
    }
}

impl<const M: u8> ModuloHasher<M> {
    fn hash(&mut self, v: u8) {
        self.0 = ((((self.0 as u16) << 8) + v as u16) % M as u16) as u8;
    }
}

impl<const M: u8> Hasher for ModuloHasher<M> {
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        if cfg!(target_endian = "big") {
            for v in bytes {
                self.hash(*v);
            }
        } else {
            for v in bytes.iter().rev() {
                self.hash(*v);
            }
        }
    }
}

#[derive(Clone)]
pub struct ModuloHasherBuilder<const M: u8>;

impl<const M: u8> Default for ModuloHasherBuilder<M> {
    fn default() -> Self {
        Self
    }
}

impl<const M: u8> BuildHasher for ModuloHasherBuilder<M> {
    type Hasher = ModuloHasher<M>;

    fn build_hasher(&self) -> ModuloHasher<M> {
        ModuloHasher::<M>::default()
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use super::*;

    #[test]
    fn test_modulo_hasher() {
        const M: u8 = 100;
        for i in 0..1_000_000u64 {
            let mut hasher = ModuloHasher::<M>::default();
            i.hash(&mut hasher);
            let hash = hasher.finish();
            assert_eq!(
                hash,
                i % M as u64,
                "i: {}, hash: {}, i % m: {}",
                i,
                hash,
                i % M as u64
            );
        }
    }
}
