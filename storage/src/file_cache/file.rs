use std::io::Read;
use std::ops::Range;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bitvec::prelude::*;
use bytes::{Buf, BufMut};
use parking_lot::RwLock;

use super::dio_file::{DioBuffer, DioFile};
use super::error::{Error, Result};
use super::fs::{FsInfo, LOGICAL_BLOCK_SIZE};

const MAGIC: &[u8; 16] = b"runkv-cache-file";
const VERSION: u64 = 1;

const META_SLOT_ALIGN_SIZE: usize = LOGICAL_BLOCK_SIZE;
const HEADER_PROBE_SIZE: usize = 16 + 8 + 8 + 8;

/// Each [`Index`] uses 20B.
const SLOT_INDEX_SIZE: usize = 20;

#[derive(Default, Debug)]
pub struct SlotIndex {
    /// sst id
    pub sst: u64,
    /// sst block idx
    pub idx: u32,
    /// block offset after cache file header and meta
    pub block_offset: u32,
    /// length by bytes
    pub len: u32,
}

impl SlotIndex {
    #[inline(always)]
    fn aligned_len(&self) -> u32 {
        Self::align_up(self.len)
    }

    #[inline(always)]
    fn align_up(v: u32) -> u32 {
        (v + META_SLOT_ALIGN_SIZE as u32 - 1) & !(META_SLOT_ALIGN_SIZE as u32 - 1)
    }
}

struct CacheFileMeta {
    /// fixed total slots count
    blocks: usize,
    /// block size
    block_size: usize,

    /// dio buffer
    buffer: DioBuffer,

    /// valid slots bitmap
    valid: BitVec,
    /// dirty blocks bitmap
    dirty: BitVec,
}

impl CacheFileMeta {
    fn with_buffer(blocks: usize, block_size: usize, buffer: DioBuffer) -> Self {
        assert_eq!(buffer.len() % block_size, 0);
        assert_eq!(buffer.len() / block_size, blocks);

        let slots = Self::total_slots(blocks, block_size);

        let dirty = bitvec![usize,Lsb0;0;blocks];
        let mut valid = bitvec![usize,Lsb0;0;slots];

        for idx in 0..slots {
            if (&buffer[Self::slot_index_range(blocks, block_size, idx)]).get_u64() != 0 {
                valid.set(idx, true);
            }
        }

        Self {
            blocks,
            buffer,
            block_size,
            valid,
            dirty,
        }
    }

    fn get(&self, idx: usize) -> Option<SlotIndex> {
        if idx >= self.slots() {
            panic!("out of range: [slots: {}] [given: {}]", self.slots(), idx);
        }

        let buf = &mut &self.buffer[self.range(idx)];

        let sst = buf.get_u64();
        if sst == 0 {
            return None;
        }
        let idx = buf.get_u32();
        let block_offset = buf.get_u32();
        let len = buf.get_u32();
        Some(SlotIndex {
            sst,
            idx,
            block_offset,
            len,
        })
    }

    fn set(&mut self, idx: usize, index: &SlotIndex) {
        if idx >= self.slots() {
            panic!("out of range: [slots: {}] [given: {}]", self.slots(), idx);
        }

        let range = self.range(idx);
        let mut buf = &mut self.buffer[range];
        buf.put_u64(index.sst);
        buf.put_u32(index.idx);
        buf.put_u32(index.block_offset);
        buf.put_u32(index.len);

        let block = self.block(idx);
        self.valid.set(idx, index.sst != 0);
        self.dirty.set(block, true);
    }

    fn flush(&mut self, dfile: &DioFile) -> Result<()> {
        for (block, mut dirty) in self.dirty.iter_mut().enumerate() {
            if !*dirty {
                continue;
            }
            let offset = block * self.block_size;
            dfile.write_at(
                &self.buffer[offset..offset + self.block_size],
                (offset + /* header segment */ self.block_size) as u64,
            )?;
            *dirty = false;
        }
        Ok(())
    }

    fn is_slot_valid(&self, idx: usize) -> bool {
        if idx >= self.slots() {
            panic!("out of range: [slots: {}] [given: {}]", self.slots(), idx);
        }
        *self.valid.get(idx).unwrap()
    }

    /// Return if all slots are already used.
    fn is_full(&self) -> bool {
        self.valid.all()
    }

    fn pick_unused_slot(&self) -> Option<usize> {
        self.valid.first_zero()
    }

    #[inline(always)]
    fn slots(&self) -> usize {
        Self::total_slots(self.blocks, self.block_size)
    }

    #[inline(always)]
    fn range(&self, idx: usize) -> Range<usize> {
        Self::slot_index_range(self.blocks, self.block_size, idx)
    }

    #[inline(always)]
    fn block(&self, idx: usize) -> usize {
        idx / (self.block_size / SLOT_INDEX_SIZE)
    }

    #[inline(always)]
    fn total_slots(blocks: usize, block_size: usize) -> usize {
        block_size / SLOT_INDEX_SIZE * blocks
    }

    #[inline(always)]
    fn slot_index_range(blocks: usize, block_size: usize, idx: usize) -> Range<usize> {
        let slots = Self::total_slots(blocks, block_size);

        if idx >= slots {
            panic!("out of range: [slots: {}] [given: {}]", slots, idx);
        }

        let slots_per_block = block_size / SLOT_INDEX_SIZE;
        let offset = idx / slots_per_block * block_size + idx % slots_per_block * SLOT_INDEX_SIZE;
        offset..offset + SLOT_INDEX_SIZE
    }
}

pub struct CacheFileOptions {
    /// Cache file id.
    pub id: u64,
    /// Cache file directory.
    pub dir: String,
    /// Block count of the meta segment. Only used when creating new cache file.
    pub meta_blocks: usize,
    /// Cache file block size, which must be a multiple of target file system block size
    /// and `LOGICAL_BLOCK_SIZE`.
    pub block_size: usize,
}

pub struct CacheFile {
    id: u64,
    block_size: usize,

    meta: CacheFileMeta,
    dfile: DioFile,
}

impl CacheFile {
    pub fn create(options: CacheFileOptions) -> Result<Self> {
        let path = PathBuf::from(options.dir).join(Self::filename(options.id));
        if std::fs::metadata(&path).is_ok() {
            return Err(Error::Other(format!(
                "cache file {} already exists: {:?}",
                options.id, path
            )));
        }

        let dfile = DioFile::open(&path, options.block_size, true)?;

        // Write header segment.
        let mut buf = DioBuffer::with_size(options.block_size);
        buf.append(MAGIC);
        buf.append(&VERSION.to_be_bytes());
        buf.append(&options.meta_blocks.to_be_bytes());
        buf.append(&options.block_size.to_be_bytes());
        dfile.append(&buf[..])?;

        // Write empty meta segment.
        let buf = DioBuffer::with_size(options.block_size * options.meta_blocks);
        dfile.append(&buf[..])?;

        let meta = CacheFileMeta::with_buffer(options.meta_blocks, options.block_size, buf);

        Ok(Self {
            id: options.id,
            block_size: options.block_size,
            meta,
            dfile,
        })
    }

    pub fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let path = dir.as_ref().join(Self::filename(id));

        // Probe header.
        let tempf = std::fs::File::open(&path)?;
        let mut buf = vec![0; HEADER_PROBE_SIZE];
        tempf.read_exact_at(&mut buf, 0)?;
        drop(tempf);

        let mut cursor = 0;

        if &buf[cursor..cursor + MAGIC.len()] != MAGIC {
            return Err(Error::MagicNotMatch);
        }
        cursor += MAGIC.len();

        let version = (&buf[cursor..cursor + 8]).get_u64();
        if version != VERSION {
            return Err(Error::InvalidVersion(version));
        }
        cursor += 8;

        let meta_blocks = (&buf[cursor..cursor + 8]).get_u64() as usize;
        cursor += 8;

        let block_size = (&buf[cursor..cursor + 8]).get_u64() as usize;
        cursor += 8;

        assert_eq!(cursor, buf.len());

        // FIXME: Check if cache file block size is a multiple of target file system block size.
        // If not, mark it as read-only. Delete the file after all its slots are invalid.

        // Open with O_DIRECT and read meta.
        let dfile = DioFile::open(&path, block_size, false)?;
        let buffer = dfile.read(1, meta_blocks)?;
        let meta = CacheFileMeta::with_buffer(meta_blocks, block_size, buffer);

        Ok(Self {
            id,
            block_size,
            meta,
            dfile,
        })
    }

    /// NOTE: `data` buffer must be aligned.
    pub fn append(&mut self, sst: u64, idx: u32, data: &[u8]) -> Result<()> {
        let slot_idx = self.meta.pick_unused_slot().ok_or(Error::Full)?;
        let block_offset = self.dfile.append(data)? as u32;
        let index = SlotIndex {
            sst,
            idx,
            block_offset,
            len: data.len() as u32,
        };
        self.meta.set(slot_idx, &index);
        Ok(())
    }

    pub fn get(&self, idx: usize) -> Result<Option<DioBuffer>> {
        let index = match self.meta.get(idx) {
            Some(index) => index,
            None => return Ok(None),
        };
        let mut buf = self.dfile.read(
            index.block_offset as u64,
            index.aligned_len() as usize / self.block_size,
        )?;
        buf.resize(index.len as usize);
        Ok(Some(buf))
    }

    pub fn invalidate(&mut self, idx: usize) {
        self.meta.set(idx, &SlotIndex::default());
    }

    pub fn flush(&mut self) -> Result<()> {
        self.meta.flush(&self.dfile)?;
        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.dfile.len() + /* header & meta */ (1 + self.meta.blocks) * self.block_size
    }

    pub fn filename(id: u64) -> String {
        format!("cf-{:024}", id)
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;
}
