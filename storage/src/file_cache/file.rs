use std::os::unix::prelude::{FileExt, OpenOptionsExt};
use std::path::Path;

use bitvec::prelude::*;
use bitvec::slice::BitSlice;
use bytes::{Buf, BufMut};

use super::buffer::AlignedBuffer;
use super::error::{Error, Result};

const MAGIC: &[u8] = b"singularity-data-inc";
const VERSION: u32 = 1;

// TODO: Get logical block size at `/sys/block/{device}/queue/logical_block_size`.
const LOGICAL_BLOCK_SIZE: usize = 512;
const SMOOTH_GROWTH_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
const DEFAULT_BUFFER_SIZE: usize = 128 * 1024; // 128 KiB

/// Each [`CacheFileSlot`] uses 24B.
const SLOT_META_SIZE: usize = 20;
const SLOT_METAS_PER_LOGICAL_BLOCK: usize = LOGICAL_BLOCK_SIZE / SLOT_META_SIZE;

pub type CacheFileBuffer =
    AlignedBuffer<LOGICAL_BLOCK_SIZE, SMOOTH_GROWTH_SIZE, DEFAULT_BUFFER_SIZE>;

#[derive(PartialEq, Eq, Debug)]
pub struct CacheFileSlot {
    pub sst: u64,
    pub idx: u32,
    pub offset: u32,
    pub len: u32,
}

pub struct CacheFileMeta {
    slots: u64,
    buffer: CacheFileBuffer,
    valid: BitVec,
    dirty: BitVec,
}

impl CacheFileMeta {
    fn with_buffer(slots: u64, buffer: CacheFileBuffer) -> Self {
        let blocks = Self::blocks(slots);
        let capacity = blocks * LOGICAL_BLOCK_SIZE;

        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(buffer.len(), capacity);

        let dirty = bitvec![usize, Lsb0; 0; Self::blocks(slots)];
        let mut valid = bitvec![usize, Lsb0; 0; Self::blocks(slots)];

        for (slot, cursor) in (0..buffer.len()).step_by(SLOT_META_SIZE).enumerate() {
            if (&buffer[cursor..cursor + 8]).get_u64() != 0 {
                valid.set(slot, true);
            }
        }

        Self {
            slots,
            buffer,
            valid,
            dirty,
        }
    }

    fn slots(&self) -> u64 {
        self.slots
    }

    fn get(&self, slot: u64) -> CacheFileSlot {
        if slot >= self.slots {
            panic!("out of range: [slots: {}] [given: {}]", self.slots, slot);
        }
        let buf = &mut &self.buffer[Self::range(slot)];
        let sst = buf.get_u64();
        let idx = buf.get_u32();
        let offset = buf.get_u32();
        let len = buf.get_u32();
        CacheFileSlot {
            sst,
            idx,
            offset,
            len,
        }
    }

    fn set(&mut self, slot: u64, data: &CacheFileSlot) {
        if slot >= self.slots {
            panic!("out of range: [slots: {}] [given: {}]", self.slots, slot);
        }
        let mut buf = &mut self.buffer[Self::range(slot)];
        buf.put_u64(data.sst);
        buf.put_u32(data.idx);
        buf.put_u32(data.offset);
        buf.put_u32(data.len);
        self.dirty.set(Self::block(slot), true);
    }

    fn flush(&mut self, file: &std::fs::File) -> Result<()> {
        let mut flushed = -1isize;
        for (block, mut dirty) in self.dirty.iter_mut().enumerate() {
            if *dirty {
                if block as isize != flushed {
                    let offset = block * LOGICAL_BLOCK_SIZE;
                    file.write_all_at(
                        &self.buffer[offset..offset + LOGICAL_BLOCK_SIZE],
                        offset as u64,
                    )?;
                    flushed = block as isize;
                }
                *dirty = false;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn blocks(slots: u64) -> usize {
        (slots as usize + SLOT_METAS_PER_LOGICAL_BLOCK - 1) / SLOT_METAS_PER_LOGICAL_BLOCK
    }

    #[inline(always)]
    fn range(slot: u64) -> core::ops::Range<usize> {
        let offset = slot as usize / SLOT_METAS_PER_LOGICAL_BLOCK * LOGICAL_BLOCK_SIZE
            + slot as usize % SLOT_METAS_PER_LOGICAL_BLOCK * SLOT_META_SIZE;
        offset..offset + SLOT_META_SIZE
    }

    #[inline(always)]
    fn block(slot: u64) -> usize {
        slot as usize / SLOT_METAS_PER_LOGICAL_BLOCK
    }
}

pub struct CacheFile {
    slots: u64,
    file: std::fs::File,
    meta: CacheFileMeta,
}

impl CacheFile {
    pub fn create(dir: impl AsRef<Path>, id: u64, slots: u64) -> Result<Self> {
        let path = dir.as_ref().join(format!("cache-{:08}", id));
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT);
        let file = options.open(path)?;

        Self::write_header(&file, slots)?;

        let meta_buffer = {
            let capacity = CacheFileMeta::blocks(slots) * LOGICAL_BLOCK_SIZE;
            let mut buffer = CacheFileBuffer::with_capacity(capacity);
            buffer.resize(capacity);
            buffer
        };
        file.write_all_at(&meta_buffer[..], LOGICAL_BLOCK_SIZE as u64)?;

        let meta = CacheFileMeta::with_buffer(slots, meta_buffer);
        Ok(Self { slots, file, meta })
    }

    pub fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let path = dir.as_ref().join(format!("cache-{:08}", id));
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT);
        let file = options.open(path)?;

        let slots = Self::read_header(&file)?;
        let mut meta_buffer = {
            let capacity = CacheFileMeta::blocks(slots) * LOGICAL_BLOCK_SIZE;
            let mut buffer = CacheFileBuffer::with_capacity(capacity);
            buffer.resize(capacity);
            buffer
        };
        file.read_exact_at(&mut meta_buffer[..], LOGICAL_BLOCK_SIZE as u64)?;

        let meta = CacheFileMeta::with_buffer(slots, meta_buffer);
        Ok(Self { slots, file, meta })
    }

    #[inline(always)]
    pub fn slots(&self) -> u64 {
        self.slots
    }

    pub fn flush_meta(&mut self) -> Result<()> {
        self.meta.flush(&self.file)
    }

    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

impl CacheFile {
    fn write_header(file: &std::fs::File, slots: u64) -> Result<()> {
        let mut buf = CacheFileBuffer::with_capacity(LOGICAL_BLOCK_SIZE);
        buf.resize(LOGICAL_BLOCK_SIZE);

        let mut cursor = 0;

        (&mut buf[cursor..MAGIC.len()]).put_slice(MAGIC);
        cursor += MAGIC.len();

        (&mut buf[cursor..cursor + 4]).put_u32(VERSION);
        cursor += 4;

        (&mut buf[cursor..cursor + 8]).put_u64(slots);
        cursor += 8;

        assert!(
            cursor < LOGICAL_BLOCK_SIZE,
            "cursor={}, logical block size={}",
            cursor,
            LOGICAL_BLOCK_SIZE
        );

        file.write_all_at(&buf[..], 0)?;

        Ok(())
    }

    fn read_header(file: &std::fs::File) -> Result<u64> {
        let mut buf = CacheFileBuffer::with_capacity(LOGICAL_BLOCK_SIZE);
        buf.resize(LOGICAL_BLOCK_SIZE);
        file.read_exact_at(&mut buf[..], 0)?;

        let mut cursor = 0;

        if MAGIC != &buf[cursor..cursor + MAGIC.len()] {
            return Err(Error::MagicNotMatch);
        }
        cursor += MAGIC.len();

        let version = (&buf[cursor..cursor + 4]).get_u32();
        if version != VERSION {
            return Err(Error::InvalidVersion(version));
        }
        cursor += 4;

        let slots = (&buf[cursor..cursor + 8]).get_u64();

        Ok(slots)
    }
}

#[cfg(test)]
mod tests {

    use std::io::{IoSlice, Seek, SeekFrom, Write};
    use std::os::unix::prelude::{AsRawFd, FileExt};

    use nix::sys::uio::pwrite;
    use nix::unistd::write;
    use test_log::test;

    use super::*;

    fn new_meta_buffer(slots: u64) -> CacheFileBuffer {
        let capacity = CacheFileMeta::blocks(slots) * LOGICAL_BLOCK_SIZE;
        let mut buffer = CacheFileBuffer::with_capacity(capacity);
        buffer.resize(capacity);
        buffer
    }

    #[test]
    fn test_slot_meta_ranges() {
        let meta = CacheFileMeta::with_buffer(4096, new_meta_buffer(4096));
        let mut range = 0..0;
        for i in 0..4096 {
            let r = CacheFileMeta::range(i);
            if i > 0 {
                assert!(
                    r.start >= range.end,
                    "r.start={}, range.end={}",
                    r.start,
                    range.end
                );
            }
            assert_eq!(
                r.start / LOGICAL_BLOCK_SIZE,
                (r.end - 1) / LOGICAL_BLOCK_SIZE
            );
            assert_eq!(r.start / LOGICAL_BLOCK_SIZE, CacheFileMeta::block(i));
            range = r;
        }
    }

    #[test]
    fn test_cache_file_meta() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT);
        let file = options.open(tempdir.path().join("test-file-001")).unwrap();

        let mut meta0 = CacheFileMeta::with_buffer(4096, new_meta_buffer(4096));
        file.write_all_at(&meta0.buffer[..], 0).unwrap();

        for slot in 0..meta0.slots() {
            assert_eq!(
                meta0.get(slot),
                CacheFileSlot {
                    sst: 0,
                    idx: 0,
                    offset: 0,
                    len: 0
                }
            );
        }
        meta0.flush(&file).unwrap();
        file.sync_all().unwrap();

        let data = CacheFileSlot {
            sst: 1,
            idx: 1,
            offset: 1,
            len: 1,
        };
        meta0.set(0, &data);
        assert_eq!(meta0.get(0), data);

        meta0.flush(&file).unwrap();
        file.sync_all().unwrap();
        let mut buffer1 = new_meta_buffer(4096);
        file.read_exact_at(&mut buffer1[..], 0).unwrap();
        let meta1 = CacheFileMeta::with_buffer(4096, buffer1);
        assert_eq!(meta1.get(0), data);
        assert!(meta1.valid.get(0).unwrap());
    }

    #[test]
    fn test_cache_file() {
        let tempdir = tempfile::tempdir().unwrap();

        let cf = CacheFile::create(tempdir.path(), 1, 4096).unwrap();
        cf.sync_all().unwrap();
        drop(cf);

        let cf = CacheFile::open(tempdir.path(), 1).unwrap();
        assert_eq!(cf.slots(), 4096);
    }

    #[test]
    fn test_dio() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT);
        let file = options.open(tempdir.path().join("test-file-001")).unwrap();

        let mut buf = CacheFileBuffer::default();
        buf.append(&[b'x'; 4096]);
        file.write_all_at(&buf[0..4096], 0).unwrap();

        buf.write_at(&[b'a'; 512], 0);
        nix::sys::uio::pwritev(
            file.as_raw_fd(),
            &[
                IoSlice::new(&buf[0..512]),
                IoSlice::new(&buf[0..512]),
                IoSlice::new(&buf[0..512]),
                IoSlice::new(&buf[0..512]),
            ],
            0,
        )
        .unwrap();

        let mut read = CacheFileBuffer::default();
        read.resize(2048);
        file.read_exact_at(&mut read[0..2048], 0).unwrap();
        assert_eq!(read[0..2048], [b'a'; 2048]);
    }
}
