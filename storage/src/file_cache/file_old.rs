use std::collections::BTreeMap;
use std::os::unix::prelude::{FileExt, OpenOptionsExt};
use std::path::Path;

use bitvec::prelude::*;
use bitvec::slice::BitSlice;
use bytes::{Buf, BufMut};
use rangemap::RangeSet;

use super::buffer::AlignedBuffer;
use super::error::{Error, Result};

const MAGIC: &[u8] = b"runkv";
const VERSION: u32 = 1;

// TODO: Get logical block size by ioctl(2) BLKSSZGET, see open(2) for details.
const LOGICAL_BLOCK_SIZE: usize = 512;
const SMOOTH_GROWTH_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
const DEFAULT_BUFFER_SIZE: usize = 128 * 1024; // 128 KiB

/// Each [`CacheFileSlot`] uses 24B.
const SLOT_META_SIZE: usize = 20;
const SLOT_METAS_PER_LOGICAL_BLOCK: usize = LOGICAL_BLOCK_SIZE / SLOT_META_SIZE;

pub type CacheFileBuffer =
    AlignedBuffer<LOGICAL_BLOCK_SIZE, SMOOTH_GROWTH_SIZE, DEFAULT_BUFFER_SIZE>;

#[derive(PartialEq, Eq, Debug)]
struct CacheFileSlot {
    /// sst id
    sst: u64,
    /// sst block index
    idx: u32,
    /// Logical block offset after cache file header and cache file meta.
    block_offset: u32,
    /// length by bytes
    len: u32,
}

impl CacheFileSlot {
    #[inline(always)]
    fn aligned_len(&self) -> u32 {
        Self::align_up(self.len)
    }

    #[inline(always)]
    fn block_len(&self) -> u32 {
        self.aligned_len() / LOGICAL_BLOCK_SIZE as u32
    }

    #[inline(always)]
    fn align_up(v: u32) -> u32 {
        (v + LOGICAL_BLOCK_SIZE as u32 - 1) & !(LOGICAL_BLOCK_SIZE as u32 - 1)
    }

    #[inline(always)]
    fn align_down(v: u32) -> u32 {
        v & !(LOGICAL_BLOCK_SIZE as u32 - 1)
    }
}

struct CacheFileMeta {
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

    fn is_slot_valid(&self, slot: u64) -> bool {
        if slot >= self.slots {
            panic!("out of range: [slots: {}] [given: {}]", self.slots, slot);
        }
        *self.valid.get(slot as usize).unwrap()
    }

    /// Return used slot count.
    fn used_slots(&self) -> u64 {
        self.valid.count_ones() as u64
    }

    /// Return unused slot count.
    fn unused_slots(&self) -> u64 {
        self.valid.count_zeros() as u64
    }

    /// Return if all slots are already used.
    fn is_full(&self) -> bool {
        self.valid.all()
    }

    fn is_used(&self, slot: u64) -> bool {
        if slot >= self.slots {
            panic!("out of range: [slots: {}] [given: {}]", self.slots, slot);
        }
        let offset = Self::range(slot).start;
        let buf = &mut &self.buffer[offset..offset + 8];
        buf.get_u64() != 0
    }

    fn first_unused_slot(&self) -> Option<u64> {
        self.valid.first_zero().map(|slot| slot as u64)
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
            block_offset: offset,
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
        buf.put_u32(data.block_offset);
        buf.put_u32(data.len);
        if data.sst != 0 {
            self.valid.set(slot as usize, true);
        }
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
    file: std::fs::File,
    meta: CacheFileMeta,

    /// total slots
    slots: u64,
    /// cache data offset
    offset: u64,
    /// total cache data blocks
    blocks: usize,
    /// unused block ranges
    fragmentations: RangeSet<usize>,
    /// { (sst id, block idx) -> slot }
    mapping: BTreeMap<(u64, u32), u64>,
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

        let meta_blocks = CacheFileMeta::blocks(slots);
        let meta_buffer = {
            let capacity = meta_blocks * LOGICAL_BLOCK_SIZE;
            let mut buffer = CacheFileBuffer::with_capacity(capacity);
            buffer.resize(capacity);
            buffer
        };
        file.write_all_at(&meta_buffer[..], LOGICAL_BLOCK_SIZE as u64)?;
        let meta = CacheFileMeta::with_buffer(slots, meta_buffer);

        let offset = ((meta_blocks + 1) * LOGICAL_BLOCK_SIZE) as u64;
        let fragmentations = RangeSet::new();
        let mapping = BTreeMap::default();

        Ok(Self {
            slots,
            file,
            meta,
            offset,
            blocks: 0,
            fragmentations,
            mapping,
        })
    }

    pub fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let path = dir.as_ref().join(format!("cache-{:08}", id));
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT);
        let file = options.open(path)?;

        let slots = Self::read_header(&file)?;

        let meta_blocks = CacheFileMeta::blocks(slots);
        let mut meta_buffer = {
            let capacity = meta_blocks * LOGICAL_BLOCK_SIZE;
            let mut buffer = CacheFileBuffer::with_capacity(capacity);
            buffer.resize(capacity);
            buffer
        };
        file.read_exact_at(&mut meta_buffer[..], LOGICAL_BLOCK_SIZE as u64)?;
        let meta = CacheFileMeta::with_buffer(slots, meta_buffer);

        let offset = ((meta_blocks + 1) * LOGICAL_BLOCK_SIZE) as u64;
        let blocks = (file.metadata()?.len() as usize + LOGICAL_BLOCK_SIZE - 1)
            / LOGICAL_BLOCK_SIZE
            - meta_blocks
            - 1;

        let mut mapping = BTreeMap::default();
        let mut fragmentations = RangeSet::new();
        if blocks > 0 {
            fragmentations.insert(0..blocks);
        }
        for slot in 0..slots {
            let s = meta.get(slot);
            if s.sst != 0 {
                fragmentations.remove(
                    s.block_offset as usize..s.block_offset as usize + s.block_len() as usize,
                );
            }
            mapping.insert((s.sst, s.idx), slot);
        }

        Ok(Self {
            slots,
            file,
            meta,
            offset,
            blocks,
            fragmentations,
            mapping,
        })
    }

    pub fn insert(&mut self, sst: u64, idx: u32, buffer: &CacheFileBuffer) -> Result<()> {
        // TODO: Use first-fit policy here, optimize this later based on the real workload.

        let slot = self.meta.first_unused_slot().ok_or(Error::Full)?;
        let mut offset = None;
        let len = buffer.alignments();

        // Find a fragmentation that fits the given buffer.
        for gap in self.fragmentations.gaps(&(0..self.blocks)) {
            if gap.end - gap.start >= len {
                offset = Some(gap.start);
                self.fragmentations.remove(gap.start..gap.start + len);
                break;
            }
        }

        // Append to the end of the file if there is no fit fragmentations.
        if offset.is_none() {
            offset = Some(self.blocks);
            self.blocks += len;
        }

        let slot_meta = CacheFileSlot {
            sst,
            idx,
            block_offset: offset.unwrap() as u32,
            len: buffer.len() as u32,
        };
        self.meta.set(slot, &slot_meta);
        self.write_at(buffer, slot_meta.block_offset)?;
        self.mapping.insert((sst, idx), slot);

        Ok(())
    }

    pub fn invalidate(&mut self) -> Result<()> {
        todo!()
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

    /// Write data from [`buffer`] at the given block (header and meta blocks excluded).
    fn write_at(&self, buffer: &CacheFileBuffer, block_at: u32) -> Result<()> {
        self.file.write_all_at(
            &buffer[..],
            self.offset + block_at as u64 * LOGICAL_BLOCK_SIZE as u64,
        )?;
        Ok(())
    }

    /// Read data from file at the given block and length, returns a new [`CacheFileBuffer`].
    fn read_at_to_buffer(&self, block_at: u32, block_len: u32) -> Result<CacheFileBuffer> {
        let capacity = block_len as usize * LOGICAL_BLOCK_SIZE;
        let mut buffer = CacheFileBuffer::with_capacity(capacity);
        buffer.resize(capacity);
        debug_assert_eq!(buffer.capacity(), capacity);
        debug_assert_eq!(buffer.len(), capacity);
        self.file.read_exact_at(
            &mut buffer[..],
            self.offset + block_at as u64 * LOGICAL_BLOCK_SIZE as u64,
        )?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {

    use std::io::{IoSlice, Seek, SeekFrom, Write};
    use std::os::unix::prelude::{AsRawFd, FileExt};
    use std::time::Instant;

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
                    block_offset: 0,
                    len: 0
                }
            );
        }
        meta0.flush(&file).unwrap();
        file.sync_all().unwrap();

        let data = CacheFileSlot {
            sst: 1,
            idx: 1,
            block_offset: 1,
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

    // TODO: REMOVE ME!!
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

    #[test]
    fn test_dio_drop_buffer() {
        const SIZE: usize = 1024 * 1024 * 1024;

        let tempdir = tempfile::tempdir().unwrap();

        // Buffered I/O

        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        let file = options.open(tempdir.path().join("test-file-001")).unwrap();

        let mut buf = CacheFileBuffer::with_capacity(SIZE);
        buf.append(&[b'x'; SIZE]);

        let start = Instant::now();
        file.write_all_at(&buf[..], 0).unwrap();
        println!("write: {:?}", start.elapsed());
        drop(buf);
        file.sync_data().unwrap();
        println!("sync: {:?}", start.elapsed());
        file.sync_data().unwrap();
        println!("sync: {:?}", start.elapsed());

        // Direct I/O

        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT);
        let file = options.open(tempdir.path().join("test-file-002")).unwrap();

        let mut buf = CacheFileBuffer::with_capacity(SIZE);
        buf.append(&[b'x'; SIZE]);

        let start = Instant::now();
        file.write_all_at(&buf[..], 0).unwrap();
        println!("write: {:?}", start.elapsed());
        drop(buf);
        file.sync_data().unwrap();
        println!("sync: {:?}", start.elapsed());
        file.sync_data().unwrap();
        println!("sync: {:?}", start.elapsed());

        // Sync Direct I/O

        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT | libc::O_SYNC);
        let file = options.open(tempdir.path().join("test-file-003")).unwrap();

        let mut buf = CacheFileBuffer::with_capacity(SIZE);
        buf.append(&[b'x'; SIZE]);

        let start = Instant::now();
        file.write_all_at(&buf[..], 0).unwrap();
        println!("write: {:?}", start.elapsed());
        drop(buf);
        file.sync_data().unwrap();
        println!("sync: {:?}", start.elapsed());
        file.sync_data().unwrap();
        println!("sync: {:?}", start.elapsed());
    }
}
