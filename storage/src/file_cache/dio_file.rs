use std::fs::{File, OpenOptions};
use std::os::unix::prelude::{AsRawFd, FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::buffer::AlignedBuffer;
use super::error::Result;
use super::fs::LOGICAL_BLOCK_SIZE;

// Get logical block size by ioctl(2) BLKSSZGET or shell command `blockdev --getss`, see open(2) man
// page for more details.
const SMOOTH_GROWTH_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024; // 64 KiB

const FSTAT_BLOCK_SIZE: usize = 512;

pub type DioBuffer = AlignedBuffer<LOGICAL_BLOCK_SIZE, SMOOTH_GROWTH_SIZE, DEFAULT_BUFFER_SIZE>;

/// [`DioFile`] is a wrapper of a O_DIRECT sparse file.
///
/// Buffers of I/O requests to [`DioFile`] must be aligned to the logical block size, and buffer
/// sizes must be a multiple of `DioFileOptions.block_size`.
pub struct DioFile {
    file: File,

    block_size: usize,

    cursor: AtomicUsize,
}

impl DioFile {
    /// NOTE: `block_size` must be a multiple of target file system block size.
    ///
    /// Hint: use `FsInfo.block_size` as `block_size`.
    pub fn open(path: impl AsRef<Path>, block_size: usize, create: bool) -> Result<Self> {
        assert_eq!(block_size % LOGICAL_BLOCK_SIZE, 0);

        let mut opts = OpenOptions::new();
        opts.create(create);
        opts.read(true);
        opts.write(true);
        opts.custom_flags(libc::O_DIRECT);

        let file = opts.open(path.as_ref())?;
        let cursor = AtomicUsize::new(file.metadata()?.len() as usize);

        Ok(Self {
            file,
            block_size,
            cursor,
        })
    }

    /// Append data to the cache file.
    ///
    /// Given `buf` must be aligned to the logical block size, and the size of `buf` must be a
    /// multiple of block size.
    ///
    /// Returns the block idx of the written data.
    ///
    /// # Panics
    ///
    /// * Panic if given `buf` is not aligned to the logical block size or the size of `buf` is not
    ///   multiple of block size.
    pub fn append(&self, buf: &[u8]) -> Result<u64> {
        assert_eq!(buf.len() % self.block_size, 0);
        let cursor = self.cursor.fetch_add(buf.len(), Ordering::SeqCst) as u64;
        self.file.write_all_at(buf, cursor)?;
        Ok(cursor / self.block_size as u64)
    }

    /// Write data at the given `offset`.
    ///
    /// Written position must not exceed the file end position.
    ///
    /// Given `buf` must be aligned to the logical block size, and the size of `buf` must be a
    /// multiple of block size.
    ///
    /// # Panics
    ///
    /// * Panic if given `buf` is not aligned to the logical block size or the size of `buf` is not
    ///   multiple of block size.
    pub fn write_at(&self, buf: &[u8], block_offset: u64) -> Result<()> {
        assert_eq!(buf.len() % self.block_size, 0);
        let offset = block_offset * self.block_size as u64;
        let cursor = self.cursor.load(Ordering::Acquire);
        assert!(
            offset as usize + buf.len() <= cursor,
            "offset + len: {}, cursor: {}",
            offset as usize + buf.len(),
            cursor
        );
        self.file.write_all_at(buf, offset)?;
        Ok(())
    }

    /// Read data by blocks.
    pub fn read(&self, block_offset: u64, block_len: usize) -> Result<DioBuffer> {
        let offset = block_offset * self.block_size as u64;
        let len = block_len * self.block_size;
        let mut buf = DioBuffer::with_size(len);
        self.file.read_exact_at(&mut buf[..], offset)?;
        Ok(buf)
    }

    /// Reclaim disk space by blocks.
    pub fn reclaim(&self, block_offset: u64, block_len: usize) -> Result<()> {
        let fd = self.file.as_raw_fd();
        let mode = nix::fcntl::FallocateFlags::FALLOC_FL_PUNCH_HOLE
            | nix::fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE;
        let offset = block_offset * self.block_size as u64;
        let len = block_len * self.block_size;
        nix::fcntl::fallocate(fd, mode, offset as i64, len as i64)?;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Actually occupied disk space.
    pub fn len(&self) -> usize {
        let fd = self.file.as_raw_fd();
        let stat = nix::sys::stat::fstat(fd).unwrap();
        stat.st_blocks as usize * FSTAT_BLOCK_SIZE
    }

    /// Length of the sparse file, including the sizes of holes.
    pub fn length(&self) -> usize {
        self.cursor.load(Ordering::Acquire)
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

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;
    use crate::file_cache::fs::fs_info;

    #[test]
    fn test_dio_file() {
        let dir = tempfile::tempdir().unwrap();

        let fs_info = fs_info(dir.path()).unwrap();
        let bs = fs_info.block_size;

        let cf =
            DioFile::open(dir.path().join("test-dio-file-1"), fs_info.block_size, true).unwrap();
        assert_eq!(cf.len(), 0);

        let mut buf = DioBuffer::default();
        buf.append(&vec![b'x'; bs * 10]);
        buf.align_up_to(fs_info.block_size);
        assert_eq!(buf.len(), bs * 10);

        cf.append(&buf[..]).unwrap();
        assert_eq!(cf.len(), bs * 10);

        assert_eq!(&cf.read(0, 2).unwrap()[..], &buf[0..2 * fs_info.block_size]);

        cf.reclaim(0, 2).unwrap();
        assert_eq!(cf.len(), bs * 8);
        assert_eq!(&cf.read(0, 2).unwrap()[..], &vec![0; bs * 2]);

        let mut buf2 = DioBuffer::default();
        buf2.append(&vec![b'z'; bs * 2]);
        buf2.align_up_to(fs_info.block_size);
        cf.write_at(&buf2[..], 8).unwrap();
        assert_eq!(cf.len(), bs * 8);
        assert_eq!(&cf.read(8, 2).unwrap()[..], &buf2[..]);

        // Test rewrite holes.
        assert_eq!(&cf.read(0, 2).unwrap()[..], &vec![0; bs * 2]);
        cf.write_at(&buf[0..bs * 2], 0).unwrap();
        assert_eq!(&cf.read(0, 2).unwrap()[..], &buf[0..2 * fs_info.block_size]);
        assert_eq!(cf.len(), bs * 10);
    }
}
