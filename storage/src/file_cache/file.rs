use std::os::unix::prelude::OpenOptionsExt;
use std::path::Path;

use super::buffer::AlignedBuffer;
use super::error::Result;

// TODO: Get logical block size at `/sys/block/{device}/queue/logical_block_size`.
const LOGICAL_BLOCK_SIZE: usize = 512;
const SMOOTH_GROWTH_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
const DEFAULT_BUFFER_SIZE: usize = 128 * 1024; // 128 KiB

pub type CacheFileBuffer =
    AlignedBuffer<LOGICAL_BLOCK_SIZE, SMOOTH_GROWTH_SIZE, DEFAULT_BUFFER_SIZE>;

pub struct CacheFile {
    file: std::fs::File,
}

impl CacheFile {
    pub fn open(dir: impl AsRef<Path>, id: u64) -> Result<Self> {
        let path = dir.as_ref().join(format!("cache-{:08}", id));
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        options.custom_flags(libc::O_DIRECT | libc::O_SYNC);
        let file = options.open(path)?;
        Ok(Self { file })
    }
}

#[cfg(test)]
mod tests {

    use std::io::{Seek, SeekFrom, Write};
    use std::os::unix::prelude::{AsRawFd, FileExt};

    use nix::sys::uio::pwrite;
    use nix::unistd::write;
    use test_log::test;

    use super::*;

    // #[test]
    // fn test_dio() {
    //     let cf = CacheFile::open("/home/mrcroxx/test", 1).unwrap();

    //     let mut buf = CacheFileBuffer::default();
    //     buf.append(&[b'x'; 4096]);

    //     cf.file.write_all_at(&buf[0..4096], 0).unwrap();
    // }
}
