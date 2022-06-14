use std::path::Path;

use super::error::{Error, Result};

pub const LOGICAL_BLOCK_SIZE: usize = 512;

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Ext4,
    Xfs,
}

#[derive(Clone, Copy, Debug)]
pub struct FsInfo {
    pub fs_type: FsType,
    pub block_size: usize,
}

/// Check if the filesystem is supported by file cache system.
/// If supported, return [`FsInfo`].
///
/// `path` is the pathname of any file within the mounted filesystem.
pub fn fs_info(path: impl AsRef<Path>) -> Result<FsInfo> {
    let statfs = nix::sys::statfs::statfs(path.as_ref())?;

    // File cache system requires syscall `fallocate` with flag `FALLOC_FL_PUNCH_HOLE`.
    // Not all file systems support it.
    // See https://man7.org/linux/man-pages/man2/fallocate.2.html
    let fs_type = match statfs.filesystem_type() {
        nix::sys::statfs::EXT4_SUPER_MAGIC => FsType::Ext4,
        // See https://github.com/nix-rust/nix/issues/1742
        nix::sys::statfs::FsType(libc::XFS_SUPER_MAGIC) => FsType::Xfs,
        _ => return Err(Error::UnsupportedFs(statfs.filesystem_type().0 as u64)),
    };

    let block_size = statfs.block_size() as usize;

    Ok(FsInfo {
        fs_type,
        block_size,
    })
}
