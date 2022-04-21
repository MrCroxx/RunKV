use std::ops::Range;

use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Command {
    Apply {
        group: u64,
        range: Range<u64>,
    },
    BuildSnapshot {
        group: u64,
        index: u64,
        notifier: oneshot::Sender<Vec<u8>>,
    },
    InstallSnapshot {
        group: u64,
        index: u64,
        snapshot: Vec<u8>,
        notifier: oneshot::Sender<()>,
    },
}
