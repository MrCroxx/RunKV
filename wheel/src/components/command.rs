use std::ops::Range;

use tokio::sync::oneshot;

#[derive(Debug)]
pub struct Apply {
    pub group: u64,
    pub range: Range<u64>,
}

#[derive(Debug)]
pub enum Snapshot {
    Build {
        group: u64,
        index: u64,
        notifier: oneshot::Sender<Vec<u8>>,
    },
    Install {
        group: u64,
        index: u64,
        snapshot: Vec<u8>,
        notifier: oneshot::Sender<()>,
    },
}
