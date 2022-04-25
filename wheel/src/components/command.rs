use std::ops::Range;

use runkv_proto::kv::{BytesSerde, TxnRequest};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    TxnRequest {
        request_id: u64,
        sequence: u64,
        request: TxnRequest,
    },
    CompactRaftLog {
        index: u64,
        sequence: u64,
    },
}

impl<'de> BytesSerde<'de> for Command {}

#[derive(Debug)]
pub enum GearCommand {
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
