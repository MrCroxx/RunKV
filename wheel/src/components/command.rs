use runkv_common::coding::BytesSerde;
use runkv_proto::kv::TxnRequest;
use serde::{Deserialize, Serialize};

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
