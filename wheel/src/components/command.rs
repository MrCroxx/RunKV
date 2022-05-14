use runkv_common::coding::BytesSerde;
use runkv_proto::kv::KvRequest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    KvRequest {
        request_id: u64,
        sequence: u64,
        request: KvRequest,
    },
    CompactRaftLog {
        index: u64,
        sequence: u64,
    },
}

impl<'de> BytesSerde<'de> for Command {}
