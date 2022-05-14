use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use runkv_common::coding::BytesSerde;
use runkv_proto::kv::KvRequest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
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

impl Command {
    pub fn id(&self) -> u64 {
        match self {
            Self::KvRequest { request_id, .. } => *request_id,
            Self::CompactRaftLog { index, sequence } => {
                let mut hasher = DefaultHasher::default();
                index.hash(&mut hasher);
                sequence.hash(&mut hasher);
                hasher.finish()
            }
        }
    }

    pub fn is_read_only(&self) -> bool {
        match self {
            Self::KvRequest { request, .. } => request.is_read_only(),
            Self::CompactRaftLog { .. } => false,
        }
    }
}
