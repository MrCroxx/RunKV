use runkv_proto::wheel::{KvRequest, KvResponse};
use tokio::sync::oneshot;

use crate::error::Result;

#[derive(Debug)]
pub enum CommandRequest {
    Kv(KvRequest),
    BuildSnapshot,
    InstallSnapshot(Vec<u8>),
}

#[derive(Debug)]
pub enum CommandResponse {
    Kv(KvResponse),
    BuildSnapshot(Vec<u8>),
    InstallSnapshot,
}

#[derive(Debug)]
pub struct AsyncCommand {
    pub request: CommandRequest,
    pub response: oneshot::Sender<Result<CommandResponse>>,
}
