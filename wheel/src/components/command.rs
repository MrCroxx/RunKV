use tokio::sync::oneshot;

use crate::error::Result;

#[derive(Debug)]
pub enum CommandRequest {
    Data {
        group: u64,
        index: u64,
        data: Vec<u8>,
    },
    BuildSnapshot {
        group: u64,
        index: u64,
    },
    InstallSnapshot {
        group: u64,
        index: u64,
        snapshot: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum CommandResponse {
    Data {
        group: u64,
        index: u64,
    },
    BuildSnapshot {
        group: u64,
        index: u64,
        snapshot: Vec<u8>,
    },
    InstallSnapshot {
        group: u64,
        index: u64,
    },
}

#[derive(Debug)]
pub struct AsyncCommand {
    pub request: CommandRequest,
    pub response: oneshot::Sender<Result<CommandResponse>>,
}
