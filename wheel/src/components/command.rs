use tokio::sync::oneshot;

use crate::error::Result;

#[derive(Debug)]
pub enum CommandRequest {
    ApplyToExclusive(u64),
    BuildSnapshot(u64),
    InstallSnapshot(u64, Vec<u8>),
}

#[derive(Debug)]
pub enum CommandResponse {
    ApplyToExclusive(u64),
    BuildSnapshot(u64, Vec<u8>),
    InstallSnapshot(u64),
}

#[derive(Debug)]
pub struct AsyncCommand {
    pub request: CommandRequest,
    pub response: oneshot::Sender<Result<CommandResponse>>,
}
