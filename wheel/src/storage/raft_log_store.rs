use std::io::Cursor;

use async_trait::async_trait;
use runkv_storage::raft_log_store::RaftLogStore;
use serde::{Deserialize, Serialize};

pub type RaftNodeId = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RaftRequest {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RaftResponse {}

openraft::declare_raft_types!(
    pub RaftTypeConfig: D = RaftRequest, R = RaftResponse, NodeId = RaftNodeId
);

pub struct RaftGroupLogStore {
    group: u64,
    core: RaftLogStore,
}

impl RaftGroupLogStore {
    pub fn new(group: u64, core: RaftLogStore) -> Self {
        Self { group, core }
    }
}

#[async_trait]
impl openraft::RaftStorage<RaftTypeConfig> for RaftGroupLogStore {
    type SnapshotData = Cursor<Vec<u8>>;

    type LogReader = Self;

    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        _vote: &openraft::Vote<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> Result<(), openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>
    {
        todo!()
    }

    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<openraft::Vote<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        todo!()
    }

    async fn append_to_log(
        &mut self,
        _entries: &[&openraft::raft::Entry<RaftTypeConfig>],
    ) -> Result<(), openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>
    {
        todo!()
    }

    async fn delete_conflict_logs_since(
        &mut self,
        _log_id: openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> Result<(), openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>
    {
        todo!()
    }

    async fn purge_logs_upto(
        &mut self,
        _log_id: openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> Result<(), openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>
    {
        todo!()
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>,
            openraft::EffectiveMembership<RaftTypeConfig>,
        ),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }

    async fn apply_to_state_machine(
        &mut self,
        _entries: &[&openraft::raft::Entry<RaftTypeConfig>],
    ) -> Result<
        Vec<<RaftTypeConfig as openraft::RaftTypeConfig>::R>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        todo!()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<Self::SnapshotData>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }

    async fn install_snapshot(
        &mut self,
        _meta: &openraft::SnapshotMeta<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        _snapshot: Box<Self::SnapshotData>,
    ) -> Result<
        openraft::StateMachineChanges<RaftTypeConfig>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<RaftTypeConfig, Self::SnapshotData>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }
}

#[async_trait]
impl openraft::RaftLogReader<RaftTypeConfig> for RaftGroupLogStore {
    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::storage::LogState<RaftTypeConfig>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }

    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send + Sync,
    >(
        &mut self,
        _range: RB,
    ) -> Result<
        Vec<openraft::raft::Entry<RaftTypeConfig>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }
}
#[async_trait]
impl openraft::RaftSnapshotBuilder<RaftTypeConfig, Cursor<Vec<u8>>> for RaftGroupLogStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::storage::Snapshot<RaftTypeConfig, Cursor<Vec<u8>>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }
}
