// use async_trait::async_trait;
// use openraft::{declare_raft_types, RaftStorage};
// use runkv_storage::RaftStore as RaftStoreCore;
// use serde::{Deserialize, Serialize};

// pub type RaftNodeId = u64;

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub enum RaftRequest {}

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub enum RaftResponse {}

// declare_raft_types!(
//     pub RaftTypeConfig: D = RaftRequest, R = RaftResponse, NodeId = RaftNodeId
// );

// pub struct RaftStore {
//     core: RaftStoreCore,
// }

// #[async_trait]
// impl RaftStorage<RaftTypeConfig> for RaftStore {
//     type SnapshotData;

//     type LogReader;

//     type SnapshotBuilder;

//     async fn save_vote(
//         &mut self,
//         vote: &openraft::Vote<RaftTypeConfig>,
//     ) -> Result<(), openraft::StorageError<RaftTypeConfig>> {
//         todo!()
//     }

//     async fn read_vote(
//         &mut self,
//     ) -> Result<Option<openraft::Vote<RaftTypeConfig>>, openraft::StorageError<RaftTypeConfig>>
//     {
//         todo!()
//     }

//     async fn get_log_reader(&mut self) -> Self::LogReader {
//         todo!()
//     }

//     async fn append_to_log(
//         &mut self,
//         entries: &[&openraft::raft::Entry<RaftTypeConfig>],
//     ) -> Result<(), openraft::StorageError<RaftTypeConfig>> {
//         todo!()
//     }

//     async fn delete_conflict_logs_since(
//         &mut self,
//         log_id: openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
//     ) -> Result<(), openraft::StorageError<RaftTypeConfig>> {
//         todo!()
//     }

//     async fn purge_logs_upto(
//         &mut self,
//         log_id: openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
//     ) -> Result<(), openraft::StorageError<RaftTypeConfig>> {
//         todo!()
//     }

//     async fn last_applied_state(
//         &mut self,
//     ) -> Result<
//         (
//             Option<openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>,
//             Option<openraft::EffectiveMembership<RaftTypeConfig>>,
//         ),
//         openraft::StorageError<RaftTypeConfig>,
//     > {
//         todo!()
//     }

//     async fn apply_to_state_machine(
//         &mut self,
//         entries: &[&openraft::raft::Entry<RaftTypeConfig>],
//     ) -> Result<
//         Vec<<RaftTypeConfig as openraft::RaftTypeConfig>::R>,
//         openraft::StorageError<RaftTypeConfig>,
//     > {
//         todo!()
//     }

//     async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
//         todo!()
//     }

//     async fn begin_receiving_snapshot(
//         &mut self,
//     ) -> Result<Box<Self::SnapshotData>, openraft::StorageError<RaftTypeConfig>> {
//         todo!()
//     }

//     async fn install_snapshot(
//         &mut self,
//         meta: &openraft::SnapshotMeta<RaftTypeConfig>,
//         snapshot: Box<Self::SnapshotData>,
//     ) -> Result<openraft::StateMachineChanges<RaftTypeConfig>,
// openraft::StorageError<RaftTypeConfig>>     {
//         todo!()
//     }

//     async fn get_current_snapshot(
//         &mut self,
//     ) -> Result<
//         Option<openraft::storage::Snapshot<RaftTypeConfig, Self::SnapshotData>>,
//         openraft::StorageError<RaftTypeConfig>,
//     > {
//         todo!()
//     }
// }
