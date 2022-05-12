use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use runkv_common::notify_pool::NotifyPool;
use runkv_proto::kv::{
    kv_op_request, kv_op_response, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    KvOpResponse, PutRequest, PutResponse, SnapshotRequest, SnapshotResponse, TxnRequest,
    TxnResponse,
};
use runkv_storage::raft_log_store::error::RaftLogStoreError;
use tracing::error;

use super::command::Command;
use super::lsm_tree::ObjectStoreLsmTree;
use super::raft_log_store::RaftGroupLogStore;
use crate::error::{Error, Result};

#[async_trait]
pub trait Fsm: Send + Sync + Clone + 'static {
    async fn apply(
        &self,
        group: u64,
        is_leader: bool,
        entries: Vec<raft::prelude::Entry>,
    ) -> Result<()>;

    /// Load raft applied index, used for initializing or restarting raft node.
    async fn raft_applied_index(&self) -> Result<u64>;
}

const DONE_INDEX_KEY: &[u8] = b"done_index";
const AVAILABLE_INDEX_KEY: &[u8] = b"available_index";

fn gap(range: Range<u64>) -> Error {
    Error::StorageError(
        RaftLogStoreError::RaftLogGap {
            start: range.start,
            end: range.end,
        }
        .into(),
    )
}

/// Note: Range of `entries` shoule be smaller than `range`.
fn check_log_gap(entries: &[raft::prelude::Entry], range: Range<u64>) -> Result<()> {
    if range.end - range.start == 0 && entries.is_empty() {
        return Ok(());
    }
    if entries.is_empty() {
        return Err(gap(range));
    }
    let first_index = entries.first().unwrap().index;
    if first_index != range.start {
        return Err(gap(range.start..first_index));
    }
    let last_index = entries.last().unwrap().index;
    if last_index + 1 != range.end {
        return Err(gap(last_index + 1..range.end));
    }
    Ok(())
}

pub struct ObjectLsmTreeFsmOptions {
    pub node: u64,
    pub group: u64,
    pub raft_node: u64,

    pub raft_log_store: RaftGroupLogStore,
    pub lsm_tree: ObjectStoreLsmTree,
    pub txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
}

#[derive(Clone)]
pub struct ObjectLsmTreeFsm {
    node: u64,
    group: u64,
    raft_node: u64,

    raft_log_store: RaftGroupLogStore,
    lsm_tree: ObjectStoreLsmTree,
    txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
}

impl std::fmt::Debug for ObjectLsmTreeFsm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectLsmTreeFsm")
            .field("node", &self.node)
            .field("group", &self.group)
            .field("raft_node", &self.raft_node)
            .finish()
    }
}

impl ObjectLsmTreeFsm {
    pub fn new(options: ObjectLsmTreeFsmOptions) -> Self {
        Self {
            node: options.node,
            group: options.group,
            raft_node: options.raft_node,

            raft_log_store: options.raft_log_store,
            lsm_tree: options.lsm_tree,
            txn_notify_pool: options.txn_notify_pool,
        }
    }

    #[tracing::instrument(level = "trace")]
    async fn store_index(&self, key: &[u8], index: u64) -> Result<()> {
        let buf = bincode::serialize(&index).map_err(Error::serde_err)?;
        self.raft_log_store.put(key.to_vec(), buf).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn load_index(&self, key: &[u8]) -> Result<u64> {
        let buf = match self.raft_log_store.get(key.to_vec()).await? {
            None => return Ok(0),
            Some(buf) => buf,
        };
        let index = bincode::deserialize(&buf).map_err(Error::serde_err)?;
        Ok(index)
    }

    async fn apply_entry(&self, entry: raft::prelude::Entry) -> Result<()> {
        match entry.entry_type() {
            raft::prelude::EntryType::EntryNormal => self.apply_normal(entry).await,
            _ => todo!(),
        }
    }

    async fn apply_normal(&self, entry: raft::prelude::Entry) -> Result<()> {
        if entry.data.is_empty() {
            return Ok(());
        }
        let cmds: Vec<Command> = bincode::deserialize(&entry.data).map_err(Error::serde_err)?;
        for cmd in cmds {
            match cmd {
                Command::TxnRequest {
                    request_id,
                    sequence,
                    request,
                } => {
                    #[cfg(feature = "tracing")]
                    {
                        use runkv_common::time::timestamp;

                        use crate::trace::{TRACE_CTX, TRACE_RAFT_LATENCY_HISTOGRAM_VEC};

                        let duration = {
                            let guard = TRACE_CTX.propose_ts.read(&request_id);
                            let ts = guard.get().unwrap();
                            std::time::Duration::from_millis(timestamp() - *ts)
                        };
                        TRACE_RAFT_LATENCY_HISTOGRAM_VEC
                            .with_label_values(&[
                                "apply",
                                &self.node.to_string(),
                                &self.group.to_string(),
                                &self.raft_node.to_string(),
                            ])
                            .observe(duration.as_secs_f64());
                    }

                    let response = self.txn(request, sequence, entry.index).await;
                    if let Err(e) = self.txn_notify_pool.notify(request_id, response) {
                        error!(request_id = request_id, "notify txn result error: {}", e);
                    }
                }
                Command::CompactRaftLog { index, sequence } => {
                    self.compact_raft_log(index, sequence).await?;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn txn(
        &self,
        request: TxnRequest,
        sequence: u64,
        raft_log_index: u64,
    ) -> Result<TxnResponse> {
        let mut ops = Vec::with_capacity(request.ops.len());
        for op in request.ops {
            let op = match op.request.unwrap() {
                kv_op_request::Request::Get(GetRequest { key, sequence: seq }) => {
                    kv_op_response::Response::Get(GetResponse {
                        value: self
                            .get(key, if seq > 0 { seq } else { sequence })
                            .await?
                            .unwrap_or_default(),
                    })
                }
                kv_op_request::Request::Put(PutRequest { key, value }) => {
                    self.put(key, value, raft_log_index, sequence).await?;
                    kv_op_response::Response::Put(PutResponse::default())
                }
                kv_op_request::Request::Delete(DeleteRequest { key }) => {
                    self.delete(key, raft_log_index, sequence).await?;
                    kv_op_response::Response::Delete(DeleteResponse::default())
                }
                kv_op_request::Request::Snapshot(SnapshotRequest { .. }) => {
                    kv_op_response::Response::Snapshot(SnapshotResponse { sequence })
                }
            };
            ops.push(KvOpResponse { response: Some(op) });
        }
        Ok(TxnResponse { ops })
    }

    #[tracing::instrument(level = "trace")]
    async fn get(&self, key: Vec<u8>, sequence: u64) -> Result<Option<Vec<u8>>> {
        let key = Bytes::from(key);
        let result = self.lsm_tree.get(&key, sequence).await?;
        Ok(result.map(|v| v.to_vec()))
    }

    #[tracing::instrument(level = "trace")]
    async fn put(&self, key: Vec<u8>, value: Vec<u8>, index: u64, sequence: u64) -> Result<()> {
        let key = Bytes::from(key);
        let value = Bytes::from(value);
        self.lsm_tree.put(&key, &value, sequence, index).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn delete(&self, key: Vec<u8>, index: u64, sequence: u64) -> Result<()> {
        let key = Bytes::from(key);
        self.lsm_tree.delete(&key, sequence, index).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn compact_raft_log(&self, _compact_index: u64, _sequence: u64) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl Fsm for ObjectLsmTreeFsm {
    #[tracing::instrument(level = "trace")]
    async fn apply(
        &self,
        _group: u64,
        is_leader: bool,
        entries: Vec<raft::prelude::Entry>,
    ) -> Result<()> {
        // Update `available index`.
        let mut available_index = None;
        if let Some(last_entry) = entries.last() {
            self.store_index(AVAILABLE_INDEX_KEY, last_entry.index)
                .await?;
            available_index = Some(last_entry.index);
        }

        // If current `FSM` does not belong to the raft leader, `FSM` won't actually apply entries.
        if !is_leader {
            return Ok(());
        }

        // Get apply progress.
        let avaiable_index = match available_index {
            Some(index) => index,
            None => self.load_index(AVAILABLE_INDEX_KEY).await?,
        };
        let last_done_index = self.load_index(DONE_INDEX_KEY).await?;

        // Entries to apply: [ first apply index ..= last apply index].
        let first_apply_index = last_done_index + 1;
        // TODO: Limit apply the amount of apply entries.
        let last_apply_index = avaiable_index;
        let first_carried_index = entries
            .first()
            .map(|first_entry| first_entry.index)
            .unwrap_or(last_apply_index + 1);

        // Load entries [ first apply index .. first carried index ] from raft log store then apply.
        let load_len = (first_carried_index - first_apply_index) as usize;
        if load_len > 0 {
            let loaded_entries = self
                .raft_log_store
                .entries(first_apply_index, load_len)
                .await?;
            check_log_gap(&entries, first_apply_index..first_carried_index)?;
            for entry in loaded_entries {
                self.apply_entry(entry).await?;
            }
        }

        // Apply carried entries.
        for entry in entries {
            self.apply_entry(entry).await?;
        }

        // Update `done index`.
        let done_index = last_apply_index;
        if last_done_index != done_index {
            self.store_index(DONE_INDEX_KEY, done_index).await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn raft_applied_index(&self) -> Result<u64> {
        self.load_index(AVAILABLE_INDEX_KEY).await
    }
}

#[cfg(test)]
pub mod tests {

    use tokio::sync::mpsc;

    use super::*;

    #[derive(Clone)]
    pub struct MockFsm {
        leader_apply: bool,
        tx: mpsc::UnboundedSender<raft::prelude::Entry>,
    }

    impl MockFsm {
        pub fn new(leader_apply: bool) -> (Self, mpsc::UnboundedReceiver<raft::prelude::Entry>) {
            let (tx, rx) = mpsc::unbounded_channel();
            (Self { leader_apply, tx }, rx)
        }
    }

    #[async_trait]
    impl Fsm for MockFsm {
        async fn apply(
            &self,
            _group: u64,
            is_leader: bool,
            entries: Vec<raft::prelude::Entry>,
        ) -> Result<()> {
            if !self.leader_apply || is_leader {
                for entry in entries {
                    self.tx.send(entry).unwrap()
                }
            }
            Ok(())
        }

        async fn raft_applied_index(&self) -> Result<u64> {
            Ok(0)
        }
    }
}
