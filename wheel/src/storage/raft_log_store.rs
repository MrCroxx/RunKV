use std::io::Cursor;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use runkv_storage::raft_log_store::entry::RaftLogBatchBuilder;
use runkv_storage::raft_log_store::RaftLogStore;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::{Error, Result};

const VOTE_KEY: &[u8] = b"vote";
const MEMBERSHIP_KEY: &[u8] = b"membership";
const APPLIED_STATE_KEY: &[u8] = b"applied_state";
const SNAPSHOT_META: &[u8] = b"snapshot_meta";
const SNAPSHOT_DATA: &[u8] = b"snapshot_data";
const PRUGE_LOG_ID_KEY: &[u8] = b"purge_log_id";

const DEFAULT_SNAPSHOT_BUFFER_CAPACITY: usize = 64 << 10;

pub type RaftNodeId = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RaftRequest {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RaftResponse {}

openraft::declare_raft_types!(
    pub RaftTypeConfig: D = RaftRequest, R = RaftResponse, NodeId = RaftNodeId
);

#[derive(Clone)]
pub struct RaftGroupLogStore {
    group: u64,
    core: RaftLogStore,
}

impl RaftGroupLogStore {
    pub fn new(group: u64, core: RaftLogStore) -> Self {
        Self { group, core }
    }
}

pub fn storage_io_error(
    e: Error,
    subject: openraft::ErrorSubject<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    verb: openraft::ErrorVerb,
) -> openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId> {
    openraft::StorageIOError::new(subject, verb, openraft::AnyError::new(&e)).into()
}

impl RaftGroupLogStore {
    async fn apply(&self, entry: &openraft::raft::Entry<RaftTypeConfig>) -> Result<RaftResponse> {
        let resp = match entry.payload {
            openraft::raft::EntryPayload::Blank => RaftResponse {},
            openraft::raft::EntryPayload::Normal(ref _request) => {
                // TODO: impl me!!
                // TODO: impl me!!
                // TODO: impl me!!
                RaftResponse {}
            }
            openraft::raft::EntryPayload::Membership(ref membership) => {
                let effective_membership =
                    openraft::EffectiveMembership::new(Some(entry.log_id), membership.to_owned());
                let value = bincode::serialize(&effective_membership).map_err(Error::serde_err)?;
                self.core
                    .put(self.group, MEMBERSHIP_KEY.to_vec(), value)
                    .await
                    .map_err(Error::storage_err)?;
                RaftResponse {}
            }
        };
        Ok(resp)
    }
}

#[async_trait]
impl openraft::RaftStorage<RaftTypeConfig> for RaftGroupLogStore {
    type SnapshotData = Cursor<Vec<u8>>;

    type LogReader = Self;

    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> core::result::Result<
        (),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write);
        let value = bincode::serialize(&vote)
            .map_err(Error::serde_err)
            .map_err(err)?;
        self.core
            .put(self.group, VOTE_KEY.to_vec(), value)
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> core::result::Result<
        Option<openraft::Vote<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Vote, openraft::ErrorVerb::Read);
        let buf = self
            .core
            .get(self.group, VOTE_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        match buf {
            Some(buf) => bincode::deserialize(&buf)
                .map_err(Error::serde_err)
                .map_err(err),
            None => Ok(None),
        }
    }

    /// Get the log reader.
    ///
    /// The method is intentionally async to give the implementation a chance to use asynchronous
    /// sync primitives to serialize access to the common internal object, if needed.
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Append a payload of entries to the log.
    ///
    /// Though the entries will always be presented in order, each entry's index should be used to
    /// determine its location to be written in the log.
    async fn append_to_log(
        &mut self,
        entries: &[&openraft::raft::Entry<RaftTypeConfig>],
    ) -> core::result::Result<
        (),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write);

        let mut builder = RaftLogBatchBuilder::default();
        for entry in entries.iter() {
            let data = bincode::serialize(&entry.payload)
                .map_err(Error::serde_err)
                .map_err(err)?;
            let mut ctx = Vec::with_capacity(8);
            ctx.put_u64_le(entry.log_id.leader_id.node_id);
            builder.add(
                self.group,
                entry.log_id.leader_id.term,
                entry.log_id.index,
                &ctx,
                &data,
            );
        }
        Ok(())
    }

    /// Delete conflict log entries since `log_id`, inclusive.
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> core::result::Result<
        (),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e, id| {
            storage_io_error(
                e,
                openraft::ErrorSubject::Log(id),
                openraft::ErrorVerb::Write,
            )
        };

        self.core
            .truncate(self.group, log_id.index)
            .await
            .map_err(Error::storage_err)
            .map_err(|e| err(e, log_id))?;
        Ok(())
    }

    /// Delete applied log entries upto `log_id`, inclusive.
    async fn purge_logs_upto(
        &mut self,
        log_id: openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> core::result::Result<
        (),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e, id| {
            storage_io_error(
                e,
                openraft::ErrorSubject::Log(id),
                openraft::ErrorVerb::Write,
            )
        };

        // Record purge log id.
        let value = bincode::serialize(&log_id)
            .map_err(Error::serde_err)
            .map_err(|e| err(e, log_id))?;
        self.core
            .put(self.group, PRUGE_LOG_ID_KEY.to_vec(), value)
            .await
            .map_err(Error::storage_err)
            .map_err(|e| err(e, log_id))?;

        // Perform log compact.
        self.core
            .compact(self.group, log_id.index + 1)
            .await
            .map_err(Error::storage_err)
            .map_err(|e| err(e, log_id))?;

        Ok(())
    }

    /// Returns the last applied log id which is recorded in state machine, and the last applied
    /// membership log id and membership config.
    // NOTE: This can be made into sync, provided all state machines will use atomic read or the
    // like.
    async fn last_applied_state(
        &mut self,
    ) -> core::result::Result<
        (
            Option<openraft::LogId<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>>,
            openraft::EffectiveMembership<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        ),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err =
            |e| storage_io_error(e, openraft::ErrorSubject::Store, openraft::ErrorVerb::Write);

        let applied_state = self
            .core
            .get(self.group, APPLIED_STATE_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let applied_state = match applied_state {
            None => None,
            Some(raw) => bincode::deserialize(&raw)
                .map_err(Error::serde_err)
                .map_err(err)?,
        };

        let membership = self
            .core
            .get(self.group, MEMBERSHIP_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let membership = membership.ok_or_else(|| {
            err(Error::storage_err(runkv_storage::Error::Other(format!(
                "membership not found in raft log store, group: {}",
                self.group
            ))))
        })?;
        let membership = bincode::deserialize(&membership)
            .map_err(Error::serde_err)
            .map_err(err)?;
        Ok((applied_state, membership))
    }

    /// Apply the given payload of entries to the state machine.
    ///
    /// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
    /// have been replicated to a quorum of the cluster, will be applied to the state machine.
    ///
    /// This is where the business logic of interacting with your application's state machine
    /// should live. This is 100% application specific. Perhaps this is where an application
    /// specific transaction is being started, or perhaps committed. This may be where a key/value
    /// is being stored.
    ///
    /// An impl should do:
    /// - Store the last applied log id.
    /// - Deal with the EntryPayload::Normal() log, which is business logic log.
    /// - Deal with EntryPayload::Membership, store the membership config.
    // TODO The reply should happen asynchronously, somehow. Make this method synchronous and
    // instead of using the result, pass a channel where to post the completion. The Raft core can
    // then collect completions on this channel and update the client with the result once all
    // the preceding operations have been applied to the state machine. This way we'll reach
    // operation pipelining w/o the need to wait for the completion of each operation inline.
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&openraft::raft::Entry<RaftTypeConfig>],
    ) -> core::result::Result<
        Vec<<RaftTypeConfig as openraft::RaftTypeConfig>::R>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write);

        let mut resps = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            let resp = self.apply(entry).await.map_err(err)?;
            resps.push(resp);
        }
        if let Some(&entry) = entries.last() {
            let value = bincode::serialize(entry)
                .map_err(Error::serde_err)
                .map_err(err)?;
            self.core
                .put(self.group, APPLIED_STATE_KEY.to_vec(), value)
                .await
                .map_err(Error::storage_err)
                .map_err(err)?;
        }
        Ok(resps)
    }

    /// Get the snapshot builder for the state machine.
    ///
    /// The method is intentionally async to give the implementation a chance to use asynchronous
    /// sync primitives to serialize access to the common internal object, if needed.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Create a new blank snapshot, returning a writable handle to the snapshot object.
    ///
    /// Raft will use this handle to receive snapshot data.
    ///
    /// ### implementation guide
    /// See the [storage chapter of the guide](https://datafuselabs.github.io/openraft/storage.html)
    /// for details on log compaction / snapshotting.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> core::result::Result<
        Box<Self::SnapshotData>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    /// Install a snapshot which has finished streaming from the cluster leader.
    ///
    /// All other snapshots should be deleted at this point.
    ///
    /// ### snapshot
    /// A snapshot created from an earlier call to `begin_receiving_snapshot` which provided the
    /// snapshot.
    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        mut snapshot: Box<Self::SnapshotData>,
    ) -> core::result::Result<
        openraft::StateMachineChanges<RaftTypeConfig>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| {
            storage_io_error(
                e,
                openraft::ErrorSubject::Snapshot(meta.to_owned()),
                openraft::ErrorVerb::Write,
            )
        };

        // TODO: impl me!!
        // TODO: impl me!!
        // TODO: impl me!!

        let buf_meta = bincode::serialize(meta)
            .map_err(Error::serde_err)
            .map_err(err)?;
        self.core
            .put(self.group, SNAPSHOT_META.to_vec(), buf_meta)
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;

        let mut buf_data = Vec::with_capacity(DEFAULT_SNAPSHOT_BUFFER_CAPACITY);
        snapshot
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(Error::err)
            .map_err(err)?;
        snapshot
            .read_to_end(&mut buf_data)
            .await
            .map_err(Error::err)
            .map_err(err)?;
        self.core
            .put(self.group, SNAPSHOT_DATA.to_vec(), buf_data)
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;

        Ok(openraft::StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    /// Get a readable handle to the current snapshot, along with its metadata.
    ///
    /// ### implementation algorithm
    /// Implementing this method should be straightforward. Check the configured snapshot
    /// directory for any snapshot files. A proper implementation will only ever have one
    /// active snapshot, though another may exist while it is being created. As such, it is
    /// recommended to use a file naming pattern which will allow for easily distinguishing between
    /// the current live snapshot, and any new snapshot which is being created.
    ///
    /// A proper snapshot implementation will store the term, index and membership config as part
    /// of the snapshot, which should be decoded for creating this method's response data.
    async fn get_current_snapshot(
        &mut self,
    ) -> core::result::Result<
        Option<openraft::storage::Snapshot<RaftTypeConfig, Self::SnapshotData>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err =
            |e| storage_io_error(e, openraft::ErrorSubject::Store, openraft::ErrorVerb::Write);

        let meta = self
            .core
            .get(self.group, SNAPSHOT_META.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;

        let meta = match meta {
            None => return Ok(None),
            Some(meta) => bincode::deserialize(&meta)
                .map_err(Error::serde_err)
                .map_err(err)?,
        };

        let data = self
            .core
            .get(self.group, SNAPSHOT_DATA.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let data = data.ok_or_else(|| {
            err(Error::storage_err(runkv_storage::Error::Other(format!(
                "snapshot data not found in raft log store, group: {}",
                self.group
            ))))
        })?;

        Ok(Some(openraft::storage::Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

#[async_trait]
impl openraft::RaftLogReader<RaftTypeConfig> for RaftGroupLogStore {
    /// Returns the last deleted log id and the last log id.
    ///
    /// The impl should not consider the applied log id in state machine.
    /// The returned `last_log_id` could be the log id of the last present log entry, or the
    /// `last_purged_log_id` if there is no entry at all.
    // NOTE: This can be made into sync, provided all state machines will use atomic read or the
    // like.
    async fn get_log_state(
        &mut self,
    ) -> core::result::Result<
        openraft::storage::LogState<RaftTypeConfig>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Store, openraft::ErrorVerb::Read);

        // Get purge log id.
        let purge_log_id = self
            .core
            .get(self.group, PRUGE_LOG_ID_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let purge_log_id = match purge_log_id {
            None => None,
            Some(purge_log_id) => bincode::deserialize(&purge_log_id)
                .map_err(Error::serde_err)
                .map_err(err)?,
        };

        // Get last log id.
        let next_index = self
            .core
            .next_index(self.group)
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let last_log_id = match next_index {
            Ok(next_index) => {
                let last_index = next_index - 1;
                let term = self
                    .core
                    .term(self.group, last_index)
                    .await
                    .map_err(Error::storage_err)
                    .map_err(err)?
                    .unwrap();
                let ctx = self
                    .core
                    .ctx(self.group, last_index)
                    .await
                    .map_err(Error::storage_err)
                    .map_err(err)?
                    .unwrap();
                let node_id = (&ctx[..]).get_u64_le();
                Some(openraft::LogId {
                    leader_id: openraft::LeaderId::<u64> { term, node_id },
                    index: last_index,
                })
            }
            Err(_) => purge_log_id,
        };

        Ok(openraft::storage::LogState {
            last_purged_log_id: purge_log_id,
            last_log_id,
        })
    }

    /// Get a series of log entries from storage.
    ///
    /// The start value is inclusive in the search and the stop value is non-inclusive: `[start,
    /// stop)`.
    ///
    /// Entry that is not found is allowed.
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send + Sync,
    >(
        &mut self,
        _range: RB,
    ) -> core::result::Result<
        Vec<openraft::raft::Entry<RaftTypeConfig>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }
}
#[async_trait]
impl openraft::RaftSnapshotBuilder<RaftTypeConfig, Cursor<Vec<u8>>> for RaftGroupLogStore {
    /// Build snapshot
    ///
    /// A snapshot has to contain information about exactly all logs up to the last applied.
    ///
    /// Building snapshot can be done by:
    /// - Performing log compaction, e.g. merge log entries that operates on the same key, like a
    ///   LSM-tree does,
    /// - or by fetching a snapshot from the state machine.
    async fn build_snapshot(
        &mut self,
    ) -> core::result::Result<
        openraft::storage::Snapshot<RaftTypeConfig, Cursor<Vec<u8>>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        todo!()
    }
}
