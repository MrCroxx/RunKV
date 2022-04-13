use std::io::Cursor;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use openraft::EffectiveMembership;
use runkv_proto::wheel::KvResponse;
use runkv_storage::raft_log_store::entry::RaftLogBatchBuilder;
use runkv_storage::raft_log_store::RaftLogStore;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::trace;

use super::fsm::KvFsm;
use super::RaftTypeConfig;
use crate::error::{Error, Result};

const VOTE_KEY: &[u8] = b"vote";
const MEMBERSHIP_KEY: &[u8] = b"membership";
const APPLIED_LOG_ID_KEY: &[u8] = b"applied_state";
const SNAPSHOT_META: &[u8] = b"snapshot_meta";
const SNAPSHOT_DATA: &[u8] = b"snapshot_data";
const PRUGE_LOG_ID_KEY: &[u8] = b"purge_log_id";

const DEFAULT_SNAPSHOT_BUFFER_CAPACITY: usize = 64 << 10;

#[derive(Clone)]
pub struct RaftGroupLogStore<F: KvFsm> {
    group: u64,
    core: RaftLogStore,
    fsm: F,
}

impl<F: KvFsm> RaftGroupLogStore<F> {
    pub fn new(group: u64, core: RaftLogStore, fsm: F) -> Self {
        Self { group, core, fsm }
    }
}

pub fn storage_io_error(
    e: Error,
    subject: openraft::ErrorSubject<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    verb: openraft::ErrorVerb,
) -> openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId> {
    openraft::StorageIOError::new(subject, verb, openraft::AnyError::new(&e)).into()
}

impl<F: KvFsm> RaftGroupLogStore<F> {
    async fn apply(&self, entry: &openraft::Entry<RaftTypeConfig>) -> Result<Option<KvResponse>> {
        let resp = match entry.payload {
            openraft::EntryPayload::Blank => None,
            openraft::EntryPayload::Normal(ref request) => Some(self.fsm.apply(request).await?),
            openraft::EntryPayload::Membership(ref membership) => {
                trace!("save membership: {:?}", membership);
                let effective_membership =
                    openraft::EffectiveMembership::new(Some(entry.log_id), membership.to_owned());
                let value = bincode::serialize(&effective_membership).map_err(Error::serde_err)?;
                self.core
                    .put(self.group, MEMBERSHIP_KEY.to_vec(), value)
                    .await
                    .map_err(Error::storage_err)?;
                None
            }
        };
        Ok(resp)
    }
}

#[async_trait]
impl<F: KvFsm> openraft::RaftStorage<RaftTypeConfig> for RaftGroupLogStore<F> {
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
        let vote = match buf {
            None => None,
            Some(buf) => Some(
                bincode::deserialize(&buf)
                    .map_err(Error::serde_err)
                    .map_err(err)?,
            ),
        };
        Ok(vote)
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
        entries: &[&openraft::Entry<RaftTypeConfig>],
    ) -> core::result::Result<
        (),
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write);

        trace!("append to log: {:?}", entries);

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
        let batches = builder.build();
        for batch in batches {
            self.core
                .append(batch)
                .await
                .map_err(Error::storage_err)
                .map_err(err)?;
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
            .mask(self.group, log_id.index + 1)
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
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Store, openraft::ErrorVerb::Read);

        let applied_log_id = self
            .core
            .get(self.group, APPLIED_LOG_ID_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let applied_log_id = match applied_log_id {
            None => None,
            Some(raw) => Some(
                bincode::deserialize(&raw)
                    .map_err(Error::serde_err)
                    .map_err(err)?,
            ),
        };

        let membership = self
            .core
            .get(self.group, MEMBERSHIP_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;
        let membership = match membership {
            Some(raw) => bincode::deserialize(&raw)
                .map_err(Error::serde_err)
                .map_err(err)?,
            None => EffectiveMembership::default(),
        };
        Ok((applied_log_id, membership))
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
        entries: &[&openraft::Entry<RaftTypeConfig>],
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
            let value = bincode::serialize(&entry.log_id)
                .map_err(Error::serde_err)
                .map_err(err)?;
            self.core
                .put(self.group, APPLIED_LOG_ID_KEY.to_vec(), value)
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

        self.fsm
            .install_snapshot(snapshot.as_ref())
            .await
            .map_err(err)?;

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
impl<F: KvFsm> openraft::RaftLogReader<RaftTypeConfig> for RaftGroupLogStore<F> {
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
            Some(purge_log_id) => Some(
                bincode::deserialize(&purge_log_id)
                    .map_err(Error::serde_err)
                    .map_err(err)?,
            ),
        };

        // Get last log id.
        let next_index = self
            .core
            .next_index(self.group, false)
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
        range: RB,
    ) -> core::result::Result<
        Vec<openraft::Entry<RaftTypeConfig>>,
        openraft::StorageError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Store, openraft::ErrorVerb::Read);

        let start = match range.start_bound() {
            std::ops::Bound::Included(v) => *v,
            std::ops::Bound::Excluded(v) => *v + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Excluded(v) => *v,
            std::ops::Bound::Included(v) => *v + 1,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        trace!("try get log entries: {:?} => [{}..{})", range, start, end);

        let raw_entries = self
            .core
            .may_entries(self.group, start, (end - start) as usize, false)
            .await
            .map_err(Error::storage_err)
            .map_err(err)?;

        trace!("raw entries: {:?}", raw_entries);

        let mut entries = Vec::with_capacity(raw_entries.len());
        for raw_entry in raw_entries.into_iter() {
            let payload = bincode::deserialize(&raw_entry.data)
                .map_err(Error::serde_err)
                .map_err(err)?;
            let entry = openraft::Entry {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: raw_entry.term,
                        node_id: (&raw_entry.ctx[..]).get_u64_le(),
                    },
                    index: raw_entry.index,
                },
                payload,
            };
            entries.push(entry);
        }

        trace!("entries: {:?}", entries);

        Ok(entries)
    }
}

#[async_trait]
impl<F: KvFsm> openraft::RaftSnapshotBuilder<RaftTypeConfig, Cursor<Vec<u8>>>
    for RaftGroupLogStore<F>
{
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
        let err = |e| storage_io_error(e, openraft::ErrorSubject::Store, openraft::ErrorVerb::Read);

        let applied_log_id = self
            .core
            .get(self.group, APPLIED_LOG_ID_KEY.to_vec())
            .await
            .map_err(Error::storage_err)
            .map_err(err)?
            .unwrap();
        let applied_log_id: openraft::LogId<u64> = bincode::deserialize(&applied_log_id)
            .map_err(Error::serde_err)
            .map_err(err)?;
        let snapshot_id = format!("snapshot-{}", applied_log_id.index);

        let snapshot_meta = openraft::storage::SnapshotMeta {
            last_log_id: applied_log_id,
            snapshot_id,
        };

        let snapshot_data = self.fsm.build_snapshot().await.map_err(err)?;
        let snapshot = openraft::storage::Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(snapshot_data),
        };

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use openraft::testing::Suite;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use runkv_storage::raft_log_store::store::RaftLogStoreOptions;
    use test_log::test;

    use super::*;
    use crate::components::fsm::MockKvFsm;

    #[test]
    fn test_raft_log_store() {
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_ref = &tempdir;

        Suite::test_all(|| async move {
            let path = Path::new(tempdir_ref.path()).join(
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(16)
                    .map(char::from)
                    .collect::<String>(),
            );
            trace!("create new raft log store in path: {:?}", path);
            tokio::fs::create_dir_all(path.clone()).await.unwrap();
            let options = RaftLogStoreOptions {
                log_dir_path: path.to_str().unwrap().to_string(),
                log_file_capacity: 100,
                block_cache_capacity: 1024,
            };
            let store = RaftLogStore::open(options).await.unwrap();
            store.add_group(1).await.unwrap();
            RaftGroupLogStore::new(1, store, MockKvFsm::default())
        })
        .unwrap();
        drop(tempdir);
    }
}
