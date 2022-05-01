use std::time::Duration;

use async_trait::async_trait;
use runkv_common::Worker;
use runkv_storage::raft_log_store_v2::entry::RaftLogBatchBuilder;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::components::fsm_v2::Fsm;
use crate::components::raft_log_store_v2::{encode_entry_data, RaftGroupLogStore};
use crate::components::raft_network::RaftNetwork;
use crate::error::{Error, Result};

const RAFT_HEARTBEAT_TICK_DURATION: Duration = Duration::from_millis(100);

#[derive(Serialize, Deserialize, Debug)]
pub struct Proposal {
    pub data: Vec<u8>,
    pub context: Vec<u8>,
}

pub enum RaftStartMode {
    Initialize { peers: Vec<u64> },
    Restart,
}

pub struct RaftWorkerOptions<RN: RaftNetwork, F: Fsm> {
    pub group: u64,
    pub node: u64,
    pub raft_node: u64,

    pub raft_start_mode: RaftStartMode,
    pub raft_log_store: RaftGroupLogStore,
    pub raft_logger: slog::Logger,
    pub raft_network: RN,

    pub proposal_rx: mpsc::UnboundedReceiver<Proposal>,

    pub fsm: F,
}

pub struct RaftWorker<RN: RaftNetwork, F: Fsm> {
    group: u64,
    node: u64,
    raft_node: u64,

    raft: raft::RawNode<RaftGroupLogStore>,
    raft_log_store: RaftGroupLogStore,
    raft_network: RN,
    raft_soft_state: Option<raft::SoftState>,

    message_rx: mpsc::UnboundedReceiver<raft::prelude::Message>,
    proposal_rx: mpsc::UnboundedReceiver<Proposal>,

    fsm: F,
}

impl<RN: RaftNetwork, F: Fsm> std::fmt::Debug for RaftWorker<RN, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftWorker")
            .field("node", &self.node)
            .field("group", &self.group)
            .field("raft_node", &self.raft_node)
            .finish()
    }
}

#[async_trait]
impl<RN: RaftNetwork, F: Fsm> Worker for RaftWorker<RN, F> {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => return Ok(()),
                Err(e) => warn!("error occur when raft worker running: {}", e),
            }
        }
    }
}

impl<RN: RaftNetwork, F: Fsm> RaftWorker<RN, F> {
    pub async fn build(options: RaftWorkerOptions<RN, F>) -> Result<Self> {
        let applied = match options.raft_start_mode {
            RaftStartMode::Initialize { .. } => 0,
            RaftStartMode::Restart => options.fsm.raft_applied_index().await?,
        };

        let raft_config = raft::Config {
            id: options.raft_node,
            // election_tick: todo!(),
            // heartbeat_tick: todo!(),
            applied,
            // max_size_per_msg: todo!(),
            // max_inflight_msgs: todo!(),
            check_quorum: true,
            pre_vote: true,
            // min_election_tick: todo!(),
            // max_election_tick: todo!(),
            read_only_option: raft::ReadOnlyOption::Safe,
            // skip_bcast_commit: todo!(),
            batch_append: true,
            // priority: todo!(),
            // max_uncommitted_size: todo!(),
            // max_committed_size_per_ready: todo!(),
            ..Default::default()
        };
        raft_config.validate().map_err(Error::err)?;

        let raft_log_store = options.raft_log_store.clone();

        if let RaftStartMode::Initialize { peers } = options.raft_start_mode {
            let cs = raft::prelude::ConfState {
                voters: peers,
                ..Default::default()
            };
            raft_log_store.put_conf_state(&cs).await.unwrap();
        };

        let raft =
            raft::RawNode::new(&raft_config, raft_log_store.clone(), &options.raft_logger).await?;

        let message_rx = options
            .raft_network
            .take_message_rx(options.raft_node)
            .await?;

        Ok(Self {
            group: options.group,
            node: options.node,
            raft_node: options.raft_node,

            raft,
            raft_log_store,
            raft_network: options.raft_network,
            raft_soft_state: None,

            fsm: options.fsm,

            proposal_rx: options.proposal_rx,
            message_rx,
        })
    }

    async fn run_inner(&mut self) -> Result<()> {
        // [`Interval`] with default [`MissedTickBehavior::Brust`].
        let mut ticker = tokio::time::interval(RAFT_HEARTBEAT_TICK_DURATION);

        loop {
            tokio::select! {
                biased;

                _ = ticker.tick() => {
                    self.tick().await;
                }

                Some(msg) = self.message_rx.recv() => {
                    self.step(msg).await?;
                }

                Some(proposal) = self.proposal_rx.recv() => {
                    self.propose(proposal).await?;
                }

                true = self.raft.has_ready() => {
                    self.handle_ready().await?;
                }

            }
        }
    }

    #[tracing::instrument(level = "trace")]
    async fn tick(&mut self) {
        self.raft.tick().await;
    }

    #[tracing::instrument(level = "trace")]
    async fn propose(&mut self, proposal: Proposal) -> Result<()> {
        self.raft
            .propose(proposal.context, proposal.data)
            .await
            .map_err(Error::RaftV2Error)
    }

    #[tracing::instrument(level = "trace")]
    async fn step(&mut self, msg: raft::prelude::Message) -> Result<()> {
        self.raft.step(msg).await.map_err(Error::RaftV2Error)
    }

    #[tracing::instrument(level = "trace")]
    async fn handle_ready(&mut self) -> Result<()> {
        let mut ready = self.raft.ready().await;

        // 0. Update soft state.
        if let Some(ss) = ready.ss() {
            self.raft_soft_state = Some(raft::SoftState {
                leader_id: ss.leader_id,
                raft_state: ss.raft_state,
            });
        }

        // 1. Send messages.
        self.send_messages(ready.take_messages()).await?;

        // 2. Apply snapshot if there is one.
        if !ready.snapshot().is_empty() {
            self.apply_snapshot(ready.snapshot()).await?;
        }

        // 3. Apply committed logs.
        self.apply_log_entries(ready.take_committed_entries())
            .await?;

        // 4. Append entries to log store.
        self.append_log_entries(ready.take_entries()).await?;

        // 5. Store `HardState` if needed.
        if let Some(hs) = ready.hs() {
            self.store_hard_state(hs).await?;
        }

        // 6. Send messages after persisting hard state.
        self.send_messages(ready.take_persisted_messages()).await?;

        // 7. Advance raft node and get `LightReady`.
        let mut ready = self.raft.advance(ready).await;

        // 8. Send messages of light ready.
        self.send_messages(ready.take_messages()).await?;

        // 9. Apply committed logs of light ready.
        self.apply_log_entries(ready.take_committed_entries())
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn send_messages(&mut self, messages: Vec<raft::prelude::Message>) -> Result<()> {
        self.raft_network.send(messages).await
    }

    #[tracing::instrument(level = "trace")]
    async fn apply_snapshot(&mut self, snapshot: &raft::prelude::Snapshot) -> Result<()> {
        // Impl me!!!
        // Impl me!!!
        // Impl me!!!
        todo!()
    }

    #[tracing::instrument(level = "trace")]
    async fn apply_log_entries(&mut self, entries: Vec<raft::prelude::Entry>) -> Result<()> {
        let is_leader = match &self.raft_soft_state {
            None => false,
            Some(ss) => ss.raft_state == raft::StateRole::Leader,
        };
        self.fsm.apply(self.group, is_leader, entries).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn append_log_entries(&mut self, entries: Vec<raft::prelude::Entry>) -> Result<()> {
        let mut builder = RaftLogBatchBuilder::default();
        for entry in entries {
            let data = encode_entry_data(&entry);
            builder.add(
                self.raft_node,
                entry.term,
                entry.index,
                &entry.context,
                &data,
            );
        }
        let batches = builder.build();
        trace!(
            "raft::append_log_entries generated {} batches: {:?}",
            batches.len(),
            batches
        );
        for batch in batches {
            self.raft_log_store.append(batch).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    async fn store_hard_state(&mut self, hs: &raft::prelude::HardState) -> Result<()> {
        self.raft_log_store.put_hard_state(hs).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use assert_matches::assert_matches;
    use runkv_common::tracing_slog_drain::TracingSlogDrain;
    use runkv_common::Worker;
    use runkv_storage::raft_log_store_v2::store::RaftLogStoreOptions;
    use runkv_storage::raft_log_store_v2::RaftLogStore;
    use test_log::test;

    use super::*;
    use crate::components::fsm_v2::tests::MockFsm;
    use crate::components::raft_network::tests::MockRaftNetwork;

    #[test(tokio::test)]
    async fn test_raft_basic() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap();
        let raft_logger = build_raft_logger();
        let raft_log_store = build_raft_log_store(path).await;
        raft_log_store.add_group(1).await.unwrap();
        raft_log_store.add_group(2).await.unwrap();
        raft_log_store.add_group(3).await.unwrap();
        let raft_network = MockRaftNetwork::default();
        raft_network
            .register(100, BTreeMap::from_iter([(1, 10), (2, 10), (3, 10)]))
            .await
            .unwrap();

        macro_rules! worker {
            ($id:expr) => {
                build_raft_worker(
                    100,
                    10,
                    $id,
                    vec![1, 2, 3],
                    RaftGroupLogStore::new($id, raft_log_store.clone()),
                    raft_logger.clone(),
                    raft_network.clone(),
                )
                .await
            };
        }

        let (proposal_tx_1, mut apply_rx_1) = worker!(1);
        let (_proposal_tx_2, mut apply_rx_2) = worker!(2);
        let (_proposal_tx_3, mut apply_rx_3) = worker!(3);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let data = vec![b'd'; 16];
        let context = vec![b'c'; 16];

        proposal_tx_1
            .send(Proposal {
                data: data.clone(),
                context: context.clone(),
            })
            .unwrap();

        loop {
            let entry = tokio::select! {
                entry = apply_rx_1.recv() => entry,
                entry = apply_rx_2.recv() => entry,
                entry = apply_rx_3.recv() => entry,
            };
            let entry = entry.unwrap();
            if entry.entry_type() != raft::prelude::EntryType::EntryNormal || entry.data.is_empty()
            {
                continue;
            }
            assert_matches!(entry, raft::prelude::Entry {
                data: edata,
                context: econtext,
                ..
            } => {
                assert_eq!(edata, data);
                assert_eq!(econtext, context);
            });
            break;
        }
    }

    fn build_raft_logger() -> slog::Logger {
        slog::Logger::root(TracingSlogDrain, slog::o!("namespace" => "raft"))
    }

    async fn build_raft_log_store(path: &str) -> RaftLogStore {
        let options = RaftLogStoreOptions {
            log_dir_path: path.to_string(),
            log_file_capacity: 64 << 20,
            block_cache_capacity: 64 << 20,
        };
        RaftLogStore::open(options).await.unwrap()
    }

    async fn build_raft_worker<RN: RaftNetwork>(
        group: u64,
        node: u64,
        raft_node: u64,
        peers: Vec<u64>,
        raft_log_store: RaftGroupLogStore,
        raft_logger: slog::Logger,
        raft_network: RN,
    ) -> (
        mpsc::UnboundedSender<Proposal>,
        mpsc::UnboundedReceiver<raft::prelude::Entry>,
    ) {
        let (proposal_tx, proposal_rx) = mpsc::unbounded_channel();
        let (fsm, apply_rx) = MockFsm::new(true);
        let options = RaftWorkerOptions {
            group,
            node,
            raft_node,
            raft_start_mode: RaftStartMode::Initialize { peers },
            raft_log_store,
            raft_logger,
            raft_network,

            proposal_rx,

            fsm,
        };
        let mut worker = RaftWorker::build(options).await.unwrap();
        let _handle = tokio::spawn(async move { worker.run().await });
        (proposal_tx, apply_rx)
    }
}
