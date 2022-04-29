use std::time::Duration;

use async_trait::async_trait;
use runkv_common::Worker;
use runkv_storage::raft_log_store_v2::entry::RaftLogBatchBuilder;
use runkv_storage::raft_log_store_v2::RaftLogStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{trace, warn};

use crate::components::command::GearCommand;
use crate::components::raft_log_store_v2::{encode_entry_data, RaftGroupLogStore};
use crate::components::raft_network::RaftNetwork;
use crate::error::{Error, Result};

const RAFT_HEARTBEAT_TICK_DURATION: Duration = Duration::from_millis(100);

#[derive(Serialize, Deserialize, Debug)]
pub struct Proposal {
    data: Vec<u8>,
    context: Vec<u8>,
}

pub enum RaftStartMode {
    Initialize { peers: Vec<u64> },
    Restart { applied: u64 },
}

impl RaftStartMode {
    fn applied(&self) -> u64 {
        match self {
            Self::Initialize { .. } => 0,
            Self::Restart { applied } => *applied,
        }
    }
}

pub struct RaftWorkerOptions<RN: RaftNetwork> {
    pub group: u64,
    pub node: u64,
    pub raft_node: u64,

    pub raft_start_mode: RaftStartMode,
    pub raft_log_store: RaftLogStore,
    pub raft_logger: slog::Logger,
    pub raft_network: RN,

    pub proposal_rx: mpsc::UnboundedReceiver<Proposal>,
    pub apply_tx: mpsc::UnboundedSender<GearCommand>,
}

pub struct RaftWorker<RN: RaftNetwork> {
    group: u64,
    _node: u64,
    _raft_node: u64,

    raft: raft::RawNode<RaftGroupLogStore>,
    raft_log_store: RaftGroupLogStore,
    raft_network: RN,

    message_rx: mpsc::UnboundedReceiver<raft::prelude::Message>,
    proposal_rx: mpsc::UnboundedReceiver<Proposal>,
    apply_tx: mpsc::UnboundedSender<GearCommand>,
}

#[async_trait]
impl<RN: RaftNetwork> Worker for RaftWorker<RN> {
    async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Gracefully kill.
        loop {
            match self.run_inner().await {
                Ok(_) => return Ok(()),
                Err(e) => warn!("error occur when uploader running: {}", e),
            }
        }
    }
}

impl<RN: RaftNetwork> RaftWorker<RN> {
    pub async fn build(options: RaftWorkerOptions<RN>) -> Self {
        let raft_config = raft::Config {
            id: options.raft_node,
            // election_tick: todo!(),
            // heartbeat_tick: todo!(),
            applied: options.raft_start_mode.applied(),
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
        raft_config.validate().unwrap();

        let raft_log_store = RaftGroupLogStore::new(options.group, options.raft_log_store.clone());

        if let RaftStartMode::Initialize { peers } = options.raft_start_mode {
            let cs = raft::prelude::ConfState {
                voters: peers,
                ..Default::default()
            };
            raft_log_store.put_conf_state(&cs).await.unwrap();
        };

        let logger = options.raft_logger.new(slog::o!("group" => options.group));
        let raft = raft::RawNode::new(&raft_config, raft_log_store.clone(), &logger)
            .await
            .unwrap();

        let message_rx = options
            .raft_network
            .take_message_rx(options.raft_node)
            .await
            .unwrap();

        Self {
            group: options.group,
            _node: options.node,
            _raft_node: options.raft_node,

            raft,
            raft_log_store,
            raft_network: options.raft_network,

            proposal_rx: options.proposal_rx,
            message_rx,
            apply_tx: options.apply_tx,
        }
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn tick(&mut self) {
        self.raft.tick().await;
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn propose(&mut self, proposal: Proposal) -> Result<()> {
        self.raft
            .propose(proposal.context, proposal.data)
            .await
            .map_err(Error::RaftV2Error)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn step(&mut self, msg: raft::prelude::Message) -> Result<()> {
        self.raft.step(msg).await.map_err(Error::RaftV2Error)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_ready(&mut self) -> Result<()> {
        let mut ready = self.raft.ready().await;

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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn send_messages(&mut self, messages: Vec<raft::prelude::Message>) -> Result<()> {
        self.raft_network.send(messages).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn apply_snapshot(&mut self, snapshot: &raft::prelude::Snapshot) -> Result<()> {
        // Impl me!!!
        // Impl me!!!
        // Impl me!!!
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn apply_log_entries(&mut self, entries: Vec<raft::prelude::Entry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.apply_tx
            .send(GearCommand::Apply {
                group: self.group,
                range: entries.first().unwrap().index..entries.last().unwrap().index + 1,
            })
            .map_err(Error::err)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn append_log_entries(&mut self, entries: Vec<raft::prelude::Entry>) -> Result<()> {
        let mut builder = RaftLogBatchBuilder::default();
        for entry in entries {
            let data = encode_entry_data(&entry);
            builder.add(self.group, entry.term, entry.index, &entry.context, &data);
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

    #[tracing::instrument(level = "trace", skip(self))]
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
    use test_log::test;

    use super::*;
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
            .register(1, BTreeMap::from_iter([(1, 1), (2, 1), (3, 1)]))
            .await
            .unwrap();
        macro_rules! worker {
            ($id:expr) => {
                build_raft_worker(
                    1,
                    1,
                    $id,
                    vec![1, 2, 3],
                    raft_log_store.clone(),
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

        proposal_tx_1
            .send(Proposal {
                data: vec![b'x'; 16],
                context: vec![],
            })
            .unwrap();

        let cmd = tokio::select! {
            cmd = apply_rx_1.recv() => cmd,
            cmd = apply_rx_2.recv() => cmd,
            cmd = apply_rx_3.recv() => cmd,
        };

        assert_matches!(cmd, Some(GearCommand::Apply { group: 1, .. }));
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
        raft_log_store: RaftLogStore,
        raft_logger: slog::Logger,
        raft_network: RN,
    ) -> (
        mpsc::UnboundedSender<Proposal>,
        mpsc::UnboundedReceiver<GearCommand>,
    ) {
        let (proposal_tx, proposal_rx) = mpsc::unbounded_channel();
        let (apply_tx, apply_rx) = mpsc::unbounded_channel();
        let options = RaftWorkerOptions {
            group,
            node,
            raft_node,
            raft_start_mode: RaftStartMode::Initialize { peers },
            raft_log_store,
            raft_logger,
            raft_network,

            proposal_rx,
            apply_tx,
        };
        let mut worker = RaftWorker::build(options).await;
        let _handle = tokio::spawn(async move { worker.run().await });
        (proposal_tx, apply_rx)
    }
}
