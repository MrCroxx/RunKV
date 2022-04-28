use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use runkv_common::Worker;
use runkv_proto::wheel::raft_service_client::RaftServiceClient;
use runkv_proto::wheel::RaftRequest;
use runkv_storage::raft_log_store::entry::RaftLogBatchBuilder;
use runkv_storage::raft_log_store::RaftLogStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Request;
use tracing::{trace, warn};

use crate::components::command::GearCommand;
use crate::components::raft_log_store_v2::{encode_entry_data, RaftGroupLogStore};
use crate::components::raft_network::RaftNetwork;
use crate::error::{Error, Result};

const RAFT_HEARTBEAT_TICK_DURATION: Duration = Duration::from_millis(100);

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftMessage {
    Proposal { data: Vec<u8>, context: Vec<u8> },
    Raft(Box<raft::prelude::Message>),
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

pub struct RaftWorkerOptions {
    pub group: u64,
    pub node: u64,
    pub raft_node: u64,

    pub raft_start_mode: RaftStartMode,
    pub raft_log_store: RaftLogStore,
    pub raft_logger: slog::Logger,
    pub raft_network: RaftNetwork,

    pub message_rx: mpsc::UnboundedReceiver<RaftMessage>,
    pub apply_tx: mpsc::UnboundedSender<GearCommand>,
}

pub struct RaftWorker {
    group: u64,
    _node: u64,
    _raft_node: u64,

    raft: raft::RawNode<RaftGroupLogStore>,
    raft_log_store: RaftGroupLogStore,
    raft_network: RaftNetwork,
    raft_clients: BTreeMap<u64, RaftServiceClient<Channel>>,

    message_rx: mpsc::UnboundedReceiver<RaftMessage>,
    apply_tx: mpsc::UnboundedSender<GearCommand>,
}

#[async_trait]
impl Worker for RaftWorker {
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

impl RaftWorker {
    pub async fn build(options: RaftWorkerOptions) -> Self {
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

        // let drain = tracing_slog::TracingSlogDrain;
        // let drain = slog_async::Async::new(drain).build().fuse();
        // let logger = slog::Logger::root(drain, slog::o!("namespace" => "raft"));

        let logger = options.raft_logger.new(slog::o!("group" => options.group));
        let raft = raft::RawNode::new(&raft_config, raft_log_store.clone(), &logger)
            .await
            .unwrap();

        Self {
            group: options.group,
            _node: options.node,
            _raft_node: options.raft_node,

            raft,
            raft_log_store,
            raft_network: options.raft_network,
            raft_clients: BTreeMap::default(),

            message_rx: options.message_rx,
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
                    self.raft.tick().await;
                }

                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        RaftMessage::Proposal{ data, context } => {
                            self.propose(data, context).await?;
                        }
                        RaftMessage::Raft(rm) => {
                            self.step(*rm).await?;
                        }
                    }
                }

                true = self.raft.has_ready() => {
                    self.handle_ready().await?;
                }

            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn propose(&mut self, data: Vec<u8>, context: Vec<u8>) -> Result<()> {
        self.raft
            .propose(context, data)
            .await
            .map_err(Error::raft_v2_err)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn step(&mut self, msg: raft::prelude::Message) -> Result<()> {
        self.raft.step(msg).await.map_err(Error::raft_v2_err)
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

    // #[tracing::instrument(level = "trace", skip(self))]
    async fn send_messages(&mut self, messages: Vec<raft::prelude::Message>) -> Result<()> {
        let mut raft_node_messages = BTreeMap::default();
        for message in messages {
            raft_node_messages
                .entry(message.to)
                .or_insert_with(|| Vec::with_capacity(10))
                .push(message);
        }
        for (raft_node, messages) in raft_node_messages {
            let mut client = self.raft_client(raft_node).await?;
            for message in messages {
                let data = bincode::serialize(&message).map_err(Error::serde_err)?;
                client.raft(Request::new(RaftRequest { data })).await?;
            }
        }
        Ok(())
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
        // for entry in entries.iter() {
        //     match entry.entry_type() {
        //         raft::prelude::EntryType::EntryNormal => todo!(),
        //         raft::prelude::EntryType::EntryConfChange => todo!(),
        //         raft::prelude::EntryType::EntryConfChangeV2 => todo!(),
        //     }
        // }
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

    async fn raft_client(&mut self, raft_node: u64) -> Result<RaftServiceClient<Channel>> {
        // Avoid `.or_insert` for async function will always called.
        if let Some(client) = self.raft_clients.get(&raft_node) {
            return Ok(client.clone());
        }
        let client = self.raft_network.client(raft_node).await?;
        self.raft_clients.insert(raft_node, client.clone());
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<raft::RawNode<RaftGroupLogStore>>();
        is_send_sync::<RaftWorker>();
    }
}
