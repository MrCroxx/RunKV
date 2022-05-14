use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use itertools::Itertools;
use lazy_static::lazy_static;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::config::Node;
use runkv_common::notify_pool::NotifyPool;
use runkv_common::sync::TicketLock;
use runkv_proto::common::Endpoint;
use runkv_proto::kv::kv_service_server::KvService;
use runkv_proto::kv::*;
use runkv_proto::meta::KeyRangeInfo;
use runkv_proto::wheel::raft_service_server::RaftService;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::*;
use tonic::{Request, Response, Status};
use tracing::{trace_span, Instrument};

use crate::components::command::Command;
use crate::components::raft_manager::RaftManager;
use crate::components::raft_network::{GrpcRaftNetwork, RaftNetwork};
use crate::error::{Error, Result};
use crate::meta::MetaStoreRef;

lazy_static! {
    static ref KV_SERVICE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "kv_service_latency_histogram_vec",
            "kv service latency histogram vec",
            &["service", "node"],
            vec![0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0, 2.0, 3.0]
        )
        .unwrap();
}

struct WheelServiceMetrics {
    kv_service_get_latency_histogram: prometheus::Histogram,
    kv_service_put_latency_histogram: prometheus::Histogram,
    kv_service_delete_latency_histogram: prometheus::Histogram,
    kv_service_snapshot_latency_histogram: prometheus::Histogram,
    kv_service_txn_latency_histogram: prometheus::Histogram,

    kv_service_propose_latency_histogram: prometheus::Histogram,
    kv_service_wait_latency_histogram: prometheus::Histogram,
}

impl WheelServiceMetrics {
    fn new(node: u64) -> Self {
        Self {
            kv_service_get_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["get", &node.to_string()])
                .unwrap(),
            kv_service_put_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["put", &node.to_string()])
                .unwrap(),
            kv_service_delete_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["delete", &node.to_string()])
                .unwrap(),
            kv_service_snapshot_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["snapshot", &node.to_string()])
                .unwrap(),
            kv_service_txn_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["txn", &node.to_string()])
                .unwrap(),

            kv_service_propose_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["propose", &node.to_string()])
                .unwrap(),
            kv_service_wait_latency_histogram: KV_SERVICE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&["wait", &node.to_string()])
                .unwrap(),
        }
    }
}

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct WheelOptions {
    pub node: u64,
    pub meta_store: MetaStoreRef,
    pub channel_pool: ChannelPool,
    pub raft_network: GrpcRaftNetwork,
    pub raft_manager: RaftManager,
    pub txn_notify_pool: NotifyPool<u64, Result<KvResponse>>,
}

struct WheelInner {
    meta_store: MetaStoreRef,
    channel_pool: ChannelPool,
    raft_network: GrpcRaftNetwork,
    raft_manager: RaftManager,
    txn_notify_pool: NotifyPool<u64, Result<KvResponse>>,
    request_id: AtomicU64,

    sequence_lock: TicketLock,

    metrics: WheelServiceMetrics,
}

#[derive(Clone)]
pub struct Wheel {
    node: u64,
    inner: Arc<WheelInner>,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            node: options.node,
            inner: Arc::new(WheelInner {
                meta_store: options.meta_store,
                channel_pool: options.channel_pool,
                raft_network: options.raft_network,
                raft_manager: options.raft_manager,
                txn_notify_pool: options.txn_notify_pool,
                request_id: AtomicU64::new(0),

                sequence_lock: TicketLock::default(),

                metrics: WheelServiceMetrics::new(options.node),
            }),
        }
    }
}

impl std::fmt::Debug for Wheel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wheel").field("node", &self.node).finish()
    }
}

impl Wheel {
    async fn kv_inner(&self, request: KvRequest) -> Result<KvResponse> {
        let start = Instant::now();

        let target = request.target;
        let read_only = request.is_read_only();

        let command_packer = match self.inner.raft_manager.get_command_packer(target).await {
            Some(packer) => packer,
            None => {
                return Ok(KvResponse {
                    ops: request.ops,
                    err: ErrCode::Redirect.into(),
                })
            }
        };

        let sequence = self.inner.raft_manager.get_sequence(target).await?;

        // Critical area for propose sequence.
        let rx = {
            self.inner.sequence_lock.async_acquire().await;

            // Increase sequence if request is not read-only.
            let sequence = if read_only {
                sequence.load(Ordering::Acquire)
            } else {
                sequence.fetch_add(1, Ordering::SeqCst) + 1
            };

            // Register request.
            let request_id = self.inner.request_id.fetch_add(1, Ordering::SeqCst) + 1;
            let rx = self
                .inner
                .txn_notify_pool
                .register(request_id)
                .map_err(Error::err)?;

            // Propose cmd with raft leader.
            let cmd = Command::KvRequest {
                request_id,
                sequence,
                request,
            };
            command_packer.append(cmd, None);

            #[cfg(feature = "tracing")]
            {
                use runkv_common::time::timestamp;

                use crate::trace::TRACE_CTX;

                TRACE_CTX.propose_ts.insert(request_id, timestamp());
            }

            self.inner.sequence_lock.release();

            rx
        };

        self.inner
            .metrics
            .kv_service_propose_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        let start = Instant::now();

        // Wait for resposne.
        let response = rx
            .instrument(trace_span!("wait_apply"))
            .await
            .map_err(Error::err)?;

        self.inner
            .metrics
            .kv_service_wait_latency_histogram
            .observe(start.elapsed().as_secs_f64());

        response
    }
}

#[async_trait]
impl WheelService for Wheel {
    #[tracing::instrument(level = "trace")]
    async fn add_wheels(
        &self,
        request: Request<AddWheelsRequest>,
    ) -> core::result::Result<Response<AddWheelsResponse>, Status> {
        let req = request.into_inner();
        for (node, Endpoint { host, port }) in req.wheels.iter() {
            let node = Node {
                id: *node,
                host: host.to_owned(),
                port: *port as u16,
            };
            self.inner.channel_pool.put_node(node).await;
        }
        Ok(Response::new(AddWheelsResponse::default()))
    }

    #[tracing::instrument(level = "trace")]
    async fn add_key_ranges(
        &self,
        request: Request<AddKeyRangesRequest>,
    ) -> core::result::Result<Response<AddKeyRangesResponse>, Status> {
        let req = request.into_inner();
        for KeyRangeInfo {
            group,
            key_range,
            raft_nodes,
            ..
        } in req.key_ranges
        {
            let key_range = key_range.unwrap();
            self.inner
                .meta_store
                .add_key_range(key_range, group, &raft_nodes.keys().copied().collect_vec())
                .await
                .map_err(internal)?;
            self.inner
                .raft_network
                .register(
                    group,
                    BTreeMap::from_iter(
                        raft_nodes
                            .iter()
                            .map(|(&raft_node, &node)| (raft_node, node)),
                    ),
                )
                .await
                .map_err(internal)?;
            for (raft_node, node) in raft_nodes.into_iter() {
                if node != self.node {
                    continue;
                }
                self.inner
                    .raft_manager
                    .create_raft_node(group, raft_node)
                    .await
                    .map_err(internal)?;
            }
        }

        let rsp = AddKeyRangesResponse::default();
        Ok(Response::new(rsp))
    }
}

#[async_trait]
impl RaftService for Wheel {
    async fn raft(
        &self,
        request: Request<RaftRequest>,
    ) -> core::result::Result<Response<RaftResponse>, Status> {
        let req = request.into_inner();
        let msgs = bincode::deserialize(&req.data)
            .map_err(Error::serde_err)
            .map_err(internal)?;
        self.inner.raft_network.recv(msgs).await.map_err(internal)?;
        let rsp = RaftResponse::default();
        Ok(Response::new(rsp))
    }
}

#[async_trait]
impl KvService for Wheel {
    #[tracing::instrument(level = "trace")]
    async fn kv(
        &self,
        request: Request<KvRequest>,
    ) -> core::result::Result<Response<KvResponse>, Status> {
        let start = Instant::now();

        let req = request.into_inner();
        let r#type = req.r#type();
        let rsp = self.kv_inner(req).await.map_err(internal)?;

        let elapsed = start.elapsed().as_secs_f64();

        match r#type {
            Type::TNone => {}
            Type::TGet => self
                .inner
                .metrics
                .kv_service_get_latency_histogram
                .observe(elapsed),
            Type::TPut => self
                .inner
                .metrics
                .kv_service_put_latency_histogram
                .observe(elapsed),
            Type::TDelete => self
                .inner
                .metrics
                .kv_service_delete_latency_histogram
                .observe(elapsed),
            Type::TSnapshot => self
                .inner
                .metrics
                .kv_service_snapshot_latency_histogram
                .observe(elapsed),
            Type::TTxn => self
                .inner
                .metrics
                .kv_service_txn_latency_histogram
                .observe(elapsed),
        }
        Ok(Response::new(rsp))
    }
}
