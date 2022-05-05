#![allow(dead_code)]

use runkv_proto::kv::kv_service_client::KvServiceClient;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct RunKVClient {
    client: KvServiceClient<Channel>,
}
