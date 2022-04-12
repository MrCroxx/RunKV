use async_trait::async_trait;

use super::RaftTypeConfig;
pub struct RaftNetworkConnection {}

#[async_trait]
impl openraft::RaftNetwork<RaftTypeConfig> for RaftNetworkConnection {
    async fn send_append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<RaftTypeConfig>,
    ) -> core::result::Result<
        openraft::raft::AppendEntriesResponse<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            RaftTypeConfig,
            openraft::error::AppendEntriesError<
                <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
            >,
        >,
    > {
        todo!()
    }

    async fn send_install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<RaftTypeConfig>,
    ) -> core::result::Result<
        openraft::raft::InstallSnapshotResponse<
            <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
        >,
        openraft::error::RPCError<
            RaftTypeConfig,
            openraft::error::InstallSnapshotError<
                <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
            >,
        >,
    > {
        todo!()
    }

    async fn send_vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<RaftTypeConfig>,
    ) -> core::result::Result<
        openraft::raft::VoteResponse<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            RaftTypeConfig,
            openraft::error::VoteError<<RaftTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        todo!()
    }
}

pub struct RaftNetwork {}

#[async_trait]
impl openraft::RaftNetworkFactory<RaftTypeConfig> for RaftNetwork {
    type Network = RaftNetworkConnection;

    async fn connect(
        &mut self,
        _target: <RaftTypeConfig as openraft::RaftTypeConfig>::NodeId,
        _node: Option<&openraft::Node>,
    ) -> Self::Network {
        todo!()
    }
}
