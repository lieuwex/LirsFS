use std::net::SocketAddr;

use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use tarpc::context::Context;

use crate::{service::Service, RAFT};

#[derive(Debug, Clone)]
pub struct Server {
    pub remote_addr: SocketAddr,
}

#[tarpc::server]
impl Service for Server {
    async fn append_entries(
        self,
        context: Context,
        request: async_raft::raft::AppendEntriesRequest<crate::client_req::AppClientRequest>,
    ) -> AppendEntriesResponse {
        let raft = &RAFT.get().unwrap().app;
        raft.append_entries(request).await.unwrap()
    }

    async fn install_snapshot(
        self,
        context: tarpc::context::Context,
        request: async_raft::raft::InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let raft = &RAFT.get().unwrap().app;
        raft.install_snapshot(request).await.unwrap()
    }

    async fn vote(self, context: Context, request: async_raft::raft::VoteRequest) -> VoteResponse {
        let raft = &RAFT.get().unwrap().app;
        raft.vote(request).await.unwrap()
    }

    async fn ping(self, context: Context) {}
}
