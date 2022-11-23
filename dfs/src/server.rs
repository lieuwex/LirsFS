use std::future::Future;
use std::pin::Pin;

use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use tarpc::context::Context;

use crate::{service::Service, RAFT};

struct Server;

impl Service for Server {
    type AppendEntriesFut = Pin<Box<dyn Future<Output = AppendEntriesResponse>>>;
    fn append_entries(
        self,
        context: Context,
        request: async_raft::raft::AppendEntriesRequest<crate::client_req::AppClientRequest>,
    ) -> Self::AppendEntriesFut {
        Box::pin(async {
            let raft = RAFT.get().unwrap();
            raft.append_entries(request).await.unwrap()
        })
    }

    type InstallSnapshotFut = Pin<Box<dyn Future<Output = InstallSnapshotResponse>>>;
    fn install_snapshot(
        self,
        context: tarpc::context::Context,
        request: async_raft::raft::InstallSnapshotRequest,
    ) -> Self::InstallSnapshotFut {
        Box::pin(async {
            let raft = RAFT.get().unwrap();
            raft.install_snapshot(request).await.unwrap()
        })
    }

    type VoteFut = Pin<Box<dyn Future<Output = VoteResponse>>>;
    fn vote(self, context: Context, request: async_raft::raft::VoteRequest) -> Self::VoteFut {
        Box::pin(async {
            let raft = RAFT.get().unwrap();
            raft.vote(request).await.unwrap()
        })
    }
}
