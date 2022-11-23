use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::client_req::AppClientRequest;

#[tarpc::service]
pub trait Service {
    async fn append_entries(
        request: AppendEntriesRequest<AppClientRequest>,
    ) -> AppendEntriesResponse;
    async fn install_snapshot(request: InstallSnapshotRequest) -> InstallSnapshotResponse;
    async fn vote(request: VoteRequest) -> VoteResponse;
}
