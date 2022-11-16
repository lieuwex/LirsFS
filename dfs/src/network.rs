use std::sync::Arc;

use anyhow::Result;
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    Config, RaftNetwork,
};

use crate::client_req::AppClientRequest;

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct AppRaftNetwork {
    // ... some internal state ...
}

impl AppRaftNetwork {
    pub fn new(config: Arc<Config>) -> Self {
        Self {}
    }
}

#[async_trait]
impl RaftNetwork<AppClientRequest> for AppRaftNetwork {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<AppClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        // ... snip ...
        todo!();
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        // ... snip ...
        todo!();
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        // ... snip ...
        todo!();
    }
}
