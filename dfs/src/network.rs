use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    Config, RaftNetwork,
};
use tarpc::context;
use tokio::sync::Mutex;

use crate::{client_req::AppClientRequest, service::ServiceClient};

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct AppRaftNetwork {
    nodes: HashMap<u64, Arc<Mutex<ServiceClient>>>,
}

impl AppRaftNetwork {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    fn assume_node(&self, node_id: u64) -> Result<Arc<Mutex<ServiceClient>>> {
        self.nodes
            .get(&node_id)
            .ok_or_else(|| anyhow!("no node {} known", node_id))
            .cloned()
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
        let node = self.assume_node(target)?;
        let node = node.lock().await;

        Ok(node.append_entries(context::current(), rpc).await?)
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let node = self.assume_node(target)?;
        let node = node.lock().await;

        Ok(node.install_snapshot(context::current(), rpc).await?)
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        let node = self.assume_node(target)?;
        let node = node.lock().await;

        Ok(node.vote(context::current(), rpc).await?)
    }
}
