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

use crate::{client_req::AppClientRequest, connection::NodeConnection, CONFIG};

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct AppRaftNetwork {
    nodes: HashMap<u64, NodeConnection>,
}

impl AppRaftNetwork {
    pub fn new(raft_config: Arc<Config>) -> Self {
        let nodes: HashMap<_, _> = (CONFIG.nodes)
            .iter()
            .map(|n| (n.id, NodeConnection::new(n.id, n.addr)))
            .collect();

        Self { nodes }
    }

    pub fn assume_node(&self, node_id: u64) -> Result<&NodeConnection> {
        self.nodes
            .get(&node_id)
            .ok_or_else(|| anyhow!("no node {} known", node_id))
    }
}

macro_rules! assume_client {
    ($network:expr, $node_id:expr) => {{
        let node = $network.assume_node($node_id)?;
        node.get_client().await
    }};
}

#[async_trait]
impl RaftNetwork<AppClientRequest> for AppRaftNetwork {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<AppClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let client = assume_client!(self, target);
        Ok(client.append_entries(context::current(), rpc).await?)
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let client = assume_client!(self, target);
        Ok(client.install_snapshot(context::current(), rpc).await?)
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        let client = assume_client!(self, target);
        Ok(client.vote(context::current(), rpc).await?)
    }
}
