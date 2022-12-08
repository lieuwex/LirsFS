use std::{collections::HashMap, sync::Arc, time::Duration};

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

pub const CLIENT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

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

#[macro_export]
macro_rules! assume_client {
    ($network:expr, $node_id:expr, $timeout:expr) => {{
        let node = $network.assume_node($node_id)?;

        let fut = node.get_client();
        if let Some(timeout) = $timeout {
            tokio::time::timeout(timeout, fut).await?
        } else {
            fut.await
        }
    }};

    ($network:expr, $node_id:expr) => {{
        use crate::network::CLIENT_ACQUIRE_TIMEOUT;
        assume_client!($network, $node_id, Some(CLIENT_ACQUIRE_TIMEOUT))
    }};

    ($node_id:expr) => {{
        let network = NETWORK.get().unwrap();
        assume_client!(network, $node_id)
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
        let client = assume_client!(self, target, None);
        Ok(client.append_entries(context::current(), rpc).await?)
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let client = assume_client!(self, target, None);
        Ok(client.install_snapshot(context::current(), rpc).await?)
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        let client = assume_client!(self, target, None);
        Ok(client.vote(context::current(), rpc).await?)
    }
}
