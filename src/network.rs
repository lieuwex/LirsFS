use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    ClientWriteError, Config, NodeId, RaftNetwork,
};
use tarpc::context;

use crate::{
    client_req::AppClientRequest, client_res::AppClientResponse, connection::NodeConnection, CONFIG,
};

pub const CLIENT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
#[derive(Debug)]
pub struct AppRaftNetwork {
    nodes: HashMap<NodeId, NodeConnection>,
}

impl AppRaftNetwork {
    pub fn new(raft_config: Arc<Config>) -> Self {
        let nodes: HashMap<_, _> = (CONFIG.nodes)
            .iter()
            .map(|n| (n.id, NodeConnection::new(n.id, n.tarpc_addr)))
            .collect();

        Self { nodes }
    }

    pub fn assume_node(&self, node_id: NodeId) -> Result<&NodeConnection> {
        self.nodes
            .get(&node_id)
            .ok_or_else(|| anyhow!("no node {} known", node_id))
    }
}

// TODO: also retry on file read error, make sure we won't retry the same keeper node
#[macro_export]
macro_rules! assume_client {
    ($network:expr, $node_id:expr, $timeout:expr) => {{
        let node = $network.assume_node($node_id)?;

        let fut = node.get_client();
        if let Some(timeout) = $timeout {
            (node, tokio::time::timeout(timeout, fut).await?)
        } else {
            (node, fut.await)
        }
    }};

    ($network:expr, $node_id:expr) => {{
        use $crate::network::CLIENT_ACQUIRE_TIMEOUT;
        assume_client!($network, $node_id, Some(CLIENT_ACQUIRE_TIMEOUT))
    }};

    ($node_id:expr) => {{
        let network = NETWORK.get().unwrap();
        assume_client!(network, $node_id)
    }};
}

impl AppRaftNetwork {
    pub async fn client_write(
        &self,
        target: NodeId,
        rpc: AppClientRequest,
    ) -> Result<ClientWriteResponse<AppClientResponse>> {
        let (node, client) = assume_client!(self, target, None);
        match client.client_write(context::current(), rpc).await {
            Err(e) => {
                let e = e.into();
                node.mark_dead(&e).await;
                Err(e)
            }
            Ok(r) => anyhow::Ok(r),
        }
    }
}

#[async_trait]
impl RaftNetwork<AppClientRequest> for AppRaftNetwork {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<AppClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let (node, client) = assume_client!(self, target, None);
        match client.append_entries(context::current(), rpc).await {
            Err(e) => {
                let e = e.into();
                node.mark_dead(&e).await;
                Err(e)
            }
            Ok(r) => Ok(r),
        }
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let (node, client) = assume_client!(self, target, None);
        match client.install_snapshot(context::current(), rpc).await {
            Err(e) => {
                let e = e.into();
                node.mark_dead(&e).await;
                Err(e)
            }
            Ok(r) => Ok(r),
        }
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let (node, client) = assume_client!(self, target, None);
        match client.vote(context::current(), rpc).await {
            Err(e) => {
                let e = e.into();
                node.mark_dead(&e).await;
                Err(e)
            }
            Ok(r) => Ok(r),
        }
    }
}
