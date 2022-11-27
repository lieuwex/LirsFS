use std::net::SocketAddr;

use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub cluster_name: String,
    pub nodes: Vec<Node>,
    pub listen_port: usize,
    pub ping_interval: Duration,
}
