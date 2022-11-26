use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub cluster_name: String,
    pub nodes: Vec<SocketAddr>,
    pub listen_port: usize,
    pub ping_interval: Duration,
}
