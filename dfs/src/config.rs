use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub cluster_name: String,
    pub nodes: Vec<SocketAddr>,
}
