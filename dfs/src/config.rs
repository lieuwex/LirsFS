use std::borrow::Cow;
use std::net::SocketAddr;

use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

const fn default_file_registry() -> Cow<'static, str> {
    if cfg!(debug_assertions) {
        Cow::Borrowed("sqlite:///tmp/db/dev.db")
    } else {
        Cow::Borrowed("sqlite:///local/ddps2221/fileregistry.db")
    }
}

const fn default_ping_interval() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_missed_pings() -> usize {
    2
}

const fn default_reconnect_try_interval() -> Duration {
    Duration::from_millis(500)
}

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

    #[serde(default = "default_file_registry")]
    pub file_registry: Cow<'static, str>,

    #[serde(default = "default_ping_interval")]
    pub ping_interval: Duration,
    #[serde(default = "default_max_missed_pings")]
    pub max_missed_pings: usize,
    #[serde(default = "default_reconnect_try_interval")]
    pub reconnect_try_interval_ms: Duration,
}
