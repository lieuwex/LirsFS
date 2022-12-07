use std::net::SocketAddr;
use std::path::PathBuf;

use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

const fn default_ping_interval() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_missed_pings() -> usize {
    2
}

#[cfg(debug_assertions)]
fn default_file_registry() -> PathBuf {
    PathBuf::from("/tmp/db/dev.db")
}
#[cfg(not(debug_assertions))]
fn default_file_registry() -> PathBuf {
    PathBuf::from("/local/ddps2221/fileregistry.db")
}

#[cfg(debug_assertions)]
fn default_hardstate_file() -> PathBuf {
    PathBuf::from("/tmp/raft_hardstate")
}

#[cfg(not(debug_assertions))]
fn default_hardstate_file() -> PathBuf {
    PathBuf::from("/local/ddps2221/raft_hardstate")
}

#[cfg(debug_assertions)]
fn default_file_registry_snapshot() -> PathBuf {
    PathBuf::from("/tmp/db/snapshot.db")
}

#[cfg(not(debug_assertions))]
fn default_file_registry_snapshot() -> PathBuf {
    PathBuf::from("/local/ddps2221/snapshot.db")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_id: NodeId,
    pub cluster_name: String,
    pub nodes: Vec<Node>,
    pub listen_port: usize,

    #[serde(default = "default_ping_interval")]
    pub ping_interval: Duration,
    #[serde(default = "default_max_missed_pings")]
    pub max_missed_pings: usize,
    #[serde(default = "default_file_registry")]
    pub file_registry: PathBuf,
    #[serde(default = "default_hardstate_file")]
    pub hardstate_file: PathBuf,
    #[serde(default = "default_file_registry_snapshot")]
    pub file_registry_snapshot: PathBuf,
}

impl Config {
    /// Return the filename of a work-in-progress snapshot.
    /// After the snapshot is finalized, the Raft cluster's master must
    /// change this file's name to [Config]'s `file_registry_snapshot`
    pub fn wip_file_registry_snapshot(&self) -> PathBuf {
        self.file_registry_snapshot.with_extension("db.wip")
    }
}
