use std::{borrow::Cow, net::SocketAddr, path::PathBuf};

use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

fn default_file_dir() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from("/tmp/db/files")
    } else {
        PathBuf::from("/local/ddps2221/files")
    }
}

const fn default_ping_interval() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_missed_pings() -> usize {
    2
}

fn default_file_registry() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from("/tmp/db/dev.db")
    } else {
        PathBuf::from("/local/ddps2221/fileregistry.db")
    }
}

fn default_hardstate_file() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from("/tmp/raft_hardstate")
    } else {
        PathBuf::from("/local/ddps2221/raft_hardstate")
    }
}

fn default_file_registry_snapshot() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from("/tmp/db/snapshot.db")
    } else {
        PathBuf::from("/local/ddps2221/snapshot.db")
    }
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
    pub node_id: NodeId,
    pub cluster_name: String,
    pub nodes: Vec<Node>,
    pub listen_port: usize,

    #[serde(default = "default_file_dir")]
    pub file_dir: PathBuf,

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
    #[serde(default = "default_reconnect_try_interval")]
    pub reconnect_try_interval_ms: Duration,
}

impl Config {
    /// Return the filename of a work-in-progress snapshot.
    /// After the snapshot is finalized, the Raft cluster's master must
    /// change this file's name to [Config]'s `file_registry_snapshot`
    pub fn wip_file_registry_snapshot(&self) -> PathBuf {
        self.file_registry_snapshot.with_extension("db.wip")
    }

    /// Return the filename of a blank snapshot.
    /// Raft will create a blank snapshot (= empty file) with this name, then write an existing snapshot into it to install it.
    /// After the snapshot is finalized, the Raft cluster's master must change this file's name to [Config]'s `file_registry_snapshot`,
    /// which will complete the installation.
    pub fn blank_file_registry_snapshot(&self) -> PathBuf {
        self.file_registry_snapshot.with_extension("db.blank")
    }
}
