use std::{collections::HashSet, net::SocketAddr};

use anyhow::bail;
use async_raft::NodeId;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use crate::RAFT;

fn default_file_dir() -> Utf8PathBuf {
    if cfg!(debug_assertions) {
        Utf8PathBuf::from("/tmp/db/files")
    } else {
        Utf8PathBuf::from("/local/ddps2221/files")
    }
}

const fn default_ping_interval() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_missed_pings() -> usize {
    2
}

fn default_file_registry() -> Utf8PathBuf {
    if cfg!(debug_assertions) {
        Utf8PathBuf::from("/tmp/db/dev.db")
    } else {
        Utf8PathBuf::from("/local/ddps2221/fileregistry.db")
    }
}

fn default_hardstate_file() -> Utf8PathBuf {
    if cfg!(debug_assertions) {
        Utf8PathBuf::from("/tmp/raft_hardstate")
    } else {
        Utf8PathBuf::from("/local/ddps2221/raft_hardstate")
    }
}

fn default_file_registry_snapshot() -> Utf8PathBuf {
    if cfg!(debug_assertions) {
        Utf8PathBuf::from("/tmp/db/snapshot.db")
    } else {
        Utf8PathBuf::from("/local/ddps2221/snapshot.db")
    }
}

const fn default_reconnect_try_interval() -> Duration {
    Duration::from_millis(500)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub tarpc_addr: SocketAddr,
    pub ssh_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub cluster_name: String,
    pub node_id: NodeId,
    pub webdav_addr: SocketAddr,
    pub nodes: Vec<Node>,

    #[serde(default = "default_file_dir")]
    pub file_dir: Utf8PathBuf,

    #[serde(default = "default_file_registry")]
    pub file_registry: Utf8PathBuf,
    #[serde(default = "default_file_registry_snapshot")]
    pub file_registry_snapshot: Utf8PathBuf,
    #[serde(default = "default_hardstate_file")]
    pub hardstate_file: Utf8PathBuf,

    #[serde(default = "default_ping_interval")]
    pub ping_interval: Duration,
    #[serde(default = "default_max_missed_pings")]
    pub max_missed_pings: usize,
    #[serde(default = "default_reconnect_try_interval")]
    pub reconnect_try_interval_ms: Duration,
}

impl Config {
    pub fn check_integrity(&self) -> Result<(), anyhow::Error> {
        {
            let mut seen = HashSet::new();
            for node in &self.nodes {
                if !seen.insert(node.id) {
                    bail!(
                        "There already exists a node with id {} in the config.",
                        node.id
                    );
                }
            }
        }

        Ok(())
    }

    /// Return the filename of a work-in-progress snapshot.
    /// After the snapshot is finalized, the Raft cluster's master must
    /// change this file's name to [Config]'s `file_registry_snapshot`
    pub fn wip_file_registry_snapshot(&self) -> Utf8PathBuf {
        self.file_registry_snapshot.with_extension("db.wip")
    }

    /// Return the filename of a blank snapshot.
    /// Raft will create a blank snapshot (= empty file) with this name, then write an existing snapshot into it to install it.
    /// After the snapshot is finalized, the Raft cluster's master must change this file's name to [Config]'s `file_registry_snapshot`,
    /// which will complete the installation.
    pub fn blank_file_registry_snapshot(&self) -> Utf8PathBuf {
        self.file_registry_snapshot.with_extension("db.blank")
    }

    pub fn get_own_id(&self) -> NodeId {
        // This is just a debug assert to make sure my understanding is correct.
        // We use CONFIG.node_id and raft_own_id interchangeably in the code, so it has to be
        // correct, otherwise code is broken.
        // This is a nice place to test the assertion.
        if cfg!(debug_assertions) {
            if let Some(raft) = RAFT.get() {
                let raft_own_id = raft.metrics().borrow().id;
                assert_eq!(self.node_id, raft_own_id);
            }
        }

        self.node_id
    }

    fn find_node(&self, node_id: NodeId) -> Option<&Node> {
        // Yes, O(N), what are you going to do about it?
        self.nodes.iter().find(|n| n.id == node_id)
    }

    pub fn get_own_tarpc_addr(&self) -> SocketAddr {
        let id = self.get_own_id();
        let node = self
            .find_node(id)
            .expect("own node should be included in config");
        node.tarpc_addr
    }

    pub fn get_node_ssh_host(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.find_node(node_id).map(|n| n.ssh_addr)
    }
}
