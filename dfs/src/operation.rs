use std::path::PathBuf;

use async_raft::NodeId;
use serde::{Deserialize, Serialize};

/// Mutating operations that the Raft cluster can perform on the application's state machine (the file registry).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    /// Test operation that prints something to stdout on the receiver node.
    Print { message: String },

    /// Write a file to the destination
    Write {
        path: PathBuf,
        replication_factor: usize,
        sha512: u64,
    },

    /// Write stored on specific node
    Store {
        path: PathBuf,
        sha512: u64,
        node_id: NodeId,
    },
    /// Loss of storage due to node failure
    StorageLoss {
        path: PathBuf,
        sha512: u64,
        node_id: NodeId,
    },

    /// Node joined the network
    NodeJoin { node_id: NodeId },
    /// Node leaved the network
    NodeLeave { node_id: NodeId },
}
