use crate::db::keepers::Keepers;
use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Mutating operations that the Raft cluster can perform on the application's state machine (the file registry).
/// An operation either comes from a fellow node (a [NodeToNodeOperation]) or a client (a [ClientToNodeOperation])
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    FromClient(ClientToNodeOperation),
    FromNode(NodeToNodeOperation),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodeToNodeOperation {
    /// Replicate the file at `path` on the target node specified by `node_id`
    /// The target node will use the [Keepers] table to find out which other node already stores this file, and request the file from that node.
    StoreReplica { path: PathBuf, node_id: NodeId },

    /// Delete the file replica of the file at `path` from the node specified by `node_id`.
    DeleteReplica { path: PathBuf, node_id: NodeId },

    /// Node joined the network
    NodeJoin { node_id: NodeId },

    /// Node left the network
    NodeLeave { node_id: NodeId },

    /// Loss of storage due to node failure
    StorageLoss {
        path: PathBuf,
        sha512: u64,
        node_id: NodeId,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientToNodeOperation {
    /// Create a file at `path` with the given `replication_factor`
    Create {
        path: PathBuf,
        replication_factor: usize,
    },

    /// Create a file at `path` with the given `replication_factor`
    Write {
        path: PathBuf,
        size: usize,
        offset: usize,
    },

    /// Gives the file at `old_path` the new name `new_path`. This operation is used for both renaming and moving a file.
    Move {
        old_path: PathBuf,
        new_path: PathBuf,
    },

    /// Update the replication factor of the given file.
    ChangeReplicationFactor {
        path: PathBuf,
        new_replication_factor: usize,
    },

    /// Test operation that prints something to stdout on the receiver node.
    Print { message: String },
}
