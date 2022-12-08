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

impl From<ClientToNodeOperation> for Operation {
    fn from(o: ClientToNodeOperation) -> Self {
        Self::FromClient(o)
    }
}

impl From<NodeToNodeOperation> for Operation {
    fn from(o: NodeToNodeOperation) -> Self {
        Self::FromNode(o)
    }
}

/// Operations that a node in the Raft cluster can perform that involve other nodes in the cluster.
/// I.e., an internal operation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodeToNodeOperation {
    /// Replicate the file at `path` on the target node specified by `node_id`
    /// The target node will use the [Keepers] table to find out which other node already stores this file, and request the file from that node.
    /// This means the target node is now a keeper of the file.
    StoreReplica { path: PathBuf, node_id: NodeId },

    /// Delete the file replica of the file at `path` from the target node specified by `node_id`.
    /// This means the target node is no longer a keeper of the file.
    DeleteReplica { path: PathBuf, node_id: NodeId },

    /// Node joined the network.
    NodeJoin { node_id: NodeId },

    /// Node left the network. This could be a temporary node failure.
    /// Once the Raft cluster's master deems it necessary, it will issue [NodeToNodeOperation::NodeLost],
    /// indicating that this node is considered lost permanently.
    NodeLeave { node_id: NodeId },

    /// Raft cluster master considers the node `lost_node` to be lost permanently.
    /// The master will then issue the operations necessary to ensure replication factor of all files is respected.
    NodeLost { lost_node: NodeId },
}

/// Operations that a client (e.g. the WebDav server) can request from the Raft cluster.
/// I.e., an external operation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientToNodeOperation {
    /// Create a file at `path` with the given `replication_factor`
    Create {
        path: PathBuf,
        replication_factor: usize,
    },

    /// Write `contents` to the existing file at `path`, starting from `offset`
    Write {
        path: PathBuf,
        offset: usize,
        contents: Vec<u8>,
    },

    /// Give the file at `old_path` the new name `new_path`. This operation is used for both renaming and moving a file.
    Move {
        old_path: PathBuf,
        new_path: PathBuf,
    },

    /// Make a copy of the file at `src_path`, at `dst_path`
    Copy {
        src_path: PathBuf,
        dst_path: PathBuf,
    },

    /// Update the replication factor of the given file.
    ChangeReplicationFactor {
        path: PathBuf,
        new_replication_factor: usize,
    },

    /// Test operation that prints something to stdout on the receiver node.
    Print { message: String },
}
