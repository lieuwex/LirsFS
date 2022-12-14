use std::time::SystemTime;

use async_raft::NodeId;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

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
    StoreReplica { path: Utf8PathBuf, node_id: NodeId },

    /// Delete the file replica of the file at `path` from the target node specified by `node_id`.
    /// This means the target node is no longer a keeper of the file.
    DeleteReplica { path: Utf8PathBuf, node_id: NodeId },

    /// Node joined the network.
    NodeJoin { node_id: NodeId },

    /// Node left the network. This could be a temporary node failure.
    /// Once the Raft cluster's master deems it necessary, it will issue [NodeToNodeOperation::NodeLost],
    /// indicating that this node is considered lost permanently.
    NodeLeft { node_id: NodeId },

    /// Raft cluster master considers the node `lost_node` to be lost permanently.
    /// The master will then issue the operations necessary to ensure replication factor of all files is respected.
    NodeLost {
        lost_node: NodeId,
        last_contact: SystemTime,
    },

    /// Sent by a keeper when a previous file operation was unsuccessfully finished.
    FileCommitFail {
        /// The serial number of the operation to which this commit refers to.
        serial: u64,
        /// The reason this commit failed.
        failure_reason: String, // TODO: different type?
    },
    /// Sent by a keeper when a previous file operation was succesfully done.
    FileCommitSuccess {
        /// The serial number of the operation to which this commit refers to.
        serial: u64,
        /// XxHash64 value for the whole file content at this point.
        hash: u64,
        /// The node this file was committed on
        node_id: NodeId,
        /// Path of the file that was committed
        path: Utf8PathBuf,
    },
}

/// Operations that a client (e.g. the WebDav server) can request from the Raft cluster.
/// I.e., an external operation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientToNodeOperation {
    /// Create a file at `path` with the given `replication_factor`
    CreateFile {
        path: Utf8PathBuf,
        replication_factor: usize,
    },
    /// Create a dictionary at `path`.
    CreateDir {
        path: Utf8PathBuf,
    },

    /// Write `contents` to the existing file at `path`, starting from `offset`
    Write {
        path: Utf8PathBuf,
        offset: usize,
        contents: Vec<u8>,
    },

    /// Give the file at `old_path` the new name `new_path`. This operation is used for both renaming and moving a file.
    MoveFile {
        old_path: Utf8PathBuf,
        new_path: Utf8PathBuf,
    },

    /// Make a copy of the file at `src_path`, at `dst_path`
    CopyFile {
        src_path: Utf8PathBuf,
        dst_path: Utf8PathBuf,
    },

    RemoveFile {
        path: Utf8PathBuf,
    },
    RemoveDir {
        path: Utf8PathBuf,
    },

    /// Update the replication factor of the given file.
    ChangeReplicationFactor {
        path: Utf8PathBuf,
        new_replication_factor: usize,
    },

    /// Test operation that prints something to stdout on the receiver node.
    Print {
        message: String,
    },
}
