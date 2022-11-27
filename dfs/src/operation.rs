use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Operations that the Raft cluster can perform.
/// Most of these update the [FileRegistry] in some way.
///
/// [Operation::Internal] is for operations requested by one internal node in the Raft cluster to another (e.g., transferring data between nodes)
/// [Operation::External] is for operations requested by the user of LirsFS (e.g., creating a file in the file system)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Internal(InternalOp),
    External(ExternalOp),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalOp {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExternalOp {
    CreateFile { path: PathBuf },
}
