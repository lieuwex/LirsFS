use async_raft::{AppData, NodeId};
use serde::{Deserialize, Serialize};

use crate::{operation::Operation, RAFT};

/// Serial number the client has provided for this request.
/// If the client sends the same request again, they will send it with the same serial number.
pub type RequestSerial = u64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppClientRequest {
    /// The ID of the client which has sent the request.
    pub client: NodeId,
    /// The serial number of this request.
    pub serial: RequestSerial,

    /// Operation that has to be performed.
    pub operation: Operation,
}

impl AppClientRequest {
    pub fn new<O: Into<Operation>>(operation: O) -> Self {
        let raft = &RAFT.get().unwrap().app;
        let metrics = raft.metrics();
        let metrics = metrics.borrow();

        Self {
            client: metrics.id,
            serial: metrics.last_log_index + 1,

            operation: operation.into(),
        }
    }
}

impl<O: Into<Operation>> From<O> for AppClientRequest {
    fn from(operation: O) -> Self {
        Self::new(operation)
    }
}

impl AppData for AppClientRequest {}
