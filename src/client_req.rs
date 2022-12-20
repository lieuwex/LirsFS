use anyhow::Result;
use async_raft::{AppData, NodeId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{db::errors::raftlog_deserialize_error, operation::Operation, RAFT};

/// Serial number the client has provided for this request.
/// If the client sends the same request again, they will send it with the same serial number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(pub Uuid);

impl TryFrom<Vec<u8>> for RequestId {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let bytes: [u8; 16] = value
            .as_slice()
            .try_into()
            .map_err(raftlog_deserialize_error)?;
        Ok(Self(Uuid::from_bytes(bytes)))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppClientRequest {
    /// The ID of the client which has sent the request.
    pub client: NodeId,
    /// The id of this request.
    pub id: RequestId,

    /// Operation that has to be performed.
    pub operation: Operation,
}

impl AppClientRequest {
    pub fn new<O: Into<Operation>>(operation: O) -> Self {
        let raft = &RAFT.get().unwrap().app;
        let metrics = raft.metrics();
        let client = metrics.borrow().id;

        Self {
            client,
            id: RequestId(Uuid::new_v4()),

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
