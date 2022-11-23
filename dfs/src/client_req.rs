use async_raft::AppData;
use serde::{Deserialize, Serialize};

use crate::operation::Operation;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    pub serial: u64,

    /// Operation that has to be performed.
    pub operation: Operation,
}

impl AppData for AppClientRequest {}
