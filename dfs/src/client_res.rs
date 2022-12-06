use async_raft::AppDataResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientError();

/// The application data response type.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppClientResponse(pub Result<String, ClientError>);

impl AppDataResponse for AppClientResponse {}
