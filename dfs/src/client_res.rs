use async_raft::AppDataResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientError();

/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppClientResponse(Result<Option<String>, ClientError>);

impl AppDataResponse for AppClientResponse {}
