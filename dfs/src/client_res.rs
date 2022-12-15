use async_raft::AppDataResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientError(String);

impl From<anyhow::Error> for ClientError {
    fn from(e: anyhow::Error) -> Self {
        Self(format!("{:?}", e))
    }
}

/// The application data response type.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppClientResponse(pub Result<String, ClientError>);

impl From<String> for AppClientResponse {
    fn from(s: String) -> Self {
        Self(Ok(s))
    }
}
impl<'a> From<&'a str> for AppClientResponse {
    fn from(s: &'a str) -> Self {
        Self(Ok(String::from(s)))
    }
}

impl From<anyhow::Error> for AppClientResponse {
    fn from(e: anyhow::Error) -> Self {
        Self(Err(e))
    }
}

impl AppDataResponse for AppClientResponse {}
