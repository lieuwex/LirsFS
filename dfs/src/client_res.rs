use async_raft::{raft::ClientWriteResponse, AppDataResponse};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientError(String);

impl<E> From<E> for ClientError
where
    E: Into<anyhow::Error>,
{
    fn from(e: E) -> Self {
        let e: anyhow::Error = e.into();
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

impl From<ClientWriteResponse<AppClientResponse>> for AppClientResponse {
    fn from(value: ClientWriteResponse<AppClientResponse>) -> Self {
        value.data
    }
}

impl From<anyhow::Error> for AppClientResponse {
    fn from(e: anyhow::Error) -> Self {
        Self(Err(e.into()))
    }
}

impl AppDataResponse for AppClientResponse {}
