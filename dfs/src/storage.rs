use crate::{client_req::AppClientRequest, client_res::AppClientResponse};
use async_raft::{Config, NodeId, RaftStorage};
use std::{fmt::Display, sync::Arc};
use thiserror::Error;

#[derive(Error, Debug)]
pub struct AppError {
    message: String,
}

impl Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.message)
    }
}

pub struct AppRaftStorage {}

impl AppRaftStorage {
    pub fn new(config: Arc<Config>) -> Self {
        Self {}
    }

    pub async fn get_own_id(&self) -> NodeId {
        0 as NodeId
    }
}

impl RaftStorage<AppClientRequest, AppClientResponse> for AppRaftStorage {
    type Snapshot = tokio::fs::File;
    type ShutdownError = AppError;

    // TODO: Implement the storage we want Raft to take care of
}
