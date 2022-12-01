use std::{fmt::Display, sync::Arc};

use crate::{client_req::AppClientRequest, client_res::AppClientResponse};
use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{Entry, MembershipConfig},
    storage::InitialState,
    storage::{CurrentSnapshotData, HardState},
    Config, NodeId, RaftStorage,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub struct AppError {
    pub message: String,
}

impl Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.message)
    }
}

#[derive(Debug)]
pub struct AppRaftStorage {}

impl AppRaftStorage {
    pub fn new(config: Arc<Config>) -> Self {
        Self {}
    }

    pub async fn get_own_id(&self) -> NodeId {
        0 as NodeId
    }
}

#[async_trait]
impl RaftStorage<AppClientRequest, AppClientResponse> for AppRaftStorage {
    type Snapshot = tokio::fs::File;
    type ShutdownError = AppError;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        todo!()
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        todo!()
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        todo!()
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<AppClientRequest>>> {
        todo!()
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        todo!()
    }

    async fn append_entry_to_log(&self, entry: &Entry<AppClientRequest>) -> Result<()> {
        todo!()
    }

    async fn replicate_to_log(&self, entries: &[Entry<AppClientRequest>]) -> Result<()> {
        todo!()
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &AppClientRequest,
    ) -> Result<AppClientResponse> {
        todo!()
    }

    async fn replicate_to_state_machine(
        &self,
        entries: &[(&u64, &AppClientRequest)],
    ) -> Result<()> {
        todo!()
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        todo!()
    }

    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        todo!()
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        todo!()
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        todo!()
    }
}
