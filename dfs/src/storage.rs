use crate::{
    client_req::AppClientRequest,
    client_res::AppClientResponse,
    db::{
        curr_snapshot, db,
        raftlog::{RaftLog, RaftLogId},
        schema::Schema,
        snapshot_meta::{SnapshotMeta, SnapshotMetaRow},
    },
    operation::{ClientToNodeOperation, NodeToNodeOperation, Operation},
    CONFIG,
};
use anyhow::Result;
use async_raft::{
    async_trait::async_trait,
    raft::{Entry, MembershipConfig},
    storage::InitialState,
    storage::{CurrentSnapshotData, HardState},
    Config, NodeId, RaftStorage,
};
use std::{fmt::Display, io::ErrorKind, sync::Arc};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

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
pub struct AppRaftStorage {
    config: Arc<Config>,
}

impl AppRaftStorage {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    pub fn get_own_id(&self) -> NodeId {
        CONFIG.node_id
    }

    async fn read_hard_state(&self) -> Result<Option<HardState>> {
        let path = &CONFIG.hardstate_file;
        let mut file = File::open(path).await?;
        let mut buff: Vec<u8> = vec![];
        file.read_exact(&mut buff).await?;
        let hardstate = bincode::deserialize(&buff)?;
        Ok(hardstate)
    }

    /// Handle a [ClientToNodeOperation], possibly mutating the file registry.
    /// If `last_entry_id` is [Some], it must be atomically committed to
    /// the `snapshot_meta` table together with the operation being performed.
    async fn handle_client_operation(
        &self,
        op: &ClientToNodeOperation,
        last_entry_id: Option<RaftLogId>,
    ) {
    }

    /// Handle a [NodeToNodeOperation], possibly mutating the file registry.
    /// If `last_entry_id` is [Some], it must be atomically committed to
    /// the `snapshot_meta` table together with the operation being performed.
    async fn handle_node_operation(
        &self,
        op: &NodeToNodeOperation,
        last_entry_id: Option<RaftLogId>,
    ) {
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
        let hard_state = match self.read_hard_state().await? {
            Some(hs) => hs,
            None => {
                // This Raft node is pristine, return an empty initial config
                let id = CONFIG.node_id;
                return Ok(InitialState::new_initial(id));
            }
        };
        let membership = self.get_membership_config().await?;

        let state = InitialState {
            hard_state,
            membership,
            // TODO: figure these out from our saved raft log
            last_applied_log: todo!(),
            last_log_index: todo!(),
            last_log_term: todo!(),
        };

        Ok(state)
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        // TODO: Also just store in SQLite?
        let path = &CONFIG.hardstate_file;
        let mut file = OpenOptions::new().write(true).open(path).await?;
        file.write_all(&bincode::serialize(hs)?).await?;
        file.flush().await?;
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<AppClientRequest>>> {
        if start > stop {
            panic!(
                "Invalid request to `get_log_entries`, start ({:?}) > stop ({:?})",
                start, stop
            );
        }
        Ok(RaftLog::with(db()).get_range(start, stop).await)
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        match stop {
            Some(stop) => RaftLog::with(db()).delete_range(start, stop).await,
            None => RaftLog::with(db()).delete_from(start).await,
        };
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<AppClientRequest>) -> Result<()> {
        RaftLog::with(db())
            .insert(std::slice::from_ref(entry))
            .await;
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<AppClientRequest>]) -> Result<()> {
        RaftLog::with(db()).insert(entries).await;
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &AppClientRequest,
    ) -> Result<AppClientResponse> {
        match &data.operation {
            Operation::FromClient(op) => self.handle_client_operation(op, Some(*index)).await,
            Operation::FromNode(op) => self.handle_node_operation(op, Some(*index)).await,
        };
        todo!("handle responses to the client");
        Ok(AppClientResponse(Ok("".into())))
    }

    async fn replicate_to_state_machine(
        &self,
        entries: &[(&u64, &AppClientRequest)],
    ) -> Result<()> {
        let mut last_entry_id: Option<RaftLogId> = None;
        let mut entries = entries.iter().peekable();
        while let Some(&(id, data)) = entries.next() {
            let is_last_entry = entries.peek().is_none();
            if is_last_entry {
                last_entry_id = Some(*id);
            }
            // The last operation's id will be committed to the `snapshot_meta` table as the last one applied
            match &data.operation {
                Operation::FromClient(op) => self.handle_client_operation(op, last_entry_id).await,
                Operation::FromNode(op) => self.handle_node_operation(op, last_entry_id).await,
            };
        }

        todo!("handle responses to client");
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let SnapshotMetaRow {
            last_applied_log, ..
        } = SnapshotMeta::with(db()).get().await;

        // Get last known membership config from the log
        let membership = RaftLog::with(db())
            .get_last_membership_before(last_applied_log)
            .await
            .unwrap_or_else(|| MembershipConfig::new_initial(self.get_own_id()));

        let term = RaftLog::with(db()).get_by_id(last_applied_log).await.unwrap_or_else(|| {
            panic!("Inconsistent log: `last_applied_log` from the `{}` table was not found in the `{}` table", SnapshotMeta::TABLENAME, RaftLog::TABLENAME)
        }).term;

        let snapshot = tokio::fs::OpenOptions::new()
            .create_new(true)
            .open(CONFIG.wip_file_registry_snapshot())
            .await
            // TODO: Better error handling
            .expect("Error while creating work-in-progess snapshot file during log compaction");
        let snapshot = Box::new(snapshot);

        // Delete the Raft log entries up until the last applied log
        RaftLog::with(db()).delete_range(0, last_applied_log).await;

        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership,
            snapshot,
        })
    }

    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((
            String::from(""), // Snapshot id is irrelevant as we only ever save one snapshot
            Box::new(
                tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .open(CONFIG.blank_file_registry_snapshot())
                    .await
                    // TODO: Better error handling
                    .expect("Error while creating work-in-progess snapshot file"),
            ),
        ))
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
        let file = match tokio::fs::File::open(&CONFIG.file_registry_snapshot).await {
            Ok(file) => file,
            Err(err) => match err.kind() {
                ErrorKind::NotFound => return Ok(None),
                _ => panic!(
                    "Error reading snapshot file at {:#?}: {:#?}",
                    CONFIG.file_registry_snapshot, err
                ),
            },
        };
        let snapshot = Box::new(file);
        let SnapshotMetaRow {
            last_applied_log,
            term,
            membership,
        } = SnapshotMeta::with(&curr_snapshot().await?).get().await;

        Ok(Some(CurrentSnapshotData::<Self::Snapshot> {
            index: last_applied_log,
            membership,
            term,
            snapshot,
        }))
    }
}
