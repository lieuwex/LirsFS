use crate::{
    client_req::AppClientRequest,
    client_res::AppClientResponse,
    db::{
        self, curr_snapshot, db,
        file::File,
        keepers::Keepers,
        last_applied_entries::{LastAppliedEntries, LastAppliedEntry},
        nodes::Nodes,
        raftlog::{RaftLog, RaftLogId},
        schema::Schema,
        snapshot_meta::{SnapshotMeta, SnapshotMetaRow},
    },
    db_conn,
    operation::{ClientToNodeOperation, NodeToNodeOperation, Operation},
    rsync::Rsync,
    CONFIG, RAFT,
};
use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{Entry, MembershipConfig},
    storage::InitialState,
    storage::{CurrentSnapshotData, HardState},
    ChangeConfigError, Config, NodeId, RaftStorage,
};
use chrono::offset::Utc;
use chrono::DateTime;
use futures::TryStreamExt;
use sqlx::{query, SqliteConnection};
use std::{fmt::Display, io::ErrorKind, iter::once, sync::Arc};
use thiserror::Error;
use tokio::fs::OpenOptions;

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
        let buff = tokio::fs::read(path).await?;
        let hardstate = bincode::deserialize(&buff)?;
        Ok(hardstate)
    }

    /// Handle a [ClientToNodeOperation], possibly mutating the file registry.
    async fn handle_client_operation(
        &self,
        op: &ClientToNodeOperation,
        conn: &mut SqliteConnection,
    ) -> Result<AppClientResponse> {
        todo!()
    }

    /// Handle a [NodeToNodeOperation], possibly mutating the file registry.
    async fn handle_node_operation(
        &self,
        op: &NodeToNodeOperation,
        conn: &mut SqliteConnection,
    ) -> Result<AppClientResponse> {
        use NodeToNodeOperation::*;
        match op {
            NodeLost {
                lost_node,
                last_contact,
            } => {
                Nodes::deactivate_node_by_id(conn, *lost_node).await?;
                Keepers::delete_keeper(conn, *lost_node).await?;

                let raft = &RAFT.get().unwrap();

                // Change the membership config of the Raft cluster to exclude `lost_node`
                loop {
                    let leader = raft.get_leader_or_wait().await;
                    if self.get_own_id() == leader {
                        let mut current_members = RaftLog::get_last_membership(conn)
                            .await?
                            .ok_or_else(|| anyhow!("Inconsistent raftlog: No membership found"))?
                            .members;
                        current_members.remove(lost_node);

                        // In the extremely rare scenario that the current leader was deposed in between `get_leader_or_wait`
                        // and performing this operation, we will request the new leader and try again
                        match raft.change_membership(current_members).await {
                            Ok(_) => break,
                            Err(err) => match err {
                                ChangeConfigError::NodeNotLeader(_) => {}
                                _ => {
                                    return Err(anyhow!(
                                        "Could not change membership config: {:#?}",
                                        err
                                    ))
                                }
                            },
                        }
                    } else {
                        // This node is not the leader, so the operation is finished
                        break;
                    }
                }

                Ok(AppClientResponse(Ok(format!(
                    "Last contact: {}",
                    DateTime::<Utc>::from(*last_contact)
                ))))
            }
            DeleteReplica { path, node_id } => {
                // Every node deregisters `node_id` as a keeper for this file
                Keepers::delete_keeper_for_file(conn, path.as_str(), *node_id).await?;
                // The keeper node additionally deletes the file from its filesystem
                if self.get_own_id() == *node_id {
                    tokio::fs::remove_file(path).await?;
                }
                Ok(AppClientResponse(Ok("".into())))
            }

            // This node asks a keeper node for the file, then replicates the file on its own filesystem
            StoreReplica { path, node_id } => {
                if self.get_own_id() != *node_id {
                    return Ok(AppClientResponse(Ok(format!(
                        "I (node_id: {}) am not the target of this operation",
                        node_id
                    ))));
                }
                // TODO: In case of `rsync` errors, try other keepers until we find one that works
                // TODO: If `rsync` tells us the file is not available, the keepers table lied to us. Update it and continue? Or shutdown the app because of inconsistency?

                // To spread read load, "randomly" select a keeper based on the id of this operation
                let keeper = Keepers::get_random_keeper_for_file(conn, path.as_str()).await?

                // TODO perhaps return a more structured error so the webdav client can notify a user a file has been lost
                // additionally we should not return `Err`, but `Ok(AppClientResponse(ClientError))`. Because from Raft's perspective,
                // this operation has been applied to the state machine successfully, but an error occurred outside of Raft.
                .ok_or_else(|| anyhow!("No keeper found for file {:#?}. This probably means the file has been lost.", path))?;
                Rsync::copy_from(keeper, path).await?;
                Keepers::add_keeper_for_file(conn, path.as_str(), *node_id).await?;
                Ok(AppClientResponse(Ok("".into())))
            }
            FileCommitFail {
                serial,
                failure_reason,
            } => todo!(),

            FileCommitSuccess {
                serial,
                hash,
                node_id,
                path,
            } => {
                Keepers::add_keeper_for_file(conn, path.as_str(), *node_id).await?;
                File::update_file_hash(conn, path, Some(*hash)).await?;
                Ok(AppClientResponse(Ok("".into())))
            }
            NodeJoin { node_id } => todo!(),
            NodeLeft { node_id } => todo!(),
        }
    }
}

#[async_trait]
impl RaftStorage<AppClientRequest, AppClientResponse> for AppRaftStorage {
    type Snapshot = tokio::fs::File;
    type ShutdownError = AppError;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        Ok(RaftLog::get_last_membership(db_conn!())
            .await?
            .unwrap_or_else(|| MembershipConfig::new_initial(self.get_own_id())))
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

        let last_applied_log = SnapshotMeta::get(db_conn!()).await?.last_applied_log;
        let (last_log_index, last_log_term) =
            RaftLog::get_last_log_entry_id_term(db_conn!()).await?.expect("Inconsistent `raftlog`: hardstate file was found but there were no entries in the raft log");

        let state = InitialState {
            hard_state,
            membership,
            last_applied_log,
            last_log_index,
            last_log_term,
        };

        Ok(state)
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        // TODO: Also just store in SQLite?
        let path = &CONFIG.hardstate_file;
        tokio::fs::write(path, &bincode::serialize(hs)?).await?;
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<AppClientRequest>>> {
        if start > stop {
            panic!(
                "Invalid request to `get_log_entries`, start ({:?}) > stop ({:?})",
                start, stop
            );
        }
        Ok(RaftLog::get_range(db_conn!(), start, stop).await?)
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        match stop {
            Some(stop) => RaftLog::delete_range(db_conn!(), start, stop).await,
            None => RaftLog::delete_from(db_conn!(), start).await,
        }?;
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<AppClientRequest>) -> Result<()> {
        RaftLog::insert(db_conn!(), std::slice::from_ref(entry)).await;
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<AppClientRequest>]) -> Result<()> {
        RaftLog::insert(db_conn!(), entries).await;
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &AppClientRequest,
    ) -> Result<AppClientResponse> {
        // If this node has already applied this entry to its state machine before, return the recorded response as-is
        // so we don't apply the entry twice.
        if let Some(LastAppliedEntry { id, contents }) =
            LastAppliedEntries::get(db_conn!(), data.client).await?
        {
            if id == data.serial {
                return Ok(contents);
            }
        }

        let mut tx = db().begin().await?;
        let response = match &data.operation {
            Operation::FromClient(op) => self.handle_client_operation(op, &mut tx).await,
            Operation::FromNode(op) => self.handle_node_operation(op, &mut tx).await,
        }?;
        SnapshotMeta::set_last_applied_entry(&mut tx, *index).await?;
        LastAppliedEntries::set(&mut tx, data.client, *index, &response).await?;
        tx.commit().await?;
        Ok(response)
    }

    async fn replicate_to_state_machine(
        &self,
        entries: &[(&u64, &AppClientRequest)],
    ) -> Result<()> {
        let mut tx = db().begin().await?;
        let mut entries = entries.iter().peekable();
        let mut last_entry_id = None;
        while let Some(&(id, data)) = entries.next() {
            let is_last_entry = entries.peek().is_none();
            last_entry_id = is_last_entry.then_some(*id);

            // See if this entry has already been applied, and if so, don't apply it again.
            if let Some(LastAppliedEntry { id, .. }) =
                LastAppliedEntries::get(db_conn!(), data.client).await?
            {
                if id == data.serial {
                    continue;
                }
            }

            let response = match &data.operation {
                Operation::FromClient(op) => self.handle_client_operation(op, &mut tx).await,
                Operation::FromNode(op) => self.handle_node_operation(op, &mut tx).await,
            }?;
            // Save the response to applying this entry, but don't return it
            LastAppliedEntries::set(&mut tx, data.client, *id, &response).await?;
        }

        // The last operation's id will be committed to the `snapshot_meta` table as the last one applied
        if let Some(last_entry_id) = last_entry_id {
            // N.B. this should always happen unless `entries` contained 0 entries
            SnapshotMeta::set_last_applied_entry(&mut tx, last_entry_id).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let mut tx = db().begin().await?;

        let SnapshotMetaRow {
            last_applied_log, ..
        } = SnapshotMeta::get(&mut tx).await?;

        // Get last known membership config from the log
        let membership = RaftLog::get_last_membership_before(&mut tx, last_applied_log)
            .await?
            .unwrap_or_else(|| MembershipConfig::new_initial(self.get_own_id()));

        let term = RaftLog::get_by_id(&mut tx,last_applied_log).await?.ok_or_else(|| {
            anyhow!("Inconsistent log: `last_applied_log` from the `{}` table was not found in the `{}` table", SnapshotMeta::TABLENAME, RaftLog::TABLENAME)
        })?.term;

        let snapshot_metadata = SnapshotMetaRow {
            term,
            last_applied_log,
            membership,
        };

        // TODO: Better error handling
        let snapshot = db::create_snapshot(&snapshot_metadata)
            .await
            .expect("Error creating snapshot");

        let snapshot = Box::new(snapshot);

        // Delete the Raft log entries up until the last applied log
        RaftLog::delete_range(&mut tx, 0, last_applied_log).await?;

        tx.commit().await?;

        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: snapshot_metadata.membership,
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
                    .expect("Error while creating blank snapshot file"),
            ),
        ))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        mut snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        // REVIEW (lieuwe): I am not sure if this is correct, actually.

        let tmp_path = {
            let path = CONFIG.wip_file_registry_snapshot();
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await?;
            tokio::io::copy(&mut snapshot, &mut file).await?;
            path
        };

        let mut tx = db().begin().await?;
        let membership = RaftLog::get_last_membership_before(&mut tx, index)
            .await?
            .unwrap_or_else(|| MembershipConfig::new_initial(self.get_own_id()));

        // Transfer most recent membership and log entries >`delete_through` from current db to the received `snapshot` db
        query(
            "
            ATTACH DATABASE ?1 AS snapshot;

            DELETE FROM snapshot.nodes;
            INSERT INTO snapshot.nodes SELECT * FROM nodes;

            DELETE FROM snapshot.raftlog;
            INSERT INTO snapshot.raftlog SELECT * FROM raftlog WHERE id > ?2 AND ?3;

            DETACH DATABASE snapshot;
        ",
        )
        .bind(tmp_path.as_str())
        .bind(delete_through.map(|id| id as i64).unwrap_or(-1))
        .bind(delete_through.is_some()) // HACK
        .execute_many(&mut tx)
        .await
        .try_for_each(|_| async move { Ok(()) })
        .await?;

        tokio::fs::rename(tmp_path, &CONFIG.file_registry_snapshot).await?;

        RaftLog::insert(
            &mut tx,
            once(&Entry::new_snapshot_pointer(index, term, id, membership)),
        )
        .await;

        tx.commit().await?;
        Ok(())
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        let SnapshotMetaRow {
            last_applied_log,
            term,
            membership,
        } = SnapshotMeta::get(&mut curr_snapshot().await?).await?;

        let file = match tokio::fs::File::open(&CONFIG.file_registry_snapshot).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => panic!(
                "Error reading snapshot file at {:#?}: {:#?}",
                CONFIG.file_registry_snapshot, err
            ),
        };

        Ok(Some(CurrentSnapshotData::<Self::Snapshot> {
            index: last_applied_log,
            membership,
            term,
            snapshot: Box::new(file),
        }))
    }
}
