pub mod file;
pub mod keepers;
pub mod node;
pub mod raftlog;
pub mod schema;
pub mod snapshot_meta;

use std::{borrow::Borrow, ops::Deref, path::Path};

use anyhow::{anyhow, Result};
use sqlx::{query, Connection, SqliteConnection, SqlitePool};

use crate::{CONFIG, DB};

use self::snapshot_meta::SnapshotMetaRow;

#[derive(Debug)]
pub struct Database {
    pub pool: SqlitePool,
}

impl Database {
    pub(self) fn map_path(path: &Path) -> Result<String> {
        let path = path.to_str().ok_or_else(|| anyhow!("invalid path"))?;
        let conn_str = format!("sqlite://{}", path);
        Ok(conn_str)
    }

    pub async fn from_path(path: &Path) -> Result<Self> {
        let pool = SqlitePool::connect(&Self::map_path(path)?).await?;
        Ok(Database { pool })
    }
}

impl Deref for Database {
    type Target = SqlitePool;

    fn deref(&self) -> &Self::Target {
        self.pool.borrow()
    }
}

/// Return the global instance of the SQLite database pool for the file registry
pub fn db() -> &'static Database {
    DB.get().unwrap().borrow()
}

/// Return the global instance of the SQLite database pool for the currently active snapshot
pub async fn curr_snapshot() -> Result<Database> {
    let db = Database::from_path(&CONFIG.file_registry_snapshot).await?;
    Ok(db)
}

/// Create a snapshot of the file registry into the working snapshot file location.
/// Returns a read-only file handle to the created snapshot.
pub async fn create_snapshot(snapshot_metadata: &SnapshotMetaRow) -> Result<tokio::fs::File> {
    let wip_snapshot_path = CONFIG.wip_file_registry_snapshot();
    let wip_snapshot_path = wip_snapshot_path
        .to_str()
        .ok_or_else(|| anyhow!("invalid path string"))?;

    query!("VACUUM INTO ?", wip_snapshot_path)
        .execute(db().deref())
        .await?;

    let mut conn = SqliteConnection::connect(&format!("sqlite://{}", wip_snapshot_path)).await?;
    query!("DELETE FROM raftlog").execute(&mut conn).await?;

    // Save the snapshot metadata to the newly generated snapshot
    let SnapshotMetaRow {
        last_applied_log,
        membership,
        term,
    } = snapshot_metadata;
    let term = *term as i64;
    let last_applied_log = *last_applied_log as i64;
    let membership_serialized = bincode::serialize(membership)?;
    query!(
        "
        REPLACE INTO snapshot_meta (id, term, last_applied_log, membership)
        VALUES (?, ?, ?, ?);
    ",
        0_i64,
        term,
        last_applied_log,
        membership_serialized
    )
    .execute(&mut conn)
    .await?;

    Ok(tokio::fs::File::open(wip_snapshot_path).await?)
}
