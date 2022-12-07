pub mod file;
pub mod keepers;
pub mod node;
pub mod raftlog;
pub mod schema;
pub mod snapshot_meta;

use std::{borrow::Borrow, path::Path};

use anyhow::{anyhow, Result};
use sqlx::{query, Connection, Pool, Sqlite, SqliteConnection, SqlitePool};

use crate::{CONFIG, DB};

use self::snapshot_meta::SnapshotMetaRow;

#[derive(Debug)]
pub struct Database {
    pub pool: SqlitePool,
}

impl Database {
    pub async fn from_path(path: &Path) -> Result<Self> {
        let path = path.to_str().ok_or_else(|| anyhow!("invalid path"))?;
        let pool = SqlitePool::connect(&format!("sqlite://{}", path)).await?;
        Ok(Database { pool })
    }
}

impl AsRef<SqlitePool> for Database {
    fn as_ref(&self) -> &SqlitePool {
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
pub async fn create_snapshot(_: SnapshotMetaRow) -> Result<()> {
    let wip_snapshot_path = CONFIG.wip_file_registry_snapshot();
    let wip_snapshot_path = wip_snapshot_path
        .to_str()
        .ok_or_else(|| anyhow!("invalid path string"))?;

    query!("VACUUM INTO ?", wip_snapshot_path)
        .execute(db().as_ref())
        .await?;

    let mut conn = SqliteConnection::connect(&format!("sqlite://{}", wip_snapshot_path)).await?;
    query!("DELETE FROM raftlog").execute(&mut conn).await?;
    Ok(())
}
