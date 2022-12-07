pub mod file;
pub mod keepers;
pub mod node;
pub mod raftlog;
pub mod schema;
pub mod snapshot_meta;

use std::path::Path;

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

/// Return the global instance of the SQLite database pool for the file registry
pub fn db() -> Pool<Sqlite> {
    DB.get().unwrap().pool.clone()
}

/// Return the global instance of the SQLite database pool for the currently active snapshot
pub fn curr_snapshot() -> Pool<Sqlite> {
    SNAPSHOT.get().unwrap().pool.clone()
}

pub async fn create_snapshot(snapshot_metadata: SnapshotMetaRow) -> SqlitePool {
    let current_db = &CONFIG.file_registry;
    let wip_snapshot_path = CONFIG.wip_file_registry_snapshot();

    // TODO: Error handling
    tokio::fs::copy(current_db, &wip_snapshot_path)
        .await
        .unwrap();

    let pool = SqlitePool::connect(&("sqlite://".to_owned() + &wip_snapshot_path))
        .await
        .unwrap();

    todo!("Either copy entire db and delete raft entries, OR use attach + insert into");
}
