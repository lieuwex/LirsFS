pub mod errors;
pub mod file;
pub mod keepers;
pub mod last_applied_entries;
pub mod nodes;
pub mod outstanding_writes;
pub mod raftlog;
pub mod schema;
pub mod snapshot_meta;

use std::{borrow::Borrow, ops::Deref};

use anyhow::Result;
use camino::Utf8Path;
use sqlx::{query, Connection, SqliteConnection, SqlitePool};

use crate::{CONFIG, DB};

use self::snapshot_meta::SnapshotMetaRow;

// TODO: find a way to detect db corruptions, not only hashes for the log would be nice, but for
// the whole db.

#[derive(Debug)]
pub struct Database {
    pub pool: SqlitePool,
}

impl Database {
    pub(self) fn map_path(path: &Utf8Path) -> String {
        format!("sqlite://{}", path.as_str())
    }

    #[tracing::instrument(level = "trace")]
    pub async fn from_path(path: &Utf8Path) -> Result<Self> {
        let pool = SqlitePool::connect(&Self::map_path(path)).await?;
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

/// Acquire a database connection from the global connection pool.
#[macro_export]
macro_rules! db_conn {
    () => {
        ::std::borrow::BorrowMut::borrow_mut(
            &mut db().acquire().await.expect("couldn't acquire connection"),
        )
    };
}

/// Open an SqliteConnection for the currently active snapshot
pub async fn curr_snapshot() -> Result<SqliteConnection> {
    let path = &CONFIG.file_registry_snapshot;
    let conn = SqliteConnection::connect(&Database::map_path(path)).await?;
    Ok(conn)
}

/// Create a snapshot of the file registry into the working snapshot file location.
/// Returns a read-only file handle to the created snapshot.
pub async fn create_snapshot(snapshot_metadata: &SnapshotMetaRow) -> Result<tokio::fs::File> {
    let wip_snapshot_path = CONFIG.wip_file_registry_snapshot();
    let wip_snapshot_path = wip_snapshot_path.as_str();

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
