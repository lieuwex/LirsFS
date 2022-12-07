pub mod file;
pub mod keepers;
pub mod node;
pub mod raftlog;
pub mod schema;
pub mod snapshot_meta;

use sqlx::{query, sqlite::SqlitePoolOptions, Pool, Sqlite, SqlitePool};

use crate::{CONFIG, DB, SNAPSHOT};

use self::{
    file::File,
    keepers::Keepers,
    node::Node,
    schema::{create_all_tables, Schema},
    snapshot_meta::SnapshotMetaRow,
};

#[derive(Debug)]
pub struct Database {
    pub pool: SqlitePool,
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
