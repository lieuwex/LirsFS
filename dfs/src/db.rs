pub mod file;
pub mod keepers;
pub mod node;
pub mod raftlog;
pub mod schema;

use sqlx::SqlitePool;

#[derive(Debug)]
pub struct Database {
    pub pool: SqlitePool,
}
