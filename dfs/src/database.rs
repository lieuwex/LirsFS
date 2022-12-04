use sqlx::SqlitePool;

#[derive(Debug)]
pub struct Database {
    pub pool: SqlitePool,
}
