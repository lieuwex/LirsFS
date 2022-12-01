use sqlx::SqlitePool;

pub struct Database {
    pub pool: SqlitePool,
}
