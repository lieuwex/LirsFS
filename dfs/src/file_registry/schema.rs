use async_raft::async_trait::async_trait;
use sqlx::{sqlite::SqliteQueryResult, SqlitePool};

pub type SqlxResult = Result<SqliteQueryResult, sqlx::Error>;

pub type SqlxQuery =
    sqlx::query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>>;

#[async_trait]
pub trait Schema {
    const TABLENAME: &'static str;

    fn create_table_query() -> SqlxQuery;
}

async fn create_table<T: Schema>(pool: SqlitePool) -> SqliteQueryResult {
    T::create_table_query()
        .execute(&pool)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}
