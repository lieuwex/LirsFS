use async_raft::async_trait::async_trait;
use sqlx::{sqlite::SqliteQueryResult, SqliteExecutor};

pub type SqlxResult = Result<SqliteQueryResult, sqlx::Error>;

pub type SqlxQuery =
    sqlx::query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>>;

#[async_trait]
pub trait Schema {
    const TABLENAME: &'static str;

    fn create_table_query() -> SqlxQuery;
}

async fn create_table<'a, T, E>(exec: E) -> SqliteQueryResult
where
    T: Schema,
    E: SqliteExecutor<'a>,
{
    T::create_table_query()
        .execute(exec)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}
