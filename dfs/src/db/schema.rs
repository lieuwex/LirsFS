use crate::DB;

use super::{file::File, keepers::Keepers, node::Node};
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

async fn create_table<'a, T, E>(exec: &'a E) -> SqliteQueryResult
where
    T: Schema,
    &'a E: SqliteExecutor<'a>,
{
    T::create_table_query()
        .execute(exec)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}

pub async fn create_all_tables<'a, E>(exec: &'a E)
where
    &'a E: SqliteExecutor<'a>,
{
    create_table::<File, _>(exec).await;
    create_table::<Node, _>(exec).await;
    create_table::<Keepers, _>(exec).await;
}

/// Return the global instance of the SQLite database pool
pub fn db() -> impl SqliteExecutor<'static> {
    &DB.get().unwrap().pool
}
