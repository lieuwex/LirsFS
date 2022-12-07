use super::{
    file::File, keepers::Keepers, node::Node, raftlog::RaftLog, snapshot_meta::SnapshotMeta,
};
use async_raft::async_trait::async_trait;
use sqlx::{sqlite::SqliteQueryResult, SqlitePool};

pub type SqlxResult = Result<SqliteQueryResult, sqlx::Error>;

pub type SqlxQuery =
    sqlx::query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>>;

#[async_trait]
pub trait Schema {
    const TABLENAME: &'static str;

    fn create_table_query() -> SqlxQuery;

    fn with(db: &SqlitePool) -> Self;
}

async fn create_table<'a, T>(exec: &SqlitePool) -> SqliteQueryResult
where
    T: Schema,
{
    T::create_table_query()
        .execute(exec)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}

pub async fn create_all_tables(exec: &SqlitePool) {
    create_table::<File>(exec).await;
    create_table::<Node>(exec).await;
    create_table::<Keepers>(exec).await;
    create_table::<RaftLog>(exec).await;
    create_table::<SnapshotMeta>(exec).await;
}
