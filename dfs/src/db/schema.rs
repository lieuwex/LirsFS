use super::{
    file::File, keepers::Keepers, node::Node, raftlog::RaftLog, snapshot_meta::SnapshotMeta,
    Database,
};
use async_raft::async_trait::async_trait;
use sqlx::{sqlite::SqliteQueryResult, Executor};

pub type SqlxResult = Result<SqliteQueryResult, sqlx::Error>;

pub type SqlxQuery =
    sqlx::query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>>;

#[async_trait]
pub trait Schema<'a> {
    const TABLENAME: &'static str;

    fn create_table_query() -> SqlxQuery;

    fn with(db: &'a Database) -> Self;
}

async fn create_table<'a, 'b, T>(exec: &'a Database) -> SqliteQueryResult
where
    T: Schema<'b>,
{
    T::create_table_query()
        .execute(&exec.pool)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}

pub async fn create_all_tables(exec: &Database) {
    create_table::<File>(exec).await;
    create_table::<Node>(exec).await;
    create_table::<Keepers>(exec).await;
    create_table::<RaftLog>(exec).await;
    create_table::<SnapshotMeta>(exec).await;
}
