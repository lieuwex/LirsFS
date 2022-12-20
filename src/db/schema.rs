use super::{
    file::File, keepers::Keepers, nodes::Nodes, raftlog::RaftLog, snapshot_meta::SnapshotMeta,
    Database,
};
use async_raft::async_trait::async_trait;
use sqlx::{sqlite::SqliteQueryResult, SqliteConnection};

pub type SqlxResult = Result<SqliteQueryResult, sqlx::Error>;

pub type SqlxQuery =
    sqlx::query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>>;

#[async_trait]
pub trait Schema {
    const TABLENAME: &'static str;

    fn create_table_query() -> SqlxQuery;
}

async fn create_table<T>(exec: &mut SqliteConnection) -> SqliteQueryResult
where
    T: Schema,
{
    T::create_table_query()
        .execute(exec)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}

pub async fn create_all_tables(db: &Database) {
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("couldn't acquire connection");

    create_table::<File>(&mut conn).await;
    create_table::<Nodes>(&mut conn).await;
    create_table::<Keepers>(&mut conn).await;
    create_table::<RaftLog>(&mut conn).await;
    create_table::<SnapshotMeta>(&mut conn).await;
}
