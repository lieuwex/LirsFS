use async_raft::async_trait::async_trait;
use sqlx::{
    query::Query,
    sqlite::{SqliteArguments, SqliteQueryResult},
    Sqlite, SqlitePool,
};

pub type SqlxResult = Result<SqliteQueryResult, sqlx::Error>;

#[async_trait]
pub trait Schema {
    const TABLENAME: &'static str;

    fn create_table_query() -> Query<'static, Sqlite, SqliteArguments<'static>>;
}

async fn create_table<T: Schema>(pool: SqlitePool) -> SqliteQueryResult {
    T::create_table_query()
        .execute(&pool)
        .await
        .unwrap_or_else(|err| panic!("Error creating table `{}`: {}", T::TABLENAME, err))
}
