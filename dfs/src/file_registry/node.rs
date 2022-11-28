//! The Node table keeps track of every compute node in the LirsFs
//!

use sqlx::query;

use super::schema::Schema;

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub node_id: i32,

    pub name: String,
}

impl Schema for Node {
    const TABLENAME: &'static str = "nodes";

    fn create_table_query(
    ) -> sqlx::query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>> {
        query(
            "
            CREATE TABLE IF NOT EXISTS ? (
                id integer primary key,
                name text not null,
            );
        ",
        )
        .bind(Self::TABLENAME)
    }
}
