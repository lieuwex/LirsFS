//! The Node table keeps track of every compute node in the LirsFs
//!

use serde::{Deserialize, Serialize};
use sqlx::{query, SqlitePool};

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    pub node_id: i32,

    pub name: String,
}

pub struct Node(SqlitePool);

impl Schema for Node {
    const TABLENAME: &'static str = "nodes";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_nodes.sql"))
    }

    fn with(db: &SqlitePool) -> Self {
        Self(db.clone())
    }
}
