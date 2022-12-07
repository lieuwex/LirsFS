//! The Node table keeps track of every compute node in the LirsFs
//!

use serde::{Deserialize, Serialize};
use sqlx::query;

use super::{
    schema::{Schema, SqlxQuery},
    Database,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    pub node_id: i32,

    pub name: String,
}

pub struct Node<'a>(&'a Database);

impl<'a> Schema<'a> for Node<'a> {
    const TABLENAME: &'static str = "nodes";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_nodes.sql"))
    }

    fn with(db: &'a Database) -> Self {
        Self(db)
    }
}
