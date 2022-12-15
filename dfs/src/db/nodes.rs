//! The Nodes table keeps track of every compute node in the LirsFs
//!

use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use sqlx::query;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodesRow {
    pub id: NodeId,
}

pub struct Nodes;

impl Schema for Nodes {
    const TABLENAME: &'static str = "nodes";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_nodes.sql"))
    }
}
