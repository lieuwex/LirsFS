//! The Nodes table keeps track of every compute node in the LirsFs
//!

use anyhow::Result;
use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use sqlx::{query, SqliteConnection};

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodesRow {
    pub id: NodeId,
    pub active: bool,
    pub ssh_host: String,
}

pub struct Nodes;

impl Nodes {
    pub async fn deactivate_node_by_id(conn: &mut SqliteConnection, id: NodeId) -> Result<()> {
        let id = id as i64;
        query!(
            "
            UPDATE nodes
            SET active = 0
            WHERE id = ?
        ",
            id
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

impl Schema for Nodes {
    const TABLENAME: &'static str = "nodes";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_nodes.sql"))
    }
}
