//! The Nodes table keeps track of every compute node in the LirsFs
//!

use anyhow::Result;
use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use sqlx::{query, SqliteConnection};

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Active = 0,
    Inactive = 1,
    Lost = 2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodesRow {
    pub id: NodeId,
    pub status: NodeStatus,
}

pub struct Nodes;

impl Nodes {
    pub async fn set_node_status_by_id(
        conn: &mut SqliteConnection,
        id: NodeId,
        new_status: NodeStatus,
    ) -> Result<()> {
        let id = id as i64;
        let new_status = new_status as i8;
        query!(
            "
            UPDATE nodes
            SET status = ?
            WHERE id = ?
        ",
            new_status,
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
