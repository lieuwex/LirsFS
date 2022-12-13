//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use sqlx::{query, SqliteConnection};

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepersRow {
    pub path: PathBuf,
    pub node_id: NodeId,
}

pub struct Keepers;

impl Keepers {
    pub async fn get_keepers_for_file(
        conn: &mut SqliteConnection,
        filepath: &Path,
    ) -> Result<Vec<NodeId>> {
        let filepath = filepath
            .to_str()
            .ok_or_else(|| anyhow!("Invalid argument `filepath`: Contained non-UTF8 characters"))?;

        let keeper_nodes = query!(
            "
            SELECT node_id
            FROM keepers
            WHERE path = ?
        ",
            filepath
        )
        .fetch_all(conn)
        .await?
        .iter()
        .map(|record| record.node_id as NodeId)
        .collect();
        Ok(keeper_nodes)
    }
}

impl Schema for Keepers {
    const TABLENAME: &'static str = "keepers";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_keepers.sql"))
    }
}
