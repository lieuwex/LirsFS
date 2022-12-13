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
    pub async fn get_keeper_ids_for_file(
        conn: &mut SqliteConnection,
        file: &Path,
    ) -> Result<Vec<NodeId>> {
        let filepath = file
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

    /// Return the ssh host for a keeper of the file indicated by `path`,
    /// or `None` if there is no keeper for this file.
    pub async fn get_random_keeper_for_file(
        conn: &mut SqliteConnection,
        file: &Path,
    ) -> Result<Option<String>> {
        let filepath = file
            .to_str()
            .ok_or_else(|| anyhow!("Invalid argument `filepath`: Contained non-UTF8 characters"))?;

        let record = query!(
            "
            SELECT *
            FROM nodes
            WHERE id IN (
                SELECT node_id
                FROM keepers
                WHERE path = ?
                ORDER BY RANDOM()
                LIMIT 1
            );
        ",
            filepath
        )
        .fetch_optional(conn)
        .await?;
        if let Some(record) = record {
            Ok(Some(record.ssh_host))
        } else {
            Ok(None)
        }
    }
}

impl Schema for Keepers {
    const TABLENAME: &'static str = "keepers";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_keepers.sql"))
    }
}
