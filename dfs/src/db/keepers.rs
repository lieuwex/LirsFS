//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use async_raft::NodeId;
use camino::Utf8PathBuf;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{query, sqlite::SqliteRow, Row, SqliteConnection};

use crate::util::blob_to_hash;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepersRow {
    pub id: i64,
    pub path: PathBuf,
    pub node_id: NodeId,
    pub hash: u64,
}

pub struct Keepers;

// TODO: Fix duplication in handling UTF8 errors in file paths
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
        .map(|record| record.node_id as NodeId)
        .fetch(conn)
        .try_collect()
        .await?;
        Ok(keeper_nodes)
    }

    /// Return the node id for a keeper of the file indicated by `path`,
    /// or `None` if there is no keeper for this file.
    pub async fn get_random_keeper_for_file(
        conn: &mut SqliteConnection,
        file: &Path,
    ) -> Result<Option<NodeId>> {
        let filepath = file
            .to_str()
            .ok_or_else(|| anyhow!("Invalid argument `filepath`: Contained non-UTF8 characters"))?;

        let record = query!(
            "
            SELECT id
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
            Ok(Some(record.id as NodeId))
        } else {
            Ok(None)
        }
    }

    pub async fn get_by_path(conn: &mut SqliteConnection, path: &str) -> Result<Vec<KeepersRow>> {
        let res: Vec<_> = query("SELECT keepers.* FROM keepers where path = ?1")
            .bind(path)
            .fetch(conn)
            .map(|r| anyhow::Ok(r?))
            .and_then(|row: SqliteRow| async move {
                Ok(KeepersRow {
                    id: row.get("id"),
                    path: {
                        let s: String = row.get("path");
                        PathBuf::from(s)
                    },
                    node_id: {
                        let id: i64 = row.get("node_id");
                        u64::try_from(id)?
                    },

                    hash: blob_to_hash(row.get("hash"))?,
                })
            })
            .try_collect()
            .await?;
        Ok(res)
    }

    pub async fn add_keeper_for_file(
        conn: &mut SqliteConnection,
        file: &str,
        node_id: NodeId,
    ) -> Result<()> {
        let node_id = node_id as i64;
        query!(
            "
            INSERT INTO keepers(path, node_id)
            VALUES (?, ?);
        ",
            file,
            node_id
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

impl Schema for Keepers {
    const TABLENAME: &'static str = "keepers";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_keepers.sql"))
    }
}
