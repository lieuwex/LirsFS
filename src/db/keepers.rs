//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

use anyhow::Result;
use async_raft::NodeId;
use camino::{Utf8Path, Utf8PathBuf};
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{query, sqlite::SqliteRow, Row, SqliteConnection};

use super::nodes::NodeStatus;
use crate::{filesystem::FileContentHash, util::blob_to_hash, RAFT};

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepersRow {
    pub id: i64,
    pub path: Utf8PathBuf,
    pub node_id: NodeId,
    pub hash: FileContentHash,
}

pub struct Keepers;

impl Keepers {
    pub async fn get_keeper_ids_for_file(
        conn: &mut SqliteConnection,
        file: &Utf8Path,
    ) -> Result<Vec<NodeId>> {
        let filepath = file.as_str();

        let keeper_nodes = query!(
            "
            SELECT node_id
            FROM keepers
            INNER JOIN nodes
                ON nodes.id = keepers.node_id
            WHERE path = ? AND status = ?
        ",
            filepath,
            NodeStatus::Active as i8
        )
        .map(|record| record.node_id as NodeId)
        .fetch(conn)
        .try_collect()
        .await?;
        Ok(keeper_nodes)
    }

    /// Return the node id for a keeper of the file indicated by `path`,
    /// or `None` if there is no active keeper for this file.
    pub async fn get_random_keeper_for_file(
        conn: &mut SqliteConnection,
        file: &str,
    ) -> Result<Option<NodeId>> {
        let record = query!(
            "
            SELECT id
            FROM nodes
            WHERE id IN (
                SELECT node_id
                FROM keepers
                WHERE path = ? AND status = ?
                ORDER BY RANDOM()
                LIMIT 1
            );
        ",
            file,
            NodeStatus::Active as i8
        )
        .fetch_optional(conn)
        .await?;
        if let Some(record) = record {
            Ok(Some(record.id as NodeId))
        } else {
            Ok(None)
        }
    }

    /// Returns all keepers for the file at `path`. Note that keepers may not be `active` (see [NodeStatus]).
    pub async fn get_by_path(
        conn: &mut SqliteConnection,
        path: &Utf8Path,
    ) -> Result<Vec<KeepersRow>> {
        let res: Vec<_> = query(
            "
            SELECT keepers.*
            FROM keepers
            JOIN nodes
                ON nodes.id = keepers.node_id
            WHERE path = ?1
            ",
        )
        .bind(path.as_str())
        .fetch(conn)
        .map(|r| anyhow::Ok(r?))
        .and_then(|row: SqliteRow| async move {
            Ok(KeepersRow {
                id: row.get("id"),
                path: {
                    let s: String = row.get("path");
                    Utf8PathBuf::from(s)
                },
                node_id: {
                    let id: i64 = row.get("node_id");
                    NodeId::try_from(id)?
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

    pub async fn delete_keeper_for_file(
        conn: &mut SqliteConnection,
        file: &str,
        node_id: NodeId,
    ) -> Result<()> {
        let node_id = node_id as i64;
        query!(
            "
            DELETE FROM keepers
            WHERE node_id = ? AND path = ?
        ",
            node_id,
            file
        )
        .execute(conn)
        .await?;
        Ok(())
    }

    /// Return whether or not we are a keeper of the file at `file_path`.
    pub async fn is_self_keeper(conn: &mut SqliteConnection, file_path: &Utf8Path) -> Result<bool> {
        let raft = RAFT.get().unwrap();
        let own_id = raft.metrics().borrow().id;

        let ids = Self::get_keeper_ids_for_file(conn, file_path).await?;
        Ok(ids.contains(&own_id))
    }

    /// Permanently delete a keeper, e.g. because the node has died.
    pub async fn delete_keeper(conn: &mut SqliteConnection, node_id: NodeId) -> Result<()> {
        let node_id = node_id as i64;
        query!(
            "
            DELETE FROM keepers
            WHERE node_id = ?
        ",
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
