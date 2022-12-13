//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

use anyhow::Result;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{query, sqlite::SqliteRow, Row, SqliteConnection};

use crate::util::blob_to_hash;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepersRow {
    pub id: i32,
    pub file_id: i32,
    pub node_id: i32,

    pub hash: u64,
}

pub struct Keepers;

impl Keepers {
    pub async fn get_by_path(conn: &mut SqliteConnection, path: &str) -> Result<Vec<KeepersRow>> {
        let res: Vec<_> = query("SELECT keepers.* FROM keepers JOIN files ON files.id = file_id")
            .fetch(conn)
            .map(|r| anyhow::Ok(r?))
            .and_then(|row: SqliteRow| async move {
                Ok(KeepersRow {
                    id: row.get("id"),
                    file_id: row.get("file_id"),
                    node_id: row.get("node_id"),

                    hash: blob_to_hash(row.get("hash"))?,
                })
            })
            .try_collect()
            .await?;
        Ok(res)
    }
}

impl Schema for Keepers {
    const TABLENAME: &'static str = "keepers";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_keepers.sql"))
    }
}
