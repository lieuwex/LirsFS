use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{query, SqliteConnection};

use crate::client_res::AppClientResponse;

use super::{
    errors::raftlog_deserialize_error,
    raftlog::RaftLogId,
    schema::{Schema, SqlxQuery},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LastAppliedEntry {
    pub id: RaftLogId,
    pub contents: AppClientResponse,
}

pub struct LastAppliedEntries;

impl LastAppliedEntries {
    pub async fn get(
        conn: &mut SqliteConnection,
        node_name: String,
    ) -> Result<Option<LastAppliedEntry>> {
        query!(
            "
            SELECT *
            FROM last_applied_entries
            WHERE node_name = ?;
        ",
            node_name
        )
        .fetch_optional(conn)
        .await?
        .map(|record| {
            Ok(LastAppliedEntry {
                id: record.last_entry_id as u64,
                contents: bincode::deserialize(&record.last_entry_contents)
                    .map_err(raftlog_deserialize_error)?,
            })
        })
        .transpose()
    }

    pub async fn set(
        conn: &mut SqliteConnection,
        node_name: String,
        id: RaftLogId,
        contents: &AppClientResponse,
    ) -> Result<()> {
        let id = id as i64;
        let contents_serialized = bincode::serialize(contents)?;
        query!(
            "
            INSERT INTO last_applied_entries
            VALUES(?, ?, ?)
        ",
            node_name,
            id,
            contents_serialized
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

impl Schema for LastAppliedEntries {
    const TABLENAME: &'static str = "last_applied_entries";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_last_applied_entries.sql"))
    }
}
