use anyhow::Result;
use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use sqlx::{query, SqliteConnection};

use crate::{client_req::RequestId, client_res::AppClientResponse};

use super::{
    errors::raftlog_deserialize_error,
    raftlog::RaftLogId,
    schema::{Schema, SqlxQuery},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LastAppliedEntry {
    pub index: RaftLogId,
    pub request_id: RequestId,
    pub contents: AppClientResponse,
}

pub struct LastAppliedEntries;

impl LastAppliedEntries {
    pub async fn get(
        conn: &mut SqliteConnection,
        node_id: NodeId,
    ) -> Result<Option<LastAppliedEntry>> {
        let node_id = node_id as i64;
        query!(
            "
            SELECT *
            FROM last_applied_entries
            WHERE node_id = ?;
        ",
            node_id
        )
        .fetch_optional(conn)
        .await?
        .map(|record| {
            Ok(LastAppliedEntry {
                request_id: record.request_id.try_into()?,
                index: record.log_index as RaftLogId,
                contents: bincode::deserialize(&record.contents)
                    .map_err(raftlog_deserialize_error)?,
            })
        })
        .transpose()
    }

    pub async fn set(
        conn: &mut SqliteConnection,
        node_id: NodeId,
        entry_index: RaftLogId,
        request_id: RequestId,
        contents: &AppClientResponse,
    ) -> Result<()> {
        let node_id = node_id as i64;
        let request_id = request_id.0.as_bytes().as_slice();
        let entry_index = entry_index as i64;
        let contents_serialized = bincode::serialize(contents)?;
        query!(
            "
            INSERT INTO last_applied_entries (node_id, log_index, request_id, contents)
            VALUES(?, ?, ?, ?)
        ",
            node_id,
            entry_index,
            request_id,
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
