use anyhow::{anyhow, Result};
use async_raft::raft::MembershipConfig;
use serde::{Deserialize, Serialize};
use sqlx::{query, SqliteConnection};

use super::{
    errors::raftlog_deserialize_error,
    raftlog::{RaftLogId, RaftLogTerm},
    schema::Schema,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetaRow {
    pub term: RaftLogTerm,
    pub last_applied_log: RaftLogId,
    pub membership: MembershipConfig,
}

#[derive(Clone, Debug)]
pub struct SnapshotMeta;

impl SnapshotMeta {
    pub async fn get(conn: &mut SqliteConnection) -> Result<SnapshotMetaRow> {
        let record = query!(
            "
            SELECT * 
            FROM snapshot_meta;
        "
        )
        .fetch_one(conn)
        .await
        .map_err(|err| anyhow!("Error retrieving snapshot data from db: {:#?}", err))?;

        let membership =
            bincode::deserialize(&record.membership).map_err(raftlog_deserialize_error)?;

        Ok(SnapshotMetaRow {
            term: record.term as RaftLogTerm,
            last_applied_log: record.last_applied_log as RaftLogId,
            membership,
        })
    }

    pub async fn set_last_applied_entry(
        conn: &mut SqliteConnection,
        entry: RaftLogId,
    ) -> Result<()> {
        let entry = entry as i64;
        let res = query!(
            "
            UPDATE snapshot_meta
            SET last_applied_log = ?
            WHERE id = 1;
        ",
            entry
        )
        .execute(conn)
        .await?;
        assert_eq!(res.rows_affected(), 1);

        Ok(())
    }
}

impl Schema for SnapshotMeta {
    const TABLENAME: &'static str = "snapshot_meta";

    fn create_table_query() -> super::schema::SqlxQuery {
        query(include_str!("../../sql/create_snapshot_meta.sql"))
    }
}
