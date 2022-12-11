use async_raft::raft::MembershipConfig;
use serde::{Deserialize, Serialize};
use sqlx::{query, Pool, Sqlite, SqliteConnection, SqlitePool};

use super::{
    raftlog::{RaftLogId, RaftLogTerm},
    schema::Schema,
    Database,
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
    pub async fn get(conn: &mut SqliteConnection) -> SnapshotMetaRow {
        let record = query!(
            "
            SELECT * 
            FROM snapshot_meta;
        "
        )
        .fetch_one(conn)
        .await
        .unwrap_or_else(|err| panic!("Error retrieving snapshot data from db: {:#?}", err));

        SnapshotMetaRow {
            term: record.term as RaftLogTerm,
            last_applied_log: record.last_applied_log as RaftLogId,
            // TODO: Better error handling
            membership: bincode::deserialize(&record.membership)
                .expect("Deserializaton error for membership"),
        }
    }
}

impl Schema for SnapshotMeta {
    const TABLENAME: &'static str = "snapshot_meta";

    fn create_table_query() -> super::schema::SqlxQuery {
        query(include_str!("../../sql/create_snapshot_meta.sql"))
    }
}
