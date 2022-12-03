use serde::{Deserialize, Serialize};
use sqlx::query;

use super::schema::{db, Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLog {
    pub node_id: i32,

    pub name: String,
}

impl RaftLog {
    pub async fn delete_range(from: u32, to: u32) {
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ? AND id <= ?;",
            from,
            to
        )
        .execute(db())
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Could not delete id range {}-{} from {}: {}",
                from,
                to,
                Self::TABLENAME,
                err
            )
        });
    }

    pub async fn delete_from(from: u32) {
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ?;
        ",
            from
        )
        .execute(db())
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Could not delete from id {} from {}: {}",
                from,
                Self::TABLENAME,
                err
            )
        });
    }
}

impl Schema for RaftLog {
    const TABLENAME: &'static str = "raftlog";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_raftlog.sql"))
    }
}
