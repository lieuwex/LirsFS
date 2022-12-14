use anyhow::Result;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{query, sqlite::SqliteRow, Row, SqliteConnection};

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutstandingWriteRow {
    pub serial: u64,
    pub file_id: i64,
    pub node_id: i64,
}

pub struct OutstandingWrites;

impl OutstandingWrites {
    pub async fn get_all(conn: &mut SqliteConnection) -> Result<Vec<OutstandingWriteRow>> {
        let res = query("SELECT serial, file_id, node_id FROM outstanding_writes")
            .fetch(conn)
            .map(|r| anyhow::Ok(r?))
            .and_then(|row: SqliteRow| async move {
                Ok(OutstandingWriteRow {
                    serial: {
                        let val: i64 = row.get("serial");
                        u64::try_from(val)?
                    },
                    file_id: row.get("file_id"),
                    node_id: row.get("node_id"),
                })
            })
            .try_collect()
            .await?;
        Ok(res)
    }
}

impl Schema for OutstandingWrites {
    const TABLENAME: &'static str = "outstanding_writes";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_outstanding_writes.sql"))
    }
}
