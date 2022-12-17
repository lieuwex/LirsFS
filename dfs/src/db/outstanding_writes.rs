use anyhow::Result;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sqlx::{query, Error, SqliteConnection};

use crate::client_req::RequestId;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutstandingWriteRow {
    pub request_id: RequestId,
    pub file_path: Utf8PathBuf,
    pub node_id: i64,
}

pub struct OutstandingWrites;

impl OutstandingWrites {
    pub async fn get_all(conn: &mut SqliteConnection) -> Result<Vec<OutstandingWriteRow>> {
        let res = query!("SELECT request_id, file_path, node_id FROM outstanding_writes")
            .try_map(|r| {
                Ok(OutstandingWriteRow {
                    request_id: r.serial,
                    file_path: Utf8PathBuf::from(r.file_path),
                    node_id: r.node_id,
                })
            })
            .fetch_all(conn)
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
