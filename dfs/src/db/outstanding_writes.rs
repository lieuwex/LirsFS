use serde::{Deserialize, Serialize};
use sqlx::query;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutstandingWriteRow {
    pub serial: u64,
    pub file_id: i64,
    pub node_id: i64,
}

pub struct OutstandingWrites;

impl Schema for OutstandingWrites {
    const TABLENAME: &'static str = "outstanding_writes";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/outstanding_writes.sql"))
    }
}
