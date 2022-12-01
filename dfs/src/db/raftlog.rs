use serde::{Deserialize, Serialize};
use sqlx::query;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLog {
    pub node_id: i32,

    pub name: String,
}

impl Schema for RaftLog {
    const TABLENAME: &'static str = "raftlog";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_raftlog.sql"))
    }
}
