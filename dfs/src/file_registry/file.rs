//! The File table holds information about every file in the LirsFs

use serde::{Deserialize, Serialize};
use sqlx::query;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]

pub struct File {
    pub file_id: i32,

    pub file_path: String,

    pub content_hash: u64,

    pub replication_factor: u32,
}

impl Schema for File {
    const TABLENAME: &'static str = "files";

    fn create_table_query() -> SqlxQuery {
        query(
            "
            CREATE TABLE IF NOT EXISTS ? (
                id                 integer primary key,
                path               text not null,
                hash               blob not null,
                replication_factor integer not null
            );
            ",
        )
        .bind(Self::TABLENAME)
    }
}
