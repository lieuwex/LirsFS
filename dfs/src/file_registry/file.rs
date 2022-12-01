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
        query(include_str!("../../sql/create_files.sql"))
    }
}

// impl File {
//     pub async fn get_by_path(path: String, conn: &mut SqlitePool) -> Self {
//         let x = query!(
//             "
//             SELECT id, path, hash, replication_factor
//             FROM files
//             WHERE path = ?;
//             ",
//             path
//         );
//         x
//         // .fetch_one(conn)
//         // .await;
//     }
// }
