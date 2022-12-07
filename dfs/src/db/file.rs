//! The File table holds information about every file in the LirsFs

use serde::{Deserialize, Serialize};
use sqlx::query;

use super::{
    schema::{Schema, SqlxQuery},
    Database,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]

pub struct FileRow {
    pub file_id: i32,

    pub file_path: String,

    pub content_hash: u64,

    pub replication_factor: u32,
}

#[derive(Clone, Debug)]
pub struct File<'a>(&'a Database);

impl<'a> Schema<'a> for File<'a> {
    const TABLENAME: &'static str = "files";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_files.sql"))
    }

    fn with(db: &'a Database) -> Self {
        Self(db)
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
