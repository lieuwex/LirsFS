//! The File table holds information about every file in the LirsFs

use std::time::SystemTime;

use anyhow::{anyhow, Result};
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::query;
use webdav_handler::fs::{DavDirEntry, DavMetaData, FsFuture, FsResult};

use super::{
    schema::{Schema, SqlxQuery},
    Database,
};
use crate::util::flatten_result;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRow {
    pub file_id: i64,
    pub file_path: String,
    pub file_size: u64,
    pub content_hash: u64,
    pub replication_factor: u64,
}

impl DavDirEntry for FileRow {
    fn name(&self) -> Vec<u8> {
        self.file_path.clone().into_bytes()
    }

    fn metadata(&self) -> FsFuture<Box<dyn DavMetaData>> {
        let res: Box<dyn DavMetaData> = Box::new(self.clone());
        Box::pin(future::ready(Ok(res)))
    }
}

impl DavMetaData for FileRow {
    fn len(&self) -> u64 {
        self.file_size
    }

    fn modified(&self) -> FsResult<SystemTime> {
        todo!()
    }

    fn is_dir(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub struct File<'a>(&'a Database);

impl<'a> File<'a> {
    pub async fn get_all(&self) -> Result<Vec<FileRow>> {
        let res: Vec<FileRow> =
            query!("SELECT id, path, size, hash, replication_factor FROM files")
                .map(|r| {
                    let hash = (r.hash.len() == 8)
                        .then(|| {
                            let mut bytes: [u8; 8] = [0; 8];
                            bytes.copy_from_slice(&r.hash);
                            u64::from_le_bytes(bytes)
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "expected hash to be 8 bytes, but it is {} bytes",
                                r.hash.len()
                            )
                        })?;

                    anyhow::Ok(FileRow {
                        file_id: r.id,
                        file_path: r.path,
                        file_size: 0, // TODO
                        content_hash: hash,
                        replication_factor: r.replication_factor as u64,
                    })
                })
                .fetch(self.0.as_ref())
                .map(|r| flatten_result(r))
                .try_collect()
                .await?;
        Ok(res)
    }
}

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
