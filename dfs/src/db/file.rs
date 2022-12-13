//! The File table holds information about every file in the LirsFs

use std::{
    borrow::Borrow,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::{
    schema::{Schema, SqlxQuery},
    Database,
};
use crate::util::{blob_to_hash, flatten_result};
use anyhow::{bail, Result};
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{query, sqlite::SqliteRow, Row, SqliteConnection};
use webdav_handler::fs::{DavDirEntry, DavMetaData, FsFuture, FsResult};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRow {
    pub file_id: i64,
    pub file_path: String,
    pub file_size: u64,
    pub modified_at: SystemTime,
    pub content_hash: Option<u64>,
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
        Ok(self.modified_at)
    }

    fn is_dir(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub struct File;

impl File {
    fn map_row(row: SqliteRow) -> Result<FileRow> {
        let hash: Option<Vec<u8>> = row.get("hash");
        let hash = match hash {
            None => None,
            Some(h) => Some(blob_to_hash(&h)?),
        };

        let get_u64 = |name: &str| -> Result<u64> {
            let val: i64 = row.get(name);
            Ok(u64::try_from(val)?)
        };

        Ok(FileRow {
            file_id: row.get("id"),
            file_path: row.get("path"),
            file_size: get_u64("size")?,
            modified_at: {
                let ts = get_u64("modified_at")?;
                UNIX_EPOCH + Duration::from_secs(ts)
            },
            content_hash: hash,
            replication_factor: get_u64("replication_factor")?,
        })
    }

    pub async fn get_all(conn: &mut SqliteConnection) -> Result<Vec<FileRow>> {
        let res: Vec<FileRow> = query("SELECT id, path, size, hash, replication_factor FROM files")
            .map(Self::map_row)
            .fetch(conn)
            .map(flatten_result)
            .try_collect()
            .await?;
        Ok(res)
    }

    pub async fn get_by_path(conn: &mut SqliteConnection, path: &str) -> Result<Option<FileRow>> {
        let res: Option<FileRow> =
            query("SELECT id, path, size, hash, replication_factor FROM files WHERE path = ?1")
                .bind(path)
                .map(Self::map_row)
                .fetch_optional(conn)
                .await?
                .transpose()?;
        Ok(res)
    }

    pub async fn create_file(
        conn: &mut SqliteConnection,
        path: String,
        replication_factor: u64,
    ) -> Result<FileRow> {
        query("INSERT INTO files(path, is_file, size, replication_factor) VALUES(?1, TRUE, 0, ?2)")
            .bind(path.as_str())
            .bind(replication_factor as i64)
            .execute(conn)
            .await?;

        Ok(FileRow {
            file_id: 0,
            file_path: path,
            file_size: 0,
            modified_at: SystemTime::now(),
            content_hash: None,
            replication_factor,
        })
    }
}

impl Schema for File {
    const TABLENAME: &'static str = "files";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_files.sql"))
    }
}
