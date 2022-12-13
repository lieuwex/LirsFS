//! The File table holds information about every file in the LirsFs

use std::time::SystemTime;

use super::{
    schema::{Schema, SqlxQuery},
    Database,
};
use crate::util::flatten_result;
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
        todo!()
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
            Some(h) if h.len() == 8 => {
                let mut bytes: [u8; 8] = [0; 8];
                bytes.copy_from_slice(&h);
                Some(u64::from_le_bytes(bytes))
            }
            Some(h) => {
                bail!("expected hash to be 8 bytes, but it is {} bytes", h.len())
            }
        };

        let get_u64 = |name: &str| -> u64 {
            let val: i64 = row.get(name);
            val as u64
        };

        Ok(FileRow {
            file_id: row.get("id"),
            file_path: row.get("path"),
            file_size: get_u64("size"),
            content_hash: hash,
            replication_factor: get_u64("replication_factor"),
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
}

impl Schema for File {
    const TABLENAME: &'static str = "files";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_files.sql"))
    }
}
