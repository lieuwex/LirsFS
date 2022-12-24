//! The File table holds information about every file in the LirsFs

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::schema::{Schema, SqlxQuery};
use crate::{
    filesystem::FileContentHash,
    util::{blob_to_hash, flatten_result},
};
use anyhow::{bail, Result};
use camino::{Utf8Path, Utf8PathBuf};
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{query, sqlite::SqliteRow, Row, SqliteConnection};
use webdav_handler::fs::{DavDirEntry, DavMetaData, FsFuture, FsResult};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRow {
    pub file_path: Utf8PathBuf,
    pub file_size: u64,
    pub modified_at: SystemTime,
    pub content_hash: Option<FileContentHash>,
    pub replication_factor: u64,
    /// `false` means this is a directory
    pub is_file: bool,
}

impl DavDirEntry for FileRow {
    fn name(&self) -> Vec<u8> {
        let file_name = self.file_path.file_name().unwrap();
        String::from(file_name).into_bytes()
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
        !self.is_file
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
            file_path: {
                let s: String = row.get("path");
                Utf8PathBuf::from(s)
            },
            file_size: get_u64("size")?,
            modified_at: {
                let ts = get_u64("modified_at")?;
                UNIX_EPOCH + Duration::from_secs(ts)
            },
            content_hash: hash,
            replication_factor: get_u64("replication_factor")?,
            is_file: row.get("is_file"),
        })
    }

    pub async fn get_all(conn: &mut SqliteConnection) -> Result<Vec<FileRow>> {
        let res: Vec<FileRow> = query("SELECT * FROM files")
            .map(Self::map_row)
            .fetch(conn)
            .map(flatten_result)
            .try_collect()
            .await?;
        Ok(res)
    }

    pub async fn get_by_path(
        conn: &mut SqliteConnection,
        path: &Utf8Path,
    ) -> Result<Option<FileRow>> {
        let res: Option<FileRow> = query("SELECT * FROM files WHERE path = ?1")
            .bind(path.as_str())
            .map(Self::map_row)
            .fetch_optional(conn)
            .await?
            .transpose()?;
        Ok(res)
    }

    pub async fn create_file(
        conn: &mut SqliteConnection,
        path: Utf8PathBuf,
        replication_factor: u64,
    ) -> Result<FileRow> {
        if path.file_name().is_none() {
            bail!("path does not contain a file name");
        }

        let now = SystemTime::now();
        query("INSERT INTO files(path, is_file, size, modified_at, replication_factor) VALUES(?1, TRUE, 0, ?2, ?3)")
            .bind(path.as_str())
            .bind(now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64)
            .bind(replication_factor as i64)
            .execute(conn)
            .await?;

        Ok(FileRow {
            file_path: path,
            file_size: 0,
            modified_at: now,
            content_hash: None,
            replication_factor,
            is_file: true,
        })
    }

    pub async fn create_dir(conn: &mut SqliteConnection, path: Utf8PathBuf) -> Result<FileRow> {
        let now = SystemTime::now();
        query("INSERT INTO files(path, is_file, size, modified_at, replication_factor) VALUES(?1, FALSE, 0, ?2, 0)")
            .bind(path.as_str())
            .bind(now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64)
            .execute(conn)
            .await?;

        Ok(FileRow {
            file_path: path,
            file_size: 0,
            modified_at: now,
            content_hash: None,
            replication_factor: 0,
            is_file: false,
        })
    }

    pub async fn update_file_hash(
        conn: &mut SqliteConnection,
        path: &Utf8Path,
        hash: Option<FileContentHash>,
    ) -> Result<()> {
        let hash = hash.map(|h| h.to_be_bytes().to_vec());
        let path = path.as_str();
        query!(
            "
            UPDATE files
            SET hash = ?
            WHERE path = ?
        ",
            hash,
            path
        )
        .execute(conn)
        .await?;
        Ok(())
    }

    pub async fn remove_file(conn: &mut SqliteConnection, path: &Utf8Path) -> Result<bool> {
        let path = path.as_str();
        let res = query!(
            "
            DELETE FROM files
            WHERE path = ?
        ",
            path
        )
        .execute(conn)
        .await?;
        Ok(res.rows_affected() > 0)
    }
}

impl Schema for File {
    const TABLENAME: &'static str = "files";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_files.sql"))
    }
}
