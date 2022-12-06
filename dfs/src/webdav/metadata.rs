use std::{fs, time::SystemTime};

use serde::{Deserialize, Serialize};
use webdav_handler::fs::{DavMetaData, FsError, FsResult};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileMetadata {
    len: u64,

    created: Option<SystemTime>,
    modified: Option<SystemTime>,
    accessed: Option<SystemTime>,

    is_dir: bool,
}

impl From<fs::Metadata> for FileMetadata {
    fn from(metadata: fs::Metadata) -> Self {
        Self {
            len: metadata.len(),

            created: metadata.created().ok(),
            modified: metadata.modified().ok(),
            accessed: metadata.accessed().ok(),

            is_dir: metadata.is_dir(),
        }
    }
}

impl DavMetaData for FileMetadata {
    fn len(&self) -> u64 {
        self.len
    }
    fn modified(&self) -> FsResult<SystemTime> {
        self.modified.ok_or(FsError::GeneralFailure)
    }
    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn is_symlink(&self) -> bool {
        false
    }
    fn accessed(&self) -> FsResult<SystemTime> {
        self.accessed.ok_or(FsError::GeneralFailure)
    }
    fn created(&self) -> FsResult<SystemTime> {
        self.created.ok_or(FsError::GeneralFailure)
    }
    fn status_changed(&self) -> FsResult<SystemTime> {
        todo!()
    }
    fn executable(&self) -> FsResult<bool> {
        Ok(false)
    }
}
