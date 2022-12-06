use std::os::unix::ffi::OsStringExt;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use webdav_handler::fs::{DavDirEntry, DavMetaData, FsFuture};

use super::FileMetadata;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DirEntry {
    name: Vec<u8>,
    metadata: FileMetadata,
    is_dir: bool,
    is_file: bool,
}

impl DirEntry {
    pub async fn try_from_tokio(e: tokio::fs::DirEntry) -> Result<Self> {
        let metadata = e.metadata().await?;
        let is_dir = metadata.is_dir();
        let is_file = metadata.is_file();

        Ok(Self {
            name: e.file_name().into_vec(),
            metadata: metadata.into(),
            is_dir,
            is_file,
        })
    }
}

impl DavDirEntry for DirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.clone()
    }

    fn metadata<'a>(&'a self) -> FsFuture<Box<dyn DavMetaData>> {
        Box::pin(async {
            let res: Box<dyn DavMetaData> = Box::new(self.metadata.clone());
            Ok(res)
        })
    }

    fn is_dir<'a>(&'a self) -> FsFuture<bool> {
        Box::pin(async { Ok(self.is_dir) })
    }

    fn is_file<'a>(&'a self) -> FsFuture<bool> {
        Box::pin(async { Ok(self.is_file) })
    }
}
