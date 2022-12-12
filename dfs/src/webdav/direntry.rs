use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use webdav_handler::fs::{DavDirEntry, DavMetaData, FsFuture};

use super::FileMetadata;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DirEntry {
    name: String,
    metadata: FileMetadata,
}

impl DirEntry {
    pub async fn try_from_tokio(e: tokio::fs::DirEntry) -> Result<Self> {
        let metadata = e.metadata().await?;
        let name = e
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("couldn't map file_name to string"))?;

        Ok(Self {
            name,
            metadata: metadata.into(),
        })
    }
}

impl DavDirEntry for DirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.clone().into_bytes()
    }

    fn metadata<'a>(&'a self) -> FsFuture<Box<dyn DavMetaData>> {
        Box::pin(async {
            let res: Box<dyn DavMetaData> = Box::new(self.metadata.clone());
            Ok(res)
        })
    }
}
