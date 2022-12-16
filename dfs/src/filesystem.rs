use std::io::SeekFrom;

use camino::Utf8PathBuf;
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use uuid::Uuid;

use crate::{
    storage::{QueueReadHandle, QueueWriteHandle},
    webdav::DirEntry,
    CONFIG,
};

pub type Result<T> = std::result::Result<T, FileSystemError>;

#[derive(Error, Debug)]
pub enum FileSystemError {
    #[error("unknown file with UUID {0}")]
    UnknownFile(Uuid),

    #[error(transparent)]
    Io(#[from] tokio::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug)]
pub struct FileSystem {}

// TODO: do hash checking and stuff

impl FileSystem {
    pub fn new() -> Self {
        Self {}
    }

    fn map_path<P: Into<Utf8PathBuf>>(&self, path: P) -> Utf8PathBuf {
        let path: Utf8PathBuf = path.into();
        CONFIG.file_dir.join(path)
    }

    pub async fn read_dir(&self, path: Utf8PathBuf) -> Result<Vec<DirEntry>> {
        let path = self.map_path(path);

        let mut res = Vec::new();
        let mut dir = tokio::fs::read_dir(path).await?;
        while let Some(entry) = dir.next_entry().await? {
            res.push(DirEntry::try_from_tokio(entry).await?);
        }

        Ok(res)
    }

    pub async fn write_bytes(
        &self,
        _: &QueueWriteHandle,
        path: Utf8PathBuf,
        pos: SeekFrom,
        buf: &[u8],
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.map_path(path))
            .await?;
        file.seek(pos).await?;

        file.write_all(buf).await?; // REVIEW: do we want to use write_all?
        Ok(())
    }
    pub async fn read_bytes(
        &self,
        _: &QueueReadHandle,
        path: Utf8PathBuf,
        pos: SeekFrom,
        count: usize,
    ) -> Result<Vec<u8>> {
        let mut file = File::open(self.map_path(path)).await?;
        file.seek(pos).await?;

        let mut vec = vec![0; count];
        file.read_exact(&mut vec).await?; // REVIEW: do we want to use read_exact?

        Ok(vec)
    }
}
