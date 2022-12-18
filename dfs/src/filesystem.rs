use std::{
    hash::Hasher,
    io::{ErrorKind, SeekFrom},
};

use camino::{Utf8Path, Utf8PathBuf};
use thiserror::Error;
use tokio::{
    fs::{remove_dir, remove_file, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};
use twox_hash::XxHash64;
use uuid::Uuid;

use crate::{
    queue::{QueueReadHandle, QueueWriteHandle},
    webdav::DirEntry,
    CONFIG,
};

pub type FileContentHash = u64;

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

    #[tracing::instrument(level = "trace", skip(self), ret)]
    fn map_path(&self, path: &Utf8Path) -> Utf8PathBuf {
        CONFIG.file_dir.join(path)
    }

    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn create_file(&self, _: &QueueWriteHandle, path: &Utf8Path) -> Result<File> {
        let path = self.map_path(path);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?;
        Ok(file)
    }

    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn remove_file(
        &self,
        _: &QueueWriteHandle,
        path: &Utf8Path,
        is_dir: bool,
    ) -> Result<()> {
        let path = self.map_path(path);
        if is_dir {
            remove_dir(path).await?;
        } else {
            remove_file(path).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn read_dir(&self, path: &Utf8Path) -> Result<Vec<DirEntry>> {
        let path = self.map_path(path);

        let mut res = Vec::new();
        let mut dir = tokio::fs::read_dir(path).await?;
        while let Some(entry) = dir.next_entry().await? {
            res.push(DirEntry::try_from_tokio(entry).await?);
        }

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn write_bytes(
        &self,
        _: &QueueWriteHandle,
        path: &Utf8Path,
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
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn read_bytes(
        &self,
        _: &QueueReadHandle<'_>,
        path: &Utf8Path,
        pos: SeekFrom,
        count: usize,
    ) -> Result<Vec<u8>> {
        let mut file = File::open(self.map_path(path)).await?;
        file.seek(pos).await?;

        let mut vec = vec![0; count];
        file.read_exact(&mut vec).await?; // REVIEW: do we want to use read_exact?

        Ok(vec)
    }

    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn get_hash(
        &self,
        _: &QueueReadHandle<'_>,
        path: &Utf8Path,
    ) -> Result<FileContentHash> {
        let file = File::open(self.map_path(path)).await?;
        let mut reader = BufReader::new(file);

        let mut hasher = XxHash64::default();
        loop {
            let b = match reader.read_u8().await {
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
                Ok(b) => b,
            };
            hasher.write_u8(b)
        }

        Ok(hasher.finish())
    }
}
