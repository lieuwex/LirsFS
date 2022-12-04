use std::{collections::HashMap, io::SeekFrom, path::PathBuf};

use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use uuid::Uuid;

use crate::{
    webdav::{DirEntry, FileMetadata},
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
pub struct FileSystem {
    pub open_files: HashMap<Uuid, File>,
}

impl FileSystem {
    pub fn new() -> Self {
        Self {
            open_files: HashMap::new(),
        }
    }

    fn map_path<P: Into<PathBuf>>(&self, path: P) -> PathBuf {
        let path: PathBuf = path.into();
        CONFIG.file_dir.join(path)
    }
    fn get_file<'a>(&'a mut self, uuid: &Uuid) -> Result<&'a mut File> {
        self.open_files
            .get_mut(uuid)
            .ok_or_else(|| FileSystemError::UnknownFile(uuid.to_owned()))
    }

    pub async fn open(&mut self, path: String) -> Result<Uuid> {
        let path = self.map_path(path);
        let uuid = Uuid::new_v4();

        let file = File::open(path).await?;

        assert!(
            self.open_files.insert(uuid.clone(), file).is_none(),
            "open_files had already a file with this UUID, this is highly unlikely"
        );

        Ok(uuid)
    }
    pub async fn read_dir(&self, path: String) -> Result<Vec<DirEntry>> {
        let path = self.map_path(path);

        let mut res = Vec::new();
        let mut dir = tokio::fs::read_dir(path).await?;
        while let Some(entry) = dir.next_entry().await? {
            res.push(DirEntry::try_from_tokio(entry).await?);
        }

        Ok(res)
    }
    pub async fn metadata(&self, path: String) -> Result<FileMetadata> {
        let path = self.map_path(path);
        let metadata = tokio::fs::metadata(path).await?;
        Ok(metadata.into())
    }

    pub async fn file_metadata(&mut self, uuid: Uuid) -> Result<FileMetadata> {
        let file = self.get_file(&uuid)?;
        let metadata = file.metadata().await?;
        Ok(metadata.into())
    }
    pub async fn write_bytes(&mut self, uuid: Uuid, buf: &[u8]) -> Result<()> {
        let file = self.get_file(&uuid)?;
        file.write(buf).await?;
        Ok(())
    }
    pub async fn read_bytes(&mut self, uuid: Uuid, count: usize) -> Result<Vec<u8>> {
        let file = self.get_file(&uuid)?;

        let mut vec = vec![0; count];
        file.read_exact(&mut vec).await?; // REVIEW: do we want to use read_exact?

        Ok(vec)
    }
    pub async fn seek(&mut self, uuid: Uuid, pos: SeekFrom) -> Result<u64> {
        let file = self.get_file(&uuid)?;
        Ok(file.seek(pos).await?)
    }
    pub async fn flush(&mut self, uuid: Uuid) -> Result<()> {
        let file = self.get_file(&uuid)?;
        file.flush().await?;
        Ok(())
    }
}
