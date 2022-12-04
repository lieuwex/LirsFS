use std::os::unix::ffi::OsStringExt;
use std::{convert::Infallible, fs, net::SocketAddr, time::SystemTime};

use anyhow::anyhow;
use hyper::{Body, Request};
use serde::{Deserialize, Serialize};
use webdav_handler::fs::FsFuture;
use webdav_handler::{
    fakels::FakeLs,
    fs::{DavDirEntry, DavMetaData, FsError, FsResult},
    localfs::LocalFs,
    DavHandler,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum SeekFrom {
    Start(u64),
    End(i64),
    Current(i64),
}

impl From<std::io::SeekFrom> for SeekFrom {
    fn from(v: std::io::SeekFrom) -> Self {
        use std::io;

        match v {
            io::SeekFrom::Start(v) => SeekFrom::Start(v),
            io::SeekFrom::End(v) => SeekFrom::End(v),
            io::SeekFrom::Current(v) => SeekFrom::Current(v),
        }
    }
}
impl From<SeekFrom> for std::io::SeekFrom {
    fn from(v: SeekFrom) -> Self {
        use std::io;

        match v {
            SeekFrom::Start(v) => io::SeekFrom::Start(v),
            SeekFrom::End(v) => io::SeekFrom::End(v),
            SeekFrom::Current(v) => io::SeekFrom::Current(v),
        }
    }
}

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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DirEntry {
    name: Vec<u8>,
    metadata: FileMetadata,
    is_dir: bool,
    is_file: bool,
}

impl DirEntry {
    pub async fn try_from_tokio(e: tokio::fs::DirEntry) -> anyhow::Result<Self> {
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

pub async fn listen(addr: &SocketAddr) -> Result<(), anyhow::Error> {
    let dav_server = DavHandler::builder()
        .filesystem(LocalFs::new("test", true, false, false))
        .locksystem(FakeLs::new())
        .build_handler();

    let service = hyper::service::make_service_fn(move |_| {
        let dav_server = dav_server.clone();
        async move {
            let func = move |req: Request<Body>| {
                let dav_server = dav_server.clone();
                async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
            };
            Ok::<_, Infallible>(hyper::service::service_fn(func))
        }
    });

    hyper::Server::bind(addr)
        .serve(service)
        .await
        .map_err(|e| anyhow!(e))
}
