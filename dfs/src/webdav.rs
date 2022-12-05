use std::os::unix::ffi::OsStringExt;
use std::{
    convert::Infallible,
    fs,
    future::{self, Future},
    net::SocketAddr,
    pin::Pin,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use async_raft::NodeId;
use hyper::{Body, Request, StatusCode};
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use tarpc::context::Context;
use tokio::sync::OwnedRwLockReadGuard;
use uuid::Uuid;
use webdav_handler::{
    davpath::DavPath,
    fakels::FakeLs,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsResult,
        FsStream, OpenOptions, ReadDirMeta,
    },
    localfs::LocalFs,
    DavHandler,
};

use crate::service::ServiceClient;
use crate::{assume_client, NETWORK};

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

#[derive(Debug, Clone)]
pub struct WebdavFilesystem {}

impl WebdavFilesystem {
    async fn get_keeper_nodes(&self, path: &DavPath) -> Result<Vec<NodeId>> {
        let p = path.as_rel_ospath();
        let res = if p.starts_with("a") {
            vec![0]
        } else if p.starts_with("b") {
            vec![1]
        } else if p.starts_with("c") {
            vec![2]
        } else {
            vec![]
        };
        Ok(res)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct FilePointer {
    node_id: NodeId,
    file_id: Uuid,
}

type Client = OwnedRwLockReadGuard<Option<ServiceClient>, ServiceClient>;
fn do_file<'a, Fun, FunRet, OK, ERR>(fp: FilePointer, f: Fun) -> FsFuture<'a, OK>
where
    Fun: (FnOnce(Client, FilePointer) -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<anyhow::Error>,
{
    Box::pin(async move {
        let res = async {
            let client = assume_client!(fp.node_id);
            let res = f(client, fp).await.map_err(|e| e.into())?;
            anyhow::Ok(res)
        }
        .await
        .map_err(|_| FsError::GeneralFailure)?;
        Ok(res)
    })
}

impl DavFile for FilePointer {
    fn metadata<'a>(&'a mut self) -> FsFuture<Box<dyn DavMetaData>> {
        do_file(*self, move |client: Client, fp| async move {
            let res = client.file_metadata(Context::current(), fp.file_id).await?;

            let res: Box<dyn DavMetaData> = Box::new(res);
            anyhow::Ok(res)
        })
    }

    fn write_buf<'a>(&'a mut self, buf: Box<dyn bytes::Buf + Send>) -> FsFuture<()> {
        let bytes = buf.chunk().to_vec();
        do_file(*self, move |client: Client, fp| async move {
            client
                .write_bytes(Context::current(), fp.file_id, bytes)
                .await
        })
    }

    fn write_bytes<'a>(&'a mut self, buf: bytes::Bytes) -> FsFuture<()> {
        do_file(*self, move |client: Client, fp| async move {
            client
                .write_bytes(Context::current(), fp.file_id, buf.to_vec())
                .await
        })
    }

    fn read_bytes<'a>(&'a mut self, count: usize) -> FsFuture<bytes::Bytes> {
        do_file(*self, move |client: Client, fp| async move {
            let bytes = client
                .read_bytes(Context::current(), fp.file_id, count)
                .await?;
            anyhow::Ok(bytes.into())
        })
    }

    fn seek<'a>(&'a mut self, pos: std::io::SeekFrom) -> FsFuture<u64> {
        do_file(*self, move |client: Client, fp| async move {
            client
                .seek(Context::current(), fp.file_id, pos.into())
                .await
        })
    }

    fn flush<'a>(&'a mut self) -> FsFuture<()> {
        do_file(*self, move |client: Client, fp| async move {
            client.flush(Context::current(), fp.file_id).await
        })
    }
}

impl DavFileSystem for WebdavFilesystem {
    fn open<'a>(&'a self, path: &'a DavPath, _: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        Box::pin(async {
            let res = async {
                let nodes = self.get_keeper_nodes(path).await.unwrap();

                let node = nodes
                    .choose(&mut thread_rng())
                    .ok_or_else(|| anyhow!("no nodes have the file"))?;
                let client = assume_client!(*node);
                let uuid = client.open(Context::current(), path.to_string()).await?;
                anyhow::Ok(todo!())
            }
            .await
            .map_err(|_| FsError::GeneralFailure)?;
            Ok(res)
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        meta: ReadDirMeta,
    ) -> FsFuture<FsStream<Box<dyn DavDirEntry>>> {
        todo!()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        todo!()
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        todo!()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        todo!()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        todo!()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        todo!()
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        todo!()
    }

    fn set_accessed<'a>(&'a self, path: &'a DavPath, tm: SystemTime) -> FsFuture<()> {
        todo!()
    }

    fn set_modified<'a>(&'a self, path: &'a DavPath, tm: SystemTime) -> FsFuture<()> {
        todo!()
    }

    fn have_props<'a>(
        &'a self,
        path: &'a DavPath,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(future::ready(false))
    }

    fn patch_props<'a>(
        &'a self,
        path: &'a DavPath,
        patch: Vec<(bool, DavProp)>,
    ) -> FsFuture<Vec<(StatusCode, DavProp)>> {
        todo!()
    }

    fn get_props<'a>(&'a self, path: &'a DavPath, do_content: bool) -> FsFuture<Vec<DavProp>> {
        todo!()
    }

    fn get_prop<'a>(&'a self, path: &'a DavPath, prop: DavProp) -> FsFuture<Vec<u8>> {
        todo!()
    }

    //fn get_quota<'a>(&'a self) -> FsFuture<(u64, Option<u64>)>;
}

pub async fn listen(addr: &SocketAddr) -> Result<()> {
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
