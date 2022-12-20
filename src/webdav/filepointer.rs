use std::ops::Deref;

use anyhow::Result;
use futures::prelude::*;
use tarpc::context::Context;
use tracing::error;
use webdav_handler::{
    davpath::DavPath,
    fs::{DavFile, DavFileSystem, DavMetaData, FsError, FsFuture},
};

use super::{error::FileSystemError, Client, SeekFrom, WebdavFilesystem};
use crate::{operation::ClientToNodeOperation, util::davpath_to_pathbuf, RAFT, STORAGE, WEBDAV_FS};

#[derive(Debug, Clone)]
pub struct FilePointer {
    file_path: DavPath,
    fs: &'static WebdavFilesystem,
    pos: u64,
}

impl FilePointer {
    pub(super) fn new(file_path: DavPath) -> Self {
        Self {
            file_path,
            fs: WEBDAV_FS.get().unwrap().deref(),
            pos: 0,
        }
    }

    pub(super) fn get_seek(&self) -> SeekFrom {
        SeekFrom::Start(self.pos)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) async fn _seek(&mut self, pos: SeekFrom) -> Result<u64, FsError> {
        let add = |a: u64, b: i64| -> Result<u64, FsError> {
            a.checked_add_signed(b).ok_or(FsError::GeneralFailure)
        };

        self.pos = match pos {
            SeekFrom::Start(pos) => pos,
            SeekFrom::End(delta) => {
                let metadata = self.metadata().await?;
                let len = metadata.len();
                add(len, delta)?
            }
            SeekFrom::Current(delta) => add(self.pos, delta)?,
        };

        Ok(self.pos)
    }
}

fn do_op<'a, Fun, FunRet, OK, ERR>(f: Fun) -> FsFuture<'a, OK>
where
    Fun: (FnOnce() -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<FileSystemError>,
{
    Box::pin(async move {
        let res = async {
            let res = f().await.map_err(|e| e.into())?;
            std::result::Result::Ok::<_, FileSystemError>(res)
        }
        .await
        .map_err(|e| FsError::from(e))?;
        Ok(res)
    })
}

fn do_file<'a, Fun, FunRet, OK, ERR>(fp: &'a FilePointer, f: Fun) -> FsFuture<'a, OK>
where
    Fun: (FnOnce(Client, &'a FilePointer) -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<FileSystemError>,
{
    do_op(move || async move {
        let (_, client) = fp.fs.assume_keeper(&fp.file_path).await?;
        f(client, fp).await.map_err(|e| e.into())
    })
}

impl DavFile for FilePointer {
    #[tracing::instrument(level = "trace")]
    fn metadata<'a>(&'a mut self) -> FsFuture<Box<dyn DavMetaData>> {
        self.fs.metadata(&self.file_path)
    }

    #[tracing::instrument(level = "trace", skip(buf))]
    fn write_buf<'a>(&'a mut self, buf: Box<dyn bytes::Buf + Send>) -> FsFuture<()> {
        let bytes = buf.chunk().to_vec();
        do_op(move || async move {
            let path = davpath_to_pathbuf(&self.file_path);
            RAFT.get()
                .unwrap()
                .client_write(ClientToNodeOperation::Write {
                    path,
                    offset: self.pos,
                    contents: bytes,
                })
                .await?;
            anyhow::Ok(())
        })
    }

    #[tracing::instrument(level = "trace")]
    fn write_bytes<'a>(&'a mut self, buf: bytes::Bytes) -> FsFuture<()> {
        do_op(move || async move {
            let path = davpath_to_pathbuf(&self.file_path);

            RAFT.get()
                .unwrap()
                .client_write(ClientToNodeOperation::Write {
                    path,
                    offset: self.pos,
                    contents: buf.to_vec(),
                })
                .await?;

            anyhow::Ok(())
        })
    }

    #[tracing::instrument(level = "trace")]
    fn read_bytes<'a>(&'a mut self, count: usize) -> FsFuture<bytes::Bytes> {
        do_file(self, move |client, fp| async move {
            let path = davpath_to_pathbuf(&fp.file_path);
            let bytes = client
                .read_bytes(Context::current(), path, fp.get_seek(), count)
                .await?;
            anyhow::Ok(bytes.into())
        })
    }

    #[tracing::instrument(level = "trace")]
    fn seek<'a>(&'a mut self, pos: std::io::SeekFrom) -> FsFuture<u64> {
        let pos: SeekFrom = pos.into();
        Box::pin(self._seek(pos))
    }

    #[tracing::instrument(level = "trace")]
    fn flush<'a>(&'a mut self) -> FsFuture<()> {
        // TODO: NOOP
        Box::pin(future::ready(Ok(())))
    }
}
