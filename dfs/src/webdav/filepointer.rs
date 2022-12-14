use std::ops::Deref;

use anyhow::Result;
use futures::prelude::*;
use tarpc::context::Context;
use webdav_handler::{
    davpath::DavPath,
    fs::{DavFile, DavFileSystem, DavMetaData, FsError, FsFuture},
};

use super::{Client, SeekFrom, WebdavFilesystem};
use crate::{util::davpath_to_pathbuf, WEBDAV_FS};

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

    pub(super) async fn _seek(&mut self, pos: SeekFrom) -> Result<u64, FsError> {
        let add = |a: u64, b: i64| -> Result<u64, FsError> {
            let res = i128::from(a) + i128::from(b);
            if res < 0 {
                Err(FsError::GeneralFailure)
            } else if res > i128::from(u64::MAX) {
                Err(FsError::GeneralFailure)
            } else {
                Ok(res as u64)
            }
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

fn do_file<'a, Fun, FunRet, OK, ERR>(fp: &'a FilePointer, f: Fun) -> FsFuture<'a, OK>
where
    Fun: (FnOnce(Client, &'a FilePointer) -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<anyhow::Error>,
{
    Box::pin(async move {
        let res = async {
            let (_, client) = fp.fs.assume_keeper(&fp.file_path).await?;
            let res = f(client, fp).await.map_err(|e| e.into())?;
            anyhow::Ok(res)
        }
        .await
        .map_err(|e| {
            eprintln!("Catched webdav file error: {:?}", e);
            FsError::GeneralFailure
        })?;
        Ok(res)
    })
}

impl DavFile for FilePointer {
    fn metadata<'a>(&'a mut self) -> FsFuture<Box<dyn DavMetaData>> {
        self.fs.metadata(&self.file_path)
    }

    fn write_buf<'a>(&'a mut self, buf: Box<dyn bytes::Buf + Send>) -> FsFuture<()> {
        let bytes = buf.chunk().to_vec();
        do_file(self, move |client, fp| async move {
            let path = davpath_to_pathbuf(&fp.file_path);
            client
                .write_bytes(Context::current(), path, fp.get_seek(), bytes)
                .await
        })
    }

    fn write_bytes<'a>(&'a mut self, buf: bytes::Bytes) -> FsFuture<()> {
        do_file(self, move |client, fp| async move {
            let path = davpath_to_pathbuf(&fp.file_path);
            client
                .write_bytes(Context::current(), path, fp.get_seek(), buf.to_vec())
                .await
        })
    }

    fn read_bytes<'a>(&'a mut self, count: usize) -> FsFuture<bytes::Bytes> {
        do_file(self, move |client, fp| async move {
            let path = davpath_to_pathbuf(&fp.file_path);
            let bytes = client
                .read_bytes(Context::current(), path, fp.get_seek(), count)
                .await?;
            anyhow::Ok(bytes.into())
        })
    }

    fn seek<'a>(&'a mut self, pos: std::io::SeekFrom) -> FsFuture<u64> {
        let pos: SeekFrom = pos.into();
        Box::pin(self._seek(pos))
    }

    fn flush<'a>(&'a mut self) -> FsFuture<()> {
        // TODO: NOOP
        Box::pin(future::ready(Ok(())))
    }
}
