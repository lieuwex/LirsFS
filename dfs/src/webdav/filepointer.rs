use std::future::Future;

use anyhow::Result;
use async_raft::NodeId;
use tarpc::context::Context;
use tokio::sync::OwnedRwLockReadGuard;
use uuid::Uuid;
use webdav_handler::fs::{DavFile, DavMetaData, FsError, FsFuture};

use crate::service::ServiceClient;
use crate::{assume_client, NETWORK};

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
