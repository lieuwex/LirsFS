use std::{
    future::{self, Future},
    pin::Pin,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use async_raft::NodeId;
use hyper::StatusCode;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tarpc::context::Context;
use webdav_handler::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsStream,
        OpenOptions, ReadDirMeta,
    },
};

use crate::{assume_client, NETWORK};

use super::{Client, FilePointer};

#[derive(Debug, Clone)]
pub struct WebdavFilesystem {}

impl WebdavFilesystem {
    async fn get_keeper_nodes(&self, path: &DavPath) -> Result<Vec<NodeId>> {
        // TODO: this is currently just a toy example, work this out for real.

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

async fn assume_random_keeper(fs: &WebdavFilesystem, path: &DavPath) -> Result<(NodeId, Client)> {
    let nodes = fs.get_keeper_nodes(path).await.unwrap();
    let node = *nodes
        .choose(&mut thread_rng())
        .ok_or_else(|| anyhow!("no nodes have the file"))?;

    let client = assume_client!(node);
    Ok((node, client))
}

fn do_fs<'a, Fun, FunRet, OK, ERR>(
    fs: &'a WebdavFilesystem,
    path: &'a DavPath,
    f: Fun,
) -> FsFuture<'a, OK>
where
    Fun: (FnOnce(NodeId, Client, &'a DavPath) -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<anyhow::Error>,
{
    Box::pin(async move {
        let res = async move {
            let (node, client) = assume_random_keeper(fs, path).await?;
            let res = f(node, client, path).await.map_err(|e| e.into())?;
            anyhow::Ok(res)
        }
        .await
        .map_err(|_| FsError::GeneralFailure)?;
        Ok(res)
    })
}

impl DavFileSystem for WebdavFilesystem {
    fn open<'a>(&'a self, path: &'a DavPath, _: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        do_fs(&self, path, move |node, client, path| async move {
            let uuid = client.open(Context::current(), path.to_string()).await?;

            let res = FilePointer::new(node, uuid);
            let res: Box<dyn DavFile> = Box::new(res);
            anyhow::Ok(res)
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
        do_fs(&self, path, move |_, client, path| async move {
            let res = client
                .metadata(Context::current(), path.to_string())
                .await?;
            let res: Box<dyn DavMetaData> = Box::new(res);
            anyhow::Ok(res)
        })
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

    /*
    fn have_props<'a>(
        &'a self,
        path: &'a DavPath,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
    fn get_props<'a>(&'a self, path: &'a DavPath, do_content: bool) -> FsFuture<Vec<DavProp>>;
    fn get_prop<'a>(&'a self, path: &'a DavPath, prop: DavProp) -> FsFuture<Vec<u8>>;
    fn patch_props<'a>(
        &'a self,
        path: &'a DavPath,
        patch: Vec<(bool, DavProp)>,
    ) -> FsFuture<Vec<(StatusCode, DavProp)>>;
    */

    //fn get_quota<'a>(&'a self) -> FsFuture<(u64, Option<u64>)>;
}
