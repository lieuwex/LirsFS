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
