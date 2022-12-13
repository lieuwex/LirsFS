use anyhow::{anyhow, Ok, Result};
use async_raft::{raft::ClientWriteRequest, NodeId};
use futures::prelude::*;
use hyper::StatusCode;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::borrow::BorrowMut;
use std::time::SystemTime;
use tarpc::context::Context;
use webdav_handler::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsStream,
        OpenOptions, ReadDirMeta,
    },
};

use crate::{
    assume_client,
    db::{db, file::File, schema::Schema},
    db_conn,
    operation::ClientToNodeOperation,
    util::davpath_to_pathbuf,
    NETWORK, RAFT,
};

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

async fn assume_keeper(fs: &WebdavFilesystem, path: &DavPath) -> Result<(NodeId, Client)> {
    let nodes = fs.get_keeper_nodes(path).await?;

    let (node, client) = stream::select_all(
        nodes
            .iter()
            .map(|&n| {
                Box::pin(async move {
                    // TODO: improve error handling
                    let cl = assume_client!(n);
                    Ok((n, cl))
                })
            })
            .map(|c| c.into_stream().filter_map(|c| future::ready(c.ok()))),
    )
    .next()
    .await
    .ok_or_else(|| anyhow!("no nodes available that have the file"))?;

    Ok((node, client))
}

fn do_fs<'a, Fun, FunRet, OK, ERR>(f: Fun) -> FsFuture<'a, OK>
where
    Fun: (FnOnce() -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<anyhow::Error>,
{
    Box::pin(async move {
        let res = async move {
            let res = f().await.map_err(|e| e.into())?;
            Ok(res)
        }
        .await
        .map_err(|e| {
            eprintln!("Catched webdav filesystem error: {:?}", e);
            FsError::GeneralFailure
        })?;
        std::result::Result::Ok(res)
    })
}

fn do_fs_file<'a, Fun, FunRet, OK, ERR>(
    fs: &'a WebdavFilesystem,
    path: &'a DavPath,
    f: Fun,
) -> FsFuture<'a, OK>
where
    Fun: (FnOnce(NodeId, Client, &'a DavPath) -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<anyhow::Error>,
{
    do_fs(move || async move {
        let (node, client) = assume_keeper(fs, path).await?;
        f(node, client, path).await.map_err(|e| e.into())
    })
}

impl DavFileSystem for WebdavFilesystem {
    fn open<'a>(&'a self, path: &'a DavPath, _: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        do_fs_file(self, path, move |node, client, path| async move {
            let uuid = client.open(Context::current(), path.to_string()).await?;

            let res = FilePointer::new(node, uuid);
            let res: Box<dyn DavFile> = Box::new(res);
            Ok(res)
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _: ReadDirMeta,
    ) -> FsFuture<FsStream<Box<dyn DavDirEntry>>> {
        do_fs(move || async move {
            let path = path.to_string();

            let files = File::get_all(db_conn!()).await?;
            let files = files
                .into_iter()
                .filter(move |f| f.file_path.starts_with(&path));

            Ok(stream::iter(files)
                .map(|f| {
                    let res: Box<dyn DavDirEntry> = Box::new(f);
                    res
                })
                .boxed())
        })
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        let path = path.to_string();
        do_fs(move || async move {
            let file = File::get_by_path(db_conn!(), &path).await?;
            let file = file.ok_or_else(|| anyhow!("file not found"))?;
            let res: Box<dyn DavMetaData> = Box::new(file);
            Ok(res)
        })
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        do_fs(move || async move {
            let raft = RAFT.get().unwrap();
            raft.client_write(ClientToNodeOperation::CreateDir {
                // REVIEW: does `as_pathbuf()` give the correct path?
                path: davpath_to_pathbuf(path),
            })
            .await?;
            Ok(())
        })
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        do_fs(move || async move {
            let stream = self.read_dir(path, ReadDirMeta::None).await?;
            stream
                .map(Ok)
                .try_for_each(|f| async move {
                    let name = todo!();

                    if f.is_dir().await? {
                        self.remove_dir(&name).await?
                    } else {
                        self.remove_file(&name).await?
                    }

                    Ok(())
                })
                .await?;

            let raft = RAFT.get().unwrap();
            raft.client_write(ClientToNodeOperation::RemoveDir {
                // REVIEW: does `as_pathbuf()` give the correct path?
                path: davpath_to_pathbuf(path),
            })
            .await?;

            Ok(())
        })
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        do_fs(move || async move {
            let raft = RAFT.get().unwrap();
            raft.client_write(ClientToNodeOperation::RemoveFile {
                // REVIEW: does `as_pathbuf()` give the correct path?
                path: davpath_to_pathbuf(path),
            })
            .await?;
            Ok(())
        })
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        do_fs(move || async move {
            let raft = RAFT.get().unwrap();
            raft.client_write(ClientToNodeOperation::MoveFile {
                // REVIEW: does `as_pathbuf()` give the correct path?
                old_path: davpath_to_pathbuf(from),
                new_path: davpath_to_pathbuf(to),
            })
            .await?;
            Ok(())
        })
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        do_fs(move || async move {
            let raft = RAFT.get().unwrap();
            raft.client_write(ClientToNodeOperation::CopyFile {
                // REVIEW: does `as_pathbuf()` give the correct path?
                src_path: davpath_to_pathbuf(from),
                dst_path: davpath_to_pathbuf(to),
            })
            .await?;
            Ok(())
        })
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
