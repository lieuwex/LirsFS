use anyhow::{anyhow, ensure, Ok, Result};
use async_raft::NodeId;
use futures::prelude::*;
use hyper::StatusCode;
use std::{pin::Pin, time::SystemTime};
use tracing::error;
use webdav_handler::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsStream,
        OpenOptions, ReadDirMeta,
    },
};

use crate::{
    assume_client,
    db::{
        db,
        file::{File, FileRow},
        keepers::Keepers,
    },
    db_conn,
    operation::ClientToNodeOperation,
    util::davpath_to_pathbuf,
    NETWORK, RAFT,
};

use super::{error::FileSystemError, Client, FilePointer};

#[derive(Debug, Clone)]
pub struct WebdavFilesystem {}

impl WebdavFilesystem {
    async fn get_keeper_nodes(&self, path: &DavPath) -> Result<Vec<NodeId>> {
        let path = davpath_to_pathbuf(path);
        let res = Keepers::get_keeper_ids_for_file(db_conn!(), &path).await?;
        Ok(res)
    }

    pub async fn assume_keeper(&self, path: &DavPath) -> Result<(NodeId, Client)> {
        let nodes = self.get_keeper_nodes(path).await?;

        let (node, client) = stream::select_all(
            nodes
                .iter()
                .map(|&n| {
                    Box::pin(async move {
                        // TODO: improve error handling
                        let (_, cl) = assume_client!(n);
                        anyhow::Ok((n, cl))
                    })
                })
                .map(|c| c.into_stream().filter_map(|c| future::ready(c.ok()))),
        )
        .next()
        .await
        .ok_or_else(|| anyhow!("no nodes available that have the file"))?;

        Ok((node, client))
    }
}

fn do_fs<'a, Fun, FunRet, OK, ERR>(f: Fun) -> FsFuture<'a, OK>
where
    Fun: (FnOnce() -> FunRet) + Send + 'a,
    FunRet: Future<Output = Result<OK, ERR>> + Send,
    ERR: Into<FileSystemError>,
{
    Box::pin(async move {
        let res = async move {
            let res = f().await.map_err(|e| e.into())?;
            std::result::Result::Ok::<_, FileSystemError>(res)
        }
        .await
        .map_err(|e| FsError::from(e))?;
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
    ERR: Into<FileSystemError>,
{
    do_fs(move || async move {
        let (node, client) = fs.assume_keeper(path).await?;
        f(node, client, path).await.map_err(|e| e.into())
    })
}

macro_rules! notimplemented_fut {
    ($method:expr) => {
        Box::pin(future::ready(Err(FsError::NotImplemented)))
    };
}

impl DavFileSystem for WebdavFilesystem {
    #[tracing::instrument(level = "trace", skip(self))]
    fn open<'a>(&'a self, path: &'a DavPath, options: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        let path = path.clone();
        do_fs(move || async move {
            if options.create || options.create_new {
                let path = davpath_to_pathbuf(&path);

                if options.create_new {
                    let f = File::get_by_path(db_conn!(), &path).await?;
                    ensure!(f.is_none(), "file already exists");
                }

                let raft = RAFT.get().unwrap();
                raft.client_write(ClientToNodeOperation::CreateFile {
                    path,
                    replication_factor: 0,
                    initial_keepers: vec![],
                })
                .await?;
            }

            let res = FilePointer::new(path);
            let res: Box<dyn DavFile> = Box::new(res);
            Ok(res)
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        let path = davpath_to_pathbuf(path);

        do_fs(move || async move {
            // handle / as a special case
            let file = if path == "/" {
                FileRow {
                    file_path: path,
                    file_size: 0,
                    modified_at: SystemTime::UNIX_EPOCH, // TODO: make this actually something smh
                    content_hash: None,
                    replication_factor: 0,
                    is_file: false,
                }
            } else {
                let file = File::get_by_path(db_conn!(), &path).await?;
                file.ok_or_else(|| FsError::NotFound)?
            };

            let res: Box<dyn DavMetaData> = Box::new(file);
            Ok(res)
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    fn set_accessed<'a>(&'a self, path: &'a DavPath, tm: SystemTime) -> FsFuture<()> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn set_modified<'a>(&'a self, path: &'a DavPath, tm: SystemTime) -> FsFuture<()> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    #[allow(unused_variables)]
    fn have_props<'a>(
        &'a self,
        path: &'a DavPath,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(future::ready(false))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    #[allow(unused_variables)]
    fn patch_props<'a>(
        &'a self,
        path: &'a DavPath,
        patch: Vec<(bool, DavProp)>,
    ) -> FsFuture<Vec<(StatusCode, DavProp)>> {
        notimplemented_fut!("patch_props")
    }

    #[tracing::instrument(level = "trace", skip(self))]
    #[allow(unused_variables)]
    fn get_props<'a>(&'a self, path: &'a DavPath, do_content: bool) -> FsFuture<Vec<DavProp>> {
        notimplemented_fut!("get_props")
    }

    #[tracing::instrument(level = "trace", skip(self))]
    #[allow(unused_variables)]
    fn get_prop<'a>(&'a self, path: &'a DavPath, prop: DavProp) -> FsFuture<Vec<u8>> {
        notimplemented_fut!("get_prop`")
    }

    #[tracing::instrument(level = "trace", skip(self))]
    #[allow(unused_variables)]
    fn get_quota<'a>(&'a self) -> FsFuture<(u64, Option<u64>)> {
        notimplemented_fut!("get_quota`")
    }
}
