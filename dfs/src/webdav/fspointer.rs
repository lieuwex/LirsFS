///! Very very dumb stuff this is.
use std::{pin::Pin, sync::Arc};

use futures::prelude::*;
use hyper::StatusCode;
use std::time::SystemTime;
use webdav_handler::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsFuture, FsStream, OpenOptions,
        ReadDirMeta,
    },
};

use super::WebdavFilesystem;

#[derive(Debug, Clone)]
pub struct FsPointer(pub Arc<WebdavFilesystem>);

impl DavFileSystem for FsPointer {
    fn open<'a>(&'a self, path: &'a DavPath, options: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        self.0.open(path, options)
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        meta: ReadDirMeta,
    ) -> FsFuture<FsStream<Box<dyn DavDirEntry>>> {
        self.0.read_dir(path, meta)
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        self.0.metadata(path)
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        self.0.create_dir(path)
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        self.0.remove_dir(path)
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        self.0.remove_file(path)
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        self.0.rename(from, to)
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        self.0.copy(from, to)
    }

    fn set_accessed<'a>(&'a self, path: &'a DavPath, tm: SystemTime) -> FsFuture<()> {
        self.0.set_accessed(path, tm)
    }

    fn set_modified<'a>(&'a self, path: &'a DavPath, tm: SystemTime) -> FsFuture<()> {
        todo!()
    }

    fn have_props<'a>(
        &'a self,
        path: &'a DavPath,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        self.0.have_props(path)
    }
    fn get_props<'a>(&'a self, path: &'a DavPath, do_content: bool) -> FsFuture<Vec<DavProp>> {
        self.0.get_props(path, do_content)
    }
    fn get_prop<'a>(&'a self, path: &'a DavPath, prop: DavProp) -> FsFuture<Vec<u8>> {
        self.0.get_prop(path, prop)
    }
    fn patch_props<'a>(
        &'a self,
        path: &'a DavPath,
        patch: Vec<(bool, DavProp)>,
    ) -> FsFuture<Vec<(StatusCode, DavProp)>> {
        self.0.patch_props(path, patch)
    }

    fn get_quota<'a>(&'a self) -> FsFuture<(u64, Option<u64>)> {
        self.0.get_quota()
    }
}
