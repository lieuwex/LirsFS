use std::{convert::Infallible, net::SocketAddr, time::SystemTime};

use anyhow::anyhow;
use hyper::{Body, Request};
use serde::{Deserialize, Serialize};
use webdav_handler::{
    fakels::FakeLs,
    fs::{DavMetaData, FsResult},
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
pub struct FileMetadata {}

impl DavMetaData for FileMetadata {
    fn len(&self) -> u64 {
        todo!()
    }
    fn modified(&self) -> FsResult<SystemTime> {
        todo!()
    }
    fn is_dir(&self) -> bool {
        todo!()
    }

    fn is_symlink(&self) -> bool {
        false
    }
    fn accessed(&self) -> FsResult<SystemTime> {
        todo!()
    }
    fn created(&self) -> FsResult<SystemTime> {
        todo!()
    }
    fn status_changed(&self) -> FsResult<SystemTime> {
        todo!()
    }
    fn executable(&self) -> FsResult<bool> {
        todo!()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DirEntry {}

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
