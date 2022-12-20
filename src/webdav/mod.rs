pub use direntry::*;
pub use filepointer::*;
pub use filesystem::*;
pub use metadata::*;
pub use seekfrom::*;

mod direntry;
mod error;
mod filepointer;
mod filesystem;
mod fspointer;
mod metadata;
mod seekfrom;

use std::{convert::Infallible, net::SocketAddr};

use anyhow::{anyhow, Result};
use hyper::{Body, Request, Response};
use tokio::{sync::OwnedRwLockReadGuard, time::Instant};
use tracing::{error, info, trace};
use webdav_handler::{fakels::FakeLs, fs::DavFileSystem, DavHandler};

use crate::{service::ServiceClient, RAFT, WEBDAV_FS};

use self::fspointer::FsPointer;

type Client = OwnedRwLockReadGuard<Option<ServiceClient>, ServiceClient>;

#[tracing::instrument(level = "trace")]
pub async fn listen(addr: &SocketAddr) -> Result<()> {
    let fs: Box<dyn DavFileSystem> = Box::new(FsPointer(WEBDAV_FS.get().unwrap().clone()));
    let dav_server = DavHandler::builder()
        .filesystem(fs)
        .locksystem(FakeLs::new())
        .build_handler();

    let service = hyper::service::make_service_fn(move |_| {
        let dav_server = dav_server.clone();
        async move {
            let func = move |req: Request<Body>| {
                let dav_server = dav_server.clone();

                async move {
                    let raft = RAFT.get().unwrap();

                    // (lieuwe): I have the feeling this is very slow, look at the source...
                    // But to be sure I will keep it here and profile it.
                    let start = Instant::now();
                    let read = raft.client_read().await;
                    trace!("raft.client_read() took {:?}", start.elapsed());

                    let res = match read {
                        Ok(_) => dav_server.handle(req).await,
                        Err(e) => {
                            error!("catched raft error before webdav: {:?}", e);

                            let mut resp = Response::builder();
                            resp = resp.header("Content-Length", "0").status(500); // TODO
                            resp = resp.header("connection", "close");
                            resp.body(webdav_handler::body::Body::empty()).unwrap()
                        }
                    };
                    Ok::<_, Infallible>(res)
                }
            };
            Ok::<_, Infallible>(hyper::service::service_fn(func))
        }
    });

    info!("binding WEBDAV server");
    hyper::Server::bind(addr)
        .serve(service)
        .await
        .map_err(|e| anyhow!(e))
}
