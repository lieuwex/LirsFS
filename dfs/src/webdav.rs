use std::{convert::Infallible, net::SocketAddr};

use anyhow::anyhow;
use hyper::{Body, Request};
use webdav_handler::{fakels::FakeLs, localfs::LocalFs, DavHandler};

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