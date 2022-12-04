use std::net::SocketAddr;

use crate::server::Server;
use crate::service::Service;

use futures_util::StreamExt;
use tarpc::{
    serde_transport::tcp::listen,
    server::{incoming::Incoming, BaseChannel, Channel},
};
use tokio_serde::formats::Bincode;

pub async fn server(listen_addr: SocketAddr) -> ! {
    let listener = listen(listen_addr, Bincode::default).await.unwrap();
    listener
        // Ignore accept errors.
        .filter_map(|r| std::future::ready(r.ok()))
        .map(BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let server = Server {
                remote_addr: channel.transport().peer_addr().unwrap(),
            };
            channel.execute(server.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    unreachable!()
}
