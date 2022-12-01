use std::{env, net::SocketAddr, sync::Arc};

use crate::file_registry::schema::create_all_tables;
use async_raft::{Config, Raft};
use client_req::AppClientRequest;
use client_res::AppClientResponse;
use futures_util::StreamExt;
use network::AppRaftNetwork;
use once_cell::sync::{Lazy, OnceCell};
use server::Server;
use service::Service;
use sqlx::SqlitePool;
use storage::AppRaftStorage;
use tarpc::{
    serde_transport::tcp::listen,
    server::{incoming::Incoming, BaseChannel, Channel},
};
use tokio_serde::formats::MessagePack;

mod client_req;
mod client_res;
mod config;
mod connection;
mod file_registry;
mod migrations;
mod network;
mod operation;
mod server;
mod service;
mod storage;

type AppRaft = Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>;

pub static NETWORK: OnceCell<Arc<AppRaftNetwork>> = OnceCell::new();
pub static RAFT: OnceCell<AppRaft> = OnceCell::new();
pub static CONFIG: Lazy<config::Config> = Lazy::new(|| {
    let config = env::args().nth(1).expect("Provide a config file");
    toml::from_str(&config).expect("Couldn't parse config file")
});

async fn run_app(raft: AppRaft) -> ! {
    loop {}
}

#[tokio::main]
async fn main() {
    todo!("here we have to write some code to connect to the database and run the migrations");

    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let raft_config = Arc::new(
        Config::build(CONFIG.cluster_name)
            .validate()
            .expect("Failed to build Raft config"),
    );

    let pool = SqlitePool::connect(&CONFIG.file_registry)
        .await
        .expect("Error connecting to file registry");

    // TODO: put behind some CLI flag?
    create_all_tables(&pool).await;

    tokio::spawn(async {
        let listen_addr: SocketAddr = format!("[::]:{}", CONFIG.listen_port).parse().unwrap();

        let mut listener = listen(listen_addr, MessagePack::default).await.unwrap();

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
    });

    let network = Arc::new(AppRaftNetwork::new(raft_config.clone()).await.unwrap());
    let storage = Arc::new(AppRaftStorage::new(raft_config.clone()));

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id().await;

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = Raft::new(node_id, raft_config, network.clone(), storage);

    NETWORK.set(network);
    RAFT.set(raft);

    run_app(raft).await; // This is subjective. Do it your own way.
                         // Just run your app, feeding Raft & client
                         // RPCs into the Raft node as they arrive.
}
