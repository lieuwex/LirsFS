#![allow(unused_labels)]

use std::{
    collections::HashSet,
    env, fs,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::db::schema::create_all_tables;
use async_raft::{Config, Raft};
use db::{raftlog::RaftLog, Database};
use filesystem::FileSystem;
use network::AppRaftNetwork;
use once_cell::sync::{Lazy, OnceCell};
use raft_app::RaftApp;
use storage::AppRaftStorage;
use tokio::task::JoinHandle;
use tracing::{debug, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use webdav::WebdavFilesystem;

mod client_req;
mod client_res;
mod config;
mod connection;
mod db;
mod rpc;
// mod migrations; <-- Add back later
mod filesystem;
mod network;
mod operation;
mod queue;
mod raft_app;
mod rsync;
mod server;
mod service;
mod storage;
mod util;
mod webdav;

pub static NETWORK: OnceCell<Arc<AppRaftNetwork>> = OnceCell::new();
pub static RAFT: OnceCell<RaftApp> = OnceCell::new();
pub static CONFIG: Lazy<config::Config> = Lazy::new(|| {
    let config_path = env::args().nth(1).expect("Provide a config file");
    let config = fs::read_to_string(config_path).unwrap();

    let config: config::Config = toml::from_str(&config).expect("Couldn't parse config file");
    config.check_integrity().unwrap();
    config
});
pub static FILE_SYSTEM: Lazy<FileSystem> = Lazy::new(|| FileSystem::new());
pub static WEBDAV_FS: OnceCell<Arc<WebdavFilesystem>> = OnceCell::new();
pub static STORAGE: OnceCell<Arc<AppRaftStorage>> = OnceCell::new();
pub static DB: OnceCell<Database> = OnceCell::new();

#[tracing::instrument(level = "trace")]
async fn run_app(raft: RaftApp) -> () {
    let mut server_task: Option<JoinHandle<()>> = None;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let membership: HashSet<_> = CONFIG.nodes.iter().map(|n| n.id).collect();
    if let Err(_) = raft.initialize(membership.clone()).await {
        let _ = raft.change_membership(membership).await;
    }

    let mut metrics = raft.app.metrics().clone();
    loop {
        metrics.changed().await.unwrap();
        let m = metrics.borrow();
        let am_leader = m.current_leader == Some(m.id);

        debug!("{:?} {:?}", am_leader, server_task.take());

        // control webdav server job
        match (am_leader, server_task.take()) {
            (false, None) => {}
            (true, Some(t)) => server_task = Some(t),

            (false, Some(t)) => t.abort(),
            (true, None) => {
                let handle = tokio::spawn(async {
                    webdav::listen(&CONFIG.webdav_addr).await.unwrap();
                });
                server_task = Some(handle);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = dotenv::dotenv();
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let raft_config = Arc::new(
        Config::build(CONFIG.cluster_name.clone())
            .validate()
            .expect("Failed to build Raft config"),
    );
    DB.set(
        Database::from_path(&CONFIG.file_registry)
            .await
            .expect("Error connecting to file registry"),
    )
    .expect("DB already set");

    // TODO: put behind some CLI flag?
    // create_all_tables(DB.get().unwrap()).await;

    tokio::spawn(async {
        let listen_addr = CONFIG.get_own_tarpc_addr();
        rpc::server(listen_addr).await;
    });

    // TODO: WEBDAV_FS, STORAGE

    let network = Arc::new(AppRaftNetwork::new(raft_config.clone()));
    let storage = Arc::new(AppRaftStorage::new(raft_config.clone()));

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id();

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = RaftApp::new(Raft::new(node_id, raft_config, network.clone(), storage));

    NETWORK.set(network).unwrap();
    RAFT.set(raft.clone()).unwrap();

    run_app(raft).await; // This is subjective. Do it your own way.
                         // Just run your app, feeding Raft & client
                         // RPCs into the Raft node as they arrive.
}
