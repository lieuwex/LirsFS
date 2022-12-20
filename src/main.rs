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
use tracing::{
    field::{debug, FieldSet, ValueSet},
    trace, Level,
};
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};
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

#[tracing::instrument(level = "trace", skip(raft), fields(raft, am_leader, server_task))]
async fn run_app(raft: RaftApp) -> () {
    let mut server_task: Option<JoinHandle<()>> = None;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let membership: HashSet<_> = CONFIG.nodes.iter().map(|n| n.id).collect();
    if let Err(_) = raft.initialize(membership.clone()).await {
        let _ = raft.change_membership(membership).await;
    }

    let mut metrics = raft.app.metrics().clone();
    loop {
        metrics.changed().await.expect("raft shut down");
        let m = metrics.borrow();
        let am_leader = m.current_leader == Some(m.id);

        trace!(
            raft = debug(&raft),
            am_leader = am_leader,
            server_task = debug(&server_task),
        );

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

    // stderr
    let subscriber = tracing_subscriber::fmt()
        .with_span_events(FmtSpan::FULL)
        .with_file(true)
        .with_line_number(true)
        .with_max_level(Level::TRACE)
        .finish();

    // file
    let subscriber = subscriber.with({
        tracing_subscriber::fmt::Layer::default()
            .compact()
            .with_span_events(FmtSpan::FULL)
            .with_ansi(false)
            .with_file(true)
            .with_line_number(true)
            .with_writer(|| {
                let log_file = format!("{}.log", CONFIG.get_own_id());
                tracing_appender::rolling::never("./logs/", log_file)
            })
    });

    tracing::subscriber::set_global_default(subscriber).unwrap();

    DB.set(
        Database::from_path(&CONFIG.file_registry)
            .await
            .expect("Error connecting to file registry"),
    )
    .expect("DB already set");

    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let raft_config = Arc::new(
        Config::build(CONFIG.cluster_name.clone())
            .heartbeat_interval(if CONFIG.slowdown { 5000 } else { 50 })
            .validate()
            .expect("Failed to build Raft config"),
    );

    let network = Arc::new(AppRaftNetwork::new(raft_config.clone()));
    let storage = Arc::new(AppRaftStorage::new(raft_config.clone()));

    NETWORK.set(network.clone()).expect("NETWORK already set");
    WEBDAV_FS
        .set(Arc::new(WebdavFilesystem {}))
        .expect("WEBDAV_FS already set");
    STORAGE.set(storage.clone()).expect("WEBDAV_FS already set");

    // TODO: put behind some CLI flag?
    // create_all_tables(DB.get().unwrap()).await;

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id();

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = RaftApp::new(Raft::new(node_id, raft_config, network, storage));

    RAFT.set(raft.clone()).unwrap();

    tokio::spawn(async {
        let listen_addr = CONFIG.get_own_tarpc_addr();
        rpc::server(listen_addr).await;
    });

    run_app(raft).await;
}
