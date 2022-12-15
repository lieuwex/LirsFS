#![allow(unused_labels)]

use crate::db::schema::create_all_tables;
use async_raft::{Config, Raft};
use client_req::AppClientRequest;
use client_res::AppClientResponse;
use config::AppConfig;
use db::Database;
use filesystem::FileSystem;
use network::AppRaftNetwork;
use once_cell::sync::{Lazy, OnceCell};
use raft_app::RaftApp;
use std::{
    cell::RefCell,
    env, fs,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use storage::AppRaftStorage;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};

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
mod raft_app;
mod rsync;
mod server;
mod service;
mod storage;
mod util;
mod webdav;

pub static NETWORK: OnceCell<Arc<AppRaftNetwork>> = OnceCell::new();
pub static RAFT: OnceCell<RaftApp> = OnceCell::new();
pub static APP_CONFIG: Lazy<RwLock<AppConfig>> = Lazy::new(|| RwLock::new(AppConfig::from_file()));

#[macro_export]
macro_rules! read_config {
    () => {
        APP_CONFIG.read().await
    };
}

#[macro_export]
macro_rules! write_config {
    () => {
        APP_CONFIG.write().await
    };
}
pub static FILE_SYSTEM: Lazy<Mutex<FileSystem>> = Lazy::new(|| Mutex::new(FileSystem::new()));
pub static DB: OnceCell<Database> = OnceCell::new();

async fn run_app(raft: &RaftApp) -> ! {
    let mut server_task: Option<JoinHandle<()>> = None;

    let mut metrics = raft.app.metrics().clone();
    loop {
        metrics.changed().await.unwrap();
        let m = metrics.borrow();
        let am_leader = m.current_leader == Some(m.id);

        // control webdav server job
        match (am_leader, server_task.take()) {
            (false, None) => {}
            (true, Some(t)) => server_task = Some(t),

            (false, Some(t)) => t.abort(),
            (true, None) => {
                let handle = tokio::spawn(async {
                    let addr = IpAddr::from_str("::0").unwrap();
                    let addr: SocketAddr = (addr, 25565).into();
                    webdav::listen(&addr).await.unwrap();
                });
                server_task = Some(handle);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let raft_config = Arc::new(
        Config::build(read_config!().cluster_name.clone())
            .validate()
            .expect("Failed to build Raft config"),
    );
    DB.set(
        Database::from_path(&read_config!().file_registry)
            .await
            .expect("Error connecting to file registry"),
    )
    .expect("DB already set");

    // TODO: put behind some CLI flag?
    create_all_tables(DB.get().unwrap()).await;

    tokio::spawn(async {
        let listen_addr: SocketAddr = format!("[::]:{}", read_config!().listen_port)
            .parse()
            .unwrap();
        rpc::server(listen_addr).await;
    });

    let network = Arc::new(AppRaftNetwork::new(raft_config.clone()).await);
    let storage = Arc::new(AppRaftStorage::new(raft_config.clone()));

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id().await;

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = RaftApp::new(Raft::new(node_id, raft_config, network.clone(), storage));

    NETWORK.set(network).unwrap();
    RAFT.set(raft).unwrap();

    run_app(RAFT.get().unwrap()).await; // This is subjective. Do it your own way.
                                        // Just run your app, feeding Raft & client
                                        // RPCs into the Raft node as they arrive.
}
