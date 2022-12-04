use std::{env, fs, net::SocketAddr, sync::Arc};

use crate::file_registry::schema::create_all_tables;
use async_raft::{Config, Raft};
use client_req::AppClientRequest;
use client_res::AppClientResponse;
use database::Database;
use network::AppRaftNetwork;
use once_cell::sync::{Lazy, OnceCell};
use sqlx::SqlitePool;
use storage::AppRaftStorage;

mod client_req;
mod client_res;
mod config;
mod connection;
mod database;
mod file_registry;
mod rpc;
// mod migrations; <-- Add back later
mod network;
mod operation;
mod server;
mod service;
mod storage;

type AppRaft = Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>;

pub static NETWORK: OnceCell<Arc<AppRaftNetwork>> = OnceCell::new();
pub static RAFT: OnceCell<AppRaft> = OnceCell::new();
pub static CONFIG: Lazy<config::Config> = Lazy::new(|| {
    let config_path = env::args().nth(1).expect("Provide a config file");
    let config = fs::read_to_string(config_path).unwrap();
    toml::from_str(&config).expect("Couldn't parse config file")
});
pub static DB: OnceCell<Database> = OnceCell::new();

async fn run_app(raft: AppRaft) -> ! {
    loop {}
}

#[tokio::main]
async fn main() {
    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let raft_config = Arc::new(
        Config::build(CONFIG.cluster_name.clone())
            .validate()
            .expect("Failed to build Raft config"),
    );
    DB.set(Database {
        pool: SqlitePool::connect(&CONFIG.file_registry)
            .await
            .expect("Error connecting to file registry"),
    })
    .expect("DB already set");

    // TODO: put behind some CLI flag?
    create_all_tables(&DB.get().unwrap().pool).await;

    tokio::spawn(async {
        let listen_addr: SocketAddr = format!("[::]:{}", CONFIG.listen_port).parse().unwrap();
        rpc::server(listen_addr).await;
    });

    let network = Arc::new(AppRaftNetwork::new(raft_config.clone()));
    let storage = Arc::new(AppRaftStorage::new(raft_config.clone()));

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id().await;

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = Raft::new(node_id, raft_config, network.clone(), storage);

    let _ = NETWORK.set(network);
    let _ = RAFT.set(raft.clone());

    run_app(raft).await; // This is subjective. Do it your own way.
                         // Just run your app, feeding Raft & client
                         // RPCs into the Raft node as they arrive.
}
