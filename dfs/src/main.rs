use std::{env, fs, net::SocketAddr, sync::Arc};

use crate::db::schema::create_all_tables;
use async_raft::{Config, Raft};
use db::Database;
use network::AppRaftNetwork;
use once_cell::sync::{Lazy, OnceCell};
use raft_app::RaftApp;
use storage::AppRaftStorage;

mod client_req;
mod client_res;
mod config;
mod connection;
mod db;
mod rpc;
// mod migrations; <-- Add back later
mod network;
mod operation;
mod raft_app;
mod server;
mod service;
mod storage;

pub static NETWORK: OnceCell<Arc<AppRaftNetwork>> = OnceCell::new();
pub static RAFT: OnceCell<RaftApp> = OnceCell::new();
pub static CONFIG: Lazy<config::Config> = Lazy::new(|| {
    let config_path = env::args().nth(1).expect("Provide a config file");
    let config = fs::read_to_string(config_path).unwrap();
    toml::from_str(&config).expect("Couldn't parse config file")
});
pub static DB: OnceCell<Database> = OnceCell::new();

async fn run_app(raft: &RaftApp) -> ! {
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
    DB.set(
        Database::from_path(&CONFIG.file_registry)
            .await
            .expect("Error connecting to file registry"),
    )
    .expect("DB already set");

    // TODO: put behind some CLI flag?
    create_all_tables(&DB.get().unwrap()).await;

    tokio::spawn(async {
        let listen_addr: SocketAddr = format!("[::]:{}", CONFIG.listen_port).parse().unwrap();
        rpc::server(listen_addr).await;
    });

    let network = Arc::new(AppRaftNetwork::new(raft_config.clone()));
    let storage = Arc::new(AppRaftStorage::new(raft_config.clone()));

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id();

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
