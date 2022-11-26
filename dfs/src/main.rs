use std::{env, sync::Arc};

use async_raft::{Config, Raft};
use client_req::AppClientRequest;
use client_res::AppClientResponse;
use network::AppRaftNetwork;
use once_cell::sync::{Lazy, OnceCell};
use storage::AppRaftStorage;

mod client_req;
mod client_res;
mod config;
mod connection;
mod network;
mod operation;
mod server;
mod service;
mod storage;

type AppRaft = Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>;

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
    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let config = Arc::new(
        Config::build(CONFIG.cluster_name)
            .validate()
            .expect("Failed to build Raft config"),
    );

    let network = Arc::new(AppRaftNetwork::new(config.clone()));
    let storage = Arc::new(AppRaftStorage::new(config.clone()));

    // Get our node's ID from stable storage.
    let node_id = storage.get_own_id().await;

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = Raft::new(node_id, config, network, storage);

    RAFT.set(raft);

    run_app(raft).await; // This is subjective. Do it your own way.
                         // Just run your app, feeding Raft & client
                         // RPCs into the Raft node as they arrive.
}
