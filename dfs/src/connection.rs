use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use async_raft::NodeId;
use tarpc::{client::Config, context, serde_transport::tcp};
use tokio::{
    sync::{Mutex, OwnedMutexGuard, TryLockError},
    task::JoinHandle,
    time,
};
use tokio_serde::formats::MessagePack;

use crate::{service::ServiceClient, CONFIG};

pub struct NodeConnection {
    client: Arc<Mutex<ServiceClient>>,
    addr: SocketAddr,

    pinger: JoinHandle<()>,
}

async fn connect(addr: SocketAddr) -> Result<ServiceClient> {
    let c = tcp::connect(addr, MessagePack::default).await?;
    let c = ServiceClient::new(Config::default(), c).spawn();
    Ok(c)
}

impl NodeConnection {
    pub async fn new(node_id: NodeId, addr: SocketAddr) -> Result<Self> {
        let client = connect(addr).await?;
        let client = Arc::new(Mutex::new(client));
        let client_cpy = client.clone();

        let pinger = tokio::spawn(async move {
            // TODO: add some randomization so that not all nodes fire pings all at the same time.
            // TODO: improve error handling when failing to connect after ping failure, this his
            // highly likely to fail.

            let mut interval = time::interval(CONFIG.ping_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            interval.reset(); // don't fire immediately

            let mut missed_count = 0usize;
            loop {
                interval.tick().await;

                {
                    let mut client = client.lock().await;
                    match client.ping(context::current()).await {
                        Err(e) => {
                            missed_count += 1;
                            eprintln!(
                                "error while pinging to {:?} (missed count {}/{}): {:?}",
                                addr,
                                missed_count + 1,
                                CONFIG.max_missed_pings,
                                e
                            );

                            if missed_count > CONFIG.max_missed_pings {
                                eprintln!("reconnecting {} to {}", node_id, addr);
                                *client = connect(addr).await.unwrap();
                                missed_count = 0;
                            }
                        }
                        Ok(()) => missed_count = 0,
                    }
                }
            }
        });

        Ok(Self {
            client: client_cpy,
            addr,

            pinger,
        })
    }

    pub async fn get_client(&self) -> OwnedMutexGuard<ServiceClient> {
        self.client.clone().lock_owned().await
    }

    pub fn try_get_client(&self) -> Result<OwnedMutexGuard<ServiceClient>, TryLockError> {
        self.client.clone().try_lock_owned()
    }
}
