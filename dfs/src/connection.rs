use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tarpc::{client::Config, context, serde_transport::tcp};
use tokio::{
    sync::{Mutex, OwnedMutexGuard},
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
    pub async fn new(addr: SocketAddr) -> Result<Self> {
        let client = connect(addr).await?;
        let client = Arc::new(Mutex::new(client));
        let client_cpy = client.clone();

        let pinger = tokio::spawn(async move {
            // TODO: add some randomization so that not all nodes fire pings all at the same time.

            let mut interval = time::interval(CONFIG.ping_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            interval.reset(); // don't fire immediately

            loop {
                interval.tick().await;

                {
                    let mut client = client.lock().await;
                    if let Err(e) = client.ping(context::current()).await {
                        eprintln!("error while pinging to {:?}: {:?}", addr, e);
                        *client = connect(addr).await.unwrap();
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
}
