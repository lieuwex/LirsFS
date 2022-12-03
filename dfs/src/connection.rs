use std::{net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use anyhow::Result;
use async_raft::NodeId;
use tarpc::{client::Config, context, serde_transport::tcp};
use tokio::{
    sync::{watch, OwnedRwLockReadGuard, RwLock},
    task::JoinHandle,
    time,
};
use tokio_serde::formats::MessagePack;

use crate::{service::ServiceClient, CONFIG};

pub struct NodeConnection {
    client_ready: watch::Receiver<bool>,
    client: Arc<RwLock<Option<ServiceClient>>>,

    pinger: JoinHandle<()>,
}

async fn connect(addr: SocketAddr) -> Result<ServiceClient> {
    let c = tcp::connect(addr, MessagePack::default).await?;
    let c = ServiceClient::new(Config::default(), c).spawn();
    Ok(c)
}

async fn pinger(
    node_id: NodeId,
    client: Arc<RwLock<Option<ServiceClient>>>,
    ready: watch::Sender<bool>,
    addr: SocketAddr,
) -> ! {
    // TODO: add some randomization so that not all nodes fire pings all at the same time.

    let mut interval = time::interval(CONFIG.ping_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut missed_count = 0usize;
    loop {
        interval.tick().await;

        let mut lock = client.write().await;

        let client = match lock.deref() {
            None => loop {
                let c = match connect(addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("error while connecting, retrying in 500ms: {:?}", e);
                        time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };

                missed_count = 0;
                *lock = Some(c);
                ready.send_replace(true);
                break lock.deref().as_ref().unwrap();
            },
            Some(c) => c,
        };

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
                    *lock = None;
                    ready.send_replace(false);
                }
            }
            Ok(()) => {
                missed_count = 0;
            }
        }
    }
}

impl NodeConnection {
    /// Create a new connection, this will be initialised directly in the background.
    pub async fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        let (ready_tx, ready_rx) = watch::channel(false);

        let client = Arc::new(RwLock::new(None));
        let client_cpy = client.clone();

        let pinger = tokio::spawn(async move { pinger(node_id, client_cpy, ready_tx, addr).await });

        Self {
            client_ready: ready_rx,
            client,

            pinger,
        }
    }

    /// Retrieve a read lock to the client, locks until the client is ready and obtainable.
    pub async fn get_client(&self) -> OwnedRwLockReadGuard<Option<ServiceClient>, ServiceClient> {
        loop {
            self.wait_is_ready().await;
            let guard = self.client.clone().read_owned().await;
            match guard.as_ref() {
                None => continue,
                Some(_) => {
                    break OwnedRwLockReadGuard::map(guard, |l: &Option<ServiceClient>| {
                        l.as_ref().unwrap()
                    })
                }
            }
        }
    }

    /// Returns whether or not the client is _currently_ ready to use.
    pub fn is_client_ready(&self) -> bool {
        *self.client_ready.borrow()
    }

    /// Waits until the client is marked as ready to use.
    pub async fn wait_is_ready(&self) {
        let mut rx = self.client_ready.clone();
        if *rx.borrow_and_update() {
            return;
        }

        loop {
            rx.changed().await.unwrap();
            if *rx.borrow() {
                return;
            }
        }
    }
}
