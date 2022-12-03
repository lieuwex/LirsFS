use std::{net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use anyhow::Result;
use async_raft::NodeId;
use tarpc::{
    client::{Config, RpcError},
    context,
    serde_transport::tcp,
};
use tokio::{
    sync::{
        watch::{self, Ref},
        OwnedRwLockReadGuard, RwLock,
    },
    task::JoinHandle,
    time,
};
use tokio_serde::formats::MessagePack;

use crate::{service::ServiceClient, CONFIG};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    Ready,
    Connecting,
    Reconnecting { failure_reason: RpcError },
}

pub struct NodeConnection {
    client_state: watch::Receiver<ConnectionState>,
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
    addr: SocketAddr,
    client: Arc<RwLock<Option<ServiceClient>>>,
    ready: watch::Sender<ConnectionState>,
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
                        time::sleep(CONFIG.reconnect_try_interval_ms).await;
                        continue;
                    }
                };

                missed_count = 0;
                *lock = Some(c);
                ready.send_replace(ConnectionState::Ready);
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
                    ready.send_replace(ConnectionState::Reconnecting { failure_reason: e });
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
        let (ready_tx, ready_rx) = watch::channel(ConnectionState::Connecting);

        let client = Arc::new(RwLock::new(None));
        let client_cpy = client.clone();

        let pinger = tokio::spawn(async move { pinger(node_id, addr, client_cpy, ready_tx).await });

        Self {
            client_state: ready_rx,
            client,

            pinger,
        }
    }

    /// Retrieve a read lock to the client, locks until the client is ready and obtainable.
    pub async fn get_client(&self) -> OwnedRwLockReadGuard<Option<ServiceClient>, ServiceClient> {
        loop {
            self.wait_is_ready().await;
            let guard = self.client.clone().read_owned().await;
            match OwnedRwLockReadGuard::try_map(guard, |l| l.as_ref()) {
                Err(_) => continue,
                Ok(r) => break r,
            };
        }
    }

    /// Returns whether or not the client is _currently_ ready to use.
    pub fn get_client_state<'a>(&'a self) -> Ref<'a, ConnectionState> {
        self.client_state.borrow()
    }

    /// Waits until the client is marked as ready to use.
    pub async fn wait_is_ready(&self) {
        let mut rx = self.client_state.clone();
        if *rx.borrow_and_update() == ConnectionState::Ready {
            return;
        }

        loop {
            rx.changed().await.unwrap();
            if *rx.borrow() == ConnectionState::Ready {
                return;
            }
        }
    }
}

impl Drop for NodeConnection {
    fn drop(&mut self) {
        self.pinger.abort()
    }
}
