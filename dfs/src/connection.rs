use std::{net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use anyhow::Result;
use async_raft::NodeId;
use rand::{thread_rng, Rng};
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
use tokio_serde::formats::Bincode;

use crate::{service::ServiceClient, CONFIG};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    Ready,
    Connecting,
    Reconnecting { failure_reason: RpcError },
}

#[derive(Debug)]
pub struct NodeConnection {
    client_state: watch::Receiver<ConnectionState>,
    client: Arc<RwLock<Option<ServiceClient>>>,

    pinger: JoinHandle<()>,
}

async fn connect(addr: SocketAddr) -> Result<ServiceClient> {
    let c = tcp::connect(addr, Bincode::default).await?;
    let c = ServiceClient::new(Config::default(), c).spawn();
    Ok(c)
}

async fn pinger(
    node_id: NodeId,
    addr: SocketAddr,
    client: Arc<RwLock<Option<ServiceClient>>>,
    ready: watch::Sender<ConnectionState>,
) -> ! {
    let mut interval = time::interval(CONFIG.ping_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut state = ConnectionState::Connecting;
    macro_rules! set_state {
        ($new:expr) => {
            state = $new;
            ready.send_replace(state.clone());
        };
    }

    let mut missed_count = 0usize;
    'ping: loop {
        if matches!(state, ConnectionState::Ready) {
            interval.tick().await;
            let dur = thread_rng().gen_range(Duration::ZERO..=(Duration::from_secs(5)));
            time::sleep(dur).await;
        }

        let mut lock = client.write().await;

        'reconnect: loop {
            let client = match lock.deref() {
                None => {
                    let c = match connect(addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            let interval = CONFIG.reconnect_try_interval_ms;
                            eprintln!(
                                "error while connecting, retrying in {:?}: {:?}",
                                interval, e
                            );
                            time::sleep(interval).await;
                            continue 'reconnect;
                        }
                    };

                    missed_count = 0;
                    *lock = Some(c);
                    set_state!(ConnectionState::Ready);
                    lock.deref().as_ref().unwrap()
                }
                Some(c) => c,
            };

            match client.ping(context::current()).await {
                Err(e) => {
                    missed_count += 1;
                    eprintln!(
                        "error while pinging to {:?} (missed count {}/{}): {:?}",
                        addr, missed_count, CONFIG.max_missed_pings, e
                    );

                    if missed_count > CONFIG.max_missed_pings {
                        eprintln!("reconnecting {} to {}", node_id, addr);
                        *lock = None;
                        set_state!(ConnectionState::Reconnecting { failure_reason: e });
                        continue 'reconnect;
                    }
                }
                Ok(()) => {
                    missed_count = 0;
                }
            }

            break 'reconnect;
        }
    }
}

impl NodeConnection {
    /// Create a new connection, this will be initialised directly in the background.
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
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
    pub fn get_client_state(&self) -> Ref<'_, ConnectionState> {
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
