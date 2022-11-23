use std::net::SocketAddr;

use anyhow::Result;
use tarpc::{client::Config, serde_transport::tcp::connect};
use tokio_serde::formats::MessagePack;

use crate::service::ServiceClient;

pub struct NodeConnection {
    client: Option<ServiceClient>,
    addr: SocketAddr,
}

impl NodeConnection {
    pub fn new(addr: SocketAddr) -> Self {
        Self { client: None, addr }
    }

    pub async fn get_client<'a>(&'a mut self) -> Result<&mut ServiceClient> {
        if self.client.is_some() {
            // REVIEW: do check if client is writable?

            // SAFETY: we just checked self.client is Some.
            let res = unsafe { self.client.as_mut().unwrap_unchecked() };
            return Ok(res);
        }

        let c = connect(self.addr, || MessagePack::default()).await?;
        let c = ServiceClient::new(Config::default(), c).spawn();
        Ok(self.client.get_or_insert(c))
    }

    pub async fn into_client(mut self) -> Result<ServiceClient> {
        self.get_client().await?;
        Ok(self.client.unwrap())
    }
}
