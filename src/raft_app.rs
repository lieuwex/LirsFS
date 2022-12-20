use std::{fmt::Debug, ops::Deref, time::Duration};

use async_raft::{
    raft::{ClientWriteRequest, ClientWriteResponse},
    ClientWriteError, NodeId, Raft,
};

use crate::{
    client_req::AppClientRequest, client_res::AppClientResponse, network::AppRaftNetwork,
    storage::AppRaftStorage, NETWORK,
};

/// Wrapper over [Raft]. Necessary because [Raft] does not implement [Debug].
#[derive(Clone)]
pub struct RaftApp {
    pub app: Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>,
}

impl RaftApp {
    pub fn new(
        app: Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>,
    ) -> Self {
        Self { app }
    }

    pub async fn client_write<Req: Into<AppClientRequest>>(
        &self,
        request: Req,
    ) -> Result<ClientWriteResponse<AppClientResponse>, ClientWriteError<AppClientRequest>> {
        match self
            .app
            .client_write(ClientWriteRequest::new(request.into()))
            .await
        {
            Ok(r) => Ok(r),
            Err(ClientWriteError::ForwardToLeader(req, Some(node_id))) => {
                let network = NETWORK.get().unwrap();
                Ok(network
                    .client_write(node_id, req.clone())
                    .await
                    .map_err(|err| ClientWriteError::ForwardToLeader(req, None))?)
            }
            Err(e) => Err(e),
        }
    }

    /// Get the current leader of the Raft cluster. If no leader exists (i.e., an election is in progress), wait for the new leader.
    pub async fn get_leader_or_wait(&self) -> NodeId {
        let mut metrics = self.metrics().clone();
        loop {
            if let Some(leader) = metrics.borrow().current_leader {
                break leader;
            }
            metrics
                .changed()
                .await
                // If this happens, application is probably dead anyway
                .expect("Error: Raft metrics sender was dropped")
        }
    }
}

impl Deref for RaftApp {
    type Target = Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>;
    fn deref(&self) -> &Self::Target {
        &self.app
    }
}

impl Debug for RaftApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metrics = self.metrics();
        let mref = metrics.borrow();

        f.debug_struct("RaftApp")
            .field("metrics", mref.deref())
            .finish()
    }
}
