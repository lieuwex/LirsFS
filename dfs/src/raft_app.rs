use std::{fmt::Debug, ops::Deref, time::Duration};

use async_raft::{
    raft::{ClientWriteRequest, ClientWriteResponse},
    ClientWriteError, NodeId, Raft,
};

use crate::{
    client_req::AppClientRequest, client_res::AppClientResponse, network::AppRaftNetwork,
    storage::AppRaftStorage,
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
        self.app
            .client_write(ClientWriteRequest::new(request.into()))
            .await
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
        f.debug_struct("RaftApp")
            .field("app.metrics().borrow()", &self.metrics().borrow())
            .finish()
    }
}
