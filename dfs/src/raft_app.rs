use std::fmt::Debug;

use async_raft::Raft;

use crate::{
    client_req::AppClientRequest, client_res::AppClientResponse, network::AppRaftNetwork,
    storage::AppRaftStorage,
};

/// Wrapper over [Raft]. Necessary because [Raft] does not implement [Debug].
pub struct RaftApp {
    pub app: Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>,
}

impl RaftApp {
    pub fn new(
        app: Raft<AppClientRequest, AppClientResponse, AppRaftNetwork, AppRaftStorage>,
    ) -> Self {
        Self { app }
    }
}

impl Debug for RaftApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let leader = futures::executor::block_on(self.app.current_leader())
            .map_or("<no leader>".to_string(), |leader_id| leader_id.to_string());

        write!(f, "Raft app - {:#?}", leader)
    }
}
