use std::{fmt::Debug, ops::Deref};

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
