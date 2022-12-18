use std::net::SocketAddr;

use async_raft::{
    raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse},
    NodeId,
};
use camino::Utf8PathBuf;
use tarpc::context::Context;
use tracing::trace;

use crate::{
    rsync,
    service::Service,
    webdav::{DirEntry, SeekFrom},
    FILE_SYSTEM, RAFT, STORAGE,
};

#[derive(Debug, Clone)]
pub struct Server {
    pub remote_addr: SocketAddr,
}

// TODO: error handling other than unwrap()

#[tarpc::server]
impl Service for Server {
    #[tracing::instrument(level = "trace")]
    async fn append_entries(
        self,
        _: Context,
        request: async_raft::raft::AppendEntriesRequest<crate::client_req::AppClientRequest>,
    ) -> AppendEntriesResponse {
        let raft = RAFT.get().unwrap();
        raft.append_entries(request).await.unwrap()
    }

    #[tracing::instrument(level = "trace")]
    async fn install_snapshot(
        self,
        _: tarpc::context::Context,
        request: async_raft::raft::InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let raft = RAFT.get().unwrap();
        raft.install_snapshot(request).await.unwrap()
    }

    #[tracing::instrument(level = "trace")]
    async fn vote(self, _: Context, request: async_raft::raft::VoteRequest) -> VoteResponse {
        let raft = RAFT.get().unwrap();
        raft.vote(request).await.unwrap()
    }

    #[tracing::instrument(level = "trace")]
    async fn ping(self, _: Context) {
        trace!("rx ping")
    }

    #[tracing::instrument(level = "trace")]
    async fn read_dir(self, _: Context, path: Utf8PathBuf) -> Vec<DirEntry> {
        FILE_SYSTEM.read_dir(path).await.unwrap()
    }

    #[tracing::instrument(level = "trace")]
    async fn read_bytes(
        self,
        _: Context,
        path: Utf8PathBuf,
        pos: SeekFrom,
        count: usize,
    ) -> Vec<u8> {
        let storage = STORAGE.get().unwrap();
        let lock = storage.get_queue().read(path.clone()).await;

        let pos: std::io::SeekFrom = pos.into();

        FILE_SYSTEM
            .read_bytes(&lock, path, pos, count)
            .await
            .unwrap()
    }

    #[tracing::instrument(level = "trace")]
    async fn copy_file_to(
        self,
        _: Context,
        target: NodeId,
        path: Utf8PathBuf,
    ) -> Result<(), String> {
        let storage = STORAGE.get().unwrap();
        let lock = storage.get_queue().read(path.clone()).await;
        rsync::copy_to(target, &path)
            .await
            .map_err(|err| err.to_string())?;

        drop(lock);
        Ok(())
    }
}
