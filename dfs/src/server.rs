use std::net::SocketAddr;

use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use camino::Utf8PathBuf;
use tarpc::context::Context;

use crate::{
    service::Service,
    webdav::{DirEntry, SeekFrom},
    FILE_SYSTEM, RAFT,
};

#[derive(Debug, Clone)]
pub struct Server {
    pub remote_addr: SocketAddr,
}

// TODO: error handling other than unwrap()

#[tarpc::server]
impl Service for Server {
    async fn append_entries(
        self,
        _: Context,
        request: async_raft::raft::AppendEntriesRequest<crate::client_req::AppClientRequest>,
    ) -> AppendEntriesResponse {
        let raft = RAFT.get().unwrap();
        raft.append_entries(request).await.unwrap()
    }

    async fn install_snapshot(
        self,
        _: tarpc::context::Context,
        request: async_raft::raft::InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let raft = RAFT.get().unwrap();
        raft.install_snapshot(request).await.unwrap()
    }

    async fn vote(self, _: Context, request: async_raft::raft::VoteRequest) -> VoteResponse {
        let raft = RAFT.get().unwrap();
        raft.vote(request).await.unwrap()
    }

    async fn ping(self, _: Context) {}

    async fn read_dir(self, _: Context, path: Utf8PathBuf) -> Vec<DirEntry> {
        FILE_SYSTEM.read_dir(path).await.unwrap()
    }
    async fn write_bytes(self, _: Context, path: Utf8PathBuf, pos: SeekFrom, buf: Vec<u8>) -> () {
        let pos: std::io::SeekFrom = pos.into();
        FILE_SYSTEM.write_bytes(path, pos, &buf).await.unwrap()
    }
    async fn read_bytes(
        self,
        _: Context,
        path: Utf8PathBuf,
        pos: SeekFrom,
        count: usize,
    ) -> Vec<u8> {
        let pos: std::io::SeekFrom = pos.into();
        FILE_SYSTEM.read_bytes(path, pos, count).await.unwrap()
    }
}
