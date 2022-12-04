use std::net::SocketAddr;

use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use tarpc::context::Context;
use uuid::Uuid;

use crate::{
    service::Service,
    webdav::{DirEntry, FileMetadata, SeekFrom},
    RAFT,
};

#[derive(Debug, Clone)]
pub struct Server {
    pub remote_addr: SocketAddr,
}

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

    async fn open(self, _: Context, path: String) -> Uuid {
        todo!()
    }
    async fn read_dir(self, _: Context, path: String) -> Vec<DirEntry> {
        todo!()
    }
    async fn metadata(self, _: Context, path: String) -> FileMetadata {
        todo!()
    }

    async fn file_metadata(self, _: Context, file_id: Uuid) -> FileMetadata {
        todo!()
    }
    async fn write_bytes(self, _: Context, file_id: Uuid, buf: Vec<u8>) -> () {
        todo!()
    }
    async fn read_bytes(self, _: Context, file_id: Uuid, count: usize) -> Vec<u8> {
        todo!()
    }
    async fn seek(self, _: Context, file_id: Uuid, pos: SeekFrom) -> u64 {
        todo!()
    }
    async fn flush(self, _: Context, file_id: Uuid) -> () {
        todo!()
    }
}
