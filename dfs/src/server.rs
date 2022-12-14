use std::net::SocketAddr;

use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use camino::Utf8PathBuf;
use tarpc::context::Context;
use uuid::Uuid;

use crate::{
    service::Service,
    webdav::{DirEntry, FileMetadata, SeekFrom},
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

    async fn open(self, _: Context, path: Utf8PathBuf) -> Uuid {
        let mut fs = FILE_SYSTEM.lock().await;
        fs.open(path).await.unwrap()
    }
    async fn read_dir(self, _: Context, path: Utf8PathBuf) -> Vec<DirEntry> {
        let fs = FILE_SYSTEM.lock().await;
        fs.read_dir(path).await.unwrap()
    }
    async fn metadata(self, _: Context, path: Utf8PathBuf) -> FileMetadata {
        let fs = FILE_SYSTEM.lock().await;
        fs.metadata(path).await.unwrap()
    }

    async fn file_metadata(self, _: Context, file_id: Uuid) -> FileMetadata {
        let mut fs = FILE_SYSTEM.lock().await;
        fs.file_metadata(file_id).await.unwrap()
    }
    async fn write_bytes(self, _: Context, file_id: Uuid, buf: Vec<u8>) -> () {
        let mut fs = FILE_SYSTEM.lock().await;
        fs.write_bytes(file_id, &buf).await.unwrap()
    }
    async fn read_bytes(self, _: Context, file_id: Uuid, count: usize) -> Vec<u8> {
        let mut fs = FILE_SYSTEM.lock().await;
        fs.read_bytes(file_id, count).await.unwrap()
    }
    async fn seek(self, _: Context, file_id: Uuid, pos: SeekFrom) -> u64 {
        let mut fs = FILE_SYSTEM.lock().await;
        fs.seek(file_id, pos.into()).await.unwrap()
    }
    async fn flush(self, _: Context, file_id: Uuid) -> () {
        let mut fs = FILE_SYSTEM.lock().await;
        fs.flush(file_id).await.unwrap()
    }
}
