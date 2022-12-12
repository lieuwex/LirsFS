use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use uuid::Uuid;

use crate::{
    client_req::AppClientRequest,
    webdav::{DirEntry, FileMetadata, SeekFrom},
};

#[tarpc::service]
pub trait Service {
    /* Raft entries */
    async fn append_entries(
        request: AppendEntriesRequest<AppClientRequest>,
    ) -> AppendEntriesResponse;
    async fn install_snapshot(request: InstallSnapshotRequest) -> InstallSnapshotResponse;
    async fn vote(request: VoteRequest) -> VoteResponse;

    async fn ping();

    /* Filesystem operations */
    async fn open(path: String) -> Uuid;
    async fn read_dir(path: String) -> Vec<DirEntry>;
    async fn metadata(path: String) -> FileMetadata;

    async fn file_metadata(file_id: Uuid) -> FileMetadata;
    async fn write_bytes(file_id: Uuid, buf: Vec<u8>) -> ();
    async fn read_bytes(file_id: Uuid, count: usize) -> Vec<u8>;
    async fn seek(file_id: Uuid, pos: SeekFrom) -> u64;
    async fn flush(file_id: Uuid) -> ();
}
