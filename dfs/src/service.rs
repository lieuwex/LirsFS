use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use camino::Utf8PathBuf;

use crate::{
    client_req::AppClientRequest,
    webdav::{DirEntry, SeekFrom},
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
    async fn read_dir(path: Utf8PathBuf) -> Vec<DirEntry>;
    async fn write_bytes(path: Utf8PathBuf, pos: SeekFrom, buf: Vec<u8>) -> ();
    async fn read_bytes(path: Utf8PathBuf, pos: SeekFrom, count: usize) -> Vec<u8>;
}
