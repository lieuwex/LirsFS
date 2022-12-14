use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    NodeId,
};
use camino::Utf8PathBuf;

use crate::{
    client_req::AppClientRequest,
    client_res::AppClientResponse,
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

    async fn client_write(request: AppClientRequest) -> ClientWriteResponse<AppClientResponse>;

    async fn ping();

    /* Filesystem operations */
    async fn read_dir(path: Utf8PathBuf) -> Vec<DirEntry>;
    async fn read_bytes(path: Utf8PathBuf, pos: SeekFrom, count: usize) -> Vec<u8>;

    /* Request from a different node to copy this file over */
    async fn copy_file_to(target: NodeId, path: Utf8PathBuf) -> Result<(), String>;
}
