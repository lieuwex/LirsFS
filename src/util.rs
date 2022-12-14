use crate::{config::Config, filesystem::FileContentHash, CONFIG};
use anyhow::{anyhow, bail, Result};
use async_raft::NodeId;
use camino::{Utf8Path, Utf8PathBuf};
use webdav_handler::davpath::DavPath;

pub fn flatten_result<T, E1, E2>(val: Result<Result<T, E1>, E2>) -> Result<T>
where
    E1: Into<anyhow::Error>,
    E2: Into<anyhow::Error>,
{
    match val {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => Err(e.into()),
    }
}

/// Prepend [Config]'s `fs_dir` to `file_path`, creating an absolute path to the file on any node in the filesystem.
#[tracing::instrument(level = "debug", ret)]
pub fn prepend_fs_dir(file_path: &Utf8Path, node_id: Option<NodeId>) -> Result<Utf8PathBuf> {
    // TODO: similar for non-unix platforms.
    let file_path = if file_path.starts_with("/") {
        file_path.strip_prefix("/").unwrap()
    } else {
        file_path
    };

    let file_dir = match node_id {
        Some(node_id) => CONFIG
            .find_node(node_id)
            .ok_or_else(|| anyhow!("no node with id {node_id} found"))?
            .get_file_dir(&CONFIG),
        None => CONFIG.file_dir.clone(),
    };

    let mut full_path = file_dir;
    full_path.push(file_path);
    Ok(full_path)
}

#[tracing::instrument(level = "debug", ret)]
pub fn blob_to_hash(hash: &[u8]) -> Result<FileContentHash> {
    if hash.len() != 8 {
        bail!(
            "expected hash to be 8 bytes, but it is {} bytes",
            hash.len()
        );
    }

    let mut bytes: [u8; 8] = [0; 8];
    bytes.copy_from_slice(hash);
    Ok(FileContentHash::from_be_bytes(bytes))
}

#[tracing::instrument(level = "debug", ret)]
pub fn davpath_to_pathbuf(path: &DavPath) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.as_pathbuf()).unwrap()
}
