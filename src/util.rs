use crate::{config::Config, filesystem::FileContentHash, CONFIG};
use anyhow::{anyhow, bail, Result};
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
pub fn prepend_fs_dir(file_path: impl AsRef<Utf8Path>) -> Utf8PathBuf {
    let file_path = file_path.as_ref();
    // TODO: similar for non-unix platforms.
    let file_path = if file_path.starts_with("/") {
        file_path.strip_prefix("/").unwrap()
    } else {
        file_path
    };

    let mut full_path = CONFIG.file_dir.clone();
    full_path.push(file_path);
    full_path
}

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

pub fn davpath_to_pathbuf(path: &DavPath) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.as_pathbuf()).unwrap()
}
