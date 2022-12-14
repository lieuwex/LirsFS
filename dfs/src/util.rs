use std::path::Path;

use anyhow::{bail, Result};
use camino::Utf8PathBuf;
use webdav_handler::davpath::DavPath;

use crate::{config::Config, CONFIG};

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
pub fn prepend_fs_dir(file_path: &Path) -> String {
    let mut full_path = CONFIG.file_dir.clone();
    full_path.push(file_path);
    full_path
        .to_str()
        .expect("Error: Non-UTF8 characters in filename")
        .to_owned()
}

pub fn blob_to_hash(hash: &[u8]) -> Result<u64> {
    if hash.len() != 8 {
        bail!(
            "expected hash to be 8 bytes, but it is {} bytes",
            hash.len()
        );
    }

    let mut bytes: [u8; 8] = [0; 8];
    bytes.copy_from_slice(hash);
    Ok(u64::from_be_bytes(bytes))
}

pub fn davpath_to_pathbuf(path: &DavPath) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.as_pathbuf()).unwrap()
}
