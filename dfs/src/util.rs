use std::path::Path;

use anyhow::Result;

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
