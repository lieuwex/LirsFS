use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use tokio::process::Command;

use crate::{util, CONFIG};

pub struct Rsync {}

impl Rsync {
    pub async fn read_from() -> Result<()> {
        todo!()
    }

    pub async fn write_to() -> Result<()> {
        todo!()
    }

    pub async fn copy_from(remote_host: String, filename: &Path) -> Result<()> {
        let full_path = util::prepend_fs_dir(filename);

        let output = Command::new("rsync")
            .arg(format!("{remote_host}:{full_path}"))
            .arg(full_path)
            .output()
            .await?;
        if !output.status.success() {
            // TODO: If we implement logging, log the stderr/stdout of `rsync` on error
            Err(anyhow!(
                "Rsync returned an error status code: {:#?}",
                output.status.code()
            ))
        } else {
            Ok(())
        }
    }
}
