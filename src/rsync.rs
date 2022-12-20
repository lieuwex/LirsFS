use anyhow::{anyhow, Result};
use async_raft::NodeId;
use camino::Utf8Path;
use tokio::process::Command;

use crate::{util, CONFIG};

pub async fn copy_to(target_node: NodeId, filename: impl AsRef<Utf8Path>) -> Result<()> {
    let filename = filename.as_ref();
    let remote_host = CONFIG
        .get_node_ssh_host(target_node)
        .ok_or_else(|| anyhow!("Target node with id {:?} not found in config", target_node))?;
    let full_path = util::prepend_fs_dir(filename);

    let output = Command::new("rsync")
        .arg("--compress")
        .arg("--archive")
        .arg("--checksum")
        .arg("--preallocate")
        .arg(&full_path)
        .arg(format!("{remote_host}:{full_path}"))
        .output()
        .await?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            "Rsync returned an error copying file {:#?} to node {}: {:#?}",
            filename,
            target_node,
            error
        );
        Err(anyhow!(
            "Rsync returned an error status code: {:#?}",
            output.status.code()
        ))
    } else {
        Ok(())
    }
}
