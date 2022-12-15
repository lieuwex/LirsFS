use anyhow::{anyhow, Result};
use async_raft::NodeId;
use camino::Utf8Path;
use tokio::process::Command;

use crate::{read_config, util, APP_CONFIG};

pub struct Rsync {}

impl Rsync {
    pub async fn read_from() -> Result<()> {
        todo!()
    }

    pub async fn write_to() -> Result<()> {
        todo!()
    }

    pub async fn copy_from(node_id: NodeId, filename: &Utf8Path) -> Result<()> {
        let remote_host = read_config!()
            .get_node_ssh_host(node_id)
            .ok_or_else(|| anyhow!("Node with id {:?} has no known socket address", node_id))?;
        let full_path = util::prepend_fs_dir(filename).await;

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
