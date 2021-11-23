//! Management of per-sled updates

use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use omicron_common::NexusClient;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub enum UpdateArtifactKind {
    Zone,
}

// TODO: De-duplicate this struct with the one in iliana's PR?
#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct UpdateArtifact {
    pub name: String,
    pub version: i64,
    pub kind: UpdateArtifactKind,
}

/// Downloads an entire update artifact.
pub async fn apply_update(nexus: &NexusClient, artifact: &UpdateArtifact) -> anyhow::Result<()> {
    let file_name = format!("{}-{}", artifact.name, artifact.version);
    let response = nexus.cpapi_artifact_download(&file_name).await?;

    let mut path = PathBuf::from("/opt/oxide");
    path.push(file_name);
    tokio::fs::write(path, response.content()).await?;

    // TODO: O_TRUNC file

    // TODO: Call nexus endpoint, download the thing
    // TODO: put it in the right spot
    Ok(())
}
