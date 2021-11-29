// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use nexus_client::Client as NexusClient;

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
// TODO: Fix error types
pub async fn apply_update(nexus: &NexusClient, artifact: &UpdateArtifact) -> anyhow::Result<()> {
    let file_name = format!("{}-{}", artifact.name, artifact.version);
    let response = nexus.cpapi_artifact_download(&file_name).await?;

    let mut path = PathBuf::from("/opt/oxide");
    path.push(file_name);

    // Write the file in its entirety, replacing it if it exists.
    tokio::fs::write(path, response.bytes().await?).await?;

    // TODO: Call nexus endpoint, download the thing
    // TODO: put it in the right spot
    Ok(())
}
