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

impl UpdateArtifact {
    fn artifact_directory(&self) -> &str {
        match self.kind {
            UpdateArtifactKind::Zone => "/var/tmp/zones",
        }
    }

    /// Downloads an update artifact.
    // TODO: Fix error types
    pub async fn download(&self, nexus: &NexusClient) -> anyhow::Result<()> {
        let file_name = format!("{}-{}", self.name, self.version);
        let response = nexus.cpapi_artifact_download(&file_name).await?;

        let mut path = PathBuf::from(self.artifact_directory());
        tokio::fs::create_dir_all(&path).await?;

        // We download the file to a location named "<artifact-name>-<version>".
        // We then rename it to "<artifact-name>" after it has successfully
        // downloaded, to signify that it is ready for usage.
        let mut tmp_path = path.clone();
        tmp_path.push(file_name);
        path.push(&self.name);

        // Write the file in its entirety, replacing it if it exists.
        tokio::fs::write(&tmp_path, response.bytes().await?).await?;
        tokio::fs::rename(&tmp_path, &path).await?;

        Ok(())
    }
}
