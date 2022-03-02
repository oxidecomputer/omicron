// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use crate::nexus::NexusClient;
use omicron_common::api::internal::nexus::UpdateArtifactKind;
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::Path;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to contact nexus: {0}")]
    Nexus(anyhow::Error),

    #[error("Failed to read response from Nexus: {0}")]
    Response(reqwest::Error),
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct UpdateArtifact {
    pub name: String,
    pub version: i64,
    pub kind: UpdateArtifactKind,
}

impl UpdateArtifact {
    fn artifact_directory(&self) -> &'static Path {
        match self.kind {
            // TODO flesh this out
            UpdateArtifactKind::GimletRamdisk => Path::new("/var/tmp/zones"),
            UpdateArtifactKind::Zone => Path::new("/var/tmp/zones"),
        }
    }

    /// Downloads an update artifact.
    ///
    /// The artifact is eventually stored in the path:
    ///     <artifact_directory()> / <artifact name>
    ///
    /// Such as:
    ///     /var/tmp/zones/myzone
    ///
    /// While being downloaded, it is stored in a path also containing the
    /// version:
    ///     <artifact_directory()> / <artifact name> - <version>
    ///
    /// Such as:
    ///     /var/tmp/zones/myzone-3
    pub async fn download(&self, nexus: &NexusClient) -> Result<(), Error> {
        let file_name = format!("{}-{}", self.name, self.version);
        let response = nexus
            .cpapi_artifact_download(&file_name)
            .await
            .map_err(|e| Error::Nexus(e.into()))?;

        let mut path = self.artifact_directory().to_path_buf();
        tokio::fs::create_dir_all(&path).await?;

        // We download the file to a location named "<artifact-name>-<version>".
        // We then rename it to "<artifact-name>" after it has successfully
        // downloaded, to signify that it is ready for usage.
        let mut tmp_path = path.clone();
        tmp_path.push(file_name);
        path.push(&self.name);

        // Write the file in its entirety, replacing it if it exists.
        // TODO: Would love to stream this instead.
        let contents =
            response.bytes().await.map_err(|e| Error::Response(e))?;
        tokio::fs::write(&tmp_path, contents).await?;
        tokio::fs::rename(&tmp_path, &path).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mocks::MockNexusClient;
    use http::{Response, StatusCode};

    #[tokio::test]
    #[serial_test::serial]
    async fn test_write_artifact_to_filesystem() {
        // The (completely fabricated) artifact we'd like to download.
        let expected_name = "test_artifact";
        let expected_contents = "test_artifact contents";
        let artifact = UpdateArtifact {
            name: expected_name.to_string(),
            version: 3,
            kind: UpdateArtifactKind::Zone,
        };
        let expected_path = artifact.artifact_directory().join(expected_name);

        // Remove the file if it already exists.
        let _ = tokio::fs::remove_file(&expected_path).await;

        // Let's pretend this is an artifact Nexus can actually give us.
        let mut nexus_client = MockNexusClient::default();
        nexus_client.expect_cpapi_artifact_download().times(1).return_once(
            move |name| {
                assert_eq!(name, "test_artifact-3");
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(expected_contents)
                    .unwrap();
                Ok(response.into())
            },
        );

        // This should download the file to our local filesystem.
        artifact.download(&nexus_client).await.unwrap();

        // Confirm the download succeeded.
        assert!(expected_path.exists());
        let contents = tokio::fs::read(&expected_path).await.unwrap();
        assert_eq!(std::str::from_utf8(&contents).unwrap(), expected_contents);
    }
}
