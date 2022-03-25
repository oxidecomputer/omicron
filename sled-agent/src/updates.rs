// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use crate::nexus::NexusClient;
use omicron_common::api::internal::nexus::{
    UpdateArtifact, UpdateArtifactKind,
};
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to contact nexus: {0}")]
    Nexus(anyhow::Error),

    #[error("Failed to read response from Nexus: {0}")]
    Response(reqwest::Error),
}

pub async fn download_artifact(
    artifact: UpdateArtifact,
    nexus: &NexusClient,
) -> Result<(), Error> {
    match artifact.kind {
        UpdateArtifactKind::Zone => {
            let directory = PathBuf::from("/var/tmp/zones");
            tokio::fs::create_dir_all(&directory).await?;

            // We download the file to a location named "<artifact-name>-<version>".
            // We then rename it to "<artifact-name>" after it has successfully
            // downloaded, to signify that it is ready for usage.
            let tmp_path = directory
                .join(format!("{}-{}", artifact.name, artifact.version));

            // Fetch the artifact and write to the file in its entirety,
            // replacing it if it exists.
            // TODO: Would love to stream this instead.
            // ALSO TODO: This is, for the moment, using the endpoint directly
            // instead of using the client method to work around issues in
            // dropshot/progenitor for getting raw response bodies.
            let response = nexus
                .client()
                .get(format!(
                    "{}/artifacts/{}/{}/{}",
                    nexus.baseurl(),
                    artifact.kind,
                    artifact.name,
                    artifact.version
                ))
                .send()
                .await
                .map_err(Error::Response)?;
            let contents =
                response.bytes().await.map_err(|e| Error::Response(e))?;
            tokio::fs::write(&tmp_path, contents).await?;

            // Write the file to its final path.
            tokio::fs::rename(&tmp_path, directory.join(artifact.name)).await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mocks::MockNexusClient;
    use http::{Response, StatusCode};

    #[tokio::test]
    #[serial_test::serial]
    // TODO this is hard to mock out when not using the generated client
    // methods :( but the logic is covered in the updates integration test
    #[ignore]
    async fn test_write_artifact_to_filesystem() {
        // The (completely fabricated) artifact we'd like to download.
        let expected_name = "test_artifact";
        let expected_contents = "test_artifact contents";
        let artifact = UpdateArtifact {
            name: expected_name.to_string(),
            version: 3,
            kind: UpdateArtifactKind::Zone,
        };
        let expected_path = PathBuf::from("/var/tmp/zones").join(expected_name);

        // Remove the file if it already exists.
        let _ = tokio::fs::remove_file(&expected_path).await;

        // Let's pretend this is an artifact Nexus can actually give us.
        let mut nexus_client = MockNexusClient::default();
        nexus_client.expect_cpapi_artifact_download().times(1).return_once(
            move |kind, name, version| {
                assert_eq!(name, "test_artifact");
                assert_eq!(version, 3);
                assert_eq!(kind.to_string(), "zone");
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(expected_contents)
                    .unwrap();
                Ok(response.into())
            },
        );

        // This should download the file to our local filesystem.
        download_artifact(artifact, &nexus_client).await.unwrap();

        // Confirm the download succeeded.
        assert!(expected_path.exists());
        let contents = tokio::fs::read(&expected_path).await.unwrap();
        assert_eq!(std::str::from_utf8(&contents).unwrap(), expected_contents);
    }
}
