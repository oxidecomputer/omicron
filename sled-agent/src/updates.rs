// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use crate::nexus::NexusClient;
use futures::TryStreamExt;
use omicron_common::api::internal::nexus::{
    UpdateArtifact, UpdateArtifactKind,
};
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed request to Nexus: {0}")]
    Response(nexus_client::Error<nexus_client::types::Error>),
}

pub async fn download_artifact(
    artifact: UpdateArtifact,
    nexus: &NexusClient,
) -> Result<(), Error> {
    match artifact.kind {
        UpdateArtifactKind::Zone => {
            let directory = PathBuf::from("/var/tmp/zones");
            tokio::fs::create_dir_all(&directory).await.map_err(|err| {
                Error::Io {
                    message: format!("creating diretory {directory:?}"),
                    err,
                }
            })?;

            // We download the file to a location named "<artifact-name>-<version>".
            // We then rename it to "<artifact-name>" after it has successfully
            // downloaded, to signify that it is ready for usage.
            let tmp_path = directory
                .join(format!("{}-{}", artifact.name, artifact.version));

            // Fetch the artifact and write to the file in its entirety,
            // replacing it if it exists.

            let response = nexus
                .cpapi_artifact_download(
                    nexus_client::types::UpdateArtifactKind::Zone,
                    &artifact.name,
                    artifact.version,
                )
                .await
                .map_err(Error::Response)?;

            // TODO it would be better to stream this into the file rather than
            // accumulating it in memory.
            let contents = response
                .into_inner_stream()
                .try_fold(Vec::new(), |mut acc, x| async move {
                    acc.extend(x);
                    Ok(acc)
                })
                .await
                .map_err(|e| Error::Response(e.into()))?;

            tokio::fs::write(&tmp_path, contents).await.map_err(|err| {
                Error::Io {
                    message: format!(
                        "Downloading artifact to temporary path: {tmp_path:?}"
                    ),
                    err,
                }
            })?;

            // Write the file to its final path.
            let destination = directory.join(artifact.name);
            tokio::fs::rename(&tmp_path, &destination).await.map_err(
                |err| Error::Io {
                    message: format!(
                        "Renaming {tmp_path:?} to {destination:?}"
                    ),
                    err,
                },
            )?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mocks::MockNexusClient;
    use bytes::Bytes;
    use http::StatusCode;
    use progenitor::progenitor_client::{ByteStream, ResponseValue};
    use reqwest::{header::HeaderMap, Result};

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
                let response = ByteStream::new(Box::pin(
                    futures::stream::once(futures::future::ready(Result::Ok(
                        Bytes::from(expected_contents),
                    ))),
                ));
                Ok(ResponseValue::new(
                    response,
                    StatusCode::OK,
                    HeaderMap::default(),
                ))
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
