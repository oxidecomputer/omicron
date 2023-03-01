// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use crate::nexus::NexusClient;
use futures::{TryFutureExt, TryStreamExt};
use omicron_common::api::internal::nexus::{
    KnownArtifactKind, UpdateArtifactId,
};
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error(
        "sled-agent only supports applying zones, found artifact ID {}/{} with kind {}",
        .0.name, .0.version, .0.kind
    )]
    UnsupportedKind(UpdateArtifactId),

    #[error("Failed request to Nexus: {0}")]
    Response(nexus_client::Error<nexus_client::types::Error>),
}

pub struct UpdateManager {
    // Path where zone artifacts are stored.
    zone_artifact_path: PathBuf,
}

impl UpdateManager {
    pub fn new() -> Self {
        Self { zone_artifact_path: PathBuf::from("/opt/oxide") }
    }

    pub async fn download_artifact(
        &self,
        artifact: UpdateArtifactId,
        nexus: &NexusClient,
    ) -> Result<(), Error> {
        match artifact.kind {
            // TODO This is a demo for tests, for now.
            KnownArtifactKind::ControlPlane => {
                let directory = &self.zone_artifact_path.as_path();
                tokio::fs::create_dir_all(&directory).await.map_err(|err| {
                    Error::Io {
                        message: format!("creating directory {directory:?}"),
                        err,
                    }
                })?;

                // We download the file to a temporary file. We then rename it to
                // "<artifact-name>" after it has successfully downloaded, to
                // signify that it is ready for usage.
                let (file, temp_path) = NamedTempFile::new_in(&directory)
                    .map_err(|err| Error::Io {
                        message: "create temp file".to_string(),
                        err,
                    })?
                    .into_parts();
                let mut file = tokio::fs::File::from_std(file);

                // Fetch the artifact and write to the file in its entirety,
                // replacing it if it exists.

                let response = nexus
                    .cpapi_artifact_download(
                        nexus_client::types::KnownArtifactKind::ControlPlane,
                        &artifact.name,
                        &artifact.version,
                    )
                    .await
                    .map_err(Error::Response)?;

                let mut stream = response.into_inner_stream();
                while let Some(chunk) = stream
                    .try_next()
                    .await
                    .map_err(|e| Error::Response(e.into()))?
                {
                    file.write_all(&chunk)
                        .map_err(|err| Error::Io {
                            message: "write_all".to_string(),
                            err,
                        })
                        .await?;
                }
                file.flush().await.map_err(|err| Error::Io {
                    message: "flush temp file".to_string(),
                    err,
                })?;
                drop(file);

                // Move the file to its final path.
                let destination = directory.join(artifact.name);
                temp_path.persist(&destination).map_err(|err| Error::Io {
                    message: format!(
                        "renaming {:?} to {destination:?}",
                        err.path
                    ),
                    err: err.error,
                })?;

                Ok(())
            }
            _ => Err(Error::UnsupportedKind(artifact)),
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
        let artifact = UpdateArtifactId {
            name: expected_name.to_string(),
            version: "0.0.0".to_string(),
            kind: KnownArtifactKind::ControlPlane,
        };

        let tempdir = tempfile::tempdir().expect("Failed to make tempdir");
        let expected_path = tempdir.path().join(expected_name);

        // Remove the file if it already exists.
        let _ = tokio::fs::remove_file(&expected_path).await;

        // Let's pretend this is an artifact Nexus can actually give us.
        let mut nexus_client = MockNexusClient::default();
        nexus_client.expect_cpapi_artifact_download().times(1).return_once(
            move |kind, name, version| {
                assert_eq!(name, "test_artifact");
                assert_eq!(version, "0.0.0");
                assert_eq!(kind.to_string(), "control_plane");
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

        let updates =
            UpdateManager { zone_artifact_path: tempdir.path().into() };
        // This should download the file to our local filesystem.
        updates.download_artifact(artifact, &nexus_client).await.unwrap();

        // Confirm the download succeeded.
        assert!(expected_path.exists());
        let contents = tokio::fs::read(&expected_path).await.unwrap();
        assert_eq!(std::str::from_utf8(&contents).unwrap(), expected_contents);
    }
}
