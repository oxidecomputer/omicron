// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use crate::nexus::NexusClient;
use futures::{TryFutureExt, TryStreamExt};
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::nexus::{
    KnownArtifactKind, UpdateArtifactId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
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

    #[error("Version not found in artifact {}", .0.display())]
    VersionNotFound(PathBuf),

    #[error("Malformed version in artifact {path}: {why}", path = path.display())]
    VersionMalformed { path: PathBuf, why: String },

    #[error("Cannot parse semver in {path}: {err}", path = path.display())]
    Semver { path: PathBuf, err: semver::Error },

    #[error("Failed request to Nexus: {0}")]
    Response(nexus_client::Error<nexus_client::types::Error>),
}

fn default_zone_artifact_path() -> PathBuf {
    PathBuf::from("/opt/oxide")
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ConfigUpdates {
    // Path where zone artifacts are stored.
    #[serde(default = "default_zone_artifact_path")]
    pub zone_artifact_path: PathBuf,
}

impl Default for ConfigUpdates {
    fn default() -> Self {
        Self { zone_artifact_path: default_zone_artifact_path() }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Component {
    pub name: String,
    pub version: SemverVersion,
}

// Helper functions for returning errors
fn version_malformed_err(path: &Path, key: &str) -> Error {
    Error::VersionMalformed {
        path: path.to_path_buf(),
        why: format!("Missing '{key}'"),
    }
}

fn io_err(path: &Path, err: std::io::Error) -> Error {
    Error::Io { message: format!("Cannot access {}", path.display()), err }
}

pub struct UpdateManager {
    config: ConfigUpdates,
}

impl UpdateManager {
    pub fn new(config: ConfigUpdates) -> Self {
        Self { config }
    }

    pub async fn download_artifact(
        &self,
        artifact: UpdateArtifactId,
        nexus: &NexusClient,
    ) -> Result<(), Error> {
        match artifact.kind {
            // TODO This is a demo for tests, for now.
            KnownArtifactKind::ControlPlane => {
                let directory = &self.config.zone_artifact_path.as_path();
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

    // Gets the component version information from a single zone artifact.
    async fn component_get_zone_version(
        &self,
        path: &Path,
    ) -> Result<Component, Error> {
        // Decode the zone image
        let file =
            std::fs::File::open(path).map_err(|err| io_err(path, err))?;
        let gzr = flate2::read::GzDecoder::new(file);
        let mut component_reader = tar::Archive::new(gzr);
        let entries =
            component_reader.entries().map_err(|err| io_err(path, err))?;

        // Look for the JSON file which contains the package information
        for entry in entries {
            let entry = entry.map_err(|err| io_err(path, err))?;
            let entry_path = entry.path().map_err(|err| io_err(path, err))?;
            if entry_path == Path::new("oxide.json") {
                let contents = std::fs::read_to_string(entry_path)
                    .map_err(|err| io_err(path, err))?;
                let json = serde_json::Value::from(contents.as_str());

                // Parse keys from the JSON file
                let serde_json::Value::String(pkg) = &json["pkg"] else {
                    return Err(version_malformed_err(path, "pkg"));
                };
                let serde_json::Value::String(version) = &json["version"] else {
                    return Err(version_malformed_err(path, "version"));
                };

                // Extract the name and semver version
                let name = pkg.to_string();
                let version = omicron_common::api::external::SemverVersion(
                    semver::Version::parse(version).map_err(|err| {
                        Error::Semver { path: path.to_path_buf(), err }
                    })?,
                );
                return Ok(crate::updates::Component { name, version });
            }
        }
        Err(Error::VersionNotFound(path.to_path_buf()))
    }

    pub async fn components_get(&self) -> Result<Vec<Component>, Error> {
        let mut components = vec![];

        let dir = &self.config.zone_artifact_path;
        for entry in std::fs::read_dir(dir).map_err(|err| io_err(dir, err))? {
            let entry = entry.map_err(|err| io_err(dir, err))?;
            let file_type =
                entry.file_type().map_err(|err| io_err(dir, err))?;

            if file_type.is_file()
                && entry.file_name().to_string_lossy().ends_with(".tar.gz")
            {
                // Zone Images are currently identified as individual components.
                //
                // This logic may be tweaked in the future, depending on how we
                // bundle together zones.
                components.push(
                    self.component_get_zone_version(&entry.path()).await?,
                );
            } else if file_type.is_dir()
                && entry.file_name().to_string_lossy() == "sled-agent"
            {
                // Sled Agent is the only non-zone file recognized as a component.
                let version_path = entry.path().join("VERSION");
                let version = tokio::fs::read_to_string(&version_path)
                    .await
                    .map_err(|err| io_err(&version_path, err))?;

                // Extract the name and semver version
                let name = "sled-agent".to_string();
                let version = omicron_common::api::external::SemverVersion(
                    semver::Version::parse(&version).map_err(|err| {
                        Error::Semver { path: version_path.to_path_buf(), err }
                    })?,
                );

                components.push(crate::updates::Component { name, version });
            }
        }

        Ok(components)
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

        let config =
            ConfigUpdates { zone_artifact_path: tempdir.path().into() };
        let updates = UpdateManager { config };
        // This should download the file to our local filesystem.
        updates.download_artifact(artifact, &nexus_client).await.unwrap();

        // Confirm the download succeeded.
        assert!(expected_path.exists());
        let contents = tokio::fs::read(&expected_path).await.unwrap();
        assert_eq!(std::str::from_utf8(&contents).unwrap(), expected_contents);
    }
}
