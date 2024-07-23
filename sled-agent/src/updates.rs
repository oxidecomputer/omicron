// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use crate::nexus::NexusClient;
use bootstrap_agent_api::Component;
use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::NamedUtf8TempFile;
use futures::{TryFutureExt, TryStreamExt};
use omicron_common::api::internal::nexus::{
    KnownArtifactKind, UpdateArtifactId,
};
use serde::{Deserialize, Serialize};
use std::io::Read;
use tokio::io::AsyncWriteExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Utf-8 error converting path: {0}")]
    FromPathBuf(#[from] camino::FromPathBufError),

    #[error(
        "sled-agent only supports applying zones, found artifact ID {}/{} with kind {}",
        .0.name, .0.version, .0.kind
    )]
    UnsupportedKind(UpdateArtifactId),

    #[error("Version not found in artifact {}", 0)]
    VersionNotFound(Utf8PathBuf),

    #[error("Cannot parse json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Malformed version in artifact {path}: {why}")]
    VersionMalformed { path: Utf8PathBuf, why: String },

    #[error("Cannot parse semver in {path}: {err}")]
    Semver { path: Utf8PathBuf, err: semver::Error },

    #[error("Failed request to Nexus: {0}")]
    Response(nexus_client::Error<nexus_client::types::Error>),
}

fn default_zone_artifact_path() -> Utf8PathBuf {
    Utf8PathBuf::from("/opt/oxide")
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ConfigUpdates {
    // Path where zone artifacts are stored.
    #[serde(default = "default_zone_artifact_path")]
    pub zone_artifact_path: Utf8PathBuf,
}

impl Default for ConfigUpdates {
    fn default() -> Self {
        Self { zone_artifact_path: default_zone_artifact_path() }
    }
}

// Helper functions for returning errors
fn version_malformed_err(path: &Utf8Path, key: &str) -> Error {
    Error::VersionMalformed {
        path: path.to_path_buf(),
        why: format!("Missing '{key}'"),
    }
}

fn io_err(path: &Utf8Path, err: std::io::Error) -> Error {
    Error::Io { message: format!("Cannot access {path}"), err }
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
                let (file, temp_path) = NamedUtf8TempFile::new_in(&directory)
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
                        &KnownArtifactKind::ControlPlane.to_string(),
                        &artifact.name,
                        &artifact.version.clone().into(),
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
        path: &Utf8Path,
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
            let mut entry = entry.map_err(|err| io_err(path, err))?;
            let entry_path = entry.path().map_err(|err| io_err(path, err))?;
            if entry_path == Utf8Path::new("oxide.json") {
                let mut contents = String::new();
                entry
                    .read_to_string(&mut contents)
                    .map_err(|err| io_err(path, err))?;
                let json: serde_json::Value =
                    serde_json::from_str(contents.as_str())?;

                // Parse keys from the JSON file
                let serde_json::Value::String(pkg) = &json["pkg"] else {
                    return Err(version_malformed_err(path, "pkg"));
                };
                let serde_json::Value::String(version) = &json["version"]
                else {
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
        for entry in dir.read_dir_utf8().map_err(|err| io_err(dir, err))? {
            let entry = entry.map_err(|err| io_err(dir, err))?;
            let file_type =
                entry.file_type().map_err(|err| io_err(dir, err))?;
            let path = entry.path();

            if file_type.is_file() && entry.file_name().ends_with(".tar.gz") {
                // Zone Images are currently identified as individual components.
                //
                // This logic may be tweaked in the future, depending on how we
                // bundle together zones.
                components.push(self.component_get_zone_version(&path).await?);
            } else if file_type.is_dir() && entry.file_name() == "sled-agent" {
                // Sled Agent is the only non-zone file recognized as a component.
                let version_path = path.join("VERSION");
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
    use crate::fakes::nexus::FakeNexusServer;
    use flate2::write::GzEncoder;
    use nexus_client::Client as NexusClient;
    use omicron_common::api::external::{Error, SemverVersion};
    use omicron_common::api::internal::nexus::UpdateArtifactId;
    use omicron_test_utils::dev::test_setup_log;
    use std::io::Write;
    use tar::Builder;

    #[tokio::test]
    async fn test_write_artifact_to_filesystem() {
        let logctx = test_setup_log("test_write_artifact_to_filesystem");
        let log = &logctx.log;
        // The (completely fabricated) artifact we'd like to download.
        let expected_name = "test_artifact";
        const EXPECTED_CONTENTS: &'static str = "test_artifact contents";
        let artifact = UpdateArtifactId {
            name: expected_name.to_string(),
            version: "0.0.0".parse().unwrap(),
            kind: KnownArtifactKind::ControlPlane,
        };

        let tempdir =
            camino_tempfile::tempdir().expect("Failed to make tempdir");
        let expected_path = tempdir.path().join(expected_name);

        // Remove the file if it already exists.
        let _ = tokio::fs::remove_file(&expected_path).await;

        // Let's pretend this is an artifact Nexus can actually give us.
        struct NexusServer {}
        impl FakeNexusServer for NexusServer {
            fn cpapi_artifact_download(
                &self,
                artifact_id: UpdateArtifactId,
            ) -> Result<Vec<u8>, Error> {
                assert_eq!(artifact_id.name, "test_artifact");
                assert_eq!(artifact_id.version.to_string(), "0.0.0");
                assert_eq!(artifact_id.kind.to_string(), "control_plane");

                Ok(EXPECTED_CONTENTS.as_bytes().to_vec())
            }
        }

        let nexus_server = crate::fakes::nexus::start_test_server(
            log.clone(),
            Box::new(NexusServer {}),
        );
        let nexus_client = NexusClient::new(
            &format!("http://{}", nexus_server.local_addr()),
            log.clone(),
        );

        let config =
            ConfigUpdates { zone_artifact_path: tempdir.path().into() };
        let updates = UpdateManager { config };
        // This should download the file to our local filesystem.
        updates.download_artifact(artifact, &nexus_client).await.unwrap();

        // Confirm the download succeeded.
        assert!(expected_path.exists());
        let contents = tokio::fs::read(&expected_path).await.unwrap();
        assert_eq!(std::str::from_utf8(&contents).unwrap(), EXPECTED_CONTENTS);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_query_no_components() {
        let tempdir =
            camino_tempfile::tempdir().expect("Failed to make tempdir");
        let config =
            ConfigUpdates { zone_artifact_path: tempdir.path().to_path_buf() };
        let um = UpdateManager::new(config);
        let components =
            um.components_get().await.expect("Failed to get components");
        assert!(components.is_empty());
    }

    #[tokio::test]
    async fn test_query_zone_version() {
        let tempdir =
            camino_tempfile::tempdir().expect("Failed to make tempdir");

        // Construct something that looks like a zone image in the tempdir.
        let zone_path = tempdir.path().join("test-pkg.tar.gz");
        let file = std::fs::File::create(&zone_path).unwrap();
        let gzw = GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = Builder::new(gzw);
        archive.mode(tar::HeaderMode::Deterministic);

        let mut json = NamedUtf8TempFile::new().unwrap();
        json.write_all(
            &r#"{"v":"1","t":"layer","pkg":"test-pkg","version":"2.0.0"}"#
                .as_bytes(),
        )
        .unwrap();
        archive.append_path_with_name(json.path(), "oxide.json").unwrap();
        let mut other_data = NamedUtf8TempFile::new().unwrap();
        other_data
            .write_all("lets throw in another file for good measure".as_bytes())
            .unwrap();
        archive.append_path_with_name(json.path(), "oxide.json").unwrap();
        archive.into_inner().unwrap().finish().unwrap();

        drop(json);
        drop(other_data);

        let config =
            ConfigUpdates { zone_artifact_path: tempdir.path().to_path_buf() };
        let um = UpdateManager::new(config);

        let components =
            um.components_get().await.expect("Failed to get components");
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].name, "test-pkg".to_string());
        assert_eq!(
            components[0].version,
            SemverVersion(semver::Version::new(2, 0, 0))
        );
    }

    #[tokio::test]
    async fn test_query_sled_agent_version() {
        let tempdir =
            camino_tempfile::tempdir().expect("Failed to make tempdir");

        // Construct something that looks like the sled agent.
        let sled_agent_dir = tempdir.path().join("sled-agent");
        std::fs::create_dir(&sled_agent_dir).unwrap();
        std::fs::write(sled_agent_dir.join("VERSION"), "1.2.3".as_bytes())
            .unwrap();

        let config =
            ConfigUpdates { zone_artifact_path: tempdir.path().to_path_buf() };
        let um = UpdateManager::new(config);

        let components =
            um.components_get().await.expect("Failed to get components");
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].name, "sled-agent".to_string());
        assert_eq!(
            components[0].version,
            SemverVersion(semver::Version::new(1, 2, 3))
        );
    }
}
