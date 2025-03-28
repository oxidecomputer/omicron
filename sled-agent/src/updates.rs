// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of per-sled updates

use bootstrap_agent_api::Component;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactVersionError;
use tufaceous_brand_metadata::Metadata;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error: while accessing {path}")]
    Io {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("failed to read zone version from {path}")]
    ZoneVersion {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("Cannot parse artifact version in {path}")]
    ArtifactVersion { path: Utf8PathBuf, err: ArtifactVersionError },
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

fn io_err(path: &Utf8Path, err: std::io::Error) -> Error {
    Error::Io { path: path.into(), err }
}

pub struct UpdateManager {
    config: ConfigUpdates,
}

impl UpdateManager {
    pub fn new(config: ConfigUpdates) -> Self {
        Self { config }
    }

    // Gets the component version information from a single zone artifact.
    async fn component_get_zone_version(
        &self,
        path: &Utf8Path,
    ) -> Result<Component, Error> {
        let file =
            std::fs::File::open(path).map_err(|err| io_err(path, err))?;
        let gzr = flate2::read::GzDecoder::new(file);
        let mut component_reader = tar::Archive::new(gzr);
        let metadata = Metadata::read_from_tar(&mut component_reader)
            .map_err(|err| Error::ZoneVersion { path: path.into(), err })?;
        let info = metadata
            .layer_info()
            .map_err(|err| Error::ZoneVersion { path: path.into(), err })?;
        Ok(Component { name: info.pkg.clone(), version: info.version.clone() })
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

                // Extract the name and artifact version
                let name = "sled-agent".to_string();
                let version =
                    version.parse().map_err(|err| Error::ArtifactVersion {
                        path: version_path.to_path_buf(),
                        err,
                    })?;

                components.push(crate::updates::Component { name, version });
            }
        }

        Ok(components)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use camino_tempfile::NamedUtf8TempFile;
    use flate2::write::GzEncoder;
    use std::io::Write;
    use tar::Builder;
    use tufaceous_artifact::ArtifactVersion;

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
        assert_eq!(components[0].version, ArtifactVersion::new_const("2.0.0"));
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
        assert_eq!(components[0].version, ArtifactVersion::new_const("1.2.3"));
    }
}
