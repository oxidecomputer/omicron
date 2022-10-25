// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types related to rack updates

use semver::Version;
use serde::{Deserialize, Serialize};
use sha3::Digest;
use sha3::Sha3_256;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Sha3_256Digest([u8; 32]);

impl Serialize for Sha3_256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        hex::serde::serialize(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Sha3_256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Sha3_256Digest(hex::serde::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArtifactType {
    // Sled Artifacts
    SledSp,
    SledRoT,
    HostPhase1,
    HostPhase2,

    // PSC Artifacts
    PscSp,
    PscRot,

    // Switch Artifacts
    SwitchSp,
    SwitchRot,
}

/// A description of a software artifact that can be installed on the rack
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    filename: PathBuf,
    artifact_type: ArtifactType,
    version: Version,
    digest: Sha3_256Digest,
    length: u64,
}

/// Attempt to convert an ArtifactSpec to an Artifact
/// by reading from the filesysetm and hashing.
impl TryFrom<ArtifactSpec> for Artifact {
    type Error = std::io::Error;
    fn try_from(spec: ArtifactSpec) -> Result<Self, Self::Error> {
        let mut hasher = Sha3_256::new();
        let mut file = File::open(&spec.filename)?;
        let length = std::io::copy(&mut file, &mut hasher)?;
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&hasher.finalize());
        let digest = Sha3_256Digest(digest);
        Ok(Artifact {
            filename: spec.filename,
            artifact_type: spec.artifact_type,
            version: spec.version,
            digest,
            length,
        })
    }
}

/// User Input that describes artifacts as part of the [`RackUpdateSpec`]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSpec {
    filename: PathBuf,
    artifact_type: ArtifactType,
    version: Version,
}

/// The set of all artifacts in an update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    version: Version,
    artifacts: Vec<Artifact>,
}

impl Manifest {
    pub fn dump<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<PathBuf, std::io::Error> {
        let json = serde_json::to_string(&self)?;
        let path = path.as_ref().join("manifest.json");
        let mut file = File::create(&path)?;
        file.write_all(json.as_bytes())?;
        Ok(path)
    }

    /// Unpack a Tar archive into `unpack_dir`, read the manifest, and return
    /// the manifest.
    pub fn load(
        tarfile: impl AsRef<Path>,
        unpack_dir: impl AsRef<Path>,
    ) -> std::io::Result<Manifest> {
        let tarfile = File::open(tarfile)?;
        let mut archive = tar::Archive::new(tarfile);
        archive.unpack(&unpack_dir)?;
        let json = std::fs::read_to_string(
            &unpack_dir.as_ref().join("manifest.json"),
        )?;
        let manifest = serde_json::from_str(&json)?;
        Ok(manifest)
    }
}

/// The user input description of a [`RackUpdate`]
///
/// Files are read and processed into a [`RackUpdate`] according to the
/// [`RackUpdateSpec`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackUpdateSpec {
    version: Version,
    artifacts: Vec<ArtifactSpec>,
}

impl RackUpdateSpec {
    /// Create a new RackUpdateSpec.
    ///
    /// Typically this will be created by reading from a `rack-update-
    /// spec.toml` file.
    pub fn new(
        version: Version,
        artifacts: Vec<ArtifactSpec>,
    ) -> RackUpdateSpec {
        RackUpdateSpec { version, artifacts }
    }

    /// Return the name of the given release
    pub fn release_name(&self) -> String {
        format!("oxide-release-{}", self.version)
    }

    /// Create a Tar archive file including a generated manifest and all release
    /// files described by `self.artifacts`.
    ///
    /// Return the path of the created archive.
    ///
    /// This archive file can be loaded into a [`RackUpdate`] via
    /// [`RackUpdate::load`].
    pub fn create_archive(
        self,
        output_dir: PathBuf,
    ) -> std::io::Result<PathBuf> {
        let mut artifacts = vec![];
        let mut filename = output_dir.clone();
        filename.push(self.release_name());
        filename.set_extension("tar");
        let tarfile = File::create(&filename)?;
        let mut builder = tar::Builder::new(tarfile);
        for artifact_spec in self.artifacts {
            builder.append_path_with_name(
                &artifact_spec.filename,
                &artifact_spec.filename.file_name().ok_or(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Filename is invalid: {}",
                            artifact_spec.filename.to_string_lossy()
                        ),
                    ),
                )?,
            )?;
            artifacts.push(artifact_spec.try_into()?);
        }
        let manifest = Manifest { version: self.version, artifacts };
        let manifest_path = manifest.dump(output_dir)?;
        builder.append_path_with_name(
            &manifest_path,
            &manifest_path.file_name().unwrap(),
        )?;
        builder.finish()?;
        Ok(filename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn test_spec() -> (TempDir, RackUpdateSpec) {
        let tmp_dir = TempDir::new().unwrap();
        let sled_sp = ArtifactSpec {
            filename: tmp_dir.path().join("sled-sp-img-1.0.0.tar.gz"),
            artifact_type: ArtifactType::SledSp,
            version: Version::new(1, 0, 0),
        };

        // Create a file of junk data for testing
        let mut file = File::create(&sled_sp.filename).unwrap();
        writeln!(
            file,
            "This is not a real SP Image. Hell it's not even a tarball!"
        )
        .unwrap();
        let spec = RackUpdateSpec::new(Version::new(1, 0, 0), vec![sled_sp]);

        (tmp_dir, spec)
    }

    #[test]
    fn generate_update_archive_then_load_manifest() {
        let (input_dir, spec) = test_spec();
        let output_dir = TempDir::new().unwrap();
        let update_path =
            spec.create_archive(input_dir.path().to_owned()).unwrap();
        let manifest = Manifest::load(&update_path, output_dir.path()).unwrap();
        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.version, Version::new(1, 0, 0));
    }
}
