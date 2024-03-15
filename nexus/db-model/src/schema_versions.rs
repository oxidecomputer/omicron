// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap TODO-doc
// XXX consider moving all of this to its own crate?

use crate::schema::SCHEMA_VERSION;
use anyhow::{anyhow, bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use omicron_common::api::external::SemverVersion;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;

static KNOWN_VERSIONS: Lazy<Vec<KnownVersion>> = Lazy::new(|| {
    vec![
        // The first many schema versions only vary by major or micro number and
        // their path is predictable based on the version number.  (This was
        // historically a problem because two pull requests both adding a new
        // schema version might merge cleanly but produce an invalid result.)
        KnownVersion::legacy(1, 0),
        KnownVersion::legacy(2, 0),
        KnownVersion::legacy(3, 0),
        KnownVersion::legacy(3, 1),
        KnownVersion::legacy(3, 2),
        KnownVersion::legacy(3, 3),
        KnownVersion::legacy(4, 0),
        KnownVersion::legacy(5, 0),
        KnownVersion::legacy(6, 0),
        KnownVersion::legacy(7, 0),
        KnownVersion::legacy(8, 0),
        KnownVersion::legacy(9, 0),
        KnownVersion::legacy(10, 0),
        KnownVersion::legacy(11, 0),
        KnownVersion::legacy(12, 0),
        KnownVersion::legacy(13, 0),
        KnownVersion::legacy(14, 0),
        KnownVersion::legacy(15, 0),
        KnownVersion::legacy(16, 0),
        KnownVersion::legacy(17, 0),
        KnownVersion::legacy(18, 0),
        KnownVersion::legacy(19, 0),
        KnownVersion::legacy(20, 0),
        KnownVersion::legacy(21, 0),
        KnownVersion::legacy(22, 0),
        KnownVersion::legacy(23, 0),
        KnownVersion::legacy(23, 1),
        KnownVersion::legacy(24, 0),
        KnownVersion::legacy(25, 0),
        KnownVersion::legacy(26, 0),
        KnownVersion::legacy(27, 0),
        KnownVersion::legacy(28, 0),
        KnownVersion::legacy(29, 0),
        KnownVersion::legacy(30, 0),
        KnownVersion::legacy(31, 0),
        KnownVersion::legacy(32, 0),
        KnownVersion::legacy(33, 0),
        KnownVersion::legacy(33, 1),
        KnownVersion::legacy(34, 0),
        KnownVersion::legacy(35, 0),
        KnownVersion::legacy(36, 0),
        KnownVersion::legacy(37, 0),
        KnownVersion::legacy(37, 1),
        KnownVersion::legacy(38, 0),
        KnownVersion::legacy(39, 0),
        KnownVersion::legacy(40, 0),
        KnownVersion::legacy(41, 0),
        KnownVersion::legacy(42, 0),
        KnownVersion::legacy(43, 0),
        KnownVersion::new(44, "my-thing"),
    ]
});

#[derive(Debug, Clone)]
struct KnownVersion {
    semver: SemverVersion,
    relative_path: String,
}

impl KnownVersion {
    fn legacy(major: u64, micro: u64) -> KnownVersion {
        let semver = SemverVersion::new(major, 0, micro);
        let relative_path = semver.to_string();
        KnownVersion { semver, relative_path }
    }

    fn new(major: u64, relative_path: &str) -> KnownVersion {
        let semver = SemverVersion::new(major, 0, 0);
        KnownVersion { semver, relative_path: relative_path.to_owned() }
    }
}

impl std::fmt::Display for KnownVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.semver.fmt(f)
    }
}

#[derive(Debug, Clone)]
pub struct AllSchemaVersions {
    versions: BTreeMap<SemverVersion, SchemaVersion>,
}

impl AllSchemaVersions {
    pub async fn load(
        directory: &Utf8Path,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        let mut dir =
            tokio::fs::read_dir(directory).await.with_context(|| {
                format!("Failed to read from schema directory {directory:?}")
            })?;

        let mut versions = BTreeMap::new();
        while let Some(entry) = dir.next_entry().await.with_context(|| {
            format!("Failed to read schema dir {directory:?}")
        })? {
            let file_name =
                entry.file_name().into_string().map_err(|os_str| {
                    anyhow!(
                    "Found non-UTF8 entry in schema dir {directory:?}: {:?}",
                    os_str
                )
                })?;
            let file_type = entry.file_type().await.with_context(|| {
                format!(
                    "Failed to determine type of file \
                    {file_name:?} in {directory:?}",
                )
            })?;
            if file_type.is_dir() {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow!("Non-unicode schema dir"))?;
                if let Ok(observed_version) = name.parse::<SemverVersion>() {
                    versions.insert(
                        observed_version.clone(),
                        SchemaVersion {
                            semver: observed_version,
                            path: directory.join(file_name),
                        },
                    );
                } else {
                    bail!("Failed to parse {name} as a semver version");
                }
            }
        }

        Ok(AllSchemaVersions { versions })
    }

    pub fn iter_versions(&self) -> impl Iterator<Item = &SchemaVersion> {
        self.versions.values()
    }

    pub fn contains_version(&self, version: &SemverVersion) -> bool {
        self.versions.contains_key(version)
    }

    pub fn versions_range<'a, R>(
        &self,
        bounds: R,
    ) -> impl Iterator<Item = &'_ SemverVersion>
    where
        R: std::ops::RangeBounds<SemverVersion>,
    {
        self.versions.range(bounds).map(|(k, _)| k)
    }
}

// XXX-dap load the list of SQL files too
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SchemaVersion {
    semver: SemverVersion,
    path: Utf8PathBuf,
}

impl SchemaVersion {
    pub fn semver(&self) -> &SemverVersion {
        &self.semver
    }

    pub fn path(&self) -> &Utf8Path {
        &self.path
    }

    pub fn is_current_software_version(&self) -> bool {
        self.semver == SCHEMA_VERSION
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.semver.fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_known_versions() {
        // All known versions should be unique and increasing.
        let mut known_versions = KNOWN_VERSIONS.iter();
        let mut prev =
            known_versions.next().expect("expected at least one KNOWN_VERSION");
        for v in known_versions {
            println!("checking known version: {} -> {}", prev, v);
            assert!(
                v.semver > prev.semver,
                "KNOWN_VERSION {} appears directly after {}, but is not later",
                v,
                prev
            );

            // We currently make sure there are no gaps in the major number.
            // This is not strictly necessary but it probably reflects a
            // mistake.
            //
            // It's allowed for the major numbers to be the same because a few
            // past schema versions only bumped the micro number for whatever
            // reason.
            assert!(
                v.semver.0.major == prev.semver.0.major
                    || v.semver.0.major == prev.semver.0.major + 1,
                "KNOWN_VERSION {} appears directly after {}, but its major \
                number is neither the same nor one greater",
                v,
                prev
            );
            prev = v;
        }

        assert_eq!(
            prev.semver, SCHEMA_VERSION,
            "last KNOWN_VERSION is {}, but SCHEMA_VERSION is {}",
            prev, SCHEMA_VERSION
        );
    }
}
