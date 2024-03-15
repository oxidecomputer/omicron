// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap TODO-doc

use crate::schema::SCHEMA_VERSION;
use anyhow::{anyhow, bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use omicron_common::api::external::SemverVersion;
use std::collections::BTreeMap;

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
