// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use omicron_common::api::internal::nexus::UpdateArtifactKind;

/// Describes a new artifact to be added.
pub struct AddArtifact {
    kind: UpdateArtifactKind,
    path: Utf8PathBuf,
    name: String,
    version: String,
}

impl AddArtifact {
    /// Creates an [`AddArtifact`] from the path, name and version.
    ///
    /// If the name is `None`, it is derived from the filename of the path
    /// without matching extensions.
    pub fn new(
        kind: UpdateArtifactKind,
        path: Utf8PathBuf,
        name: Option<String>,
        version: String,
    ) -> Result<Self> {
        let name = match name {
            Some(name) => name,
            None => path
                .file_name()
                .context("artifact path is a directory")?
                .split('.')
                .next()
                .expect("str::split has at least 1 element")
                .to_owned(),
        };

        Ok(Self { kind, path, name, version })
    }

    /// Returns the kind of artifact this is.
    pub fn kind(&self) -> UpdateArtifactKind {
        self.kind
    }

    /// Returns the path to the new artifact.
    pub fn path(&self) -> &Utf8Path {
        &self.path
    }

    /// Returns the name of the new artifact.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the version of the new artifact.
    pub fn version(&self) -> &str {
        &self.version
    }
}
