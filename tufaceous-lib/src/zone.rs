// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};

/// Describes a new zone to be added.
pub struct AddZone {
    path: Utf8PathBuf,
    name: String,
    version: String,
}

impl AddZone {
    /// Creates an [`AddZone`] from the path, name and version.
    ///
    /// If the name is `None`, it is derived from the filename of the path
    /// without matching extensions.
    pub fn new(
        path: Utf8PathBuf,
        name: Option<String>,
        version: String,
    ) -> Result<Self> {
        let name = match name {
            Some(name) => name,
            None => path
                .file_name()
                .context("zone path is a directory")?
                .split('.')
                .next()
                .expect("str::split has at least 1 element")
                .to_owned(),
        };

        Ok(Self { path, name, version })
    }

    /// Returns the path to the new zone.
    pub fn path(&self) -> &Utf8Path {
        &self.path
    }

    /// Returns the name of the new zone.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the version of the new zone.
    pub fn version(&self) -> &str {
        &self.version
    }
}
