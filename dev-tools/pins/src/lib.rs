// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use camino::Utf8Path;
use semver::Version;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pins {
    pub system_version: Version,
    pub helios: Option<Helios>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Helios {
    pub commit: String,
    pub incorporation: String,
}

impl Pins {
    pub fn read() -> Result<Pins> {
        let workspace_root = cargo_metadata::MetadataCommand::new()
            .no_deps()
            .exec()?
            .workspace_root;
        Pins::read_from_dir(&workspace_root)
    }

    pub fn read_from_dir(workspace_dir: &Utf8Path) -> Result<Pins> {
        let pins_path = workspace_dir.join("tools").join("pins.toml");
        let pins_text = fs_err::read(&pins_path)?;
        let pins: Pins = toml_edit::de::from_slice(&pins_text)
            .with_context(|| format!("while reading {pins_path}"))?;
        ensure!(
            pins.system_version.pre.is_empty()
                && pins.system_version.build.is_empty(),
            "`system_version` must have empty prerelease and build metadata"
        );
        Ok(pins)
    }
}
