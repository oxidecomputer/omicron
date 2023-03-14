// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::updates::ConfigUpdates;
use dropshot::ConfigLogging;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::FindPhysicalLinkError;
use illumos_utils::dladm::PhysicalLink;
use illumos_utils::dladm::CHELSIO_LINK_PREFIX;
use illumos_utils::zpool::ZpoolName;
use omicron_common::vlan::VlanID;
use serde::Deserialize;
use sled_hardware::is_gimlet;
use std::path::{Path, PathBuf};

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// Optionally force the sled to self-identify as a scrimlet (or gimlet,
    /// if set to false).
    pub stub_scrimlet: Option<bool>,
    // TODO: Remove once this can be auto-detected.
    pub sidecar_revision: String,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
    /// Optional list of zpools to be used as "discovered disks".
    pub zpools: Option<Vec<ZpoolName>>,

    /// The data link on which we infer the bootstrap address.
    ///
    /// If unsupplied, we default to:
    ///
    /// - The first physical link on a non-Gimlet machine.
    /// - The first Chelsio link on a Gimlet.
    ///
    /// This allows continued support for development and testing on emulated
    /// systems.
    pub data_link: Option<PhysicalLink>,

    #[serde(default)]
    pub updates: ConfigUpdates,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config from {path}: {err}")]
    Io {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("Failed to parse config from {path}: {err}")]
    Parse {
        path: PathBuf,
        #[source]
        err: toml::de::Error,
    },
    #[error("Could not determine if host is a Gimlet: {0}")]
    SystemDetection(#[source] anyhow::Error),
    #[error("Could not enumerate physical links")]
    FindLinks(#[from] FindPhysicalLinkError),
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        let config = toml::from_str(&contents)
            .map_err(|err| ConfigError::Parse { path: path.into(), err })?;
        Ok(config)
    }

    pub fn get_link(&self) -> Result<PhysicalLink, ConfigError> {
        if let Some(link) = self.data_link.as_ref() {
            Ok(link.clone())
        } else {
            if is_gimlet().map_err(ConfigError::SystemDetection)? {
                Dladm::list_physical()
                    .map_err(ConfigError::FindLinks)?
                    .into_iter()
                    .find(|link| link.0.starts_with(CHELSIO_LINK_PREFIX))
                    .ok_or_else(|| {
                        ConfigError::FindLinks(
                            FindPhysicalLinkError::NoPhysicalLinkFound,
                        )
                    })
            } else {
                Dladm::find_physical().map_err(ConfigError::FindLinks)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_smf_configs() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cannot access manifest directory");
        let smf = PathBuf::from(manifest).join("../smf/sled-agent");

        let mut configs_seen = 0;
        for variant in std::fs::read_dir(smf).unwrap() {
            let variant = variant.unwrap();
            if variant.file_type().unwrap().is_dir() {
                for entry in std::fs::read_dir(variant.path()).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_name() == "config.toml" {
                        Config::from_file(entry.path()).unwrap_or_else(|_| {
                            panic!(
                                "Failed to parse config {}",
                                entry.path().display()
                            )
                        });
                        configs_seen += 1;
                    }
                }
            }
        }
        assert!(configs_seen > 0, "No sled-agent configs found");
    }
}
