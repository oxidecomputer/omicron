// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration parameters to Nexus that are usually only known
//! at deployment time.

use super::address::{Ipv6Subnet, RACK_PREFIX};
use super::postgres_config::PostgresConfigWithUrl;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::fmt;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug)]
pub struct LoadError {
    pub path: PathBuf,
    pub kind: LoadErrorKind,
}

#[derive(Debug)]
pub struct InvalidTunable {
    pub tunable: String,
    pub message: String,
}

impl std::fmt::Display for InvalidTunable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid \"{}\": \"{}\"", self.tunable, self.message)
    }
}
impl std::error::Error for InvalidTunable {}

#[derive(Debug)]
pub enum LoadErrorKind {
    Io(std::io::Error),
    Parse(toml::de::Error),
    InvalidTunable(InvalidTunable),
}

impl From<(PathBuf, std::io::Error)> for LoadError {
    fn from((path, err): (PathBuf, std::io::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Io(err) }
    }
}

impl From<(PathBuf, toml::de::Error)> for LoadError {
    fn from((path, err): (PathBuf, toml::de::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Parse(err) }
    }
}

impl std::error::Error for LoadError {}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            LoadErrorKind::Io(e) => {
                write!(f, "read \"{}\": {}", self.path.display(), e)
            }
            LoadErrorKind::Parse(e) => {
                write!(f, "parse \"{}\": {}", self.path.display(), e)
            }
            LoadErrorKind::InvalidTunable(inner) => {
                write!(
                    f,
                    "invalid tunable \"{}\": {}",
                    self.path.display(),
                    inner,
                )
            }
        }
    }
}

impl std::cmp::PartialEq<std::io::Error> for LoadError {
    fn eq(&self, other: &std::io::Error) -> bool {
        if let LoadErrorKind::Io(e) = &self.kind {
            e.kind() == other.kind()
        } else {
            false
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Database {
    FromDns,
    FromUrl {
        #[serde_as(as = "DisplayFromStr")]
        url: PostgresConfigWithUrl,
    },
}

/// Describes how ports are selected for dropshot's HTTP servers.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortPicker {
    /// Use default values for ports, defined by Nexus.
    NexusChoice,
    /// Use port zero - this is avoids conflicts during tests,
    /// by letting the OS pick free ports.
    Zero,
}

impl Default for PortPicker {
    fn default() -> Self {
        PortPicker::NexusChoice
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DeploymentConfig {
    /// Uuid of the Nexus instance
    pub id: Uuid,
    /// Uuid of the Rack where Nexus is executing.
    pub rack_id: Uuid,
    /// External address of Nexus.
    pub external_ip: IpAddr,
    /// Internal address of Nexus.
    pub internal_ip: IpAddr,
    /// Decides how ports are selected
    #[serde(default)]
    pub port_picker: PortPicker,
    /// Portion of the IP space to be managed by the Rack.
    pub subnet: Ipv6Subnet<RACK_PREFIX>,
    /// DB configuration.
    pub database: Database,
}

impl DeploymentConfig {
    /// Load a `DeploymentConfig` from the given TOML file
    ///
    /// This config object can then be used to create a new `Nexus`.
    /// The format is described in the README.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Self = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}
