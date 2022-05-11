// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration parameters to Nexus that are usually only known
//! at runtime.

use super::address::{Ipv6Subnet, RACK_PREFIX};
use dropshot::ConfigDropshot;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug)]
pub struct LoadError {
    path: PathBuf,
    kind: LoadErrorKind,
}

#[derive(Debug)]
pub enum LoadErrorKind {
    Io(std::io::Error),
    Parse(toml::de::Error),
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RuntimeConfig {
    /// Uuid of the Nexus instance
    pub id: Uuid,
    /// Dropshot configuration for external API server
    pub dropshot_external: ConfigDropshot,
    /// Dropshot configuration for internal API server
    pub dropshot_internal: ConfigDropshot,
    /// Portion of the IP space to be managed by the Rack.
    pub subnet: Ipv6Subnet<RACK_PREFIX>,

    /// An optional database address.
    ///
    /// If `None`, Nexus will use DNS to infer this value.
    pub database_address: Option<SocketAddr>,

//    /// Database parameters
//    #[serde_as(as = "DisplayFromStr")]
//    pub database_url: PostgresConfigWithUrl,
}

impl RuntimeConfig {
    /// Load a `RuntimeConfig` from the given TOML file
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
