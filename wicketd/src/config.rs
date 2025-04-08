// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration related types used by wicketd

use camino::{Utf8Path, Utf8PathBuf};
use dropshot::ConfigLogging;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub log: ConfigLogging,
}

impl Config {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can be used to create a wicketd server.
    pub fn from_file<P: AsRef<Utf8Path>>(
        path: P,
    ) -> Result<Config, ConfigError> {
        let path = path.as_ref();
        let data = std::fs::read_to_string(path).map_err(|error| {
            ConfigError::Io { error, path: path.to_owned() }
        })?;
        toml::from_str(&data).map_err(|error| ConfigError::Parse {
            error,
            path: path.to_owned(),
        })
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {path}")]
    Io {
        #[source]
        error: std::io::Error,
        path: Utf8PathBuf,
    },
    #[error("Failed to parse config file: {path}")]
    Parse {
        #[source]
        error: toml::de::Error,
        path: Utf8PathBuf,
    },
}
