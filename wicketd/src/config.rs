// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration related types used by wicketd

use dropshot::ConfigLogging;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::net::SocketAddrV6;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub mgs_addr: SocketAddrV6,
    pub log: ConfigLogging,
}

impl Config {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can be used to create a wicketd server.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let path = path.as_ref();
        let data = std::fs::read_to_string(path).context(IoSnafu { path })?;
        toml::from_str(&data).context(ParseSnafu { path })
    }
}

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Failed to read config file: {}", path.display()))]
    Io { source: std::io::Error, path: PathBuf },
    #[snafu(display("Failed to parse config file: {}", path.display()))]
    Parse { source: toml::de::Error, path: PathBuf },
}
