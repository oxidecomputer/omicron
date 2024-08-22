// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for parsing configuration files and working with a gateway server
//! configuration

use crate::management_switch::SwitchConfig;
use crate::metrics::MetricsConfig;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use dropshot::ConfigLogging;
use serde::{Deserialize, Serialize};
use slog_error_chain::SlogInlineError;
use thiserror::Error;

/// Configuration for a gateway server
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// Maximum number of host phase2 recover images we're willing to keep
    /// cached.
    pub host_phase2_recovery_image_cache_max_images: usize,
    /// Partial configuration for our dropshot server.
    pub dropshot: PartialDropshotConfig,
    /// Configuration of the management switch.
    pub switch: SwitchConfig,
    /// Server-wide logging configuration.
    pub log: ConfigLogging,
    /// Configuration for SP sensor metrics.
    pub metrics: MetricsConfig,
}

impl Config {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can then be used to create a new gateway server.
    pub fn from_file(path: &Utf8Path) -> Result<Config, LoadError> {
        let file_contents = std::fs::read_to_string(path)
            .map_err(|err| LoadError::Io { path: path.into(), err })?;
        let config_parsed: Config = toml::from_str(&file_contents)
            .map_err(|err| LoadError::Parse { path: path.into(), err })?;
        Ok(config_parsed)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PartialDropshotConfig {
    pub request_body_max_bytes: usize,
}

#[derive(Debug, Error, SlogInlineError)]
pub enum LoadError {
    #[error("error reading \"{path}\": {err}")]
    Io {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("error parsing \"{path}\": {err}")]
    Parse {
        path: Utf8PathBuf,
        #[source]
        err: toml::de::Error,
    },
}
