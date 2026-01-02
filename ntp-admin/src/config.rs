// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8Path;
use camino::Utf8PathBuf;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use slog_error_chain::SlogInlineError;
use std::io;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,
}
impl Config {
    /// Load a `Config` from the given TOML file
    pub fn from_file(path: &Utf8Path) -> Result<Self, LoadError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|err| LoadError::Read { path: path.to_owned(), err })?;
        toml::de::from_str(&contents)
            .map_err(|err| LoadError::Parse { path: path.to_owned(), err })
    }
}

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum LoadError {
    #[error("failed to read {path}")]
    Read {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("failed to parse {path} as TOML")]
    Parse {
        path: Utf8PathBuf,
        #[source]
        err: toml::de::Error,
    },
}
