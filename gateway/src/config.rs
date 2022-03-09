// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for parsing configuration files and working with a gateway server
//! configuration

use dropshot::{ConfigDropshot, ConfigLogging};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::http_entrypoints::{SpIdentifier, SpType};

// TODO: This is a placeholder; how do we determine what SPs should exist and
// how to talk to them? Just store a list of socket addrs we'll hit with UDP for
// now.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct KnownSps {
    pub ignition_controller: SocketAddr,
    pub switches: Vec<SocketAddr>,
    pub sleds: Vec<SocketAddr>,
    pub power_controllers: Vec<SocketAddr>,
}

impl KnownSps {
    pub(crate) fn addr_for_id(&self, sp: &SpIdentifier) -> Option<SocketAddr> {
        let slot = sp.slot as usize;
        match sp.typ {
            SpType::Sled => self.sleds.get(slot).copied(),
            SpType::Power => self.power_controllers.get(slot).copied(),
            SpType::Switch => self.switches.get(slot).copied(),
        }
    }

    pub(crate) fn addr_for_target(&self, target: u8) -> Option<SocketAddr> {
        let mut target = usize::from(target);
        for targets in [&self.switches, &self.sleds, &self.power_controllers] {
            if let Some(&addr) = targets.get(target) {
                return Some(addr);
            }
            target -= targets.len();
        }
        None
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Timeouts {
    /// Timeout for messages to our local ignition controller SP.
    pub ignition_controller_millis: u64,
    /// Timeout for requests sent to arbitrary SPs.
    pub sp_request_millis: u64,
    /// Default timeout for requests that collect responses from multiple
    /// targets, if the client doesn't provide one.
    pub bulk_request_default_millis: u64,
    /// Maximum timeout allowed for requests that collect responses from
    /// multiple targets; requests that specify a timeout longer than this will
    /// be silently shortened to this value.
    pub bulk_request_max_millis: u64,
    /// Timeout to send back a partial set of results from a bulk request in a
    /// single dropshot page. If we've collected at least one (but have not yet
    /// received all) response from the set of SPs we queried and we hit this
    /// value, we'll send what we have to the client along with a page token to
    /// fetch the remaining results later.
    pub bulk_request_page_millis: u64,
    /// Grace period after a bulk request ends during which we keep the results
    /// in memory so clients can continue to query them with existing page
    /// tokens.
    pub bulk_request_retain_grace_period_millis: u64,
}

/// Configuration for a gateway server
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// Identifier for this instance of MGS
    pub id: uuid::Uuid,
    /// Various timeouts
    pub timeouts: Timeouts,
    /// Bind address for UDP socket for SP communication on the management
    /// network.
    pub udp_bind_address: SocketAddr,
    /// Dropshot configuration for API server
    pub dropshot: ConfigDropshot,
    /// Placeholder description of all known SPs in the system.
    pub known_sps: KnownSps,
    /// Server-wide logging configuration.
    pub log: ConfigLogging,
}

impl Config {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can then be used to create a new gateway server.
    // The format is described in the README. // TODO add a README
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Config = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("error reading \"{}\": {}", path.display(), err)]
    Io { path: PathBuf, err: std::io::Error },
    #[error("error parsing \"{}\": {}", path.display(), err)]
    Parse { path: PathBuf, err: toml::de::Error },
}

impl From<(PathBuf, std::io::Error)> for LoadError {
    fn from((path, err): (PathBuf, std::io::Error)) -> Self {
        LoadError::Io { path, err }
    }
}

impl From<(PathBuf, toml::de::Error)> for LoadError {
    fn from((path, err): (PathBuf, toml::de::Error)) -> Self {
        LoadError::Parse { path, err }
    }
}

impl std::cmp::PartialEq<std::io::Error> for LoadError {
    fn eq(&self, other: &std::io::Error) -> bool {
        if let LoadError::Io { err, .. } = self {
            err.kind() == other.kind()
        } else {
            false
        }
    }
}
