use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use vsss_rs::Share;

use super::rack_secret::{RackSecret, Verifier};
use crate::bootstrap::agent::BootstrapError;

const FILENAME: &'static str = "trust-quorum-config.json";

// This is a short term mechanism for initializing sleds with their own shares,
// verifiers, and ancillary information.
//
// It is expected to be removed once rack initialization is done and the
// RackSecret is generated dyanmically and shares and verifiers transferred over
// SPDM channels.
//
// Users should only manually modify the `sled_index` and `enabled` fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub enabled: bool,
    pub sled_index: usize,
    pub threshold: usize,
    pub total_shares: usize,
    pub verifier: Verifier,
    pub shares: Vec<Share>,
}

impl Config {
    pub fn new(threshold: usize, total_shares: usize) -> Config {
        let secret = RackSecret::new();
        let (shares, verifier) = secret.split(threshold, total_shares).unwrap();
        Config {
            enabled: false,
            sled_index: 0,
            threshold,
            total_shares,
            verifier,
            shares,
        }
    }

    pub fn write(&self, file: &Path) {
        let json = serde_json::to_string(&self).unwrap();
        fs::write(file, &json).unwrap();
    }

    pub fn read(dir: &str) -> Result<Config, BootstrapError> {
        let mut path = PathBuf::from_str(dir).unwrap();
        path.push(FILENAME);
        let json = fs::read_to_string(path.to_str().unwrap())?;
        serde_json::from_str(&json).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_read_config() {
        let mut dir = std::env::temp_dir();
        dir.push(FILENAME);

        let config = Config::new(3, 5);
        config.write(&dir);

        let read_config =
            Config::read(dir.parent().unwrap().to_str().unwrap()).unwrap();
        assert_eq!(config, read_config);

        std::fs::remove_file(dir.as_path()).unwrap();
    }
}
