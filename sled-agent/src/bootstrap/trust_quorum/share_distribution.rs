// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::path::{Path, PathBuf};
use vsss_rs::Share;

use super::rack_secret::Verifier;
use crate::bootstrap::agent::BootstrapError;

const FILENAME: &'static str = "share.json";

/// A ShareDistribution is an individual share of a secret along with all the
/// metadata required to allow a server in possession of the share to know how
/// to correctly recreate a split secret.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShareDistribution {
    pub threshold: usize,
    pub total_shares: usize,
    pub verifier: Verifier,
    pub share: Share,
}

impl ShareDistribution {
    pub fn write(&self, dir: &Path) -> Result<(), BootstrapError> {
        let mut path = PathBuf::from(dir);
        path.push(FILENAME);
        let json = serde_json::to_string(&self)?;
        fs::write(path, &json)?;
        Ok(())
    }

    pub fn read(dir: &Path) -> Result<ShareDistribution, BootstrapError> {
        let mut path = PathBuf::from(dir);
        path.push(FILENAME);
        let json = fs::read_to_string(path.to_str().unwrap())?;
        serde_json::from_str(&json).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::super::RackSecret;
    use super::*;

    const THRESHOLD: usize = 3;
    const TOTAL: usize = 5;

    fn get_share_and_verifier() -> (Share, Verifier) {
        let secret = RackSecret::new();
        let (mut shares, verifier) = secret.split(THRESHOLD, TOTAL).unwrap();
        (shares.pop().unwrap(), verifier)
    }

    #[test]
    fn write_and_read() {
        let dir = std::env::temp_dir();

        let (share, verifier) = get_share_and_verifier();
        let share_distribution = ShareDistribution {
            threshold: THRESHOLD,
            total_shares: TOTAL,
            verifier,
            share,
        };
        share_distribution.write(&dir).unwrap();

        let read = ShareDistribution::read(&dir).unwrap();
        assert_eq!(share_distribution, read);

        let mut file = dir.clone();
        file.push(FILENAME);
        std::fs::remove_file(file.as_path()).unwrap();
    }
}
