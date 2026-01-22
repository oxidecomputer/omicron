// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for early networking types.

use std::str::FromStr;

use bootstore::schemes::v0 as bootstore;
use slog::{Logger, warn};

use crate::latest::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody,
};

impl FromStr for EarlyNetworkConfig {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        #[derive(serde::Deserialize)]
        struct ShadowConfig {
            generation: u64,
            schema_version: u32,
            body: EarlyNetworkConfigBody,
        }

        match serde_json::from_str::<ShadowConfig>(&value) {
            Ok(cfg) => Ok(EarlyNetworkConfig {
                generation: cfg.generation,
                schema_version: cfg.schema_version,
                body: cfg.body,
            }),
            Err(e) => Err(format!("unable to parse EarlyNetworkConfig: {e:?}")),
        }
    }
}

impl EarlyNetworkConfig {
    pub fn schema_version() -> u32 {
        2
    }

    // Note: This currently only converts between v0 and v1 or deserializes v1 of
    // `EarlyNetworkConfig`.
    pub fn deserialize_bootstore_config(
        log: &Logger,
        config: &bootstore::NetworkConfig,
    ) -> Result<Self, serde_json::Error> {
        // Try to deserialize the latest version of the data structure (v2). If
        // that succeeds we are done.
        match serde_json::from_slice::<EarlyNetworkConfig>(&config.blob) {
            Ok(val) => Ok(val),
            Err(error) => {
                // Log this error and continue trying to deserialize older
                // versions.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig \
                     as v2, trying next as v1: {}",
                    error,
                );
                Err(error)
            }
        }
    }
}
