// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for early networking types.

use std::str::FromStr;

use bootstore::schemes::v0 as bootstore;
use slog::Logger;

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

    /// Attempt to read the contents of the bootstore, converting from old
    /// versions if necessary.
    pub fn deserialize_bootstore_config(
        _log: &Logger,
        config: &bootstore::NetworkConfig,
    ) -> Result<Self, serde_json::Error> {
        // Try to serialize the latest version. We don't currently try to read
        // any old versions - the last time we changed it in a wire-incompatible
        // way was many releases ago.
        //
        // If a wire-incompatible change to `EarlyNetworkConfig` is made, this
        // function will need to change to account for that (at least during the
        // one major release where the change is rolled out).
        serde_json::from_slice::<EarlyNetworkConfig>(&config.blob)
    }
}
