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
// This is an exception to the rule that we only use the latest version, since
// the back_compat module is only defined for v1.
use crate::v1::early_networking::back_compat;

impl FromStr for EarlyNetworkConfig {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        #[derive(serde::Deserialize)]
        struct ShadowConfig {
            generation: u64,
            schema_version: u32,
            body: EarlyNetworkConfigBody,
        }

        let v2_err = match serde_json::from_str::<ShadowConfig>(&value) {
            Ok(cfg) => {
                return Ok(EarlyNetworkConfig {
                    generation: cfg.generation,
                    schema_version: cfg.schema_version,
                    body: cfg.body,
                });
            }
            Err(e) => format!("unable to parse EarlyNetworkConfig: {e:?}"),
        };
        // If we fail to parse the config as any known version, we return the
        // error corresponding to the parse failure of the newest schema.
        serde_json::from_str::<back_compat::EarlyNetworkConfigV1>(&value)
            .map(|v1| EarlyNetworkConfig {
                generation: v1.generation,
                schema_version: Self::schema_version(),
                body: v1.body.into(),
            })
            .map_err(|_| v2_err)
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
        let v2_error =
            match serde_json::from_slice::<EarlyNetworkConfig>(&config.blob) {
                Ok(val) => return Ok(val),
                Err(error) => {
                    // Log this error and continue trying to deserialize older
                    // versions.
                    warn!(
                        log,
                        "Failed to deserialize EarlyNetworkConfig \
                         as v2, trying next as v1: {}",
                        error,
                    );
                    error
                }
            };

        match serde_json::from_slice::<back_compat::EarlyNetworkConfigV1>(
            &config.blob,
        ) {
            Ok(v1) => {
                // Convert from v1 to v2
                return Ok(EarlyNetworkConfig {
                    generation: v1.generation,
                    schema_version: EarlyNetworkConfig::schema_version(),
                    body: v1.body.into(),
                });
            }
            Err(error) => {
                // Log this error.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig \
                         as v1, trying next as v0: {}",
                    error
                );
            }
        };

        match serde_json::from_slice::<back_compat::EarlyNetworkConfigV0>(
            &config.blob,
        ) {
            Ok(val) => {
                // Convert from v0 to v2
                return Ok(EarlyNetworkConfig {
                    generation: val.generation,
                    schema_version: 2,
                    body: EarlyNetworkConfigBody {
                        ntp_servers: val.ntp_servers,
                        rack_network_config: val.rack_network_config.map(
                            |v0_config| {
                                back_compat::RackNetworkConfigV0::to_v2(
                                    val.rack_subnet,
                                    v0_config,
                                )
                            },
                        ),
                    },
                });
            }
            Err(error) => {
                // Log this error.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig as v0: {}", error,
                );
            }
        };

        // If we fail to parse the config as any known version, we return the
        // error corresponding to the parse failure of the newest schema.
        Err(v2_error)
    }
}
