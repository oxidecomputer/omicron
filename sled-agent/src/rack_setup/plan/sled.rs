// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "how should sleds be initialized".

use crate::bootstrap::{
    config::BOOTSTRAP_AGENT_PORT, params::SledAgentRequest,
};
use crate::rack_setup::config::SetupServiceConfig as Config;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

fn rss_sled_plan_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH)
        .join("rss-sled-plan.toml")
}

/// Describes errors which may occur while generating a plan for sleds.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Cannot deserialize TOML file at {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub rack_id: Uuid,
    pub sleds: HashMap<SocketAddrV6, SledAgentRequest>,

    // TODO: Consider putting the rack subnet here? This may be operator-driven
    // in the future, so it should exist in the "plan".
    //
    // TL;DR: The more we decouple rom "rss-config.toml", the easier it'll be to
    // switch to an operator-driven interface.
}

impl Plan {
    pub async fn load(
        log: &Logger,
    ) -> Result<Option<Self>, PlanError> {
        // If we already created a plan for this RSS to allocate
        // subnets/requests to sleds, re-use that existing plan.
        let rss_sled_plan_path = rss_sled_plan_path();
        if rss_sled_plan_path.exists() {
            info!(log, "RSS plan already created, loading from file");

            let plan: Self =
                toml::from_str(
                    &tokio::fs::read_to_string(&rss_sled_plan_path).await.map_err(
                        |err| PlanError::Io {
                            message: format!(
                                "Loading RSS plan {rss_sled_plan_path:?}"
                            ),
                            err,
                        },
                    )?,
                )
                .map_err(|err| PlanError::Toml {
                    path: rss_sled_plan_path,
                    err,
                })?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        bootstrap_addrs: impl IntoIterator<Item = Ipv6Addr>,
    ) -> Result<Self, PlanError> {
        let bootstrap_addrs = bootstrap_addrs.into_iter().enumerate();

        let allocations = bootstrap_addrs.map(|(idx, bootstrap_addr)| {
            info!(
                log,
                "Creating plan for the sled at {:?}", bootstrap_addr
            );
            let bootstrap_addr =
                SocketAddrV6::new(bootstrap_addr, BOOTSTRAP_AGENT_PORT, 0, 0);
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);

            (
                bootstrap_addr,
                SledAgentRequest {
                    id: Uuid::new_v4(),
                    subnet
                },
            )
        });

        info!(log, "Serializing plan");

        let mut sleds = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            sleds.insert(addr, allocation);
        }

        let plan = Self {
            rack_id: Uuid::new_v4(),
            sleds,
        };

        // Once we've constructed a plan, write it down to durable storage.
        let serialized_plan =
            toml::Value::try_from(&plan).unwrap_or_else(|e| {
                panic!("Cannot serialize configuration: {:#?}: {}", plan, e)
            });
        let plan_str = toml::to_string(&serialized_plan)
            .expect("Cannot turn config to string");

        info!(log, "Plan serialized as: {}", plan_str);
        let path = rss_sled_plan_path();
        tokio::fs::write(&path, plan_str).await.map_err(|err| {
            PlanError::Io {
                message: format!("Storing RSS sled plan to {path:?}"),
                err,
            }
        })?;
        info!(log, "Sled plan written to storage");

        Ok(plan)
    }
}
