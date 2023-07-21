// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "how should sleds be initialized".

use crate::bootstrap::{
    config::BOOTSTRAP_AGENT_RACK_INIT_PORT, params::StartSledAgentRequest,
};
use crate::rack_setup::config::SetupServiceConfig as Config;
use crate::storage_manager::StorageResources;
use camino::Utf8PathBuf;
use omicron_common::ledger::{self, Ledger, Ledgerable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv6Addr, SocketAddrV6};
use thiserror::Error;
use uuid::Uuid;

/// Describes errors which may occur while generating a plan for sleds.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] ledger::Error),
}

impl Ledgerable for Plan {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}
const RSS_SLED_PLAN_FILENAME: &str = "rss-sled-plan.toml";

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Plan {
    pub rack_id: Uuid,
    pub sleds: HashMap<SocketAddrV6, StartSledAgentRequest>,

    // Store the provided RSS configuration as part of the sled plan; if it
    // changes after reboot, we need to know.
    pub config: Config,
}

impl Plan {
    pub async fn load(
        log: &Logger,
        storage: &StorageResources,
    ) -> Result<Option<Self>, PlanError> {
        let paths: Vec<Utf8PathBuf> = storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_SLED_PLAN_FILENAME))
            .collect();

        // If we already created a plan for this RSS to allocate
        // subnets/requests to sleds, re-use that existing plan.
        let ledger = Ledger::<Self>::new(log, paths.clone()).await;
        if let Some(ledger) = ledger {
            info!(log, "RSS plan already created, loading from file");
            Ok(Some(ledger.data().clone()))
        } else {
            Ok(None)
        }
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        storage: &StorageResources,
        bootstrap_addrs: HashSet<Ipv6Addr>,
        use_trust_quorum: bool,
    ) -> Result<Self, PlanError> {
        let rack_id = Uuid::new_v4();

        let bootstrap_addrs = bootstrap_addrs.into_iter().enumerate();
        let allocations = bootstrap_addrs.map(|(idx, bootstrap_addr)| {
            info!(log, "Creating plan for the sled at {:?}", bootstrap_addr);
            let bootstrap_addr = SocketAddrV6::new(
                bootstrap_addr,
                BOOTSTRAP_AGENT_RACK_INIT_PORT,
                0,
                0,
            );
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);

            (
                bootstrap_addr,
                StartSledAgentRequest {
                    id: Uuid::new_v4(),
                    subnet,
                    ntp_servers: config.ntp_servers.clone(),
                    dns_servers: config.dns_servers.clone(),
                    use_trust_quorum,
                    rack_id,
                },
            )
        });

        info!(log, "Serializing plan");

        let mut sleds = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            sleds.insert(addr, allocation);
        }

        let plan = Self { rack_id, sleds, config: config.clone() };

        // Once we've constructed a plan, write it down to durable storage.
        let paths: Vec<Utf8PathBuf> = storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_SLED_PLAN_FILENAME))
            .collect();

        let mut ledger = Ledger::<Self>::new_with(log, paths, plan.clone());
        ledger.commit().await?;
        info!(log, "Sled plan written to storage");
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rss_sled_plan_schema() {
        let schema = schemars::schema_for!(Plan);
        expectorate::assert_contents(
            "../schema/rss-sled-plan.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
