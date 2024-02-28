// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_deployment` crate-level docs for background.

use anyhow::{anyhow, Context};
use nexus_capabilities::Base;
use nexus_capabilities::FirewallRules;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::identity::Asset;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use slog::info;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use uuid::Uuid;

mod datasets;
mod dns;
mod omicron_zones;
mod resource_allocation;

struct Sled {
    id: Uuid,
    sled_agent_address: SocketAddrV6,
    is_scrimlet: bool,
}

impl Sled {
    pub fn subnet(&self) -> Ipv6Subnet<SLED_PREFIX> {
        Ipv6Subnet::<SLED_PREFIX>::new(*self.sled_agent_address.ip())
    }
}

impl From<nexus_db_model::Sled> for Sled {
    fn from(value: nexus_db_model::Sled) -> Self {
        Sled {
            id: value.id(),
            sled_agent_address: value.address(),
            is_scrimlet: value.is_scrimlet(),
        }
    }
}

// A vastly-restricted `Nexus` object that allows us access to some of Nexus
// proper's capabilities.
struct NexusContext<'a> {
    opctx: OpContext,
    datastore: &'a DataStore,
}

impl Base for NexusContext<'_> {
    fn log(&self) -> &Logger {
        &self.opctx.log
    }

    fn datastore(&self) -> &DataStore {
        self.datastore
    }
}

impl nexus_capabilities::SledAgent for NexusContext<'_> {
    fn opctx_sled_client(&self) -> &OpContext {
        &self.opctx
    }
}

impl nexus_capabilities::FirewallRules for NexusContext<'_> {}

/// Make one attempt to realize the given blueprint, meaning to take actions to
/// alter the real system to match the blueprint
///
/// The assumption is that callers are running this periodically or in a loop to
/// deal with transient errors or changes in the underlying system state.
pub async fn realize_blueprint<S>(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_label: S,
) -> Result<(), Vec<anyhow::Error>>
where
    String: From<S>,
{
    let opctx = opctx.child(BTreeMap::from([(
        "comment".to_string(),
        blueprint.comment.clone(),
    )]));
    let nexusctx = NexusContext { opctx, datastore };

    info!(
        nexusctx.log(),
        "attempting to realize blueprint";
        "blueprint_id" => %blueprint.id
    );

    resource_allocation::ensure_zone_resources_allocated(
        &nexusctx.opctx,
        nexusctx.datastore,
        &blueprint.omicron_zones,
    )
    .await
    .map_err(|err| vec![err])?;

    let sleds_by_id: BTreeMap<Uuid, _> = nexusctx
        .datastore
        .sled_list_all_batched(&nexusctx.opctx)
        .await
        .context("listing all sleds")
        .map_err(|e| vec![e])?
        .into_iter()
        .map(|db_sled| (db_sled.id(), Sled::from(db_sled)))
        .collect();
    omicron_zones::deploy_zones(
        &nexusctx,
        &sleds_by_id,
        &blueprint.omicron_zones,
    )
    .await?;

    // After deploying omicron zones, we may need to refresh OPTE service
    // firewall rules. This is an idempotent operation, so we don't attempt
    // to optimize out calling it in unnecessary cases, although we expect
    // _most_ cases this is not needed.
    nexusctx
        .plumb_service_firewall_rules(&nexusctx.opctx, &[])
        .await
        .context("failed to plumb service firewall rules to sleds")
        .map_err(|err| vec![err])?;

    datasets::ensure_crucible_dataset_records_exist(
        &nexusctx.opctx,
        nexusctx.datastore,
        blueprint.all_omicron_zones().map(|(_sled_id, zone)| zone),
    )
    .await
    .map_err(|err| vec![err])?;

    dns::deploy_dns(
        &nexusctx.opctx,
        nexusctx.datastore,
        String::from(nexus_label),
        blueprint,
        &sleds_by_id,
    )
    .await
    .map_err(|e| vec![anyhow!("{}", InlineErrorChain::new(&e))])?;

    Ok(())
}
