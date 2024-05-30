// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_reconfigurator_planning` crate-level docs for background.

use anyhow::{anyhow, Context};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::views::SledState;
use nexus_types::identity::Asset;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use overridables::Overridables;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;

mod cockroachdb;
mod datasets;
mod dns;
mod external_networking;
mod omicron_physical_disks;
mod omicron_zones;
mod overridables;
mod sled_state;

pub use dns::blueprint_external_dns_config;
pub use dns::blueprint_internal_dns_config;
pub use dns::blueprint_nexus_external_ips;
pub use dns::silo_dns_name;

pub struct Sled {
    id: SledUuid,
    sled_agent_address: SocketAddrV6,
    is_scrimlet: bool,
}

impl Sled {
    pub fn new(
        id: SledUuid,
        sled_agent_address: SocketAddrV6,
        is_scrimlet: bool,
    ) -> Sled {
        Sled { id, sled_agent_address, is_scrimlet }
    }

    pub(crate) fn subnet(&self) -> Ipv6Subnet<SLED_PREFIX> {
        Ipv6Subnet::<SLED_PREFIX>::new(*self.sled_agent_address.ip())
    }
}

impl From<nexus_db_model::Sled> for Sled {
    fn from(value: nexus_db_model::Sled) -> Self {
        Sled {
            id: SledUuid::from_untyped_uuid(value.id()),
            sled_agent_address: value.address(),
            is_scrimlet: value.is_scrimlet(),
        }
    }
}

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
    realize_blueprint_with_overrides(
        opctx,
        datastore,
        blueprint,
        nexus_label,
        &Default::default(),
    )
    .await
}

pub async fn realize_blueprint_with_overrides<S>(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_label: S,
    overrides: &Overridables,
) -> Result<(), Vec<anyhow::Error>>
where
    String: From<S>,
{
    let opctx = opctx.child(BTreeMap::from([(
        "comment".to_string(),
        blueprint.comment.clone(),
    )]));

    info!(
        opctx.log,
        "attempting to realize blueprint";
        "blueprint_id" => %blueprint.id
    );

    // Deallocate external networking resources for non-externally-reachable
    // zones first. This will allow external networking resource allocation to
    // succeed if we are swapping an external IP between two zones (e.g., moving
    // a specific external IP from an old external DNS zone to a new one).
    external_networking::ensure_zone_external_networking_deallocated(
        &opctx,
        datastore,
        blueprint
            .all_omicron_zones_not_in(
                BlueprintZoneFilter::ShouldBeExternallyReachable,
            )
            .map(|(_sled_id, zone)| zone),
    )
    .await
    .map_err(|err| vec![err])?;

    external_networking::ensure_zone_external_networking_allocated(
        &opctx,
        datastore,
        blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeExternallyReachable)
            .map(|(_sled_id, zone)| zone),
    )
    .await
    .map_err(|err| vec![err])?;

    let sleds_by_id: BTreeMap<SledUuid, _> = datastore
        .sled_list_all_batched(&opctx, SledFilter::InService)
        .await
        .context("listing all sleds")
        .map_err(|e| vec![e])?
        .into_iter()
        .map(|db_sled| {
            (SledUuid::from_untyped_uuid(db_sled.id()), Sled::from(db_sled))
        })
        .collect();

    omicron_physical_disks::deploy_disks(
        &opctx,
        &sleds_by_id,
        &blueprint.blueprint_disks,
    )
    .await?;

    omicron_zones::deploy_zones(
        &opctx,
        &sleds_by_id,
        &blueprint.blueprint_zones,
    )
    .await?;

    omicron_zones::migrate_sagas(&opctx, &blueprint.blueprint_zones).await?;

    // After deploying omicron zones, we may need to refresh OPTE service
    // firewall rules. This is an idempotent operation, so we don't attempt
    // to optimize out calling it in unnecessary cases, although it is only
    // needed in cases where we've changed the set of services on one or more
    // sleds, or the sleds have lost their firewall rules for some reason.
    // Fixing the latter case is a side effect and should really be handled by a
    // firewall-rule-specific RPW; once that RPW exists, we could trigger it
    // here instead of pluming firewall rules ourselves.
    nexus_networking::plumb_service_firewall_rules(
        datastore,
        &opctx,
        &[],
        &opctx,
        &opctx.log,
    )
    .await
    .context("failed to plumb service firewall rules to sleds")
    .map_err(|err| vec![err])?;

    datasets::ensure_crucible_dataset_records_exist(
        &opctx,
        datastore,
        blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .map(|(_sled_id, zone)| zone),
    )
    .await
    .map_err(|err| vec![err])?;

    dns::deploy_dns(
        &opctx,
        datastore,
        String::from(nexus_label),
        blueprint,
        &sleds_by_id,
        overrides,
    )
    .await
    .map_err(|e| vec![anyhow!("{}", InlineErrorChain::new(&e))])?;

    sled_state::decommission_sleds(
        &opctx,
        datastore,
        blueprint
            .sled_state
            .iter()
            .filter(|&(_, &state)| state == SledState::Decommissioned)
            .map(|(&sled_id, _)| sled_id),
    )
    .await?;

    // This is likely to error if any cluster upgrades are in progress (which
    // can take some time), so it should remain at the end so that other parts
    // of the blueprint can progress normally.
    cockroachdb::ensure_settings(&opctx, datastore, blueprint)
        .await
        .map_err(|err| vec![err])?;

    Ok(())
}
