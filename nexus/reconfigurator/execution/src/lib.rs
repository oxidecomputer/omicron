// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_reconfigurator_planning` crate-level docs for background.

use anyhow::{anyhow, Context};
use internal_dns::resolver::Resolver;
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
use uuid::Uuid;

mod cockroachdb;
mod datasets;
mod dns;
mod external_networking;
mod omicron_physical_disks;
mod omicron_zones;
mod overridables;
mod sagas;
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
pub async fn realize_blueprint(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    nexus_id: Uuid,
) -> Result<bool, Vec<anyhow::Error>> {
    realize_blueprint_with_overrides(
        opctx,
        datastore,
        resolver,
        blueprint,
        nexus_id,
        &Default::default(),
    )
    .await
}

pub async fn realize_blueprint_with_overrides(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    nexus_id: Uuid,
    overrides: &Overridables,
) -> Result<bool, Vec<anyhow::Error>> {
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

    datasets::ensure_dataset_records_exist(
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
        nexus_id.to_string(),
        blueprint,
        &sleds_by_id,
        overrides,
    )
    .await
    .map_err(|e| vec![anyhow!("{}", InlineErrorChain::new(&e))])?;

    omicron_zones::clean_up_expunged_zones(
        &opctx,
        datastore,
        resolver,
        blueprint.all_omicron_zones(BlueprintZoneFilter::Expunged),
    )
    .await?;

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

    // This depends on the "deploy_disks" call earlier -- disk expungement is a
    // statement of policy, but we need to be assured that the Sled Agent has
    // stopped using that disk before we can mark its state as decommissioned.
    omicron_physical_disks::decommission_expunged_disks(&opctx, datastore)
        .await?;

    // From this point on, we'll assume that any errors that we encounter do
    // *not* require stopping execution.  We'll just accumulate them and return
    // them all at the end.
    //
    // TODO We should probably do this with more of the errors above, too.
    let mut errors = Vec::new();

    // For any expunged Nexus zones, re-assign in-progress sagas to some other
    // Nexus.  If this fails for some reason, it doesn't affect anything else.
    let sec_id = nexus_db_model::SecId(nexus_id);
    let reassigned = sagas::reassign_sagas_from_expunged(
        &opctx, datastore, blueprint, sec_id,
    )
    .await
    .context("failed to re-assign sagas");
    let needs_saga_recovery = match reassigned {
        Ok(needs_recovery) => needs_recovery,
        Err(error) => {
            errors.push(error);
            false
        }
    };

    // This is likely to error if any cluster upgrades are in progress (which
    // can take some time), so it should remain at the end so that other parts
    // of the blueprint can progress normally.
    if let Err(error) =
        cockroachdb::ensure_settings(&opctx, datastore, blueprint).await
    {
        errors.push(error);
    }

    if errors.is_empty() {
        Ok(needs_saga_recovery)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::Generation;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_model::Zpool;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    // Helper function to insert sled records from an initial blueprint. Some
    // tests expect to be able to realize the the blueprint created from an
    // initial collection, and ensuring the zones' datasets exist requires first
    // inserting the sled and zpool records.
    pub(crate) async fn insert_sled_records(
        datastore: &DataStore,
        blueprint: &Blueprint,
    ) {
        let rack_id = Uuid::new_v4();
        let mut sleds_inserted = BTreeSet::new();

        for sled_id in blueprint.blueprint_zones.keys().copied() {
            if sleds_inserted.insert(sled_id) {
                let sled = SledUpdate::new(
                    sled_id.into_untyped_uuid(),
                    "[::1]:0".parse().unwrap(),
                    SledBaseboard {
                        serial_number: format!("test-{sled_id}"),
                        part_number: "test-sled".to_string(),
                        revision: 0,
                    },
                    SledSystemHardware {
                        is_scrimlet: false,
                        usable_hardware_threads: 128,
                        usable_physical_ram: (64 << 30).try_into().unwrap(),
                        reservoir_size: (16 << 30).try_into().unwrap(),
                    },
                    rack_id,
                    Generation::new(),
                );
                datastore
                    .sled_upsert(sled)
                    .await
                    .expect("failed to upsert sled");
            }
        }
    }

    // Helper function to insert zpool records from an initial blueprint. Some
    // tests expect to be able to realize the the blueprint created from an
    // initial collection, and ensuring the zones' datasets exist requires first
    // inserting the sled and zpool records.
    pub(crate) async fn create_disks_for_zones_using_datasets(
        datastore: &DataStore,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) {
        let mut pool_inserted = BTreeSet::new();

        for (sled_id, config) in
            blueprint.all_omicron_zones(BlueprintZoneFilter::All)
        {
            let Some(dataset) = config.zone_type.durable_dataset() else {
                continue;
            };

            let physical_disk_id = Uuid::new_v4();
            let pool_id = dataset.dataset.pool_name.id();

            let disk = PhysicalDisk::new(
                physical_disk_id,
                String::from("Oxide"),
                format!("PhysDisk of {}", pool_id),
                String::from("FakeDisk"),
                PhysicalDiskKind::U2,
                sled_id.into_untyped_uuid(),
            );
            datastore
                .physical_disk_insert(&opctx, disk.clone())
                .await
                .expect("failed to upsert physical disk");

            if pool_inserted.insert(pool_id) {
                let zpool = Zpool::new(
                    pool_id.into_untyped_uuid(),
                    sled_id.into_untyped_uuid(),
                    physical_disk_id,
                );
                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("failed to upsert zpool");
            }
        }
    }
}
