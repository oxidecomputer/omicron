// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for identifying when expunged zones are no longer referenced in the
//! database.
//!
//! When a zone is expunged, the expunged zone is still present in the
//! blueprint. Two high-level conditions must be satisfied before it's safe to
//! prune an expunged zone from the blueprint (i.e., delete it entirely):
//!
//! 1. The sled-agent responsible for running the zone must confirm the zone is
//!    no longer running and that it's ledgered an `OmicronSledConfig` with a
//!    generation past the point in which the zone was expunged. This guarantees
//!    the zone will never run again. This is tracked in the blueprint as the
//!    `ready_for_cleanup` field inside the expunged zone disposition, and can
//!    be queried by asking for expunged zones with the
//!    `ZoneRunningStatus::Shutdown` state.
//! 2. Any cleanup work that operates on expunged zones must be complete. This
//!    is zone-type-specific. Some zone types have no cleanup work at all and
//!    can be pruned as soon as the first condition is satisfied. Others have
//!    multiple, disparate cleanup work, all of which must be completed.
//!
//! This module is primarily considered with checking the second condition. The
//! [`BlueprintExpungedZoneAccessReason`] enum tracks a variant for every reason
//! a caller wants to access the expunged zones of a blueprint, including all
//! known cleanup actions. For each zone type, if the zone-type-specific cleanup
//! work is complete, we included the zone ID in the "expunged and unreferenced"
//! zone set in the `PlanningInput`. The planner can cheaply act on this set:
//! for every zone ID present, it can safely prune it from the blueprint (i.e.,
//! do not include it in the child blueprint it emits).

use nexus_db_model::SupportBundleState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintExpungedZoneAccessReason;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ZoneRunningStatus;
use nexus_types::deployment::blueprint_zone_type;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::PaginationOrder;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use std::cell::OnceCell;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::num::NonZeroU32;

pub(super) async fn find_expunged_and_unreferenced_zones(
    opctx: &OpContext,
    datastore: &DataStore,
    parent_blueprint: &Blueprint,
    external_ip_rows: &[nexus_db_model::ExternalIp],
    service_nic_rows: &[nexus_db_model::ServiceNetworkInterface],
) -> Result<BTreeSet<OmicronZoneUuid>, Error> {
    let mut expunged_and_unreferenced = BTreeSet::new();

    static_check_all_reasons_handled(
        BlueprintExpungedZoneAccessReason::PlanningInputDetermineUnreferenced,
    );

    let bp_refs = BlueprintReferencesCache::new(parent_blueprint);

    for (_, zone) in parent_blueprint.expunged_zones(
        ZoneRunningStatus::Shutdown,
        BlueprintExpungedZoneAccessReason::PlanningInputDetermineUnreferenced,
    ) {
        let is_referenced = match &zone.zone_type {
            BlueprintZoneType::BoundaryNtp(boundary_ntp) => {
                is_boundary_ntp_referenced(boundary_ntp, &bp_refs)
                    || is_external_networking_referenced(
                        zone.id,
                        external_ip_rows,
                        service_nic_rows,
                    )
            }
            BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::ClickhouseServer(_) => {
                is_multinode_clickhouse_referenced(zone.id, parent_blueprint)
            }
            BlueprintZoneType::ExternalDns(external_dns) => {
                is_external_dns_referenced(external_dns, &bp_refs)
                    || is_external_networking_referenced(
                        zone.id,
                        external_ip_rows,
                        service_nic_rows,
                    )
            }
            BlueprintZoneType::Nexus(_) => {
                is_nexus_referenced(opctx, datastore, zone.id).await?
                    || is_external_networking_referenced(
                        zone.id,
                        external_ip_rows,
                        service_nic_rows,
                    )
            }
            BlueprintZoneType::Oximeter(_) => {
                is_oximeter_referenced(opctx, datastore, zone.id).await?
            }
            BlueprintZoneType::CockroachDb(_) => {
                // BlueprintExpungedZoneAccessReason::CockroachDecommission
                // means we consider cockroach zones referenced until the
                // cockroach cluster has decommissioned the node that was
                // present in that zone; however, we don't currently
                // decommission cockroach nodes (tracked by
                // <https://github.com/oxidecomputer/omicron/issues/8447>). We
                // therefore always consider cockroach nodes "still referenced".
                //
                // This shouldn't be a huge deal in practice; Cockroach zones
                // are updated in place, not by an expunge/add pair, so a
                // typical update does not produce an expunged Cockroach zone
                // that needs pruning. Only expunging a disk or sled can produce
                // an expunged Cockroach node, and we expect the number of those
                // to remain relatively small for any given deployment.
                // Hopefully we can revisit decommissioning Cockroach nodes long
                // before we need to worry about the amount of garbage leftover
                // from expunged disks/sleds.
                true
            }

            // These zone types currently have no associated
            // `BlueprintExpungedZoneAccessReason`; there is no cleanup action
            // required for them, so they're considered "unreferenced" and may
            // be pruned as soon as they've been expunged.
            BlueprintZoneType::Clickhouse(_)
            | BlueprintZoneType::Crucible(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalDns(_)
            | BlueprintZoneType::InternalNtp(_) => false,
        };

        if !is_referenced {
            expunged_and_unreferenced.insert(zone.id);
        }
    }

    Ok(expunged_and_unreferenced)
}

fn is_external_networking_referenced(
    zone_id: OmicronZoneUuid,
    external_ip_rows: &[nexus_db_model::ExternalIp],
    service_nic_rows: &[nexus_db_model::ServiceNetworkInterface],
) -> bool {
    // Check
    // BlueprintExpungedZoneAccessReason::DeallocateExternalNetworkingResources;
    // if this zone's external IP or NIC are still present in the DB, then it's
    // the zone is still referenced.
    let zone_id = zone_id.into_untyped_uuid();
    external_ip_rows.iter().any(|row| row.parent_id == Some(zone_id))
        || service_nic_rows.iter().any(|row| row.service_id == zone_id)
}

fn is_boundary_ntp_referenced(
    boundary_ntp: &blueprint_zone_type::BoundaryNtp,
    bp_refs: &BlueprintReferencesCache<'_>,
) -> bool {
    // Check BlueprintExpungedZoneAccessReason::BoundaryNtpUpstreamConfig; if
    // this zone's upstream config is not covered by an in-service zone, then
    // it's still "referenced" (in that the planner needs to refer to it to set
    // up a new boundary NTP zone).
    let expunged_config = BoundaryNtpUpstreamConfig::new(boundary_ntp);
    bp_refs
        .in_service_boundary_ntp_upstream_configs()
        .contains(&expunged_config)
}

fn is_multinode_clickhouse_referenced(
    zone_id: OmicronZoneUuid,
    parent_blueprint: &Blueprint,
) -> bool {
    // Check BlueprintExpungedZoneAccessReason::ClickhouseKeeperServerConfigIps;
    // if this zone is still present in the clickhouse cluster config, it's
    // still referenced.
    let Some(clickhouse_config) = &parent_blueprint.clickhouse_cluster_config
    else {
        // If there is no clickhouse cluster config at all, the zone isn't
        // referenced in it!
        return false;
    };

    clickhouse_config.keepers.contains_key(&zone_id)
        || clickhouse_config.servers.contains_key(&zone_id)
}

fn is_external_dns_referenced(
    external_dns: &blueprint_zone_type::ExternalDns,
    bp_refs: &BlueprintReferencesCache<'_>,
) -> bool {
    // Check BlueprintExpungedZoneAccessReason::ExternalDnsExternalIps; we
    // consider an external DNS zone "still referenced" if its IP is _not_
    // assigned to an in-service external DNS zone. (If the IP _is_ assigned to
    // an in-service external DNS zone, the expunged zone is no longer
    // referenced and can be safely pruned.)
    let expunged_zone_ip = external_dns.dns_address.addr.ip();
    !bp_refs.in_service_external_dns_ips().contains(&expunged_zone_ip)
}

async fn is_nexus_referenced(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
) -> Result<bool, Error> {
    // Check BlueprintExpungedZoneAccessReason::NexusDeleteMetadataRecord: is
    // this Nexus zone still present in the `db_metadata_nexus` table?
    if !datastore
        .database_nexus_access_all(opctx, &BTreeSet::from([zone_id]))
        .await?
        .is_empty()
    {
        return Ok(true);
    }

    // Check BlueprintExpungedZoneAccessReason::NexusSagaReassignment: does
    // this Nexus zone still have unfinished sagas assigned?
    if !datastore
        .saga_list_recovery_candidates(
            opctx,
            zone_id.into(),
            &single_item_pagparams(),
        )
        .await?
        .is_empty()
    {
        return Ok(true);
    }

    // This is a no-op match that exists solely to ensure we update our logic if
    // the possible support bundle states change. We need to query for any
    // support bundle assigned to the `zone_id` Nexus in any state that might
    // require cleanup work; currently, that means "any state other than
    // `Failed`".
    //
    // If updating this match, you must also ensure you update the query below!
    match SupportBundleState::Active {
        SupportBundleState::Collecting
        | SupportBundleState::Active
        | SupportBundleState::Destroying
        | SupportBundleState::Failing => {
            // We need to query for these states.
        }
        SupportBundleState::Failed => {
            // The sole state we don't care about.
        }
    }

    // Check BlueprintExpungedZoneAccessReason::NexusSupportBundleReassign: does
    // this Nexus zone still have support bundles assigned to it in any state
    // that requires cleanup work? This requires explicitly listing the states
    // we care about; the no-op match statement above will hopefully keep this
    // in sync with any changes to the enum.
    if !datastore
        .support_bundle_list_assigned_to_nexus(
            opctx,
            &single_item_pagparams(),
            zone_id,
            vec![
                SupportBundleState::Collecting,
                SupportBundleState::Active,
                SupportBundleState::Destroying,
                SupportBundleState::Failing,
            ],
        )
        .await?
        .is_empty()
    {
        return Ok(true);
    }

    // These Nexus-related zone access reasons are documented as "planner does
    // not need to account for this", so we don't check anything:
    //
    // * BlueprintExpungedZoneAccessReason::NexusExternalConfig
    // * BlueprintExpungedZoneAccessReason::NexusSelfIsQuiescing

    Ok(false)
}

async fn is_oximeter_referenced(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
) -> Result<bool, Error> {
    // Check
    // BlueprintExpungedZoneAccessReason::OximeterExpungeAndReassignProducers:
    // this zone ID should not refer to an in-service Oximeter collector, and it
    // should have no producers assigned to it.
    match datastore.oximeter_lookup(opctx, zone_id.as_untyped_uuid()).await? {
        Some(_info) => {
            // If the lookup succeeded, we haven't yet performed the necessary
            // cleanup to mark this oximeter as expunged.
            return Ok(true);
        }
        None => {
            // Oximeter has been expunged (or was never inserted in the first
            // place); fall through to check whether there are any producers
            // assigned to it.
        }
    }

    // Ask for a page with a single item; all we care about is whether _any_
    // producers are assigned to this oximeter.
    let assigned_producers = datastore
        .producers_list_by_oximeter_id(
            opctx,
            zone_id.into_untyped_uuid(),
            &single_item_pagparams(),
        )
        .await?;

    // This oximeter is referenced if our set of assigned producers is nonempty.
    Ok(!assigned_producers.is_empty())
}

fn single_item_pagparams<T>() -> DataPageParams<'static, T> {
    DataPageParams {
        marker: None,
        direction: PaginationOrder::Ascending,
        limit: NonZeroU32::new(1).expect("1 is not 0"),
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BoundaryNtpUpstreamConfig<'a> {
    ntp_servers: &'a [String],
    dns_servers: &'a [IpAddr],
}

impl<'a> BoundaryNtpUpstreamConfig<'a> {
    fn new(config: &'a blueprint_zone_type::BoundaryNtp) -> Self {
        Self {
            ntp_servers: &config.ntp_servers,
            dns_servers: &config.dns_servers,
        }
    }
}

struct BlueprintReferencesCache<'a> {
    parent_blueprint: &'a Blueprint,
    in_service_boundary_ntp_upstream_configs:
        OnceCell<BTreeSet<BoundaryNtpUpstreamConfig<'a>>>,
    in_service_external_dns_ips: OnceCell<BTreeSet<IpAddr>>,
}

impl<'a> BlueprintReferencesCache<'a> {
    fn new(parent_blueprint: &'a Blueprint) -> Self {
        Self {
            parent_blueprint,
            in_service_boundary_ntp_upstream_configs: OnceCell::new(),
            in_service_external_dns_ips: OnceCell::new(),
        }
    }

    fn in_service_boundary_ntp_upstream_configs(
        &self,
    ) -> &BTreeSet<BoundaryNtpUpstreamConfig<'a>> {
        OnceCell::get_or_init(
            &self.in_service_boundary_ntp_upstream_configs,
            || {
                self.parent_blueprint
                    .in_service_zones()
                    .filter_map(|(_, zone)| match &zone.zone_type {
                        BlueprintZoneType::BoundaryNtp(config) => {
                            Some(BoundaryNtpUpstreamConfig::new(config))
                        }
                        _ => None,
                    })
                    .collect()
            },
        )
    }

    fn in_service_external_dns_ips(&self) -> &BTreeSet<IpAddr> {
        OnceCell::get_or_init(&self.in_service_external_dns_ips, || {
            self.parent_blueprint
                .in_service_zones()
                .filter_map(|(_, zone)| match &zone.zone_type {
                    BlueprintZoneType::ExternalDns(config) => {
                        Some(config.dns_address.addr.ip())
                    }
                    _ => None,
                })
                .collect()
        })
    }
}

// This is a no-op function that exists solely to ensure this file is considered
// when there are any changes to `BlueprintExpungedZoneAccessReason` variants.
//
// If you're adding a new variant and that variant affects whether it's safe to
// prune an expunged zone from the blueprint, you _must_ also update the code
// above to check for whatever your new variant is. If your new variant does not
// affect pruning, put it in one of the appropriate sections below (or a new one
// with a relevant comment).
fn static_check_all_reasons_handled(reason: BlueprintExpungedZoneAccessReason) {
    // Help rustfmt out (with the full enum name it gives up on formatting).
    use BlueprintExpungedZoneAccessReason as Reason;

    match reason {
        // Checked by is_boundary_ntp_referenced()
        Reason::BoundaryNtpUpstreamConfig => {}

        // Checked by is_multinode_clickhouse_referenced()
        Reason::ClickhouseKeeperServerConfigIps => {}

        // NOT CHECKED: find_expunged_and_unreferenced_zones() will never
        // consider a cockroach node "unreferenced", because we have currently
        // disabled decommissioning (see
        // https://github.com/oxidecomputer/omicron/issues/8447).
        Reason::CockroachDecommission => {}

        // Checked by is_external_networking_referenced(), which is called for
        // each zone type with external networking (boundary NTP, external DNS,
        // Nexus)
        Reason::DeallocateExternalNetworkingResources => {}

        // Checked by is_external_dns_referenced()
        Reason::ExternalDnsExternalIps => {}

        // Each of these are checked by is_nexus_referenced()
        Reason::NexusDeleteMetadataRecord
        | Reason::NexusSagaReassignment
        | Reason::NexusSupportBundleReassign => {}

        // Checked by is_oximeter_referenced()
        Reason::OximeterExpungeAndReassignProducers => {}

        // Nexus-related reasons that don't need to be checked (see
        // `BlueprintExpungedZoneAccessReason` for specifics)
        Reason::NexusExternalConfig | Reason::NexusSelfIsQuiescing => {}

        // Planner-related reasons that don't need to be checked (see
        // `BlueprintExpungedZoneAccessReason` for specifics)
        Reason::PlannerCheckReadyForCleanup
        | Reason::PlanningInputDetermineUnreferenced
        | Reason::PlanningInputExpungedZoneGuard => {}

        // Test / development reasons that don't need to be checked
        Reason::Blippy
        | Reason::Omdb
        | Reason::ReconfiguratorCli
        | Reason::Test => {}
    }
}
