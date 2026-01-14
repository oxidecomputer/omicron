// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for identifying when expunged zones are no longer referenced in the
//! database.

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintExpungedZoneAccessReason;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ZoneRunningStatus;
use nexus_types::deployment::blueprint_zone_type;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use std::cell::OnceCell;
use std::collections::BTreeSet;
use std::net::IpAddr;

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
                    || is_external_ip_referenced(zone.id, external_ip_rows)
                    || is_service_nic_referenced(zone.id, service_nic_rows)
            }
            BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::ClickhouseServer(_) => {
                is_multinode_clickhouse_referenced(zone.id, parent_blueprint)
            }
            BlueprintZoneType::ExternalDns(external_dns) => {
                is_external_dns_referenced(external_dns, &bp_refs)
                    || is_external_ip_referenced(zone.id, external_ip_rows)
                    || is_service_nic_referenced(zone.id, service_nic_rows)
            }
            BlueprintZoneType::Nexus(nexus) => {
                todo!("john-{nexus:?}")
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

fn is_external_ip_referenced(
    zone_id: OmicronZoneUuid,
    external_ip_rows: &[nexus_db_model::ExternalIp],
) -> bool {
    let zone_id = zone_id.into_untyped_uuid();
    external_ip_rows.iter().any(|row| row.parent_id == Some(zone_id))
}

fn is_service_nic_referenced(
    zone_id: OmicronZoneUuid,
    service_nic_rows: &[nexus_db_model::ServiceNetworkInterface],
) -> bool {
    let zone_id = zone_id.into_untyped_uuid();
    service_nic_rows.iter().any(|row| row.service_id == zone_id)
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

async fn is_oximeter_referenced(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
) -> Result<bool, Error> {
    // Check
    // BlueprintExpungedZoneAccessReason::OximeterExpungeAndReassignProducers:
    // this zone ID should not refer to an in-service Oximeter collector, and it
    // should have no producers assigned to it.
    match datastore.oximeter_lookup(opctx, zone_id.as_untyped_uuid()).await {
        Ok(_info) => {
            // If the lookup succeeded, we haven't yet performed the necessary
            // cleanup to mark this oximeter as expunged.
            return Ok(true);
        }
        Err(err) => {
            todo!("john")
        }
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

// TODO-john
fn static_check_all_reasons_handled(reason: BlueprintExpungedZoneAccessReason) {
    match reason {
        BlueprintExpungedZoneAccessReason::BoundaryNtpUpstreamConfig => {},
        BlueprintExpungedZoneAccessReason::ClickhouseKeeperServerConfigIps => {},
        BlueprintExpungedZoneAccessReason::CockroachDecommission => {},
        BlueprintExpungedZoneAccessReason::DeallocateExternalNetworkingResources => {},
        BlueprintExpungedZoneAccessReason::ExternalDnsExternalIps => {},
        BlueprintExpungedZoneAccessReason::NexusDeleteMetadataRecord => {},
        BlueprintExpungedZoneAccessReason::NexusExternalConfig => {},
        BlueprintExpungedZoneAccessReason::NexusSelfIsQuiescing => {},
        BlueprintExpungedZoneAccessReason::NexusSagaReassignment => {},
        BlueprintExpungedZoneAccessReason::NexusSupportBundleReassign => {},
        BlueprintExpungedZoneAccessReason::OximeterExpungeAndReassignProducers => {},
        BlueprintExpungedZoneAccessReason::PlannerCheckReadyForCleanup => {},
        BlueprintExpungedZoneAccessReason::PlanningInputDetermineUnreferenced => {},
        BlueprintExpungedZoneAccessReason::PlanningInputExpungedZoneGuard => {},
        BlueprintExpungedZoneAccessReason::Blippy => {},
        BlueprintExpungedZoneAccessReason::Omdb => {},
        BlueprintExpungedZoneAccessReason::ReconfiguratorCli => {},
        BlueprintExpungedZoneAccessReason::Test => {},
    }
}
