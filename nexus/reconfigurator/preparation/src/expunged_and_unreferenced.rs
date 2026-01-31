// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for identifying when expunged zones are no longer referenced in the
//! database.
//!
//! When a zone is first expunged, the expunged zone initially remains in the
//! blueprint. Two high-level conditions must be satisfied before it's safe to
//! prune an expunged zone from the blueprint (i.e., delete it entirely):
//!
//! 1. We must know the zone is not running and will never run again. The
//!    typical case of this confirmation is that the sled-agent responsible for
//!    running the zone has confirmed the zone is no longer running and that
//!    it's ledgered an `OmicronSledConfig` with a generation past the point in
//!    which the zone was expunged. The uncommon case of this confirmation is
//!    that the sled where this zone ran has been expunged.
//! 2. Any cleanup work that operates on expunged zones must be complete. This
//!    is zone-type-specific. Some zone types have no cleanup work at all and
//!    can be pruned as soon as the first condition is satisfied. Others have
//!    multiple, disparate cleanup steps, all of which must be completed.
//!
//! The first condition is tracked in the blueprint as the `ready_for_cleanup`
//! field inside the expunged zone disposition. We query for it below by asking
//! for expunged zones with the `ZoneRunningStatus::Shutdown` state.
//!
//! This bulk of this module is concerned with checking the second condition.
//! The [`BlueprintExpungedZoneAccessReason`] enum tracks a variant for every
//! reason a caller wants to access the expunged zones of a blueprint, including
//! all known cleanup actions. For each zone type, if the zone-type-specific
//! cleanup work is complete, we included the zone ID in the "expunged and
//! unreferenced" zone set in the `PlanningInput`. The planner can cheaply act
//! on this set: for every zone ID present, it can safely prune it from the
//! blueprint (i.e., do not include it in the child blueprint it emits).

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
use strum::IntoEnumIterator;

pub(super) struct PruneableZones {
    pruneable_zones: BTreeSet<OmicronZoneUuid>,
    #[allow(unused)] // only read by tests
    reason_checker: BlueprintExpungedZoneAccessReasonChecker,
}

impl PruneableZones {
    /// Find all Omicron zones within `parent_blueprint` that can be safely
    /// pruned by a future run of the planner.
    ///
    /// A zone ID contained in the returned set satisfies both conditions for
    /// pruning:
    ///
    /// 1. We know the zone is not running and will not run again.
    /// 2. Any cleanup work required after the zone has been expunged has been
    ///    completed.
    ///
    /// See this module's documentation for more details.
    pub async fn new(
        opctx: &OpContext,
        datastore: &DataStore,
        parent_blueprint: &Blueprint,
        external_ip_rows: &[nexus_db_model::ExternalIp],
        service_nic_rows: &[nexus_db_model::ServiceNetworkInterface],
    ) -> Result<Self, Error> {
        // Help rustfmt out (with the full enum name it gives up on formatting).
        use BlueprintExpungedZoneAccessReason as Reason;

        let mut pruneable_zones = BTreeSet::new();
        let mut reason_checker =
            BlueprintExpungedZoneAccessReasonChecker::new();

        // Create several `BTreeSet<_>`s of information we need to check in the
        // loop below. Each of these is lazily-initialized; we'll only actually
        // do the work of constructing the set if we need it during iteration.
        let in_service_boundary_ntp_configs =
            InServiceBoundaryNtpUpstreamConfigs::new(parent_blueprint);
        let in_service_external_dns_ips =
            InServiceExternalDnsIps::new(parent_blueprint);
        let zones_with_external_ip_rows =
            ZonesWithExternalIpRows::new(external_ip_rows);
        let zones_with_service_nice_rows =
            ZonesWithServiceNicRows::new(service_nic_rows);

        for (_, zone) in parent_blueprint.expunged_zones(
            ZoneRunningStatus::Shutdown,
            Reason::PlanningInputDetermineUnreferenced,
        ) {
            // Check
            // BlueprintExpungedZoneAccessReason::DeallocateExternalNetworkingResources;
            // this reason applies to multiple zone types, so we check it for
            // them all. (Technically we only need to check it for zones that
            // _can_ have external networking, but it's fine to check ones that
            // don't, and now we don't have to keep a list here of which zone
            // types could have external networking rows present.)
            reason_checker.add_reason_checked(
                Reason::DeallocateExternalNetworkingResources,
            );
            let has_external_ip_row =
                zones_with_external_ip_rows.contains(&zone.id);
            let has_service_nic_row =
                zones_with_service_nice_rows.contains(&zone.id);

            let is_pruneable = !has_external_ip_row
                && !has_service_nic_row
                && match &zone.zone_type {
                    BlueprintZoneType::BoundaryNtp(boundary_ntp) => {
                        is_boundary_ntp_pruneable(
                            boundary_ntp,
                            &in_service_boundary_ntp_configs,
                            &mut reason_checker,
                        )
                    }
                    BlueprintZoneType::ClickhouseKeeper(_)
                    | BlueprintZoneType::ClickhouseServer(_) => {
                        is_multinode_clickhouse_pruneable(
                            zone.id,
                            parent_blueprint,
                            &mut reason_checker,
                        )
                    }
                    BlueprintZoneType::ExternalDns(external_dns) => {
                        is_external_dns_pruneable(
                            external_dns,
                            &in_service_external_dns_ips,
                            &mut reason_checker,
                        )
                    }
                    BlueprintZoneType::Nexus(_) => {
                        is_nexus_pruneable(
                            opctx,
                            datastore,
                            zone.id,
                            &mut reason_checker,
                        )
                        .await?
                    }
                    BlueprintZoneType::Oximeter(_) => {
                        is_oximeter_pruneable(
                            opctx,
                            datastore,
                            zone.id,
                            &mut reason_checker,
                        )
                        .await?
                    }
                    BlueprintZoneType::CockroachDb(_) => {
                        is_cockroach_pruneable(zone.id, &mut reason_checker)
                    }

                    // These zone types currently have no associated
                    // `BlueprintExpungedZoneAccessReason`; there is no cleanup
                    // action required for them, so they may be pruned as soon
                    // as they've been expunged.
                    BlueprintZoneType::Clickhouse(_)
                    | BlueprintZoneType::Crucible(_)
                    | BlueprintZoneType::CruciblePantry(_)
                    | BlueprintZoneType::InternalDns(_)
                    | BlueprintZoneType::InternalNtp(_) => true,
                };

            if is_pruneable {
                pruneable_zones.insert(zone.id);
            }
        }

        Ok(Self { pruneable_zones, reason_checker })
    }

    pub fn into_pruneable_zones(self) -> BTreeSet<OmicronZoneUuid> {
        self.pruneable_zones
    }
}

fn is_boundary_ntp_pruneable(
    boundary_ntp: &blueprint_zone_type::BoundaryNtp,
    boundary_ntp_configs: &InServiceBoundaryNtpUpstreamConfigs<'_>,
    reason_checker: &mut BlueprintExpungedZoneAccessReasonChecker,
) -> bool {
    // If this zone's upstream config is also the config of an in-service zone,
    // then it's pruneable. Note the reason we're checking here.
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::BoundaryNtpUpstreamConfig,
    );
    let expunged_config = BoundaryNtpUpstreamConfig::new(boundary_ntp);
    boundary_ntp_configs.contains(&expunged_config)
}

fn is_cockroach_pruneable(
    _zone_id: OmicronZoneUuid,
    reason_checker: &mut BlueprintExpungedZoneAccessReasonChecker,
) -> bool {
    // BlueprintExpungedZoneAccessReason::CockroachDecommission means we
    // consider cockroach zones nonpruneable until the cockroach cluster has
    // decommissioned the node that was present in that zone; however, we don't
    // currently decommission cockroach nodes (tracked by
    // <https://github.com/oxidecomputer/omicron/issues/8447>). We therefore
    // never consider cockroach nodes pruneable
    //
    // This shouldn't be a huge deal in practice; Cockroach zones are updated in
    // place, not by an expunge/add pair, so a typical update does not produce
    // an expunged Cockroach zone that needs pruning. Only expunging a disk or
    // sled can produce an expunged Cockroach node, and we expect the number of
    // those to remain relatively small for any given deployment. Hopefully we
    // can revisit decommissioning Cockroach nodes long before we need to worry
    // about the amount of garbage leftover from expunged disks/sleds.
    //
    // Even though we do no work here, we claim we've checked this
    // BlueprintExpungedZoneAccessReason. This allows our test to pass that
    // we've _considered_ all relevant reasons; in this case, this reason means
    // the zone is _never_ pruneable.
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::CockroachDecommission,
    );
    false
}

fn is_multinode_clickhouse_pruneable(
    zone_id: OmicronZoneUuid,
    parent_blueprint: &Blueprint,
    reason_checker: &mut BlueprintExpungedZoneAccessReasonChecker,
) -> bool {
    // If this zone is still present in the clickhouse cluster config, it's
    // not pruneable. If there is no config at all or there is but it doesn't
    // contain this zone, it is prunable.
    //
    // Note the reason we've checked here.
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::ClickhouseKeeperServerConfigIps,
    );
    let Some(clickhouse_config) = &parent_blueprint.clickhouse_cluster_config
    else {
        return true;
    };

    !clickhouse_config.keepers.contains_key(&zone_id)
        && !clickhouse_config.servers.contains_key(&zone_id)
}

fn is_external_dns_pruneable(
    external_dns: &blueprint_zone_type::ExternalDns,
    external_dns_ips: &InServiceExternalDnsIps<'_>,
    reason_checker: &mut BlueprintExpungedZoneAccessReasonChecker,
) -> bool {
    // We consider an external DNS zone pruneable if its IP has been reassigned
    // to an in-service external DNS zone. (If the IP has not yet been
    // reassigned, we have to keep it around so we don't forget it!).
    //
    // Note the reason we've checked here.
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::ExternalDnsExternalIps,
    );
    let expunged_zone_ip = external_dns.dns_address.addr.ip();
    external_dns_ips.contains(&expunged_zone_ip)
}

async fn is_nexus_pruneable(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
    reason_checker: &mut BlueprintExpungedZoneAccessReasonChecker,
) -> Result<bool, Error> {
    // Nexus zones have multiple reasons they could be non-pruneable; check each
    // of them below and note them as we do.

    // Is this Nexus zone still present in the `db_metadata_nexus` table?
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::NexusDeleteMetadataRecord,
    );
    if !datastore
        .database_nexus_access_all(opctx, &BTreeSet::from([zone_id]))
        .await?
        .is_empty()
    {
        return Ok(false);
    }

    // Does this Nexus zone still have unfinished sagas assigned?
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::NexusSagaReassignment,
    );
    if !datastore
        .saga_list_recovery_candidates(
            opctx,
            zone_id.into(),
            &single_item_pagparams(),
        )
        .await?
        .is_empty()
    {
        return Ok(false);
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

    // Does this Nexus zone still have support bundles assigned to it in any
    // state that requires cleanup work? This requires explicitly listing the
    // states we care about; the no-op match statement above will hopefully keep
    // this in sync with any changes to the enum.
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::NexusSupportBundleReassign,
    );
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
        return Ok(false);
    }

    Ok(true)
}

async fn is_oximeter_pruneable(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_id: OmicronZoneUuid,
    reason_checker: &mut BlueprintExpungedZoneAccessReasonChecker,
) -> Result<bool, Error> {
    // This zone ID should not refer to an in-service Oximeter collector, and it
    // should have no producers assigned to it.
    //
    // Note the reason we've checked here.
    reason_checker.add_reason_checked(
        BlueprintExpungedZoneAccessReason::OximeterExpungeAndReassignProducers,
    );
    match datastore.oximeter_lookup(opctx, zone_id.as_untyped_uuid()).await? {
        Some(_info) => {
            // If the lookup succeeded, we haven't yet performed the necessary
            // cleanup to mark this oximeter as expunged.
            return Ok(false);
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

    // This oximeter is pruneable if our set of assigned producers is empty.
    Ok(assigned_producers.is_empty())
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

/// Lazily-initialized set of boundary NTP upstream configs from in-service
/// zones in a blueprint.
struct InServiceBoundaryNtpUpstreamConfigs<'a> {
    parent_blueprint: &'a Blueprint,
    configs: OnceCell<BTreeSet<BoundaryNtpUpstreamConfig<'a>>>,
}

impl<'a> InServiceBoundaryNtpUpstreamConfigs<'a> {
    fn new(parent_blueprint: &'a Blueprint) -> Self {
        Self { parent_blueprint, configs: OnceCell::new() }
    }

    fn contains(&self, config: &BoundaryNtpUpstreamConfig) -> bool {
        let configs = self.configs.get_or_init(|| {
            self.parent_blueprint
                .in_service_zones()
                .filter_map(|(_, zone)| match &zone.zone_type {
                    BlueprintZoneType::BoundaryNtp(config) => {
                        Some(BoundaryNtpUpstreamConfig::new(config))
                    }
                    _ => None,
                })
                .collect()
        });
        configs.contains(config)
    }
}

/// Lazily-initialized set of external DNS IPs from in-service zones in a
/// blueprint.
struct InServiceExternalDnsIps<'a> {
    parent_blueprint: &'a Blueprint,
    ips: OnceCell<BTreeSet<IpAddr>>,
}

impl<'a> InServiceExternalDnsIps<'a> {
    fn new(parent_blueprint: &'a Blueprint) -> Self {
        Self { parent_blueprint, ips: OnceCell::new() }
    }

    fn contains(&self, ip: &IpAddr) -> bool {
        let ips = self.ips.get_or_init(|| {
            self.parent_blueprint
                .in_service_zones()
                .filter_map(|(_, zone)| match &zone.zone_type {
                    BlueprintZoneType::ExternalDns(config) => {
                        Some(config.dns_address.addr.ip())
                    }
                    _ => None,
                })
                .collect()
        });
        ips.contains(ip)
    }
}

/// Lazily-initialized set of zone IDs that have external IP rows in the
/// database.
struct ZonesWithExternalIpRows<'a> {
    external_ip_rows: &'a [nexus_db_model::ExternalIp],
    zone_ids: OnceCell<BTreeSet<OmicronZoneUuid>>,
}

impl<'a> ZonesWithExternalIpRows<'a> {
    fn new(external_ip_rows: &'a [nexus_db_model::ExternalIp]) -> Self {
        Self { external_ip_rows, zone_ids: OnceCell::new() }
    }

    fn contains(&self, zone_id: &OmicronZoneUuid) -> bool {
        let zone_ids = self.zone_ids.get_or_init(|| {
            self.external_ip_rows
                .iter()
                .filter_map(|row| {
                    row.parent_id.map(OmicronZoneUuid::from_untyped_uuid)
                })
                .collect()
        });
        zone_ids.contains(zone_id)
    }
}

/// Lazily-initialized set of zone IDs that have service NIC rows in the
/// database.
struct ZonesWithServiceNicRows<'a> {
    service_nic_rows: &'a [nexus_db_model::ServiceNetworkInterface],
    zone_ids: OnceCell<BTreeSet<OmicronZoneUuid>>,
}

impl<'a> ZonesWithServiceNicRows<'a> {
    fn new(
        service_nic_rows: &'a [nexus_db_model::ServiceNetworkInterface],
    ) -> Self {
        Self { service_nic_rows, zone_ids: OnceCell::new() }
    }

    fn contains(&self, zone_id: &OmicronZoneUuid) -> bool {
        let zone_ids = self.zone_ids.get_or_init(|| {
            self.service_nic_rows
                .iter()
                .map(|row| OmicronZoneUuid::from_untyped_uuid(row.service_id))
                .collect()
        });
        zone_ids.contains(zone_id)
    }
}

/// Helper type to ensure we've covered every
/// [`BlueprintExpungedZoneAccessReason`] in our checks above.
///
/// This type has both compile-time (the `match` in `new()`) and runtime (the
/// `assert_all_reasons_checked()` function) guards that confirm we cover any
/// new variants added to [`BlueprintExpungedZoneAccessReason`] in the future.
struct BlueprintExpungedZoneAccessReasonChecker {
    reasons_checked: BTreeSet<BlueprintExpungedZoneAccessReason>,
}

impl BlueprintExpungedZoneAccessReasonChecker {
    fn new() -> Self {
        // Help rustfmt out (with the full enum name it gives up on formatting).
        use BlueprintExpungedZoneAccessReason as Reason;

        let mut reasons_checked = BTreeSet::new();

        for reason in Reason::iter() {
            // This match exists for two purposes:
            //
            // 1. Force a compilation error if a new variant is added, leading
            //    you to this module and this comment.
            // 2. Seed `reasons_checked` with reasons we _don't_ explicitly
            //    check, because they're documented as "they don't actually need
            //    to be checked".
            //
            // If you're in case 1 and you've added a new variant to
            // `BlueprintExpungedZoneAccessReason`, you must update the code in
            // this module. Either update the match below to add the reason to
            // the group of "doesn't need to be checked" cases, or update the
            // checks above for zone-specific reasons and add an appropriate
            // call to `add_reason_checked()`.
            match reason {
                // Checked by is_boundary_ntp_pruneable()
                Reason::BoundaryNtpUpstreamConfig => {}

                // Checked by is_multinode_clickhouse_pruneable()
                Reason::ClickhouseKeeperServerConfigIps => {}

                // TODO-john FIXME
                // NOT CHECKED: find_expunged_and_unreferenced_zones() will
                // never consider a cockroach node "unreferenced", because we
                // have currently disabled decommissioning (see
                // https://github.com/oxidecomputer/omicron/issues/8447).
                Reason::CockroachDecommission => {}

                // Checked directly by the main loop in `PruneableZones::new()`.
                Reason::DeallocateExternalNetworkingResources => {}

                // Checked by is_external_dns_pruneable()
                Reason::ExternalDnsExternalIps => {}

                // Each of these are checked by is_nexus_pruneable()
                Reason::NexusDeleteMetadataRecord
                | Reason::NexusSagaReassignment
                | Reason::NexusSupportBundleReassign => {}

                // Checked by is_oximeter_pruneable()
                Reason::OximeterExpungeAndReassignProducers => {}

                // ---------------------------------------------------------

                // Nexus-related reasons that don't need to be checked (see
                // `BlueprintExpungedZoneAccessReason` for specifics)
                Reason::NexusExternalConfig
                | Reason::NexusSelfIsQuiescing

                // Planner-related reasons that don't need to be checked (see
                // `BlueprintExpungedZoneAccessReason` for specifics)
                | Reason::PlannerCheckReadyForCleanup
                | Reason::PlanningInputDetermineUnreferenced
                | Reason::PlanningInputExpungedZoneGuard

                // Test / development reasons that don't need to be checked
                | Reason::Blippy
                | Reason::Omdb
                | Reason::ReconfiguratorCli
                | Reason::Test => {
                    reasons_checked.insert(reason);
                }
            }
        }

        Self { reasons_checked }
    }

    fn add_reason_checked(
        &mut self,
        reason: BlueprintExpungedZoneAccessReason,
    ) {
        self.reasons_checked.insert(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use clickhouse_admin_types::keeper::KeeperId;
    use nexus_db_model::ExternalIp;
    use nexus_db_model::IpAttachState;
    use nexus_db_model::IpKind;
    use nexus_db_model::Ipv4Addr as DbIpv4Addr;
    use nexus_db_model::MacAddr;
    use nexus_db_model::Name;
    use nexus_db_model::ServiceNetworkInterface;
    use nexus_db_model::ServiceNetworkInterfaceIdentity;
    use nexus_db_model::SqlU8;
    use nexus_db_model::SqlU16;
    use nexus_db_queries::db::datastore::CollectorReassignment;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::blueprint_editor::ExternalNetworkingChoice;
    use nexus_reconfigurator_planning::blueprint_editor::ExternalSnatNetworkingChoice;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_test_utils::db::TestDatabase;
    use nexus_types::deployment::BlueprintSource;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::ClickhouseClusterConfig;
    use nexus_types::deployment::SledFilter;
    use nexus_types::internal_api::params::OximeterInfo;
    use omicron_common::api::external;
    use omicron_common::api::external::MacAddr as ExternalMacAddr;
    use omicron_common::api::internal::nexus::ProducerEndpoint;
    use omicron_common::api::internal::nexus::ProducerKind;
    use omicron_common::api::internal::shared::PrivateIpConfig;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use std::net::Ipv4Addr;
    use std::time::Duration;
    use uuid::Uuid;

    /// Helper to construct a test ExternalIp associated with a specific zone
    fn make_external_ip_for_zone(
        zone_id: OmicronZoneUuid,
    ) -> nexus_db_model::ExternalIp {
        ExternalIp {
            id: OmicronZoneUuid::new_v4().into_untyped_uuid(),
            name: None,
            description: None,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
            ip_pool_id: Uuid::new_v4(),
            ip_pool_range_id: Uuid::new_v4(),
            is_service: true,
            is_probe: false,
            parent_id: Some(zone_id.into_untyped_uuid()),
            kind: IpKind::SNat,
            ip: "192.168.1.1/32".parse().unwrap(),
            first_port: SqlU16::new(0),
            last_port: SqlU16::new(65535),
            project_id: None,
            state: IpAttachState::Attached,
        }
    }

    /// Helper to construct a test ServiceNetworkInterface associated with a
    /// specific zone
    fn make_service_nic_for_zone(
        zone_id: OmicronZoneUuid,
    ) -> nexus_db_model::ServiceNetworkInterface {
        ServiceNetworkInterface {
            identity: ServiceNetworkInterfaceIdentity {
                id: Uuid::new_v4(),
                name: Name(
                    external::Name::try_from("test-nic".to_string()).unwrap(),
                ),
                description: "test NIC".to_string(),
                time_created: Utc::now(),
                time_modified: Utc::now(),
                time_deleted: None,
            },
            service_id: zone_id.into_untyped_uuid(),
            vpc_id: Uuid::new_v4(),
            subnet_id: Uuid::new_v4(),
            mac: MacAddr(external::MacAddr([0, 0, 0, 0, 0, 0].into())),
            ipv4: Some(DbIpv4Addr::from(
                "192.168.1.2".parse::<Ipv4Addr>().unwrap(),
            )),
            ipv6: None,
            slot: SqlU8::new(0),
            primary: true,
        }
    }

    #[tokio::test]
    async fn test_pruneable_zones_reason_checker() {
        const TEST_NAME: &str = "test_pruneable_zones_reason_checker";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system and build from there.
        let (example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME)
                .external_dns_count(1)
                .expect("1 is a valid count of external DNS zones")
                .set_boundary_ntp_count(1)
                .expect("1 is a valid count of boundary NTP zones")
                .build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Pick the first sled to add missing zone types
        let sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled");

        // Add zones for types that aren't in the default example system
        let image_source = BlueprintZoneImageSource::InstallDataset;

        builder
            .sled_add_zone_oximeter(sled_id, image_source.clone())
            .expect("added oximeter zone");
        builder
            .sled_add_zone_cockroachdb(sled_id, image_source.clone())
            .expect("added cockroach zone");
        builder
            .sled_add_zone_clickhouse_keeper(sled_id, image_source.clone())
            .expect("added clickhouse keeper zone");
        builder
            .sled_add_zone_clickhouse_server(sled_id, image_source.clone())
            .expect("added clickhouse server zone");

        // Collect one zone ID of each type that has an associated
        // `BlueprintExpungedZoneAccessReason` we ought to check.
        let mut boundary_ntp_zone_id = None;
        let mut clickhouse_keeper_zone_id = None;
        let mut external_dns_zone_id = None;
        let mut nexus_zone_id = None;
        let mut oximeter_zone_id = None;
        let mut cockroach_zone_id = None;

        for (sled_id, zone) in builder.current_in_service_zones() {
            match &zone.zone_type {
                BlueprintZoneType::BoundaryNtp(_) => {
                    boundary_ntp_zone_id = Some((sled_id, zone.id));
                }
                BlueprintZoneType::ClickhouseKeeper(_) => {
                    clickhouse_keeper_zone_id = Some((sled_id, zone.id));
                }
                BlueprintZoneType::ExternalDns(_) => {
                    external_dns_zone_id = Some((sled_id, zone.id));
                }
                BlueprintZoneType::Nexus(_) => {
                    nexus_zone_id = Some((sled_id, zone.id));
                }
                BlueprintZoneType::Oximeter(_) => {
                    oximeter_zone_id = Some((sled_id, zone.id));
                }
                BlueprintZoneType::CockroachDb(_) => {
                    cockroach_zone_id = Some((sled_id, zone.id));
                }
                _ => {}
            }
        }

        // Expunge and mark each zone as ready for cleanup
        for (sled_id, zone_id) in [
            boundary_ntp_zone_id.expect("found boundary ntp zone"),
            clickhouse_keeper_zone_id.expect("found clickhouse keeper zone"),
            external_dns_zone_id.expect("found external dns zone"),
            nexus_zone_id.expect("found nexus zone"),
            oximeter_zone_id.expect("found oximeter zone"),
            cockroach_zone_id.expect("found cockroach zone"),
        ] {
            builder.sled_expunge_zone(sled_id, zone_id).expect("expunged zone");
            builder
                .sled_mark_expunged_zone_ready_for_cleanup(sled_id, zone_id)
                .expect("marked zone ready for cleanup");
        }

        let blueprint = builder.build(BlueprintSource::Test);

        // Check for pruneable zones; this test isn't primarily concerned with
        // which ones actually are pruneable - it'll be a mix - but whether
        // attempting to find PruneableZones did in fact check all possible
        // `BlueprintExpungedZoneAccessReason`s. After this function returns, we
        // interrogate the internal `reason_checker` and assert that it contains
        // all known reasons.
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");

        let mut unchecked = BTreeSet::new();

        for reason in BlueprintExpungedZoneAccessReason::iter() {
            if !pruneable_zones.reason_checker.reasons_checked.contains(&reason)
            {
                unchecked.insert(reason);
            }
        }

        assert!(
            unchecked.is_empty(),
            "PruneableZones failed to consider some \
             `BlueprintExpungedZoneAccessReason`s: {unchecked:?}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_oximeter_pruneable_reasons() {
        const TEST_NAME: &str = "test_oximeter_pruneable_reasons";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system and build from there.
        let (example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME).build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Pick the first sled to add an Oximeter zone
        let sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled");

        // Add an Oximeter zone, then expunge it.
        let image_source = BlueprintZoneImageSource::InstallDataset;
        builder
            .sled_add_zone_oximeter(sled_id, image_source)
            .expect("added oximeter zone");
        let oximeter_zone_id = builder
            .current_in_service_zones()
            .find_map(|(_, zone)| {
                matches!(zone.zone_type, BlueprintZoneType::Oximeter(_))
                    .then_some(zone.id)
            })
            .expect("found oximeter zone");
        builder
            .sled_expunge_zone(sled_id, oximeter_zone_id)
            .expect("expunged zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(
                sled_id,
                oximeter_zone_id,
            )
            .expect("marked zone ready for cleanup");

        let blueprint = builder.build(BlueprintSource::Test);

        // Check that the zone is pruneable (no oximeter record, no producers)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&oximeter_zone_id),
            "oximeter zone should be pruneable when there's no \
             oximeter record and no producers"
        );

        // Now insert an oximeter collector record for this zone
        // This simulates the case where cleanup hasn't yet happened
        datastore
            .oximeter_create(
                opctx,
                &nexus_db_model::OximeterInfo::new(&OximeterInfo {
                    collector_id: oximeter_zone_id.into_untyped_uuid(),
                    address: "[::1]:12223".parse().unwrap(),
                }),
            )
            .await
            .expect("failed to insert oximeter record");

        // Check that the zone is no longer pruneable
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&oximeter_zone_id),
            "oximeter zone should not be pruneable when there's an \
             oximeter record"
        );

        // Now add producers assigned to this oximeter
        use omicron_uuid_kinds::GenericUuid;
        let producer1 = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: "[::1]:12345".parse().unwrap(),
            interval: Duration::from_secs(30),
        };
        let producer2 = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: "[::1]:12346".parse().unwrap(),
            interval: Duration::from_secs(30),
        };

        datastore
            .producer_endpoint_upsert_and_assign(opctx, &producer1)
            .await
            .expect("failed to insert producer 1");
        datastore
            .producer_endpoint_upsert_and_assign(opctx, &producer2)
            .await
            .expect("failed to insert producer 2");

        // Mark the oximeter as expunged in the datastore
        datastore
            .oximeter_expunge(opctx, oximeter_zone_id.into_untyped_uuid())
            .await
            .expect("failed to expunge oximeter");

        // Check that the zone is still not pruneable (producers are assigned)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&oximeter_zone_id),
            "oximeter zone should not be pruneable when it has \
             producers assigned, even if expunged"
        );

        // Create a second dummy oximeter to reassign producers to
        datastore
            .oximeter_create(
                opctx,
                &nexus_db_model::OximeterInfo::new(&OximeterInfo {
                    collector_id: Uuid::new_v4(),
                    address: "[::1]:12224".parse().unwrap(),
                }),
            )
            .await
            .expect("failed to insert dummy oximeter");

        // Reassign the producers from the expunged oximeter to the new one
        let reassignment = datastore
            .oximeter_reassign_all_producers(
                opctx,
                oximeter_zone_id.into_untyped_uuid(),
            )
            .await
            .expect("failed to reassign producers");
        match reassignment {
            CollectorReassignment::Complete(n) => {
                assert_eq!(n, 2, "expected 2 producers to be reassigned");
            }
            CollectorReassignment::NoCollectorsAvailable => {
                panic!(
                    "expected producers to be reassigned, but no collectors \
                     available"
                );
            }
        }

        // Check that the zone is now pruneable again (no producers assigned)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&oximeter_zone_id),
            "oximeter zone should be pruneable when expunged and \
             all producers have been reassigned"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_boundary_ntp_pruneable_reasons() {
        const TEST_NAME: &str = "test_boundary_ntp_pruneable_reasons";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system with one boundary NTP zone
        let (example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME)
                .set_boundary_ntp_count(1)
                .expect("1 is a valid count of boundary NTP zones")
                .build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find the boundary NTP zone and expunge it.
        let (sled_id, boundary_ntp_zone_id, external_ip) = builder
            .current_in_service_zones()
            .find_map(|(sled_id, zone)| {
                if let BlueprintZoneType::BoundaryNtp(config) = &zone.zone_type
                {
                    Some((sled_id, zone.id, config.external_ip))
                } else {
                    None
                }
            })
            .expect("found boundary ntp zone");
        builder
            .sled_expunge_zone(sled_id, boundary_ntp_zone_id)
            .expect("expunged zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(
                sled_id,
                boundary_ntp_zone_id,
            )
            .expect("marked zone ready for cleanup");

        let blueprint = builder.build(BlueprintSource::Test);

        // Confirm that it's NOT pruneable when there's no other boundary NTP
        // zone with the same upstream config.
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&boundary_ntp_zone_id),
            "boundary NTP zone should not be pruneable when there's no \
             other in-service zone with the same upstream config"
        );

        // Now add another boundary NTP zone with the same upstream config
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder2", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find a different sled to add the second boundary NTP zone
        let other_sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .find(|id| *id != sled_id)
            .expect("at least two sleds");

        builder
            .sled_promote_internal_ntp_to_boundary_ntp(
                other_sled_id,
                BlueprintZoneImageSource::InstallDataset,
                {
                    // Construct a ExternalSnatNetworkingChoice; steal the
                    // existing zone's snat_cfg
                    let nic_ip_config = PrivateIpConfig::new_ipv4(
                        Ipv4Addr::new(10, 0, 0, 1),
                        "10.0.0.0/24".parse().unwrap(),
                    )
                    .expect("valid private IP config");
                    ExternalSnatNetworkingChoice {
                        snat_cfg: external_ip.snat_cfg,
                        nic_ip_config,
                        nic_mac: ExternalMacAddr(
                            omicron_common::api::external::MacAddr(
                                [0, 0, 0, 0, 0, 1].into(),
                            )
                            .0,
                        ),
                    }
                },
            )
            .expect("promoted boundary NTP zone");

        let blueprint = builder.build(BlueprintSource::Test);

        // Check that the zone IS now pruneable (another zone has the same
        // config)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&boundary_ntp_zone_id),
            "boundary NTP zone should be pruneable when there's another \
             in-service zone with the same upstream config"
        );

        // Check that it's NOT pruneable when there's an associated external IP
        // db row
        let external_ip = make_external_ip_for_zone(boundary_ntp_zone_id);
        let pruneable_zones = PruneableZones::new(
            opctx,
            datastore,
            &blueprint,
            &[external_ip],
            &[],
        )
        .await
        .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&boundary_ntp_zone_id),
            "boundary NTP zone should not be pruneable when there's an \
             associated external IP row"
        );

        // Check that it's NOT pruneable when there's an associated service NIC
        // db row
        let service_nic = make_service_nic_for_zone(boundary_ntp_zone_id);
        let pruneable_zones = PruneableZones::new(
            opctx,
            datastore,
            &blueprint,
            &[],
            &[service_nic],
        )
        .await
        .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&boundary_ntp_zone_id),
            "boundary NTP zone should not be pruneable when there's an \
             associated service NIC row"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_nexus_pruneable_reasons() {
        const TEST_NAME: &str = "test_nexus_pruneable_reasons";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system (which includes Nexus zones)
        let (_example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME).build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find a Nexus zone and expunge it
        let (sled_id, nexus_zone_id) = builder
            .current_in_service_zones()
            .find_map(|(sled_id, zone)| {
                matches!(zone.zone_type, BlueprintZoneType::Nexus(_))
                    .then_some((sled_id, zone.id))
            })
            .expect("found nexus zone");
        builder
            .sled_expunge_zone(sled_id, nexus_zone_id)
            .expect("expunged zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(sled_id, nexus_zone_id)
            .expect("marked zone ready for cleanup");

        let blueprint = builder.build(BlueprintSource::Test);

        // Check that the zone IS pruneable when no database reasons exist
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&nexus_zone_id),
            "nexus zone should be pruneable when no database reasons exist"
        );

        // Check that it's NOT pruneable when there's an associated external IP
        // db row
        let external_ip = make_external_ip_for_zone(nexus_zone_id);
        let pruneable_zones = PruneableZones::new(
            opctx,
            datastore,
            &blueprint,
            &[external_ip],
            &[],
        )
        .await
        .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&nexus_zone_id),
            "nexus zone should not be pruneable when there's an \
             associated external IP row"
        );

        // Check that it's NOT pruneable when there's an associated service NIC
        // db row
        let service_nic = make_service_nic_for_zone(nexus_zone_id);
        let pruneable_zones = PruneableZones::new(
            opctx,
            datastore,
            &blueprint,
            &[],
            &[service_nic],
        )
        .await
        .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&nexus_zone_id),
            "nexus zone should not be pruneable when there's an \
             associated service NIC row"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_external_dns_pruneable_reasons() {
        const TEST_NAME: &str = "test_external_dns_pruneable_reasons";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system with one external DNS zone
        let (example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME)
                .external_dns_count(1)
                .expect("1 is a valid count of external DNS zones")
                .build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find the external DNS zone and expunge it
        let (sled_id, external_dns_zone_id, dns_address) = builder
            .current_in_service_zones()
            .find_map(|(sled_id, zone)| {
                if let BlueprintZoneType::ExternalDns(config) = &zone.zone_type
                {
                    Some((sled_id, zone.id, config.dns_address))
                } else {
                    None
                }
            })
            .expect("found external dns zone");
        builder
            .sled_expunge_zone(sled_id, external_dns_zone_id)
            .expect("expunged zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(
                sled_id,
                external_dns_zone_id,
            )
            .expect("marked zone ready for cleanup");

        let blueprint = builder.build(BlueprintSource::Test);

        // Confirm that it's NOT pruneable when there's no other external DNS
        // zone with the same IP (meaning the IP hasn't been reassigned yet).
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&external_dns_zone_id),
            "external DNS zone should not be pruneable when its IP \
             hasn't been reassigned to another in-service zone"
        );

        // Now add another external DNS zone with the same IP address
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder2", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find a different sled to add the second external DNS zone
        let other_sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .find(|id| *id != sled_id)
            .expect("at least two sleds");

        // Construct an ExternalNetworkingChoice with the same external IP
        let external_ip_choice = {
            let nic_ip_config = PrivateIpConfig::new_ipv4(
                Ipv4Addr::new(10, 0, 0, 2),
                "10.0.0.0/24".parse().unwrap(),
            )
            .expect("valid private IP config");
            ExternalNetworkingChoice {
                external_ip: dns_address.addr.ip(),
                nic_ip_config,
                nic_mac: ExternalMacAddr(
                    omicron_common::api::external::MacAddr(
                        [0, 0, 0, 0, 0, 2].into(),
                    )
                    .0,
                ),
            }
        };

        builder
            .sled_add_zone_external_dns(
                other_sled_id,
                BlueprintZoneImageSource::InstallDataset,
                external_ip_choice,
            )
            .expect("added external DNS zone");

        let blueprint = builder.build(BlueprintSource::Test);

        // Check that the zone IS now pruneable (the IP has been reassigned to
        // another in-service zone)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&external_dns_zone_id),
            "external DNS zone should be pruneable when its IP has been \
             reassigned to another in-service zone"
        );

        // Check that it's NOT pruneable when there's an associated external IP
        // db row
        let external_ip = make_external_ip_for_zone(external_dns_zone_id);
        let pruneable_zones = PruneableZones::new(
            opctx,
            datastore,
            &blueprint,
            &[external_ip],
            &[],
        )
        .await
        .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&external_dns_zone_id),
            "external DNS zone should not be pruneable when there's an \
             associated external IP row"
        );

        // Check that it's NOT pruneable when there's an associated service NIC
        // db row
        let service_nic = make_service_nic_for_zone(external_dns_zone_id);
        let pruneable_zones = PruneableZones::new(
            opctx,
            datastore,
            &blueprint,
            &[],
            &[service_nic],
        )
        .await
        .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&external_dns_zone_id),
            "external DNS zone should not be pruneable when there's an \
             associated service NIC row"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_clickhouse_keeper_pruneable_reasons() {
        const TEST_NAME: &str = "test_clickhouse_keeper_pruneable_reasons";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system and add a clickhouse keeper zone
        let (_example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME).build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find a sled and add a clickhouse keeper zone
        let sled_id = builder
            .current_in_service_zones()
            .map(|(sled_id, _)| sled_id)
            .next()
            .expect("at least one sled");

        builder
            .sled_add_zone_clickhouse_keeper(
                sled_id,
                BlueprintZoneImageSource::InstallDataset,
            )
            .expect("added clickhouse keeper zone");

        // Find the keeper zone we just added
        let keeper_zone_id = builder
            .current_in_service_zones()
            .find_map(|(_, zone)| {
                matches!(zone.zone_type, BlueprintZoneType::ClickhouseKeeper(_))
                    .then_some(zone.id)
            })
            .expect("found clickhouse keeper zone");

        // Manually add the keeper to the clickhouse cluster config
        // (normally the planner does this, but we need it for testing)
        let config =
            builder.clickhouse_cluster_config().cloned().unwrap_or_else(|| {
                ClickhouseClusterConfig::new(
                    "test_cluster".to_string(),
                    "test_secret".to_string(),
                )
            });
        let mut new_config = config.clone();
        new_config.keepers.insert(keeper_zone_id, KeeperId(1));
        builder.set_clickhouse_cluster_config(Some(new_config));

        // Expunge the keeper zone
        builder
            .sled_expunge_zone(sled_id, keeper_zone_id)
            .expect("expunged zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(sled_id, keeper_zone_id)
            .expect("marked zone ready for cleanup");

        let blueprint = builder.build(BlueprintSource::Test);

        // Confirm that it's NOT pruneable when it's still in the clickhouse
        // cluster config
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&keeper_zone_id),
            "clickhouse keeper zone should not be pruneable when it's \
             still in the clickhouse cluster config"
        );

        // Now remove it from the clickhouse cluster config
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder2", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Get the current config, remove the keeper, and set it back
        let mut config = builder
            .clickhouse_cluster_config()
            .expect("clickhouse cluster config exists")
            .clone();
        config.keepers.remove(&keeper_zone_id);
        builder.set_clickhouse_cluster_config(Some(config));

        let blueprint = builder.build(BlueprintSource::Test);

        // Check that the zone IS now pruneable (removed from cluster config)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&keeper_zone_id),
            "clickhouse keeper zone should be pruneable when it's been \
             removed from the clickhouse cluster config"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_clickhouse_server_pruneable_reasons() {
        const TEST_NAME: &str = "test_clickhouse_server_pruneable_reasons";

        let logctx = dev::test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with the base example system and add a clickhouse server zone
        let (_example, initial_blueprint) =
            ExampleSystemBuilder::new(log, TEST_NAME).build();
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Find a sled and add a clickhouse server zone
        let sled_id = builder
            .current_in_service_zones()
            .map(|(sled_id, _)| sled_id)
            .next()
            .expect("at least one sled");

        builder
            .sled_add_zone_clickhouse_server(
                sled_id,
                BlueprintZoneImageSource::InstallDataset,
            )
            .expect("added clickhouse server zone");

        // Find the server zone we just added
        let server_zone_id = builder
            .current_in_service_zones()
            .find_map(|(_, zone)| {
                matches!(zone.zone_type, BlueprintZoneType::ClickhouseServer(_))
                    .then_some(zone.id)
            })
            .expect("found clickhouse server zone");

        // Manually add the server to the clickhouse cluster config
        // (normally the planner does this, but we need it for testing)
        use clickhouse_admin_types::server::ServerId;
        use nexus_types::deployment::ClickhouseClusterConfig;
        let config =
            builder.clickhouse_cluster_config().cloned().unwrap_or_else(|| {
                ClickhouseClusterConfig::new(
                    "test_cluster".to_string(),
                    "test_secret".to_string(),
                )
            });
        let mut new_config = config.clone();
        new_config.servers.insert(server_zone_id, ServerId(1));
        builder.set_clickhouse_cluster_config(Some(new_config));

        // Expunge the server zone
        builder
            .sled_expunge_zone(sled_id, server_zone_id)
            .expect("expunged zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(sled_id, server_zone_id)
            .expect("marked zone ready for cleanup");

        let blueprint = builder.build(BlueprintSource::Test);

        // Confirm that it's NOT pruneable when it's still in the clickhouse
        // cluster config
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            !pruneable_zones.pruneable_zones.contains(&server_zone_id),
            "clickhouse server zone should not be pruneable when it's \
             still in the clickhouse cluster config"
        );

        // Now remove it from the clickhouse cluster config
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint,
            TEST_NAME,
            PlannerRng::from_seed(("builder2", TEST_NAME)),
        )
        .expect("failed to create builder");

        // Get the current config, remove the server, and set it back
        let mut config = builder
            .clickhouse_cluster_config()
            .expect("clickhouse cluster config exists")
            .clone();
        config.servers.remove(&server_zone_id);
        builder.set_clickhouse_cluster_config(Some(config));

        let blueprint = builder.build(BlueprintSource::Test);

        // Check that the zone IS now pruneable (removed from cluster config)
        let pruneable_zones =
            PruneableZones::new(opctx, datastore, &blueprint, &[], &[])
                .await
                .expect("failed to find pruneable zones");
        assert!(
            pruneable_zones.pruneable_zones.contains(&server_zone_id),
            "clickhouse server zone should be pruneable when it's been \
             removed from the clickhouse cluster config"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
