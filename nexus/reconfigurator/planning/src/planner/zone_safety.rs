// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks whether various zones are safe to shut down for the purposes of
//! planning steps that involve bouncing other components.

use crate::blueprint_builder::BlueprintBuilder;
use itertools::Itertools;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::CockroachdbUnsafeToShutdown;
use nexus_types::deployment::ZoneUnsafeToShutdown;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug)]
pub(crate) struct ZoneSafetyChecks {
    // The actual content of all zones unsafe to shut down, mapped by sled ID
    // first then zone ID.
    sleds_with_unsafe_zones:
        BTreeMap<SledUuid, BTreeMap<OmicronZoneUuid, ZoneUnsafeToShutdown>>,
    // Helper to look up specific zones by ID; each zone in this map will also
    // be present in `sleds_with_unsafe_zones()`, under the sled stored in
    // this map.
    //
    // NOTE: An invariant of this type is that for every sled ID in
    // `sleds_with_unsafe_zones`, all of the zone IDs it points to are also
    // present as keys in this map pointing back to the sled ID. The (private)
    // `insert()` method handles this invariant correctly; any changes to the
    // fields of this type or the way it's constructed need to take this into
    // account.
    zone_to_sled: BTreeMap<OmicronZoneUuid, SledUuid>,
}

impl ZoneSafetyChecks {
    /// Calculate the set of zones that are not currently safe to shut down.
    ///
    /// This is inherently racy: our decisions are based mostly on `inventory`,
    /// which may be out of date. But we should do our best to not make bad
    /// situations _we're aware of_ worse. (E.g., if we know the Cockroach
    /// cluster is down a node, we shouldn't shut down any other nodes. Our
    /// total level of redundancy should be high enough to protect us against
    /// making a decision that it's okay to shut down one node while
    /// simulateously losing another one.)
    pub fn new(
        blueprint: &BlueprintBuilder<'_>,
        inventory: &Collection,
        current_internal_dns_generation: Generation,
    ) -> Self {
        ZoneSafetyChecksBuilder::new(
            blueprint,
            inventory,
            current_internal_dns_generation,
        )
        .build()
    }

    pub fn empty() -> Self {
        Self {
            sleds_with_unsafe_zones: BTreeMap::new(),
            zone_to_sled: BTreeMap::new(),
        }
    }

    /// If the specified zone is unsafe to shut down, returns `Some(reason)`;
    /// otherwise, returns `None`.
    pub fn zone_unsafe_shutdown_reason(
        &self,
        zone_id: &OmicronZoneUuid,
    ) -> Option<&ZoneUnsafeToShutdown> {
        let sled_id = self.zone_to_sled.get(zone_id)?;

        // This first lookup should always succeed, but it's harmless to `?` it.
        self.sleds_with_unsafe_zones.get(sled_id)?.get(zone_id)
    }

    /// If this sled is hosting any zones that are unsafe to shut down, returns
    /// `Some(reason)`; otherwise, returns `None`.
    pub fn sled_unsafe_shutdown_reason(
        &self,
        sled_id: &SledUuid,
    ) -> Option<String> {
        let zones = self.sleds_with_unsafe_zones.get(sled_id)?;
        Some(
            zones
                .iter()
                .map(|(zone_id, reason)| format!("{zone_id}: {reason}"))
                .join(", "),
        )
    }

    // Helper method only used by `ZoneSafetyChecksBuilder` during our
    // construction; ensure we update both our fields together.
    fn insert(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
        reason: ZoneUnsafeToShutdown,
    ) {
        self.zone_to_sled.insert(zone_id, sled_id);
        self.sleds_with_unsafe_zones
            .entry(sled_id)
            .or_default()
            .insert(zone_id, reason);
    }
}

struct ZoneSafetyChecksBuilder<'a> {
    blueprint: &'a BlueprintBuilder<'a>,
    inventory: &'a Collection,
    current_internal_dns_generation: Generation,
    internal_dns_zones: BTreeSet<OmicronZoneUuid>,
    boundary_ntp_zones: BTreeSet<OmicronZoneUuid>,
    checks: ZoneSafetyChecks,
}

impl<'a> ZoneSafetyChecksBuilder<'a> {
    fn new(
        blueprint: &'a BlueprintBuilder<'a>,
        inventory: &'a Collection,
        current_internal_dns_generation: Generation,
    ) -> Self {
        let mut internal_dns_zones = BTreeSet::new();
        let mut boundary_ntp_zones = BTreeSet::new();

        // Precalculate sets of particular zone kinds that we care about in
        // other checks below.
        for (_sled_id, zone) in
            blueprint.current_zones(BlueprintZoneDisposition::is_in_service)
        {
            match zone.zone_type.kind() {
                ZoneKind::BoundaryNtp => {
                    boundary_ntp_zones.insert(zone.id);
                }
                ZoneKind::InternalDns => {
                    internal_dns_zones.insert(zone.id);
                }
                _ => (),
            }
        }

        Self {
            blueprint,
            inventory,
            current_internal_dns_generation,
            internal_dns_zones,
            boundary_ntp_zones,
            checks: ZoneSafetyChecks::empty(),
        }
    }

    fn build(mut self) -> ZoneSafetyChecks {
        for (sled_id, zone) in self
            .blueprint
            .current_zones(BlueprintZoneDisposition::is_in_service)
        {
            if let Some(reason) = self.reason_zone_unsafe_to_shut_down(zone) {
                self.checks.insert(sled_id, zone.id, reason);
            }
        }
        self.checks
    }

    fn reason_zone_unsafe_to_shut_down(
        &self,
        zone: &BlueprintZoneConfig,
    ) -> Option<ZoneUnsafeToShutdown> {
        match zone.kind() {
            ZoneKind::BoundaryNtp => {
                self.reason_boundary_ntp_unsafe_to_shut_down()
            }
            ZoneKind::CockroachDb => {
                self.reason_cockroach_unsafe_to_shut_down()
            }
            ZoneKind::InternalDns => {
                self.reason_internal_dns_unsafe_to_shut_down()
            }
            // No safety checks implemented for these zone types; we assume we
            // can always shut them down, regardless of whether other instances
            // within the rack are healthy.
            ZoneKind::Clickhouse
            | ZoneKind::ClickhouseKeeper
            | ZoneKind::ClickhouseServer
            | ZoneKind::Crucible
            | ZoneKind::CruciblePantry
            | ZoneKind::ExternalDns
            | ZoneKind::InternalNtp
            | ZoneKind::Nexus
            | ZoneKind::Oximeter => None,
        }
    }

    fn reason_boundary_ntp_unsafe_to_shut_down(
        &self,
    ) -> Option<ZoneUnsafeToShutdown> {
        // Count synchronized boundary NTP zones by checking timesync data.
        let mut synchronized_boundary_ntp_count = 0;
        for timesync in self.inventory.ntp_timesync.iter() {
            // We only consider zones which we expect to be in-service from our
            // blueprint - this means that old inventory collections including
            // data for expunged zones will not be considered in the total count
            // of synchronized boundary NTP zones.
            if self.boundary_ntp_zones.contains(&timesync.zone_id)
                && timesync.synced
            {
                synchronized_boundary_ntp_count += 1;
            }
        }

        let target_boundary_ntp_zone_count = self
            .blueprint
            .planning_input()
            .set_target_boundary_ntp_zone_count();
        if synchronized_boundary_ntp_count < target_boundary_ntp_zone_count {
            return Some(ZoneUnsafeToShutdown::BoundaryNtp {
                total_boundary_ntp_zones: self.boundary_ntp_zones.len(),
                synchronized_count: synchronized_boundary_ntp_count,
            });
        }

        None
    }

    fn reason_cockroach_unsafe_to_shut_down(
        &self,
    ) -> Option<ZoneUnsafeToShutdown> {
        use CockroachdbUnsafeToShutdown::*;
        use ZoneUnsafeToShutdown::Cockroachdb;

        let target_cockroachdb_zone_count =
            self.blueprint.planning_input().set_target_cockroachdb_zone_count();

        // We must hear from all nodes
        let all_statuses = &self.inventory.cockroach_status;

        if all_statuses.len() < target_cockroachdb_zone_count {
            return Some(Cockroachdb { reason: NotEnoughNodes });
        }

        // All nodes must report: "We have the necessary redundancy, and
        // have observed no underreplicated ranges".
        for (_node_id, status) in all_statuses {
            let Some(ranges_underreplicated) = status.ranges_underreplicated
            else {
                return Some(Cockroachdb {
                    reason: MissingUnderreplicatedStat,
                });
            };
            if ranges_underreplicated != 0 {
                return Some(Cockroachdb {
                    reason: UnderreplicatedRanges { n: ranges_underreplicated },
                });
            }
            let Some(live_nodes) = status.liveness_live_nodes else {
                return Some(Cockroachdb { reason: MissingLiveNodesStat });
            };
            if live_nodes < target_cockroachdb_zone_count as u64 {
                return Some(Cockroachdb {
                    reason: NotEnoughLiveNodes { live_nodes },
                });
            }
        }
        None
    }

    fn reason_internal_dns_unsafe_to_shut_down(
        &self,
    ) -> Option<ZoneUnsafeToShutdown> {
        // Count the number of Internal DNS servers exactly at our expected
        // generation number.
        let mut synchronized_internal_dns_count = 0;
        for status in self.inventory.internal_dns_generation_status.iter() {
            // We consider internal DNS servers up-to-date if they have
            // a generation number matching what we observed in the DB
            // at the start of blueprint generation.
            //
            // - If we observe an older generation number in inventory,
            // the DNS server is out-of-date.
            // - If we observe a newer generation number in inventory,
            // the value the planner read from the database is
            // out-of-date.
            //
            // Either way, from our perspective, the internal DNS zone
            // shouldn't be considered "ready-to-shutdown".
            if self.internal_dns_zones.contains(&status.zone_id)
                && status.generation == self.current_internal_dns_generation
            {
                synchronized_internal_dns_count += 1;
            }
        }

        // Our goal is to have enough Internal DNS servers running at a
        // sufficiently up-to-date version such that if the system powers off
        // and restarts, at least one exists and can get the control plane back
        // up and running.
        //
        // The target internal DNS zone count should be set so that we can
        // tolerate "at least one upgrade, and at least one failure during that
        // upgrade window" (e.g., it should be "at least 3" in production).
        let target_internal_dns_zone_count = self
            .blueprint
            .planning_input()
            .set_target_internal_dns_zone_count();
        if synchronized_internal_dns_count >= target_internal_dns_zone_count {
            return None;
        } else {
            return Some(ZoneUnsafeToShutdown::InternalDns {
                total_internal_dns_zones: self.internal_dns_zones.len(),
                synchronized_count: synchronized_internal_dns_count,
            });
        }
    }
}
