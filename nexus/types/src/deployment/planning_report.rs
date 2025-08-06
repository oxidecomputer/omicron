// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing a report on a planning run that produced a blueprint.

use super::ArtifactHash;
use super::BlueprintZoneConfig;
use super::BlueprintZoneImageSource;
use super::CockroachDbPreserveDowngrade;
use super::PendingMgsUpdates;
use super::PlannerChickenSwitches;

use daft::Diffable;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

/// A full blueprint planning report. Other than the blueprint ID, each
/// field corresponds to a step in the update planner, i.e., a subroutine
/// of `omicron_nexus::reconfigurator::planning::Planner::do_plan`.
///
/// The intent of a planning report is to capture information useful to an
/// operator or developer about the planning process itself, especially if
/// it has become "stuck" (unable to proceed with an update). It is *not* a
/// summary of the plan (blueprint), but rather a description of non-fatal
/// conditions the planner is waiting on, unexpected or invalid
/// configurations encountered during planning, etc. The planner may make
/// internal decisions based on the step reports; the intent is that an
/// operator may make administrative decisions based on the full report.
///
/// Only successful planning runs are currently covered by this report.
/// Failures to plan (i.e., to generate a valid blueprint) are represented
/// by `nexus-reconfigurator-planning::blueprint_builder::Error`.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[must_use = "an unread report is not actionable"]
pub struct PlanningReport {
    /// The blueprint produced by the planning run this report describes.
    pub blueprint_id: BlueprintUuid,

    /// The set of "chicken switches" in effect for this planning run.
    pub chicken_switches: PlannerChickenSwitches,

    // Step reports.
    pub expunge: PlanningExpungeStepReport,
    pub decommission: PlanningDecommissionStepReport,
    pub noop_image_source: PlanningNoopImageSourceStepReport,
    pub mgs_updates: PlanningMgsUpdatesStepReport,
    pub add: PlanningAddStepReport,
    pub zone_updates: PlanningZoneUpdatesStepReport,
    pub cockroachdb_settings: PlanningCockroachdbSettingsStepReport,
}

impl PlanningReport {
    pub fn new(blueprint_id: BlueprintUuid) -> Self {
        Self {
            blueprint_id,
            chicken_switches: PlannerChickenSwitches::default(),
            expunge: PlanningExpungeStepReport::new(),
            decommission: PlanningDecommissionStepReport::new(),
            noop_image_source: PlanningNoopImageSourceStepReport::new(),
            mgs_updates: PlanningMgsUpdatesStepReport::new(
                PendingMgsUpdates::new(),
            ),
            add: PlanningAddStepReport::new(),
            zone_updates: PlanningZoneUpdatesStepReport::new(),
            cockroachdb_settings: PlanningCockroachdbSettingsStepReport::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.expunge.is_empty()
            && self.decommission.is_empty()
            && self.noop_image_source.is_empty()
            && self.mgs_updates.is_empty()
            && self.add.is_empty()
            && self.zone_updates.is_empty()
            && self.cockroachdb_settings.is_empty()
    }
}

impl fmt::Display for PlanningReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_empty() {
            writeln!(
                f,
                "Empty planning report for blueprint {}.",
                self.blueprint_id,
            )?;
        } else {
            let Self {
                blueprint_id,
                chicken_switches,
                expunge,
                decommission,
                noop_image_source,
                mgs_updates,
                add,
                zone_updates,
                cockroachdb_settings,
            } = self;
            writeln!(f, "Planning report for blueprint {blueprint_id}:")?;
            if *chicken_switches != PlannerChickenSwitches::default() {
                writeln!(
                    f,
                    "Chicken switches:\n{}",
                    chicken_switches.display()
                )?;
            }
            expunge.fmt(f)?;
            decommission.fmt(f)?;
            noop_image_source.fmt(f)?;
            mgs_updates.fmt(f)?;
            add.fmt(f)?;
            zone_updates.fmt(f)?;
            cockroachdb_settings.fmt(f)?;
        }
        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningExpungeStepReport {
    /// Expunged disks not present in the parent blueprint.
    pub orphan_disks: BTreeMap<SledUuid, PhysicalDiskUuid>,
}

impl PlanningExpungeStepReport {
    pub fn new() -> Self {
        Self { orphan_disks: BTreeMap::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.orphan_disks.is_empty()
    }
}

impl fmt::Display for PlanningExpungeStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { orphan_disks } = self;
        if !orphan_disks.is_empty() {
            writeln!(
                f,
                "* planning input contained expunged disks \
                   not present in parent blueprint:",
            )?;
            for (sled, disk) in orphan_disks.iter() {
                writeln!(f, "  * sled {sled}, disk {disk}",)?;
            }
        }
        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningDecommissionStepReport {
    /// Decommissioned sleds that unexpectedly appeared as commissioned.
    pub zombie_sleds: Vec<SledUuid>,
}

impl PlanningDecommissionStepReport {
    pub fn new() -> Self {
        Self { zombie_sleds: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.zombie_sleds.is_empty()
    }
}

impl fmt::Display for PlanningDecommissionStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { zombie_sleds } = self;
        if !zombie_sleds.is_empty() {
            let (n, s) = plural_vec(zombie_sleds);
            writeln!(
                f,
                "* {n} decommissioned sled{s} returned by `SledFilter::Commissioned`: {}",
                zombie_sleds
                    .iter()
                    .map(|sled_id| format!("{sled_id}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

/// How many of the total install-dataset zones were noop-converted to use
/// the artifact store on a particular sled.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningNoopImageSourceConvertedZones {
    pub num_eligible: usize,
    pub num_dataset: usize,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningNoopImageSourceStepReport {
    pub no_target_release: bool,
    pub skipped_sleds:
        BTreeMap<SledUuid, PlanningNoopImageSourceSkipSledReason>,
    pub skipped_zones:
        BTreeMap<OmicronZoneUuid, PlanningNoopImageSourceSkipZoneReason>,
    pub converted_zones:
        BTreeMap<SledUuid, PlanningNoopImageSourceConvertedZones>,
}

impl PlanningNoopImageSourceStepReport {
    pub fn new() -> Self {
        Self {
            no_target_release: false,
            skipped_sleds: BTreeMap::new(),
            skipped_zones: BTreeMap::new(),
            converted_zones: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.no_target_release
            && self.skipped_sleds.is_empty()
            && self.skipped_zones.is_empty()
            && self.converted_zones.is_empty()
    }

    pub fn skip_sled(
        &mut self,
        sled_id: SledUuid,
        reason: PlanningNoopImageSourceSkipSledReason,
    ) {
        self.skipped_sleds.insert(sled_id, reason);
    }

    pub fn skip_zone(
        &mut self,
        zone_id: OmicronZoneUuid,
        reason: PlanningNoopImageSourceSkipZoneReason,
    ) {
        self.skipped_zones.insert(zone_id, reason);
    }

    pub fn converted_zones(
        &mut self,
        sled_id: SledUuid,
        num_eligible: usize,
        num_dataset: usize,
    ) {
        self.converted_zones.insert(
            sled_id,
            PlanningNoopImageSourceConvertedZones { num_eligible, num_dataset },
        );
    }
}

impl fmt::Display for PlanningNoopImageSourceStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            no_target_release,
            skipped_sleds,
            skipped_zones: _,
            converted_zones,
        } = self;

        if *no_target_release {
            return writeln!(
                f,
                "* Skipping noop image source check for all sleds (no current TUF repo)",
            );
        }

        for (sled_id, reason) in skipped_sleds.iter() {
            writeln!(
                f,
                "* Skipping noop image source check on sled {sled_id}: {reason}"
            )?;
        }

        for (
            sled_id,
            PlanningNoopImageSourceConvertedZones { num_eligible, num_dataset },
        ) in converted_zones.iter()
        {
            if *num_eligible > 0 && *num_dataset > 0 {
                writeln!(
                    f,
                    "* Noop converting {num_eligible}/{num_dataset} install-dataset zones \
                       to artifact store on sled {sled_id}",
                )?;
            }
        }

        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum PlanningNoopImageSourceSkipSledReason {
    AllZonesAlreadyArtifact { num_total: usize },
    SledNotInInventory,
    ErrorRetrievingZoneManifest { error: String },
    RemoveMupdateOverride { id: MupdateOverrideUuid },
}

impl fmt::Display for PlanningNoopImageSourceSkipSledReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AllZonesAlreadyArtifact { num_total } => {
                write!(f, "all {num_total} zones are already from artifacts")
            }
            Self::SledNotInInventory => {
                write!(f, "sled not present in latest inventory collection")
            }
            Self::ErrorRetrievingZoneManifest { error } => {
                write!(
                    f,
                    "sled-agent encountered error retrieving zone manifest \
                     (this is abnormal): {error}"
                )
            }
            Self::RemoveMupdateOverride { id } => {
                write!(
                    f,
                    "blueprint has get_remove_mupdate_override set for sled: {id}",
                )
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum PlanningNoopImageSourceSkipZoneReason {
    ZoneNotInManifest {
        zone_kind: String,
        file_name: String,
    },
    InvalidArtifact {
        zone_kind: String,
        file_name: String,
        error: String,
    },
    ArtifactNotInRepo {
        artifact_hash: ArtifactHash,
        zone_kind: String,
        file_name: String,
    },
}

impl fmt::Display for PlanningNoopImageSourceSkipZoneReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ZoneNotInManifest { file_name, .. } => {
                write!(f, "artifact not found in zone manifest: {file_name}")
            }
            Self::InvalidArtifact { error, .. } => {
                write!(
                    f,
                    "zone manifest inventory indicated install dataset artifact \
                     is invalid, not using artifact (this is abnormal): {error}"
                )
            }
            Self::ArtifactNotInRepo { .. } => {
                write!(f, "install dataset artifact hash not found in TUF repo")
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningMgsUpdatesStepReport {
    pub pending_mgs_updates: PendingMgsUpdates,
}

impl PlanningMgsUpdatesStepReport {
    pub fn new(pending_mgs_updates: PendingMgsUpdates) -> Self {
        Self { pending_mgs_updates }
    }

    pub fn is_empty(&self) -> bool {
        self.pending_mgs_updates.is_empty()
    }
}

impl fmt::Display for PlanningMgsUpdatesStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { pending_mgs_updates } = self;
        if !pending_mgs_updates.is_empty() {
            let n = pending_mgs_updates.len();
            let s = plural(n);
            writeln!(f, "* {n} pending MGS update{s}:")?;
            for update in pending_mgs_updates.iter() {
                writeln!(
                    f,
                    "  * {}: {:?}",
                    update.baseboard_id, update.details
                )?;
            }
        }
        Ok(())
    }
}

/// How many discretionary zones we actually placed out of how many we
/// wanted to place.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningAddOutOfEligibleSleds {
    pub placed: usize,
    pub wanted_to_place: usize,
}

/// We have at least the minimum required number of zones of a given kind.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningAddSufficientZonesExist {
    pub target_count: usize,
    pub num_existing: usize,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningAddStepReport {
    pub sleds_without_ntp_zones_in_inventory: BTreeSet<SledUuid>,
    pub sleds_without_zpools_for_ntp_zones: BTreeSet<SledUuid>,
    pub sleds_waiting_for_ntp_zone: BTreeSet<SledUuid>,
    pub sleds_getting_ntp_and_discretionary_zones: BTreeSet<SledUuid>,
    pub sleds_missing_ntp_zone: BTreeSet<SledUuid>,
    pub sleds_missing_crucible_zone: BTreeMap<SledUuid, Vec<ZpoolUuid>>,

    /// Discretionary zone kind → (placed, wanted to place)
    pub out_of_eligible_sleds: BTreeMap<String, PlanningAddOutOfEligibleSleds>,

    /// Discretionary zone kind → (wanted to place, num existing)
    pub sufficient_zones_exist:
        BTreeMap<String, PlanningAddSufficientZonesExist>,

    /// Sled ID → kinds of discretionary zones placed there
    // TODO: make `sled_add_zone_*` methods return the added zone config
    // so that we can report it here.
    pub discretionary_zones_placed: BTreeMap<SledUuid, Vec<String>>,
}

impl PlanningAddStepReport {
    pub fn new() -> Self {
        Self {
            sleds_without_ntp_zones_in_inventory: BTreeSet::new(),
            sleds_without_zpools_for_ntp_zones: BTreeSet::new(),
            sleds_waiting_for_ntp_zone: BTreeSet::new(),
            sleds_getting_ntp_and_discretionary_zones: BTreeSet::new(),
            sleds_missing_ntp_zone: BTreeSet::new(),
            sleds_missing_crucible_zone: BTreeMap::new(),
            out_of_eligible_sleds: BTreeMap::new(),
            sufficient_zones_exist: BTreeMap::new(),
            discretionary_zones_placed: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sleds_without_ntp_zones_in_inventory.is_empty()
            && self.sleds_without_zpools_for_ntp_zones.is_empty()
            && self.sleds_waiting_for_ntp_zone.is_empty()
            && self.sleds_getting_ntp_and_discretionary_zones.is_empty()
            && self.sleds_missing_ntp_zone.is_empty()
            && self.sleds_missing_crucible_zone.is_empty()
            && self.out_of_eligible_sleds.is_empty()
            && self.discretionary_zones_placed.is_empty()
    }

    pub fn any_discretionary_zones_placed(&self) -> bool {
        !self.discretionary_zones_placed.is_empty()
    }

    pub fn missing_crucible_zone(
        &mut self,
        sled_id: SledUuid,
        zpool_id: ZpoolUuid,
    ) {
        self.sleds_missing_crucible_zone
            .entry(sled_id)
            .and_modify(|pools| pools.push(zpool_id))
            .or_insert_with(|| vec![zpool_id]);
    }

    pub fn out_of_eligible_sleds(
        &mut self,
        zone_kind: &str,
        placed: usize,
        wanted_to_place: usize,
    ) {
        self.out_of_eligible_sleds.insert(
            zone_kind.to_owned(),
            PlanningAddOutOfEligibleSleds { placed, wanted_to_place },
        );
    }

    pub fn sufficient_zones_exist(
        &mut self,
        zone_kind: &str,
        target_count: usize,
        num_existing: usize,
    ) {
        self.sufficient_zones_exist.insert(
            zone_kind.to_owned(),
            PlanningAddSufficientZonesExist { target_count, num_existing },
        );
    }

    pub fn discretionary_zone_placed(
        &mut self,
        sled_id: SledUuid,
        zone_kind: &str,
    ) {
        self.discretionary_zones_placed
            .entry(sled_id)
            .and_modify(|kinds| kinds.push(zone_kind.to_owned()))
            .or_insert_with(|| vec![zone_kind.to_owned()]);
    }
}

impl fmt::Display for PlanningAddStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            sleds_without_ntp_zones_in_inventory,
            sleds_without_zpools_for_ntp_zones,
            sleds_waiting_for_ntp_zone,
            sleds_getting_ntp_and_discretionary_zones,
            sleds_missing_ntp_zone,
            sleds_missing_crucible_zone,
            out_of_eligible_sleds,
            sufficient_zones_exist: _,
            discretionary_zones_placed,
        } = self;

        if !sleds_without_ntp_zones_in_inventory.is_empty() {
            writeln!(
                f,
                "* Waiting for NTP zones to appear in inventory on sleds: {}",
                sleds_without_ntp_zones_in_inventory
                    .iter()
                    .map(|sled_id| format!("{sled_id}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }

        if !sleds_without_zpools_for_ntp_zones.is_empty() {
            writeln!(
                f,
                "* No zpools in service for NTP zones on sleds: {}",
                sleds_without_zpools_for_ntp_zones
                    .iter()
                    .map(|sled_id| format!("{sled_id}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }

        if !sleds_waiting_for_ntp_zone.is_empty() {
            writeln!(
                f,
                "* Discretionary zone placement waiting for NTP zones on sleds: {}",
                sleds_waiting_for_ntp_zone
                    .iter()
                    .map(|sled_id| format!("{sled_id}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }

        if !sleds_getting_ntp_and_discretionary_zones.is_empty() {
            writeln!(
                f,
                "* Sleds getting NTP zones and which have other services already, \
                   making them eligible for discretionary zones: {}",
                sleds_getting_ntp_and_discretionary_zones
                    .iter()
                    .map(|sled_id| format!("{sled_id}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }

        for sled_id in sleds_missing_ntp_zone {
            writeln!(f, "* Missing NTP zone on sled {sled_id}",)?;
        }

        for (sled_id, zpools) in sleds_missing_crucible_zone {
            for zpool_id in zpools {
                writeln!(
                    f,
                    "* Missing Crucible zone for sled {sled_id}, zpool {zpool_id}",
                )?;
            }
        }

        for (kind, PlanningAddOutOfEligibleSleds { placed, wanted_to_place }) in
            out_of_eligible_sleds.iter()
        {
            writeln!(
                f,
                "* Only placed {placed}/{wanted_to_place} desired {kind} zones"
            )?;
        }

        if !discretionary_zones_placed.is_empty() {
            writeln!(f, "* Discretionary zones placed:")?;
            for (sled_id, kinds) in discretionary_zones_placed.iter() {
                let (n, s) = plural_vec(kinds);
                writeln!(
                    f,
                    "  * {n} zone{s} on sled {sled_id}: {}",
                    kinds.join(", ")
                )?;
            }
        }

        Ok(())
    }
}

/// We have at least the minimum required number of zones of a given kind.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningOutOfDateZone {
    pub zone_config: BlueprintZoneConfig,
    pub desired_image_source: BlueprintZoneImageSource,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningZoneUpdatesStepReport {
    /// What are we waiting on to start zone updates?
    pub waiting_on: Option<ZoneUpdatesWaitingOn>,

    pub out_of_date_zones: BTreeMap<SledUuid, Vec<PlanningOutOfDateZone>>,
    pub expunged_zones: BTreeMap<SledUuid, Vec<BlueprintZoneConfig>>,
    pub updated_zones: BTreeMap<SledUuid, Vec<BlueprintZoneConfig>>,
    pub unsafe_zones: BTreeMap<BlueprintZoneConfig, ZoneUnsafeToShutdown>,
}

impl PlanningZoneUpdatesStepReport {
    pub fn new() -> Self {
        Self {
            waiting_on: None,
            out_of_date_zones: BTreeMap::new(),
            expunged_zones: BTreeMap::new(),
            updated_zones: BTreeMap::new(),
            unsafe_zones: BTreeMap::new(),
        }
    }

    pub fn waiting_on(waiting_on: ZoneUpdatesWaitingOn) -> Self {
        let mut new = Self::new();
        new.waiting_on = Some(waiting_on);
        new
    }

    pub fn is_empty(&self) -> bool {
        self.waiting_on.is_none()
            && self.out_of_date_zones.is_empty()
            && self.expunged_zones.is_empty()
            && self.updated_zones.is_empty()
            && self.unsafe_zones.is_empty()
    }

    pub fn out_of_date_zone(
        &mut self,
        sled_id: SledUuid,
        zone_config: &BlueprintZoneConfig,
        desired_image_source: BlueprintZoneImageSource,
    ) {
        let out_of_date = PlanningOutOfDateZone {
            zone_config: zone_config.to_owned(),
            desired_image_source,
        };
        self.out_of_date_zones
            .entry(sled_id)
            .and_modify(|zones| zones.push(out_of_date.clone()))
            .or_insert_with(|| vec![out_of_date]);
    }

    pub fn expunged_zone(
        &mut self,
        sled_id: SledUuid,
        zone_config: &BlueprintZoneConfig,
    ) {
        self.expunged_zones
            .entry(sled_id)
            .and_modify(|zones| zones.push(zone_config.to_owned()))
            .or_insert_with(|| vec![zone_config.to_owned()]);
    }

    pub fn updated_zone(
        &mut self,
        sled_id: SledUuid,
        zone_config: &BlueprintZoneConfig,
    ) {
        self.updated_zones
            .entry(sled_id)
            .and_modify(|zones| zones.push(zone_config.to_owned()))
            .or_insert_with(|| vec![zone_config.to_owned()]);
    }

    pub fn unsafe_zone(
        &mut self,
        zone: &BlueprintZoneConfig,
        reason: ZoneUnsafeToShutdown,
    ) {
        self.unsafe_zones.insert(zone.clone(), reason);
    }
}

impl fmt::Display for PlanningZoneUpdatesStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            waiting_on,
            out_of_date_zones,
            expunged_zones,
            updated_zones,
            unsafe_zones,
        } = self;

        if let Some(waiting_on) = waiting_on {
            writeln!(f, "* Zone updates waiting on {}", waiting_on.as_str())?;
        }

        if !expunged_zones.is_empty() {
            let (n, s) = plural_map_of_vec(expunged_zones);
            writeln!(f, "* {n} out-of-date zone{s} expunged:")?;
            for (sled_id, zones) in expunged_zones.iter() {
                for zone in zones {
                    writeln!(
                        f,
                        "  * sled {}, zone {} ({})",
                        sled_id,
                        zone.id,
                        zone.zone_type.kind().report_str(),
                    )?;
                }
            }
        }

        if !updated_zones.is_empty() {
            let (n, s) = plural_map_of_vec(updated_zones);
            writeln!(f, "* {n} out-of-date zone{s} updated in-place:")?;
            for (sled_id, zones) in updated_zones.iter() {
                for zone in zones {
                    writeln!(
                        f,
                        "  * sled {}, zone {} ({})",
                        sled_id,
                        zone.id,
                        zone.zone_type.kind().report_str(),
                    )?;
                }
            }
        }

        if !out_of_date_zones.is_empty() {
            let (n, s) = plural_map_of_vec(out_of_date_zones);
            writeln!(f, "* {n} remaining out-of-date zone{s}")?;
        }

        if !unsafe_zones.is_empty() {
            let (n, s) = plural_map(unsafe_zones);
            writeln!(f, "* {n} zone{s} not ready to shut down safely:")?;
            for (zone, reason) in unsafe_zones.iter() {
                writeln!(
                    f,
                    "  * zone {} ({}): {}",
                    zone.id,
                    zone.zone_type.kind().report_str(),
                    reason,
                )?;
            }
        }

        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ZoneUpdatesWaitingOn {
    /// Waiting on discretionary zone placement.
    DiscretionaryZones,

    /// Waiting on updates to RoT / SP / Host OS / etc.
    PendingMgsUpdates,
}

impl ZoneUpdatesWaitingOn {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DiscretionaryZones => "discretionary zones",
            Self::PendingMgsUpdates => {
                "pending MGS updates (RoT / SP / Host OS / etc.)"
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ZoneUnsafeToShutdown {
    Cockroachdb { reason: CockroachdbUnsafeToShutdown },
    BoundaryNtp { total_boundary_ntp_zones: usize, synchronized_count: usize },
}

impl fmt::Display for ZoneUnsafeToShutdown {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Cockroachdb { reason } => write!(f, "{reason}"),
            Self::BoundaryNtp {
                total_boundary_ntp_zones: t,
                synchronized_count: s,
            } => write!(f, "only {s}/{t} boundary NTP zones are synchronized"),
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum CockroachdbUnsafeToShutdown {
    MissingLiveNodesStat,
    MissingUnderreplicatedStat,
    NotEnoughLiveNodes { live_nodes: u64 },
    NotEnoughNodes,
    UnderreplicatedRanges { n: u64 },
}

impl fmt::Display for CockroachdbUnsafeToShutdown {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::MissingLiveNodesStat => write!(f, "missing live_nodes stat"),
            Self::MissingUnderreplicatedStat => {
                write!(f, "missing ranges_underreplicated stat")
            }
            Self::NotEnoughLiveNodes { live_nodes } => {
                write!(
                    f,
                    "not enough live nodes: {live_nodes} < {COCKROACHDB_REDUNDANCY}"
                )
            }
            Self::NotEnoughNodes => write!(f, "not enough nodes"),
            Self::UnderreplicatedRanges { n } => {
                if *n > 0 {
                    write!(f, "{n} > 0 underreplicated ranges")
                } else {
                    write!(
                        f,
                        "no underreplicated ranges (this shouldn't happen)"
                    )
                }
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
pub struct PlanningCockroachdbSettingsStepReport {
    pub preserve_downgrade: CockroachDbPreserveDowngrade,
}

impl PlanningCockroachdbSettingsStepReport {
    pub fn new() -> Self {
        Self { preserve_downgrade: CockroachDbPreserveDowngrade::DoNotModify }
    }

    pub fn is_empty(&self) -> bool {
        self.preserve_downgrade == CockroachDbPreserveDowngrade::DoNotModify
    }
}

impl fmt::Display for PlanningCockroachdbSettingsStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.is_empty() {
            let PlanningCockroachdbSettingsStepReport { preserve_downgrade } =
                self;
            writeln!(
                f,
                "* Will ensure cockroachdb setting: {preserve_downgrade}"
            )?;
        }
        Ok(())
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

fn plural_vec<V>(vec: &Vec<V>) -> (usize, &'static str) {
    let n = vec.len();
    (n, plural(n))
}

fn plural_map<K, V>(map: &BTreeMap<K, V>) -> (usize, &'static str) {
    let n = map.len();
    (n, plural(n))
}

fn plural_map_of_vec<K, V>(map: &BTreeMap<K, Vec<V>>) -> (usize, &'static str) {
    let n = map.values().map(|v| v.len()).sum();
    (n, plural(n))
}
