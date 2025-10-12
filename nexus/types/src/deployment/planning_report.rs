// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing a report on a planning run that produced a blueprint.

use super::ArtifactHash;
use super::BlueprintZoneConfig;
use super::BlueprintZoneImageSource;
use super::CockroachDbPreserveDowngrade;
use super::PendingMgsUpdates;
use super::PlannerConfig;
use crate::inventory::BaseboardId;
use crate::inventory::CabooseWhich;

use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::id_upcast;
use indent_write::fmt::IndentWriter;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::external::Generation;
use omicron_common::disk::M2Slot;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
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
use std::fmt::Write;
use std::sync::Arc;
use thiserror::Error;

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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
#[must_use = "an unread report is not actionable"]
pub struct PlanningReport {
    /// The configuration in effect for this planning run.
    pub planner_config: PlannerConfig,

    // Step reports.
    pub expunge: PlanningExpungeStepReport,
    pub decommission: PlanningDecommissionStepReport,
    pub noop_image_source: PlanningNoopImageSourceStepReport,
    pub mgs_updates: PlanningMgsUpdatesStepReport,
    pub add: PlanningAddStepReport,
    pub zone_updates: PlanningZoneUpdatesStepReport,
    pub nexus_generation_bump: PlanningNexusGenerationBumpReport,
    pub cockroachdb_settings: PlanningCockroachdbSettingsStepReport,
}

impl PlanningReport {
    pub fn new() -> Self {
        Self {
            planner_config: PlannerConfig::default(),
            expunge: PlanningExpungeStepReport::new(),
            decommission: PlanningDecommissionStepReport::new(),
            noop_image_source: PlanningNoopImageSourceStepReport::new(),
            mgs_updates: PlanningMgsUpdatesStepReport::empty(),
            add: PlanningAddStepReport::new(),
            zone_updates: PlanningZoneUpdatesStepReport::new(),
            nexus_generation_bump: PlanningNexusGenerationBumpReport::new(),
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
            && self.nexus_generation_bump.is_empty()
            && self.cockroachdb_settings.is_empty()
    }
}

impl fmt::Display for PlanningReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_empty() {
            writeln!(f, "empty planning report")?;
        } else {
            let Self {
                planner_config,
                expunge,
                decommission,
                noop_image_source,
                mgs_updates,
                add,
                zone_updates,
                nexus_generation_bump,
                cockroachdb_settings,
            } = self;
            writeln!(f, "planning report:")?;
            if *planner_config != PlannerConfig::default() {
                writeln!(f, "planner config:\n{}", planner_config.display())?;
            }
            expunge.fmt(f)?;
            decommission.fmt(f)?;
            noop_image_source.fmt(f)?;
            mgs_updates.fmt(f)?;
            add.fmt(f)?;
            zone_updates.fmt(f)?;
            nexus_generation_bump.fmt(f)?;
            cockroachdb_settings.fmt(f)?;
        }
        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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

/// How many of the total install-dataset zones and/or host phase 2 slots were
/// noop-converted to use the artifact store on a particular sled.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningNoopImageSourceConverted {
    pub num_eligible: usize,
    pub num_dataset: usize,
    pub host_phase_2_slot_a_eligible: bool,
    pub host_phase_2_slot_b_eligible: bool,
    pub measurement_eligible: bool,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningNoopImageSourceStepReport {
    pub no_target_release: bool,
    // Make these maps small to avoid bloating the size of generated test data.
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub skipped_sled_zones:
        BTreeMap<SledUuid, PlanningNoopImageSourceSkipSledZonesReason>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub skipped_sled_host_phase_2:
        BTreeMap<SledUuid, PlanningNoopImageSourceSkipSledHostPhase2Reason>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub skipped_zones:
        BTreeMap<OmicronZoneUuid, PlanningNoopImageSourceSkipZoneReason>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub skipped_sled_measurements:
        BTreeMap<SledUuid, PlanningNoopImageSourceSkipSledMeasurementsReason>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub converted: BTreeMap<SledUuid, PlanningNoopImageSourceConverted>,
}

impl PlanningNoopImageSourceStepReport {
    pub fn new() -> Self {
        Self {
            no_target_release: false,
            skipped_sled_zones: BTreeMap::new(),
            skipped_sled_host_phase_2: BTreeMap::new(),
            skipped_zones: BTreeMap::new(),
            skipped_sled_measurements: BTreeMap::new(),
            converted: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.no_target_release
            && self.skipped_sled_zones.is_empty()
            && self.skipped_sled_host_phase_2.is_empty()
            && self.skipped_zones.is_empty()
            && self.skipped_sled_measurements.is_empty()
            && self.converted.is_empty()
    }

    pub fn skip_sled_zones(
        &mut self,
        sled_id: SledUuid,
        reason: PlanningNoopImageSourceSkipSledZonesReason,
    ) {
        self.skipped_sled_zones.insert(sled_id, reason);
    }

    pub fn skip_sled_host_phase_2(
        &mut self,
        sled_id: SledUuid,
        reason: PlanningNoopImageSourceSkipSledHostPhase2Reason,
    ) {
        self.skipped_sled_host_phase_2.insert(sled_id, reason);
    }

    pub fn skip_sled_measurements(
        &mut self,
        sled_id: SledUuid,
        reason: PlanningNoopImageSourceSkipSledMeasurementsReason,
    ) {
        self.skipped_sled_measurements.insert(sled_id, reason);
    }

    pub fn skip_zone(
        &mut self,
        zone_id: OmicronZoneUuid,
        reason: PlanningNoopImageSourceSkipZoneReason,
    ) {
        self.skipped_zones.insert(zone_id, reason);
    }

    pub fn converted(
        &mut self,
        sled_id: SledUuid,
        num_eligible: usize,
        num_dataset: usize,
        host_phase_2_slot_a_eligible: bool,
        host_phase_2_slot_b_eligible: bool,
        measurement_eligible: bool,
    ) {
        self.converted.insert(
            sled_id,
            PlanningNoopImageSourceConverted {
                num_eligible,
                num_dataset,
                host_phase_2_slot_a_eligible,
                host_phase_2_slot_b_eligible,
                measurement_eligible,
            },
        );
    }
}

impl fmt::Display for PlanningNoopImageSourceStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            no_target_release,
            skipped_sled_zones,
            skipped_sled_host_phase_2,
            skipped_zones: _,
            converted,
            skipped_sled_measurements,
        } = self;

        if *no_target_release {
            return writeln!(
                f,
                "* skipping noop image source check for all sleds (no current TUF repo)",
            );
        }

        for (sled_id, reason) in skipped_sled_zones.iter() {
            writeln!(
                f,
                "* skipping noop zone image source check on sled {sled_id}: {reason}"
            )?;
        }
        for (sled_id, reason) in skipped_sled_host_phase_2.iter() {
            writeln!(
                f,
                "* skipping noop host phase 2 desired contents check on sled {sled_id}: {reason}"
            )?;
        }
        for (sled_id, reason) in skipped_sled_measurements.iter() {
            writeln!(
                f,
                "* skipping noop measurement desired contents check on sled {sled_id}: {reason}"
            )?;
        }
        for (
            sled_id,
            PlanningNoopImageSourceConverted {
                num_eligible,
                num_dataset,
                host_phase_2_slot_a_eligible,
                host_phase_2_slot_b_eligible,
                measurement_eligible,
            },
        ) in converted.iter()
        {
            if *num_eligible > 0 && *num_dataset > 0 {
                writeln!(
                    f,
                    "* noop converting {num_eligible}/{num_dataset} install-dataset zones \
                       to artifact store on sled {sled_id}",
                )?;
            }
            if *host_phase_2_slot_a_eligible {
                writeln!(
                    f,
                    "* noop converting host phase 2 slot A to Artifact on sled {sled_id}"
                )?;
            }
            if *host_phase_2_slot_b_eligible {
                writeln!(
                    f,
                    "* noop converting host phase 2 slot B to Artifact on sled {sled_id}"
                )?;
            }
            if *measurement_eligible {
                writeln!(
                    f,
                    "* noop converting current measurements to Artifact on sled {sled_id}"
                )?;
            }
        }

        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum PlanningNoopImageSourceSkipSledZonesReason {
    AllZonesAlreadyArtifact { num_total: usize },
    SledNotInInventory,
    ErrorRetrievingZoneManifest { error: String },
    RemoveMupdateOverride { id: MupdateOverrideUuid },
}

impl fmt::Display for PlanningNoopImageSourceSkipSledZonesReason {
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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum PlanningNoopImageSourceSkipSledHostPhase2Reason {
    BothSlotsAlreadyArtifact,
    SledNotInInventory,
}

impl fmt::Display for PlanningNoopImageSourceSkipSledHostPhase2Reason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BothSlotsAlreadyArtifact => {
                write!(f, "both host phase 2 slots are already from artifacts")
            }
            Self::SledNotInInventory => {
                write!(f, "sled not present in latest inventory collection")
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum PlanningNoopImageSourceSkipSledMeasurementsReason {
    BothSlotsAlreadyArtifact,
    SledNotInInventory,
}

impl fmt::Display for PlanningNoopImageSourceSkipSledMeasurementsReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BothSlotsAlreadyArtifact => {
                write!(f, "both measurement sets are already from artifacts")
            }
            Self::SledNotInInventory => {
                write!(f, "sled not present in latest inventory collection")
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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
pub struct PlanningMupdateOverrideStepReport {
    pub override_ids: BTreeMap<SledUuid, MupdateOverrideUuid>,
}

impl PlanningMupdateOverrideStepReport {
    pub fn new() -> Self {
        Self { override_ids: BTreeMap::new() }
    }
}

/// Describes the reason why an SP component failed to update
#[derive(
    Error,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Diffable,
    PartialOrd,
    JsonSchema,
    Ord,
    Clone,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum FailedMgsUpdateReason {
    /// There was a failed attempt to plan a Host OS update
    #[error("failed to plan a Host OS update")]
    HostOs(#[from] FailedHostOsUpdateReason),
    /// There was a failed attempt to plan an RoT update
    #[error("failed to plan an RoT update")]
    Rot(#[from] FailedRotUpdateReason),
    /// There was a failed attempt to plan an RoT bootloader update
    #[error("failed to plan an RoT bootloader update")]
    RotBootloader(#[from] FailedRotBootloaderUpdateReason),
    /// There was a failed attempt to plan an SP update
    #[error("failed to plan an SP update")]
    Sp(#[from] FailedSpUpdateReason),
}

/// Describes the reason why an RoT bootloader failed to update
#[derive(
    Error,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Diffable,
    PartialOrd,
    JsonSchema,
    Ord,
    Clone,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum FailedRotBootloaderUpdateReason {
    /// The component's caboose was missing a value for "sign"
    #[error("caboose for {0:?} is missing sign")]
    CabooseMissingSign(CabooseWhich),
    /// The component's caboose was not found in the inventory
    #[error("caboose for {0:?} is not in inventory")]
    CabooseNotInInventory(CabooseWhich),
    /// The version in the caboose or artifact was not able to be parsed
    #[error("version from caboose {caboose:?} could not be parsed: {err}")]
    FailedVersionParse { caboose: CabooseWhich, err: String },
    /// No artifact with the required conditions for the component was found
    #[error("no matching artifact was found")]
    NoMatchingArtifactFound,
    /// The component's corresponding SP was not found in the inventory
    #[error("corresponding SP is not in inventory")]
    SpNotInInventory,
}

/// Describes the reason why an RoT failed to update
#[derive(
    Error,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Diffable,
    PartialOrd,
    JsonSchema,
    Ord,
    Clone,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum FailedRotUpdateReason {
    /// The component's caboose was missing a value for "sign"
    #[error("caboose for {0:?} is missing sign")]
    CabooseMissingSign(CabooseWhich),
    /// The component's caboose was not found in the inventory
    #[error("caboose for {0:?} is not in inventory")]
    CabooseNotInInventory(CabooseWhich),
    /// The version in the caboose or artifact was not able to be parsed
    #[error("version from caboose {caboose:?} could not be parsed: {err}")]
    FailedVersionParse { caboose: CabooseWhich, err: String },
    /// No artifact with the required conditions for the component was found
    #[error("no matching artifact was found")]
    NoMatchingArtifactFound,
    /// RoT state was not found in inventory
    #[error("rot state is not in inventory")]
    RotStateNotInInventory,
    /// The component's corresponding SP was not found in the inventory
    #[error("corresponding SP is not in inventory")]
    SpNotInInventory,
}

/// Describes the reason why an SP failed to update
#[derive(
    Error,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Diffable,
    PartialOrd,
    JsonSchema,
    Ord,
    Clone,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum FailedSpUpdateReason {
    /// The component's caboose was not found in the inventory
    #[error("caboose for {0:?} is not in inventory")]
    CabooseNotInInventory(CabooseWhich),
    /// The version in the caboose or artifact was not able to be parsed
    #[error("version from caboose {caboose:?} could not be parsed: {err}")]
    FailedVersionParse { caboose: CabooseWhich, err: String },
    /// No artifact with the required conditions for the component was found
    #[error("no matching artifact was found")]
    NoMatchingArtifactFound,
    /// The component's corresponding SP was not found in the inventory
    #[error("corresponding SP is not in inventory")]
    SpNotInInventory,
}

/// Describes the reason why a Host OS failed to update
#[derive(
    Error,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Diffable,
    PartialOrd,
    JsonSchema,
    Ord,
    Clone,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum FailedHostOsUpdateReason {
    /// The active host phase 1 slot does not match the boot disk
    #[error("active phase 1 slot {0:?} does not match boot disk")]
    ActiveHostPhase1SlotBootDiskMismatch(M2Slot),
    /// The active host phase 1 hash was not found in inventory
    #[error("active host phase 1 hash for slot {0:?} is not in inventory")]
    ActiveHostPhase1HashNotInInventory(M2Slot),
    /// The active host phase 1 slot was not found in inventory
    #[error("active host phase 1 slot is not in inventory")]
    ActiveHostPhase1SlotNotInInventory,
    /// The component's caboose was not found in the inventory
    #[error("caboose for {0:?} is not in inventory")]
    CabooseNotInInventory(CabooseWhich),
    /// The version in the caboose or artifact was not able to be parsed
    #[error("version from caboose {caboose:?} could not be parsed: {err}")]
    FailedVersionParse { caboose: CabooseWhich, err: String },
    /// The inactive host phase 1 hash was not found in inventory
    #[error("inactive host phase 1 hash for slot {0:?} is not in inventory")]
    InactiveHostPhase1HashNotInInventory(M2Slot),
    /// Last reconciliation details were not found in inventory
    #[error("sled agent last reconciliation is not in inventory")]
    LastReconciliationNotInInventory,
    /// No artifacts with the required conditions for the component were found
    #[error("no matching artifacts for phase 1 or 2 were found")]
    NoMatchingArtifactsFound,
    /// No artifact with the required conditions for phase 1 was found
    #[error("no matching artifact for phase 1 was found")]
    NoMatchingPhase1ArtifactFound,
    /// No artifact with the required conditions for phase 2 was found
    #[error("no matching artifact for phase 2 was found")]
    NoMatchingPhase2ArtifactFound,
    /// Sled agent info was not found in inventory
    #[error("sled agent info is not in inventory")]
    SledAgentInfoNotInInventory,
    /// The component's corresponding SP was not found in the inventory
    #[error("corresponding SP is not in inventory")]
    SpNotInInventory,
    /// Too many artifacts with the required conditions for the component were
    /// found
    #[error("too many matching artifacts were found")]
    TooManyMatchingArtifacts,
    /// The sled agent reported an error determining the boot disk
    #[error("sled agent was unable to determine the boot disk: {0:?}")]
    UnableToDetermineBootDisk(String),
    /// The sled agent reported an error retrieving boot disk phase 2 image
    /// details
    #[error("sled agent was unable to retrieve boot disk phase 2 image: {0:?}")]
    UnableToRetrieveBootDiskPhase2Image(String),
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct BlockedMgsUpdate {
    /// id of the baseboard that we attempted to update
    pub baseboard_id: Arc<BaseboardId>,
    /// reason why the update failed
    pub reason: FailedMgsUpdateReason,
}

impl IdOrdItem for BlockedMgsUpdate {
    type Key<'a> = &'a BaseboardId;
    fn key(&self) -> Self::Key<'_> {
        &*self.baseboard_id
    }
    id_upcast!();
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningMgsUpdatesStepReport {
    pub pending_mgs_updates: PendingMgsUpdates,
    pub blocked_mgs_updates: Vec<BlockedMgsUpdate>,
}

impl PlanningMgsUpdatesStepReport {
    pub fn new(
        pending_mgs_updates: PendingMgsUpdates,
        blocked_mgs_updates: Vec<BlockedMgsUpdate>,
    ) -> Self {
        Self { pending_mgs_updates, blocked_mgs_updates }
    }

    pub fn empty() -> Self {
        Self {
            pending_mgs_updates: PendingMgsUpdates::new(),
            blocked_mgs_updates: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending_mgs_updates.is_empty()
            && self.blocked_mgs_updates.is_empty()
    }
}

impl fmt::Display for PlanningMgsUpdatesStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { pending_mgs_updates, blocked_mgs_updates } = self;
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
        if !blocked_mgs_updates.is_empty() {
            let n = blocked_mgs_updates.len();
            let s = plural(n);
            writeln!(f, "* {n} blocked MGS update{s}:")?;
            for update in blocked_mgs_updates {
                writeln!(f, "  * {}: {}", update.baseboard_id, update.reason)?;
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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningAddOutOfEligibleSleds {
    pub placed: usize,
    pub wanted_to_place: usize,
}

/// We have at least the minimum required number of zones of a given kind.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningAddSufficientZonesExist {
    pub target_count: usize,
    pub num_existing: usize,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct DiscretionaryZonePlacement {
    kind: String,
    source: String,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum ZoneAddWaitingOn {
    /// Waiting on one or more blockers (typically MUPdate-related reasons) to
    /// clear.
    Blockers,
}

impl ZoneAddWaitingOn {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Blockers => "blockers",
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningAddStepReport {
    /// What are we waiting on to start zone additions?
    pub waiting_on: Option<ZoneAddWaitingOn>,

    /// Reasons why zone adds and any updates are blocked.
    ///
    /// This is typically a list of MUPdate-related reasons.
    #[cfg_attr(test, any(((0, 16).into(), Default::default())))]
    pub add_update_blocked_reasons: Vec<String>,

    /// The value of the homonymous planner config. (What this really means is
    /// that zone adds happen despite being blocked by one or more
    /// MUPdate-related reasons.)
    pub add_zones_with_mupdate_override: bool,

    /// Set to true if the target release generation is 1, which would allow
    /// zones to be added.
    pub target_release_generation_is_one: bool,

    // Make these sets and maps small to avoid bloating the size of generated
    // test data.
    #[cfg_attr(test, any(((0, 16).into(), Default::default())))]
    pub sleds_without_ntp_zones_in_inventory: BTreeSet<SledUuid>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default())))]
    pub sleds_without_zpools_for_ntp_zones: BTreeSet<SledUuid>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default())))]
    pub sleds_waiting_for_ntp_zone: BTreeSet<SledUuid>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default())))]
    pub sleds_getting_ntp_and_discretionary_zones: BTreeSet<SledUuid>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default())))]
    pub sleds_missing_ntp_zone: BTreeSet<SledUuid>,
    #[cfg_attr(
        test,
        any((
            (0, 16).into(),
            Default::default(),
            ((0, 16).into(), Default::default())
        ))
    )]
    pub sleds_missing_crucible_zone: BTreeMap<SledUuid, Vec<ZpoolUuid>>,

    /// Discretionary zone kind → (placed, wanted to place)
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub out_of_eligible_sleds: BTreeMap<String, PlanningAddOutOfEligibleSleds>,

    /// Discretionary zone kind → (wanted to place, num existing)
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub sufficient_zones_exist:
        BTreeMap<String, PlanningAddSufficientZonesExist>,

    /// Sled ID → kinds of discretionary zones placed there
    // TODO: make `sled_add_zone_*` methods return the added zone config
    // so that we can report it here.
    #[cfg_attr(
        test,
        any((
            (0, 16).into(),
            Default::default(),
            ((0, 16).into(), Default::default())
        ))
    )]
    pub discretionary_zones_placed:
        BTreeMap<SledUuid, Vec<DiscretionaryZonePlacement>>,
}

impl PlanningAddStepReport {
    pub fn new() -> Self {
        Self {
            waiting_on: None,
            add_update_blocked_reasons: Vec::new(),
            add_zones_with_mupdate_override: false,
            target_release_generation_is_one: false,
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

    pub fn waiting_on(waiting_on: ZoneAddWaitingOn) -> Self {
        let mut new = Self::new();
        new.waiting_on = Some(waiting_on);
        new
    }

    pub fn is_empty(&self) -> bool {
        self.waiting_on.is_none()
            && self.add_update_blocked_reasons.is_empty()
            && self.sleds_without_ntp_zones_in_inventory.is_empty()
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
        image_source: &BlueprintZoneImageSource,
    ) {
        self.discretionary_zones_placed
            .entry(sled_id)
            .and_modify(|kinds| {
                kinds.push(DiscretionaryZonePlacement {
                    kind: zone_kind.to_owned(),
                    source: image_source.to_string(),
                })
            })
            .or_insert_with(|| {
                vec![DiscretionaryZonePlacement {
                    kind: zone_kind.to_owned(),
                    source: image_source.to_string(),
                }]
            });
    }
}

impl fmt::Display for PlanningAddStepReport {
    fn fmt(&self, mut f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            waiting_on,
            add_update_blocked_reasons,
            add_zones_with_mupdate_override,
            target_release_generation_is_one,
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

        if let Some(waiting_on) = waiting_on {
            writeln!(f, "* zone adds waiting on {}", waiting_on.as_str())?;
        }

        if !add_update_blocked_reasons.is_empty() {
            // If zone adds are blocked on a set of reasons, zone updates are
            // blocked on the same reason. Make that clear by saying "zone adds
            // and updates are blocked" rather than just "zone adds are
            // blocked".
            writeln!(f, "* zone adds and updates are blocked:")?;
            for reason in add_update_blocked_reasons {
                let mut indent_writer =
                    IndentWriter::new_skip_initial("    ", f);
                writeln!(indent_writer, "  - {}", reason)?;
                f = indent_writer.into_inner();
            }
        }

        let mut add_zones_despite_being_blocked_reasons = Vec::new();
        if *add_zones_with_mupdate_override {
            add_zones_despite_being_blocked_reasons.push(
                "planner config `add_zones_with_mupdate_override` is true",
            );
        }
        if *target_release_generation_is_one {
            add_zones_despite_being_blocked_reasons
                .push("target release generation is 1");
        }
        if !add_zones_despite_being_blocked_reasons.is_empty() {
            writeln!(
                f,
                "* adding zones despite being blocked, because: {}",
                add_zones_despite_being_blocked_reasons.join(", "),
            )?;
        }

        if !sleds_without_ntp_zones_in_inventory.is_empty() {
            writeln!(
                f,
                "* waiting for NTP zones to appear in inventory on sleds: {}",
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
                "* no zpools in service for NTP zones on sleds: {}",
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
                "* discretionary zone placement waiting for NTP zones on sleds: {}",
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
                "* sleds getting NTP zones and which have other services already, \
                   making them eligible for discretionary zones: {}",
                sleds_getting_ntp_and_discretionary_zones
                    .iter()
                    .map(|sled_id| format!("{sled_id}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }

        for sled_id in sleds_missing_ntp_zone {
            writeln!(f, "* missing NTP zone on sled {sled_id}",)?;
        }

        for (sled_id, zpools) in sleds_missing_crucible_zone {
            for zpool_id in zpools {
                writeln!(
                    f,
                    "* missing Crucible zone for sled {sled_id}, zpool {zpool_id}",
                )?;
            }
        }

        for (kind, PlanningAddOutOfEligibleSleds { placed, wanted_to_place }) in
            out_of_eligible_sleds.iter()
        {
            writeln!(
                f,
                "* only placed {placed}/{wanted_to_place} desired {kind} zones"
            )?;
        }

        if !discretionary_zones_placed.is_empty() {
            writeln!(f, "* discretionary zones placed:")?;
            for (sled_id, placements) in discretionary_zones_placed.iter() {
                for DiscretionaryZonePlacement { kind, source } in placements {
                    writeln!(
                        f,
                        "  * {kind} zone on sled {sled_id} from source {source}",
                    )?;
                }
            }
        }

        Ok(())
    }
}

/// We have at least the minimum required number of zones of a given kind.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningOutOfDateZone {
    pub zone: PlanningReportBlueprintZone,
    pub desired_image_source: BlueprintZoneImageSource,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningZoneUpdatesStepReport {
    /// What are we waiting on to start zone updates?
    pub waiting_on: Option<ZoneUpdatesWaitingOn>,

    // Make these maps small to avoid bloating the size of generated test data.
    #[cfg_attr(
        test,
        any((
            (0, 16).into(),
            Default::default(),
            ((0, 16).into(), Default::default())
        ))
    )]
    pub out_of_date_zones: BTreeMap<SledUuid, Vec<PlanningOutOfDateZone>>,
    #[cfg_attr(
        test,
        any((
            (0, 16).into(),
            Default::default(),
            ((0, 16).into(), Default::default())
        ))
    )]
    pub expunged_zones: BTreeMap<SledUuid, Vec<PlanningReportBlueprintZone>>,
    #[cfg_attr(
        test,
        any((
            (0, 16).into(),
            Default::default(),
            ((0, 16).into(), Default::default())
        ))
    )]
    pub updated_zones: BTreeMap<SledUuid, Vec<PlanningReportBlueprintZone>>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub unsafe_zones: BTreeMap<OmicronZoneUuid, ZoneUnsafeToShutdown>,
    #[cfg_attr(test, any(((0, 16).into(), Default::default(), Default::default())))]
    pub waiting_zones: BTreeMap<OmicronZoneUuid, ZoneWaitingToExpunge>,
}

impl PlanningZoneUpdatesStepReport {
    pub fn new() -> Self {
        Self {
            waiting_on: None,
            out_of_date_zones: BTreeMap::new(),
            expunged_zones: BTreeMap::new(),
            updated_zones: BTreeMap::new(),
            unsafe_zones: BTreeMap::new(),
            waiting_zones: BTreeMap::new(),
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
            && self.waiting_zones.is_empty()
    }

    pub fn out_of_date_zone(
        &mut self,
        sled_id: SledUuid,
        zone_config: &BlueprintZoneConfig,
        desired_image_source: BlueprintZoneImageSource,
    ) {
        let out_of_date = PlanningOutOfDateZone {
            zone: PlanningReportBlueprintZone::new(zone_config),
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
            .and_modify(|zones| {
                zones.push(PlanningReportBlueprintZone::new(zone_config))
            })
            .or_insert_with(|| {
                vec![PlanningReportBlueprintZone::new(zone_config)]
            });

        // We check for out-of-date zones before expunging zones. If we just
        // expunged this zone, it's no longer out of date.
        if let Some(out_of_date) = self.out_of_date_zones.get_mut(&sled_id) {
            out_of_date.retain(|z| z.zone.id != zone_config.id);
        }
    }

    pub fn updated_zone(
        &mut self,
        sled_id: SledUuid,
        zone_config: &BlueprintZoneConfig,
    ) {
        self.updated_zones
            .entry(sled_id)
            .and_modify(|zones| {
                zones.push(PlanningReportBlueprintZone::new(zone_config))
            })
            .or_insert_with(|| {
                vec![PlanningReportBlueprintZone::new(zone_config)]
            });

        // We check for out-of-date zones before updating zones. If we just
        // updated this zone, it's no longer out of date.
        if let Some(out_of_date) = self.out_of_date_zones.get_mut(&sled_id) {
            out_of_date.retain(|z| z.zone.id != zone_config.id);
        }
    }

    pub fn unsafe_zone(
        &mut self,
        zone: &BlueprintZoneConfig,
        reason: ZoneUnsafeToShutdown,
    ) {
        self.unsafe_zones.insert(zone.id, reason);
    }

    pub fn waiting_zone(
        &mut self,
        zone: &BlueprintZoneConfig,
        reason: ZoneWaitingToExpunge,
    ) {
        self.waiting_zones.insert(zone.id, reason);
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
            waiting_zones,
        } = self;

        if let Some(waiting_on) = waiting_on {
            writeln!(f, "* zone updates waiting on {}", waiting_on.as_str())?;
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
                        zone.kind.report_str(),
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
                        zone.kind.report_str(),
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
            for (zone_id, reason) in unsafe_zones.iter() {
                writeln!(f, "  * zone {zone_id}: {reason}")?;
            }
        }

        if !waiting_zones.is_empty() {
            let (n, s) = plural_map(waiting_zones);
            writeln!(f, "* {n} zone{s} waiting to be expunged:")?;
            for (zone_id, reason) in waiting_zones.iter() {
                writeln!(f, "  * zone {zone_id}: {reason}")?;
            }
        }

        Ok(())
    }
}

/// Reduced form of a `BlueprintZoneConfig` stored in a [`PlanningReport`].
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct PlanningReportBlueprintZone {
    pub id: OmicronZoneUuid,
    pub kind: ZoneKind,
}

impl PlanningReportBlueprintZone {
    pub fn new(zone: &BlueprintZoneConfig) -> Self {
        Self { id: zone.id, kind: zone.zone_type.kind() }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum ZoneUpdatesWaitingOn {
    /// Waiting on blocked updates to RoT bootloader / RoT / SP / Host OS.
    BlockedMgsUpdates,

    /// Waiting on discretionary zone placement.
    DiscretionaryZones,

    /// Waiting on zones to propagate to inventory.
    InventoryPropagation,

    /// Waiting on updates to RoT bootloader / RoT / SP / Host OS.
    PendingMgsUpdates,

    /// Waiting on the same set of blockers zone adds are waiting on.
    ZoneAddBlockers,
}

impl ZoneUpdatesWaitingOn {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BlockedMgsUpdates => {
                "blocked MGS updates (RoT bootloader / RoT / SP / Host OS)"
            }
            Self::DiscretionaryZones => "discretionary zones",
            Self::InventoryPropagation => "zone propagation to inventory",
            Self::PendingMgsUpdates => {
                "pending MGS updates (RoT bootloader / RoT / SP / Host OS)"
            }
            Self::ZoneAddBlockers => "zone add blockers",
        }
    }
}

/// Zones which should not be shut down, because their lack of availability
/// could be problematic for the successful functioning of the deployed system.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum ZoneUnsafeToShutdown {
    Cockroachdb { reason: CockroachdbUnsafeToShutdown },
    BoundaryNtp { total_boundary_ntp_zones: usize, synchronized_count: usize },
    InternalDns { total_internal_dns_zones: usize, synchronized_count: usize },
}

impl fmt::Display for ZoneUnsafeToShutdown {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Cockroachdb { reason } => {
                write!(f, "cockroach unsafe to shut down: {reason}")
            }
            Self::BoundaryNtp {
                total_boundary_ntp_zones: t,
                synchronized_count: s,
            } => write!(f, "only {s}/{t} boundary NTP zones are synchronized"),
            Self::InternalDns {
                total_internal_dns_zones: t,
                synchronized_count: s,
            } => write!(f, "only {s}/{t} internal DNS zones are synchronized"),
        }
    }
}

/// Out-of-date zones which are not yet ready to be expunged.
///
/// For example, out-of-date Nexus zones should not be expunged until
/// handoff has completed.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum ZoneWaitingToExpunge {
    Nexus { zone_generation: Generation },
}

impl fmt::Display for ZoneWaitingToExpunge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Nexus { zone_generation } => {
                write!(
                    f,
                    "nexus image out-of-date, but nexus_generation \
                     {zone_generation} is still active"
                )
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(tag = "component", rename_all = "snake_case", content = "value")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum PlanningNexusGenerationBumpReport {
    /// We have no reason to bump the Nexus generation number.
    NothingToReport,

    /// We are waiting on some condition before we can bump the
    /// Nexus generation.
    WaitingOn(NexusGenerationBumpWaitingOn),

    /// We are bumping the Nexus generation number to this value.
    BumpingGeneration(Generation),
}

impl PlanningNexusGenerationBumpReport {
    pub fn new() -> Self {
        Self::NothingToReport
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::NothingToReport)
    }

    pub fn set_waiting_on(&mut self, why: NexusGenerationBumpWaitingOn) {
        *self = Self::WaitingOn(why);
    }

    pub fn set_next_generation(&mut self, next_generation: Generation) {
        *self = Self::BumpingGeneration(next_generation);
    }
}

impl fmt::Display for PlanningNexusGenerationBumpReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::WaitingOn(why) => {
                writeln!(
                    f,
                    "* waiting to update top-level nexus_generation: {}",
                    why.as_str()
                )?;
            }
            Self::BumpingGeneration(gen) => {
                writeln!(f, "* updating top-level nexus_generation to: {gen}")?;
            }
            // Nothing to report
            Self::NothingToReport => (),
        }
        Ok(())
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub enum NexusGenerationBumpWaitingOn {
    /// Waiting for the planner to finish updating all non-Nexus zones
    FoundOldNonNexusZones,

    /// Waiting for the planner to deploy new-generation Nexus zones
    MissingNewNexusInBlueprint,

    /// Waiting for `db_metadata_nexus` records to be deployed for
    /// new-generation Nexus zones
    MissingNexusDatabaseAccessRecords,

    /// Waiting for newly deployed Nexus zones to appear to inventory
    MissingNewNexusInInventory,
}

impl NexusGenerationBumpWaitingOn {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FoundOldNonNexusZones => {
                "some non-Nexus zone are not yet updated"
            }
            Self::MissingNewNexusInBlueprint => {
                "new Nexus zones have not been planned yet"
            }
            Self::MissingNexusDatabaseAccessRecords => {
                "new Nexus zones do not have database records yet"
            }
            Self::MissingNewNexusInInventory => {
                "new Nexus zones are not in inventory yet"
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Diffable, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type")]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
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
                "* will ensure cockroachdb setting: {preserve_downgrade}"
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

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;
    use test_strategy::proptest;

    // Test that planning reports can be serialized and deserialized.
    #[proptest]
    fn planning_report_json_roundtrip(planning_report: PlanningReport) {
        let json = serde_json::to_string(&planning_report).unwrap();
        let deserialized: PlanningReport = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(
            planning_report,
            deserialized,
            "input and output are equal"
        );
    }
}
