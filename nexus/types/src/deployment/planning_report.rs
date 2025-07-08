// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing a report on a planning run that produced a blueprint.

use super::ArtifactHash;
use super::BlueprintZoneConfig;
use super::BlueprintZoneImageSource;
use super::CockroachDbPreserveDowngrade;
use super::PendingMgsUpdates;

use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[must_use = "an unread report is not actionable"]
pub struct PlanningReport {
    /// The blueprint produced by the planning run this report describes.
    pub blueprint_id: BlueprintUuid,

    // Step reports.
    pub expunge: PlanningExpungeStepReport,
    pub decommission: PlanningDecommissionStepReport,
    pub noop_image_source: PlanningNoopImageSourceStepReport,
    pub mgs_updates: PlanningMgsUpdatesStepReport,
    pub add: PlanningAddStepReport,
    pub zone_updates: PlanningZoneUpdatesStepReport,
    pub cockroachdb_settings: PlanningCockroachdbSettingsStepReport,
}

impl fmt::Display for PlanningReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            blueprint_id,
            expunge,
            decommission,
            noop_image_source,
            mgs_updates,
            add,
            zone_updates,
            cockroachdb_settings,
        } = self;
        writeln!(f, "Report on planning run for blueprint {blueprint_id}:")?;
        expunge.fmt(f)?;
        decommission.fmt(f)?;
        noop_image_source.fmt(f)?;
        mgs_updates.fmt(f)?;
        add.fmt(f)?;
        zone_updates.fmt(f)?;
        cockroachdb_settings.fmt(f)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningExpungeStepReport {
    /// Expunged disks not present in the parent blueprint.
    pub orphan_disks: BTreeMap<SledUuid, PhysicalDiskUuid>,
}

impl PlanningExpungeStepReport {
    pub fn new() -> Self {
        Self { orphan_disks: BTreeMap::new() }
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningDecommissionStepReport {
    /// Decommissioned sleds that unexpectedly appeared as commissioned.
    pub zombie_sleds: Vec<SledUuid>,
}

impl PlanningDecommissionStepReport {
    pub fn new() -> Self {
        Self { zombie_sleds: Vec::new() }
    }
}

impl fmt::Display for PlanningDecommissionStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { zombie_sleds } = self;
        if !zombie_sleds.is_empty() {
            let n = zombie_sleds.len();
            let s = if n == 1 { "" } else { "s" };
            writeln!(
                f,
                "* decommissioned sled{s} returned by `SledFilter::Commissioned`: {}",
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningNoopImageSourceStepReport {
    pub no_target_release: bool,
    pub skipped_sleds:
        BTreeMap<SledUuid, PlanningNoopImageSourceSkipSledReason>,
    pub skipped_zones:
        BTreeMap<OmicronZoneUuid, PlanningNoopImageSourceSkipZoneReason>,
    pub converted_zones: BTreeMap<SledUuid, (usize, usize)>,
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
        self.converted_zones.insert(sled_id, (num_eligible, num_dataset));
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

        // Very noisy in tests.
        // for (zone_id, reason) in skipped_zones.iter() {
        //     writeln!(
        //         f,
        //         "* Skipping noop image source check for zone {zone_id}: {reason}"
        //     )?;
        // }

        for (sled_id, (m, n)) in converted_zones.iter() {
            if *m > 0 && *n > 0 {
                writeln!(
                    f,
                    "* Noop converting {m}/{n} install-dataset zones to artifact store \
                   on sled {sled_id}",
                )?;
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum PlanningNoopImageSourceSkipSledReason {
    AllZonesAlreadyArtifact(usize),
    SledNotInInventory,
    ErrorRetrievingZoneManifest(String),
    RemoveMupdateOverride(MupdateOverrideUuid),
}

impl fmt::Display for PlanningNoopImageSourceSkipSledReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AllZonesAlreadyArtifact(n) => {
                write!(f, "all {n} zones are already from artifacts")
            }
            Self::SledNotInInventory => {
                write!(f, "sled not present in latest inventory collection")
            }
            Self::ErrorRetrievingZoneManifest(error) => {
                write!(
                    f,
                    "sled-agent encountered error retrieving zone manifest \
                     (this is abnormal): {error}"
                )
            }
            Self::RemoveMupdateOverride(id) => {
                write!(
                    f,
                    "blueprint has get_remove_mupdate_override set for sled: {id}",
                )
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningMgsUpdatesStepReport {
    pub pending_mgs_updates: PendingMgsUpdates,
}

impl PlanningMgsUpdatesStepReport {
    pub fn new(pending_mgs_updates: PendingMgsUpdates) -> Self {
        Self { pending_mgs_updates }
    }

    // TODO This is not quite right.  See oxidecomputer/omicron#8285.
    pub fn any_updates_pending(&self) -> bool {
        !self.pending_mgs_updates.is_empty()
    }
}

impl fmt::Display for PlanningMgsUpdatesStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self { pending_mgs_updates } = self;
        if !pending_mgs_updates.is_empty() {
            let n = pending_mgs_updates.len();
            let s = if n == 1 { "" } else { "s" };
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningAddStepReport {
    pub sleds_with_no_zpools_for_ntp_zone: BTreeSet<SledUuid>,
    pub sleds_waiting_for_ntp_zone: BTreeSet<SledUuid>,
    pub sleds_getting_ntp_and_discretionary_zones: BTreeSet<SledUuid>,
    pub sleds_missing_ntp_zone: BTreeSet<SledUuid>,
    pub sleds_missing_crucible_zone: BTreeSet<(SledUuid, ZpoolUuid)>,

    /// Discretionary zone kind → (placed, wanted to place)
    pub out_of_eligible_sleds: BTreeMap<String, (usize, usize)>,

    /// Discretionary zone kind → (wanted to place, num existing)
    pub sufficient_zones_exist: BTreeMap<String, (usize, usize)>,

    /// List of (Sled ID, kind of discretionary zone placed there) pairs.
    // TODO: make `sled_add_zone_*` methods return the added zone config
    // so that we can report it here.
    pub discretionary_zones_placed: Vec<(SledUuid, String)>,
}

impl PlanningAddStepReport {
    pub fn new() -> Self {
        Self {
            sleds_with_no_zpools_for_ntp_zone: BTreeSet::new(),
            sleds_waiting_for_ntp_zone: BTreeSet::new(),
            sleds_getting_ntp_and_discretionary_zones: BTreeSet::new(),
            sleds_missing_ntp_zone: BTreeSet::new(),
            sleds_missing_crucible_zone: BTreeSet::new(),
            out_of_eligible_sleds: BTreeMap::new(),
            sufficient_zones_exist: BTreeMap::new(),
            discretionary_zones_placed: Vec::new(),
        }
    }

    pub fn any_discretionary_zones_placed(&self) -> bool {
        !self.discretionary_zones_placed.is_empty()
    }
}

impl fmt::Display for PlanningAddStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            sleds_with_no_zpools_for_ntp_zone,
            sleds_waiting_for_ntp_zone,
            sleds_getting_ntp_and_discretionary_zones,
            sleds_missing_ntp_zone,
            sleds_missing_crucible_zone,
            out_of_eligible_sleds,
            sufficient_zones_exist: _,
            discretionary_zones_placed,
        } = self;

        if !sleds_with_no_zpools_for_ntp_zone.is_empty() {
            writeln!(
                f,
                "* No zpools in service for NTP zones on sleds: {}",
                sleds_with_no_zpools_for_ntp_zone
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

        for (sled_id, zpool_id) in sleds_missing_crucible_zone {
            writeln!(
                f,
                "* Missing Crucible zone for sled {sled_id}, zpool {zpool_id}",
            )?;
        }

        for (kind, (placed, desired)) in out_of_eligible_sleds.iter() {
            writeln!(
                f,
                "* Only placed {placed}/{desired} desired {kind} zones"
            )?;
        }

        // Noisy in tests.
        // for (kind, (desired, existing)) in sufficient_zones_exist.iter() {
        //     writeln!(
        //         f,
        //         "* Sufficient {kind} zones exist in plan: {desired}/{existing}"
        //     )?;
        // }

        if !discretionary_zones_placed.is_empty() {
            writeln!(f, "* Discretionary zones placed:")?;
            for (sled_id, kind) in discretionary_zones_placed.iter() {
                writeln!(f, "  * a {kind} zone on sled {sled_id}")?;
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningZoneUpdatesStepReport {
    /// What are we waiting on to start zone updates?
    pub waiting_on: Option<ZoneUpdatesWaitingOn>,

    /// (Sled ID, zone, desired image)
    pub out_of_date_zones:
        Vec<(SledUuid, BlueprintZoneConfig, BlueprintZoneImageSource)>,

    pub expunged_zones: Vec<(SledUuid, BlueprintZoneConfig)>,
    pub updated_zones: Vec<(SledUuid, BlueprintZoneConfig)>,
    pub unsafe_zones: Vec<(BlueprintZoneConfig, ZoneUnsafeToShutdown)>,
}

impl PlanningZoneUpdatesStepReport {
    pub fn new() -> Self {
        Self {
            waiting_on: None,
            out_of_date_zones: Vec::new(),
            expunged_zones: Vec::new(),
            updated_zones: Vec::new(),
            unsafe_zones: Vec::new(),
        }
    }

    pub fn waiting_on(&mut self, waiting_on: ZoneUpdatesWaitingOn) {
        self.waiting_on = Some(waiting_on);
    }

    pub fn unsafe_zone(
        &mut self,
        zone: &BlueprintZoneConfig,
        reason: ZoneUnsafeToShutdown,
    ) {
        self.unsafe_zones.push((zone.clone(), reason))
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
            let n = out_of_date_zones.len();
            let s = if n == 1 { "" } else { "s" };
            writeln!(f, "* Out-of-date zone{s} expunged:")?;
            for (sled_id, zone) in expunged_zones.iter() {
                writeln!(
                    f,
                    "  * sled {}, zone {} ({})",
                    sled_id,
                    zone.id,
                    zone.zone_type.kind().report_str(),
                )?;
            }
        }

        if !updated_zones.is_empty() {
            let n = out_of_date_zones.len();
            let s = if n == 1 { "" } else { "s" };
            writeln!(f, "* Out-of-date zone{s} updated in-place:")?;
            for (sled_id, zone) in updated_zones.iter() {
                writeln!(
                    f,
                    "  * sled {}, zone {} ({})",
                    sled_id,
                    zone.id,
                    zone.zone_type.kind().report_str(),
                )?;
            }
        }

        if !out_of_date_zones.is_empty() {
            let n = out_of_date_zones.len();
            let s = if n == 1 { "" } else { "s" };
            writeln!(f, "* {n} out-of-date zone{s}:")?;
            for (sled, zone, _image_source) in out_of_date_zones.iter() {
                writeln!(
                    f,
                    "  * sled {}, zone {} ({})", // TODO: current → desired image source
                    sled,
                    zone.id,
                    zone.zone_type.kind().report_str(),
                )?;
            }
        }

        if !unsafe_zones.is_empty() {
            let n = unsafe_zones.len();
            let s = if n == 1 { "" } else { "s" };
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ZoneUnsafeToShutdown {
    Cockroachdb(CockroachdbUnsafeToShutdown),
}

impl fmt::Display for ZoneUnsafeToShutdown {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Cockroachdb(reason) => write!(f, "{reason}"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum CockroachdbUnsafeToShutdown {
    MissingLiveNodesStat,
    MissingUnderreplicatedStat,
    NotEnoughLiveNodes(u64),
    NotEnoughNodes,
    UnderreplicatedRanges(u64),
}

impl fmt::Display for CockroachdbUnsafeToShutdown {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::MissingLiveNodesStat => write!(f, "missing live_nodes stat"),
            Self::MissingUnderreplicatedStat => {
                write!(f, "missing ranges_underreplicated stat")
            }
            Self::NotEnoughLiveNodes(n) => {
                write!(
                    f,
                    "not enough live nodes: {n} < {COCKROACHDB_REDUNDANCY}"
                )
            }
            Self::NotEnoughNodes => write!(f, "not enough nodes"),
            Self::UnderreplicatedRanges(n) => {
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanningCockroachdbSettingsStepReport {
    pub preserve_downgrade: CockroachDbPreserveDowngrade,
}

impl PlanningCockroachdbSettingsStepReport {
    pub fn new() -> Self {
        Self { preserve_downgrade: CockroachDbPreserveDowngrade::DoNotModify }
    }
}

impl fmt::Display for PlanningCockroachdbSettingsStepReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let PlanningCockroachdbSettingsStepReport { preserve_downgrade } = self;
        if !matches!(
            preserve_downgrade,
            CockroachDbPreserveDowngrade::DoNotModify,
        ) {
            writeln!(
                f,
                "* Will ensure cockroachdb setting: {preserve_downgrade}"
            )?;
        }
        Ok(())
    }
}
