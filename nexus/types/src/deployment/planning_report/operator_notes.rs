// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Summarizing a [`PlanningReport`] for customer operators.

use super::ZoneAddWaitingOn;
use super::ZoneUnsafeToShutdown;
use crate::deployment::PlannerConfig;
use crate::deployment::PlanningAddStepReport;
use crate::deployment::PlanningDecommissionStepReport;
use crate::deployment::PlanningExpungeStepReport;
use crate::deployment::PlanningMgsUpdatesStepReport;
use crate::deployment::PlanningNoopImageSourceSkipSledHostPhase2Reason;
use crate::deployment::PlanningNoopImageSourceSkipSledZonesReason;
use crate::deployment::PlanningNoopImageSourceSkipZoneReason;
use crate::deployment::PlanningNoopImageSourceStepReport;
use crate::deployment::PlanningReport;
use crate::deployment::PlanningZoneUpdatesStepReport;
use crate::deployment::planning_report::PlanningAddOutOfEligibleSleds;
use crate::deployment::planning_report::PlanningNoopImageSourceConverted;
use slog_error_chain::InlineErrorChain;

#[derive(Debug, Clone)]
pub struct PlanningOperatorNotes {
    notes: Vec<String>,
}

impl PlanningOperatorNotes {
    pub(super) fn new(report: &PlanningReport) -> Self {
        let PlanningReport {
            planner_config,
            expunge,
            decommission,
            noop_image_source,
            mgs_updates,
            add,
            zone_updates,
            // nexus generation bump is a normal part of the end of an update;
            // there's nothing an operator can act on here.
            nexus_generation_bump: _,
            // cockroach cluster settings are not operator-relevant
            cockroachdb_settings: _,
        } = report;

        let mut me = Self { notes: Vec::new() };

        me.push_planner_config_notes(planner_config);
        me.push_expunge_notes(expunge);
        me.push_decommission_notes(decommission);
        me.push_noop_image_source_notes(noop_image_source);
        me.push_mgs_updates_notes(mgs_updates);
        me.push_add_notes(add);
        me.push_zone_updates_notes(zone_updates);

        me
    }

    pub fn into_notes(self) -> Vec<String> {
        self.notes
    }

    fn push_planner_config_notes(&mut self, config: &PlannerConfig) {
        let PlannerConfig { add_zones_with_mupdate_override } = config;
        if *add_zones_with_mupdate_override {
            self.notes.push(
                "support override configured: internal services can be \
                 placed even if some versions are unknown due to sled \
                 recovery operations"
                    .to_string(),
            );
        }
    }

    fn push_expunge_notes(&mut self, expunge: &PlanningExpungeStepReport) {
        let PlanningExpungeStepReport { orphan_disks } = expunge;

        let mut nsleds = 0;
        let mut ndisks = 0;
        for orphans in orphan_disks.values() {
            nsleds += 1;
            ndisks += orphans.len();
        }
        if nsleds > 0 {
            self.notes.push(format!(
                "unexpectedly found {ndisks} orphaned disk{} \
                 across {nsleds} sled{}",
                pluralize_s(ndisks),
                pluralize_s(nsleds)
            ));
        }
    }

    fn push_decommission_notes(
        &mut self,
        decommission: &PlanningDecommissionStepReport,
    ) {
        let PlanningDecommissionStepReport { zombie_sleds } = decommission;
        let nsleds = zombie_sleds.len();
        if nsleds > 0 {
            self.notes.push(format!(
                "unexpectedly found {nsleds} sled{} that \
                 have been decommissioned",
                pluralize_s(nsleds),
            ));
        }
    }

    fn push_noop_image_source_notes(
        &mut self,
        noop_image_source: &PlanningNoopImageSourceStepReport,
    ) {
        let PlanningNoopImageSourceStepReport {
            no_target_release,
            skipped_sled_zones,
            skipped_sled_host_phase_2,
            skipped_zones,
            converted,
        } = noop_image_source;

        // If there's no target release, we can't do any updates nor do any
        // no-op conversions, so just add a single note and bail out.
        if *no_target_release {
            self.notes.push(
                "no update possible: no target release has been set"
                    .to_string(),
            );
            return;
        }

        // Report notes for sleds we skipped.
        {
            let mut nsleds_missing_from_inv = 0;
            let mut nsleds_error_manifest = 0;
            let mut nsleds_mupdate_override = 0;
            for reason in skipped_sled_zones.values() {
                use PlanningNoopImageSourceSkipSledZonesReason::*;

                match reason {
                    AllZonesAlreadyArtifact { .. } => {
                        // This is the normal steady state; nothing to note.
                    }
                    SledNotInInventory => {
                        nsleds_missing_from_inv += 1;
                    }
                    ErrorRetrievingZoneManifest { .. } => {
                        nsleds_error_manifest += 1;
                    }
                    RemoveMupdateOverride { .. } => {
                        nsleds_mupdate_override += 1;
                    }
                }
            }
            // This loop does nothing, but we write it explicitly so if we add
            // more reasons in the future, we have a chance to update these
            // notes.
            for reason in skipped_sled_host_phase_2.values() {
                use PlanningNoopImageSourceSkipSledHostPhase2Reason::*;
                match reason {
                    BothSlotsAlreadyArtifact => {
                        // This is the normal steady state; nothing to note.
                    }
                    SledNotInInventory => {
                        // This will already be counted above in
                        // `nsleds_missing_from_inv`.
                    }
                }
            }

            if nsleds_missing_from_inv > 0 {
                self.notes.push(format!(
                    "{nsleds_missing_from_inv} sled{} missing from inventory",
                    pluralize_s(nsleds_missing_from_inv)
                ));
            }

            if nsleds_error_manifest > 0 {
                self.notes.push(format!(
                    "{nsleds_error_manifest} sled{} have unexpected errors \
                     on their boot disks",
                    pluralize_s(nsleds_error_manifest)
                ));
            }

            if nsleds_mupdate_override > 0 {
                self.notes.push(format!(
                    "{nsleds_mupdate_override} sled{} are waiting for a \
                     system version that matches their most recent \
                     support-driven update (mupdate)",
                    pluralize_s(nsleds_mupdate_override)
                ));
            }
        }

        // Report notes for zones we skipped. If we skipped the entire sled,
        // that will be covered above; this is only for zones on sleds we
        // considered.
        {
            let mut ninvalid_artifact = 0;
            let mut nartifact_not_in_repo = 0;
            for reason in skipped_zones.values() {
                use PlanningNoopImageSourceSkipZoneReason::*;
                match reason {
                    ZoneNotInManifest { .. } => {
                        // TODO-correctness Should we report anything for this
                        // reason? It has no constructors at the moment; see
                        // <https://github.com/oxidecomputer/omicron/issues/9092>
                    }
                    InvalidArtifact { .. } => {
                        ninvalid_artifact += 1;
                    }
                    ArtifactNotInRepo { .. } => {
                        nartifact_not_in_repo += 1;
                    }
                }
            }

            if ninvalid_artifact > 0 {
                self.notes.push(format!(
                    "{ninvalid_artifact} service zone{} have invalid artifacts",
                    pluralize_s(ninvalid_artifact)
                ));
            }

            if nartifact_not_in_repo > 0 {
                self.notes.push(format!(
                    "{nartifact_not_in_repo} service zone{} are waiting for a \
                     system version that matches their most recent \
                     support-driven update (mupdate)",
                    pluralize_s(nartifact_not_in_repo)
                ));
            }
        }

        // Report counts of converted artifacts.
        {
            let mut nsleds = 0;
            let mut tot_eligible = 0;
            let mut tot_dataset = 0;
            let mut tot_eligible_os = 0;
            for converted in converted.values() {
                let PlanningNoopImageSourceConverted {
                    num_eligible,
                    num_dataset,
                    host_phase_2_slot_a_eligible,
                    host_phase_2_slot_b_eligible,
                } = converted;

                nsleds += 1;
                tot_eligible += num_eligible;
                tot_dataset += num_dataset;
                // We don't want to report 2 OSs updated for each sled, nor do
                // we want to complain about failing to convert one slot if we
                // were able to convert the other. Count _either_ slot as
                // eligible as a success.
                if *host_phase_2_slot_a_eligible
                    || *host_phase_2_slot_b_eligible
                {
                    tot_eligible_os += 1;
                }
            }

            if nsleds > 0 {
                self.notes.push(format!(
                    "across {nsleds} sled{}, converted {}{}{} to known versions",
                    pluralize_s(nsleds),
                    if tot_dataset > 0 {
                        format_args!(
                            "{tot_eligible}/{tot_dataset} service zone{}",
                            pluralize_s(tot_eligible),
                        )
                    } else {
                        format_args!("")
                    },
                    if tot_dataset > 0 && tot_eligible_os > 0 {
                        " and "
                    } else {
                        ""
                    },
                    if tot_eligible_os > 0 {
                        format_args!(
                            "{tot_eligible_os} OS image{}",
                            pluralize_s(tot_eligible_os),
                        )
                    } else {
                        format_args!("")
                    },
                ));
            }
        }
    }

    fn push_mgs_updates_notes(
        &mut self,
        mgs_updates: &PlanningMgsUpdatesStepReport,
    ) {
        let PlanningMgsUpdatesStepReport {
            blocked_mgs_updates,
            // These are covered by the update status API
            pending_mgs_updates: _,
        } = mgs_updates;

        for update in blocked_mgs_updates {
            let serial = &update.baseboard_id.serial_number;
            let component = update.component;
            let reason = InlineErrorChain::new(&update.reason);

            self.notes.push(format!(
                "unable to update {serial} {component}: {reason}",
            ));
        }
    }

    fn push_add_notes(&mut self, add: &PlanningAddStepReport) {
        let PlanningAddStepReport {
            waiting_on,
            sleds_without_zpools_for_ntp_zones,
            // summarized by `waiting_on`, so we ignore details
            add_update_blocked_reasons: _,
            // already included in planner config
            add_zones_with_mupdate_override: _,
            // not a relevant detail to operators: this is true only for systems
            // that have never had a reconfigurator-driven update
            target_release_generation_is_one: _,
            // sleds waiting for NTP is normal while we're updaing NTP zones
            sleds_without_ntp_zones_in_inventory: _,
            sleds_waiting_for_ntp_zone: _,
            sleds_missing_ntp_zone: _,
            // not a relevant detail to operators: these are "operating
            // normally" sleds
            sleds_getting_ntp_and_discretionary_zones: _,
            sleds_missing_crucible_zone,
            out_of_eligible_sleds,
            // not a relevant detail to operators: this is normal operation
            sufficient_zones_exist: _,
            // maybe not a relevant detail to operators? we're restoring
            // redundancy or finishing an expunge+add update
            discretionary_zones_placed: _,
        } = add;

        if let Some(waiting_on) = waiting_on {
            match waiting_on {
                ZoneAddWaitingOn::MupdateBlockers => self.notes.push(
                    "service updates are waiting for system to recover \
                     from support-driven update (mupdate)"
                        .to_string(),
                ),
            }
        }

        if !sleds_without_zpools_for_ntp_zones.is_empty() {
            let n = sleds_without_zpools_for_ntp_zones.len();
            self.notes.push(format!(
                "{n} sled{} with no available disks to host NTP service",
                pluralize_s(n),
            ));
        }

        {
            let mut nsleds = 0;
            let mut npools = 0;
            for pools in sleds_missing_crucible_zone.values() {
                nsleds += 1;
                npools += pools.len();
            }
            if npools > 0 {
                self.notes.push(format!(
                    "{npools} disk{} are unavailable across {nsleds} sled{}",
                    pluralize_s(npools),
                    pluralize_s(nsleds),
                ));
            }
        }

        {
            let mut tot_placed = 0;
            let mut tot_wanted_to_place = 0;
            for out_of_eligible_sleds in out_of_eligible_sleds.values() {
                let PlanningAddOutOfEligibleSleds { placed, wanted_to_place } =
                    out_of_eligible_sleds;
                tot_placed += *placed;
                tot_wanted_to_place += *wanted_to_place;
            }
            if tot_placed < tot_wanted_to_place {
                self.notes.push(format!(
                    "out of eligible sleds: only placed \
                     {tot_placed} service{} out of \
                     {tot_wanted_to_place} desired",
                    pluralize_s(tot_placed),
                ));
            }
        }
    }

    fn push_zone_updates_notes(
        &mut self,
        zone_updates: &PlanningZoneUpdatesStepReport,
    ) {
        let PlanningZoneUpdatesStepReport {
            unsafe_zones,
            // these are normal update details covered by update status
            waiting_zones: _,
            waiting_on: _,
            out_of_date_zones: _,
            expunged_zones: _,
            updated_zones: _,
        } = zone_updates;

        // Report problems with particular zone types.
        {
            // Only report each kind of problem once (e.g., if 3/5 cockroach
            // nodes are healthy, don't emit the same note twice for both
            // unhealthy nodes).
            let mut emitted_cockroach = false;
            let mut emitted_boundary_ntp = false;
            let mut emitted_internal_dns = false;
            for kind in unsafe_zones.values() {
                let emit = match kind {
                    ZoneUnsafeToShutdown::Cockroachdb { .. } => {
                        if !emitted_cockroach {
                            emitted_cockroach = true;
                            true
                        } else {
                            false
                        }
                    }
                    ZoneUnsafeToShutdown::BoundaryNtp { .. } => {
                        if !emitted_boundary_ntp {
                            emitted_boundary_ntp = true;
                            true
                        } else {
                            false
                        }
                    }
                    ZoneUnsafeToShutdown::InternalDns { .. } => {
                        if !emitted_internal_dns {
                            emitted_internal_dns = true;
                            true
                        } else {
                            false
                        }
                    }
                };
                if emit {
                    // TODO Will we emit these notes during a normal update
                    // (e.g., when we reboot a sled hosting boundary NTP)? If
                    // so, we should probably not emit these unless we can get
                    // more confidence there's actually something wrong.
                    self.notes.push(format!(
                        "waiting for condition to clear: {kind}"
                    ));
                }
            }
        }
    }
}

// Very dumb helper to pluralize words by appending an "s". Only used within
// this file to pluralize simple nouns like "sleds" and "disks".
fn pluralize_s(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}
