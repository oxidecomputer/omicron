// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::ClickhouseAllocator;
use crate::blueprint_builder::ClickhouseZonesThatShouldBeRunning;
use crate::blueprint_builder::Ensure;
use crate::blueprint_builder::EnsureMultiple;
use crate::blueprint_builder::EnsureMupdateOverrideAction;
use crate::blueprint_builder::Error;
use crate::blueprint_builder::Operation;
use crate::blueprint_editor::DisksEditError;
use crate::blueprint_editor::ExternalNetworkingAllocator;
use crate::blueprint_editor::SledEditError;
use crate::mgs_updates::ImpossibleUpdatePolicy;
use crate::mgs_updates::MgsUpdatePlanner;
use crate::mgs_updates::PlannedMgsUpdates;
use crate::mgs_updates::UpdateableBoard;
use crate::planner::image_source::NoopConvertHostPhase2Contents;
use crate::planner::image_source::NoopConvertZoneStatus;
use crate::planner::omicron_zone_placement::PlacementError;
use iddqd::IdOrdMap;
use itertools::Either;
use itertools::Itertools;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ClickhouseClusterConfig;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::TufRepoContentsError;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::deployment::{
    NexusGenerationBumpWaitingOn, PlanningAddStepReport,
    PlanningCockroachdbSettingsStepReport, PlanningDecommissionStepReport,
    PlanningExpungeStepReport, PlanningMgsUpdatesStepReport,
    PlanningNexusGenerationBumpReport, PlanningNoopImageSourceStepReport,
    PlanningReport, PlanningZoneUpdatesStepReport, ZoneAddWaitingOn,
    ZoneUpdatesWaitingOn, ZoneWaitingToExpunge,
};
use nexus_types::external_api::views::PhysicalDiskPolicy;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use nexus_types::inventory::SpType;
use omicron_common::api::external::Generation;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types_migrations::latest::inventory::ConfigReconcilerInventoryResult;
use sled_agent_types_migrations::latest::inventory::OmicronZoneImageSource;
use sled_agent_types_migrations::latest::inventory::OmicronZoneType;
use sled_agent_types_migrations::latest::inventory::ZoneKind;
use slog::error;
use slog::{Logger, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;
use swrite::SWrite;
use swrite::swriteln;

pub(crate) use self::image_source::NoopConvertGlobalIneligibleReason;
pub(crate) use self::image_source::NoopConvertInfo;
pub(crate) use self::image_source::NoopConvertSledEligible;
pub(crate) use self::image_source::NoopConvertSledIneligibleReason;
pub(crate) use self::image_source::NoopConvertSledInfoMut;
pub(crate) use self::image_source::NoopConvertSledStatus;
pub(crate) use self::omicron_zone_placement::DiscretionaryOmicronZone;
use self::omicron_zone_placement::OmicronZonePlacement;
use self::omicron_zone_placement::OmicronZonePlacementSledState;
pub use self::rng::PlannerRng;
pub use self::rng::SledPlannerRng;
pub(crate) use self::zone_safety::ZoneSafetyChecks;

mod image_source;
mod omicron_zone_placement;
pub(crate) mod rng;
mod zone_safety;

/// Maximum number of MGS-managed updates (updates to SP, RoT, RoT bootloader,
/// or host OS) that we allow to be pending across the whole system at one time
///
/// For now, we limit this to 1 for safety.  That's for a few reasons:
///
/// - SP updates reboot the corresponding host.  Thus, if we have one of these
///   updates outstanding, we should assume that host may be offline.  Most
///   control plane services are designed to survive multiple failures (e.g.,
///   the Cockroach cluster can sustain two failures and stay online), but
///   having one sled offline eats into that margin.  And some services like
///   Crucible volumes can only sustain one failure.  Taking down two sleds
///   would render unavailable any Crucible volumes with regions on those two
///   sleds.
///
/// - There is unfortunately some risk in updating the RoT bootloader, in that
///   there's a window where a failure could render the device unbootable.  See
///   oxidecomputer/omicron#7819 for more on this.  Updating only one at a time
///   helps mitigate this risk.
///
/// More sophisticated schemes are certainly possible (e.g., allocate Crucible
/// regions in such a way that there are at least pairs of sleds we could update
/// concurrently without taking volumes down; and/or be willing to update
/// multiple sleds as long as they don't have overlapping control plane
/// services, etc.).
const NUM_CONCURRENT_MGS_UPDATES: usize = 1;

// Details of how a zone's status differs between the blueprint and the sled
// inventory
#[derive(Debug)]
#[expect(dead_code)]
struct ZonePropagationIncomplete<'a> {
    zone_id: OmicronZoneUuid,
    zone_kind: ZoneKind,
    reason: ZonePropagationStatus<'a>,
}

#[derive(Debug)]
#[expect(dead_code)]
enum ZonePropagationStatus<'a> {
    // The current blueprint and the sled inventory disagree
    // about the image source for a zone.
    //
    // This can mean that the sled inventory is out-of-date, or
    // that a different blueprint has been applied.
    ImageSourceMismatch {
        bp_image_source: &'a BlueprintZoneImageSource,
        inv_image_source: &'a OmicronZoneImageSource,
    },
    // Although this zone appears in the blueprint, it does
    // not exist on the sled's inventory.
    MissingInInventory {
        bp_image_source: &'a BlueprintZoneImageSource,
    },
    // The last reconciliation attempt for this zone failed
    ReconciliationError {
        bp_image_source: &'a BlueprintZoneImageSource,
        inv_image_source: &'a OmicronZoneImageSource,
        message: &'a str,
    },
}

pub struct Planner<'a> {
    log: Logger,
    input: &'a PlanningInput,
    blueprint: BlueprintBuilder<'a>,
    // latest inventory collection
    //
    // We must be very careful when using this during planning.  The real-world
    // state may have changed since this inventory was collected.  Planning
    // choices should not depend on this still being totally accurate.
    //
    // If we do start depending on specific criteria (e.g., it contains
    // information about all sleds that we expect), we should verify that up
    // front and update callers to ensure that it's true.
    inventory: &'a Collection,
}

impl<'a> Planner<'a> {
    pub fn new_based_on(
        log: Logger,
        parent_blueprint: &'a Blueprint,
        input: &'a PlanningInput,
        creator: &str,
        // NOTE: Right now, we just assume that this is the latest inventory
        // collection.  See the comment on the corresponding field in `Planner`.
        inventory: &'a Collection,
        rng: PlannerRng,
    ) -> anyhow::Result<Planner<'a>> {
        let mut blueprint = BlueprintBuilder::new_based_on(
            &log,
            parent_blueprint,
            creator,
            rng,
        )?;
        Self::update_builder_from_planning_input(&mut blueprint, input);
        Ok(Planner { log, input, blueprint, inventory })
    }

    /// Update the internal state of `builder` to reflect changes between its
    /// current blueprint and `input`.
    ///
    /// This method does not implement any real planning logic; it performs
    /// strictly straightforward "update this value in the blueprint", such as
    /// noting the latest internal and external DNS versions.
    ///
    /// This method is public and does not take `&self` so other planner-like
    /// entities (`reconfigurator-cli` and tests) can easily reuse it if they
    /// want to update a `BlueprintBuilder` they're using without running
    /// through the planner proper.
    pub fn update_builder_from_planning_input(
        builder: &mut BlueprintBuilder,
        input: &PlanningInput,
    ) {
        // Ensure we have an entry for every commissioned sled.
        //
        // If `input` has new sleds that weren't in `builder`'s parent
        // blueprint, this will create empty configs for them.
        for (sled_id, details) in input.all_sleds(SledFilter::Commissioned) {
            builder.ensure_sled_exists(sled_id, details.resources.subnet);
        }

        // Update various blueprint fields that track changes to the system but
        // that don't require any planning logic.
        builder.set_cockroachdb_fingerprint(
            input.cockroachdb_settings().state_fingerprint.clone(),
        );
        builder.set_internal_dns_version(input.internal_dns_version());
        builder.set_external_dns_version(input.external_dns_version());
        let oximeter_read_policy = input.oximeter_read_settings();
        builder.set_oximeter_read_policy(
            oximeter_read_policy.version.into(),
            oximeter_read_policy.mode,
        );
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        let report = self.do_plan()?;
        Ok(self.blueprint.build(BlueprintSource::Planner(Arc::new(report))))
    }

    fn do_plan(&mut self) -> Result<PlanningReport, Error> {
        // Run the planning steps, recording their step reports as we go.
        let expunge = self.do_plan_expunge()?;
        let decommission = self.do_plan_decommission()?;

        // Compute set of sleds/zones that we shouldn't choose to shut down.
        let zone_safety_checks = ZoneSafetyChecks::new(
            &self.blueprint,
            &self.inventory,
            &self.input,
        );

        let mut noop_info =
            NoopConvertInfo::new(self.input, self.inventory, &self.blueprint)?;
        let actions_by_sled = self.do_plan_mupdate_override(&mut noop_info)?;

        // Log noop-convert results after do_plan_mupdate_override, because this
        // step might alter noop_info.
        noop_info.log_to(&self.log);

        // Within `do_plan_noop_image_source`, we plan noop image sources on
        // sleds other than those currently affected by mupdate overrides. This
        // means that we don't have to consider anything
        // `do_plan_mupdate_override` does for this step.
        let noop_image_source = self.do_plan_noop_image_source(noop_info)?;

        let add_update_blocked_reasons =
            self.should_plan_add_or_update(&actions_by_sled)?;

        // Only plan MGS-based updates updates if there are no outstanding
        // MUPdate overrides.
        let mgs_updates = if add_update_blocked_reasons.is_empty() {
            self.do_plan_mgs_updates(&zone_safety_checks)?
        } else {
            PlanningMgsUpdatesStepReport::new()
        };

        // Likewise for zone additions, unless overridden by the config, or
        // unless a target release has never been set (i.e. we're effectively in
        // a pre-Nexus-driven-update world).
        //
        // We don't have to check for the minimum target release generation in
        // this case. On a freshly-installed or MUPdated system, Nexus will find
        // the mupdate overrides and clear them. The act of clearing mupdate
        // overrides always sets the minimum generation to the current target
        // release generation plus one, so the minimum generation will always be
        // exactly 2.
        let add_zones_with_mupdate_override =
            self.input.planner_config().add_zones_with_mupdate_override;
        let target_release_generation_is_one =
            self.input.tuf_repo().target_release_generation
                == Generation::from_u32(1);
        let mut add = if add_update_blocked_reasons.is_empty()
            || add_zones_with_mupdate_override
            || target_release_generation_is_one
        {
            self.do_plan_add(&mgs_updates)?
        } else {
            PlanningAddStepReport::waiting_on(ZoneAddWaitingOn::Blockers)
        };
        add.add_update_blocked_reasons = add_update_blocked_reasons;
        add.add_zones_with_mupdate_override = add_zones_with_mupdate_override;
        add.target_release_generation_is_one = target_release_generation_is_one;

        let zone_updates = if add.any_discretionary_zones_placed() {
            // Do not update any zones if we've added any discretionary zones
            // (e.g., in response to policy changes) ...
            PlanningZoneUpdatesStepReport::waiting_on(
                ZoneUpdatesWaitingOn::DiscretionaryZones,
            )
        } else if !mgs_updates.pending_mgs_updates.is_empty() {
            // ... or if there are still pending updates for the RoT / SP /
            // Host OS / etc. ...
            PlanningZoneUpdatesStepReport::waiting_on(
                ZoneUpdatesWaitingOn::PendingMgsUpdates,
            )
        } else if !mgs_updates.blocked_mgs_updates.is_empty() {
            // ... or if there are blocked updates for the RoT / SP / Host OS /
            // RoT bootloader.
            PlanningZoneUpdatesStepReport::waiting_on(
                ZoneUpdatesWaitingOn::BlockedMgsUpdates,
            )
        } else if !add.add_update_blocked_reasons.is_empty() {
            // ... or if there are pending zone add blockers.
            PlanningZoneUpdatesStepReport::waiting_on(
                ZoneUpdatesWaitingOn::ZoneAddBlockers,
            )
        } else {
            self.do_plan_zone_updates(&mgs_updates, &zone_safety_checks)?
        };

        // We may need to bump the top-level Nexus generation number
        // to update Nexus zones.
        let nexus_generation_bump = self.do_plan_nexus_generation_update()?;

        // CockroachDB settings aren't dependent on zones, so they can be
        // planned independently of the rest of the system.
        let cockroachdb_settings = self.do_plan_cockroachdb_settings();

        // Clickhouse cluster settings aren't dependent on zones, so they can be
        // planned indepdently of the rest of the system.
        //
        // TODO-cleanup We should include something about clickhouse cluster
        // configs in the PlanningReport.
        self.do_plan_clickhouse_cluster_settings();

        Ok(PlanningReport {
            planner_config: *self.input.planner_config(),
            expunge,
            decommission,
            noop_image_source,
            add,
            mgs_updates,
            zone_updates,
            nexus_generation_bump,
            cockroachdb_settings,
        })
    }

    fn do_plan_decommission(
        &mut self,
    ) -> Result<PlanningDecommissionStepReport, Error> {
        let mut report = PlanningDecommissionStepReport::new();

        // Check for any sleds that are currently commissioned but can be
        // decommissioned. Our gates for decommissioning are:
        //
        // 1. The policy indicates the sled has been removed (i.e., the policy
        //    is "expunged"; we may have other policies that satisfy this
        //    requirement in the future).
        // 2. All zones associated with the sled have been marked expunged.
        // 3. There are no instances assigned to this sled. This is blocked by
        //    omicron#4872, so today we omit this check entirely, as any sled
        //    that could be otherwise decommissioned that still has instances
        //    assigned to it needs support intervention for cleanup.
        // 4. All disks associated with the sled have been marked expunged. This
        //    happens implicitly when a sled is expunged, so is covered by our
        //    first check.
        //
        // Note that we must check both the planning input, and the parent
        // blueprint to tell if a sled is decommissioned because we carry
        // decommissioned sleds forward and do not prune them from the blueprint
        // right away.
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            // Check 1: look for sleds that are expunged.
            match (sled_details.policy, sled_details.state) {
                // If the sled is still in service, don't decommission it.
                //
                // We do still want to decommission any expunged disks if
                // possible though. For example, we can expunge disks on active
                // sleds if they are faulty.
                (SledPolicy::InService { .. }, _) => {
                    self.do_plan_decommission_expunged_disks_for_in_service_sled(sled_id)?;
                    continue;
                }
                // If the sled is already decommissioned it... why is it showing
                // up when we ask for commissioned sleds? Report, but don't try to
                // decommission it again.
                (SledPolicy::Expunged, SledState::Decommissioned) => {
                    report.zombie_sleds.push(sled_id);
                    continue;
                }
                // The sled is expunged but not yet decommissioned; fall through
                // to check the rest of the criteria.
                (SledPolicy::Expunged, SledState::Active) => (),
            }

            // Check that the sled isn't already decommissioned in the parent
            // blueprint, if it exists.
            if let Some(parent_sled_cfg) =
                self.blueprint.parent_blueprint().sleds.get(&sled_id)
            {
                if parent_sled_cfg.state == SledState::Decommissioned {
                    continue;
                }
            }

            // Check 2: have all this sled's zones been expunged? It's possible
            // we ourselves have made this change, which is fine.
            let all_zones_expunged = self
                .blueprint
                .current_sled_zones(sled_id, BlueprintZoneDisposition::any)
                .all(|zone| {
                    matches!(
                        zone.disposition,
                        BlueprintZoneDisposition::Expunged { .. }
                    )
                });

            // Check 3: Are there any instances assigned to this sled? See
            // comment above; while we wait for omicron#4872, we just assume
            // there are no instances running.
            let num_instances_assigned = 0;

            if all_zones_expunged && num_instances_assigned == 0 {
                self.blueprint.set_sled_decommissioned(sled_id)?;
            }
        }

        Ok(report)
    }

    fn do_plan_decommission_expunged_disks_for_in_service_sled(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        // The sled is not expunged. We have to see if the inventory reflects a
        // reconciled config generation. If it does, we'll check below whether
        // the reconciled generation is sufficiently advanced to decommission
        // any disks.
        let Some(seen_generation) = self
            .inventory
            .sled_agents
            .get(&sled_id)
            .and_then(|sa| sa.last_reconciliation.as_ref())
            .map(|reconciled| reconciled.last_reconciled_config.generation)
        else {
            // There is no current inventory for the sled agent, so we cannot
            // decommission any disks.
            return Ok(());
        };

        let disks_to_decommission: Vec<PhysicalDiskUuid> = self
            .blueprint
            .current_sled_disks(sled_id, |disposition| match disposition {
                BlueprintPhysicalDiskDisposition::Expunged {
                    ready_for_cleanup,
                    ..
                } => !ready_for_cleanup,
                BlueprintPhysicalDiskDisposition::InService => false,
            })
            .filter_map(|disk| {
                // Has the sled agent seen this disk's expungement yet as
                // reflected in inventory?
                //
                // SAFETY: We filtered to only have expunged disks above
                if seen_generation
                    >= disk.disposition.expunged_as_of_generation().unwrap()
                {
                    Some(disk.id)
                } else {
                    None
                }
            })
            .collect();

        self.blueprint.sled_decommission_disks(sled_id, disks_to_decommission)
    }

    fn do_plan_expunge(&mut self) -> Result<PlanningExpungeStepReport, Error> {
        let mut report = PlanningExpungeStepReport::new();

        // Remove services from sleds marked expunged. We use
        // `SledFilter::Commissioned` and have a custom `needs_zone_expungement`
        // function that allows us to produce better errors.
        let mut commissioned_sled_ids = BTreeSet::new();
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            commissioned_sled_ids.insert(sled_id);
            self.do_plan_expunge_for_commissioned_sled(
                sled_id,
                sled_details,
                &mut report,
            )?;
        }

        // Check for any decommissioned sleds (i.e., sleds for which our
        // blueprint has zones, but are not in the input sled list). Any zones
        // for decommissioned sleds must have already be expunged for
        // decommissioning to have happened; fail if we find non-expunged zones
        // associated with a decommissioned sled.
        for sled_id in self.blueprint.sled_ids_with_zones() {
            if !commissioned_sled_ids.contains(&sled_id) {
                let num_zones = self
                    .blueprint
                    .current_sled_zones(sled_id, BlueprintZoneDisposition::any)
                    .filter(|zone| {
                        !matches!(
                            zone.disposition,
                            BlueprintZoneDisposition::Expunged { .. }
                        )
                    })
                    .count();
                if num_zones > 0 {
                    return Err(
                        Error::DecommissionedSledWithNonExpungedZones {
                            sled_id,
                            num_zones,
                        },
                    );
                }
            }
        }

        Ok(report)
    }

    fn do_plan_expunge_for_commissioned_sled(
        &mut self,
        sled_id: SledUuid,
        sled_details: &SledDetails,
        report: &mut PlanningExpungeStepReport,
    ) -> Result<(), Error> {
        match sled_details.policy {
            SledPolicy::InService { .. } => {
                // Sled is still in service, but have any of its disks been
                // expunged? If so, expunge them in the blueprint (which
                // whill chain to expunging any datasets and zones that were
                // using them).
                //
                // We don't use a more specific disk filter here because we
                // want to look at _only_ the policy: if the operator has
                // said a disk should be expunged, we'll expunge it from the
                // blueprint regardless of its overall state.
                for (_, disk) in sled_details
                    .resources
                    .all_disks(DiskFilter::All)
                    .filter(|(_, disk)| match disk.policy {
                        PhysicalDiskPolicy::InService => false,
                        PhysicalDiskPolicy::Expunged => true,
                    })
                {
                    match self.blueprint.expunge_disk(sled_id, disk.disk_id) {
                        Ok(()) => (),
                        Err(Error::SledEditError {
                            err:
                                SledEditError::EditDisks(
                                    DisksEditError::ExpungeNonexistentDisk {
                                        ..
                                    },
                                ),
                            ..
                        }) => {
                            // While it should be rare, it's possible there's an
                            // expunged disk present in the planning input that
                            // isn't in the blueprint at all (e.g., a disk could
                            // have been added and then expunged since our
                            // parent blueprint was created). We don't want to
                            // fail in this case, but will report it.
                            report.orphan_disks.insert(sled_id, disk.disk_id);
                        }
                        Err(err) => return Err(err),
                    }
                }

                // Expunge any multinode clickhouse zones if the policy says
                // they shouldn't exist.
                if !self.input.clickhouse_cluster_enabled() {
                    self.blueprint.expunge_all_multinode_clickhouse(
                        sled_id,
                        ZoneExpungeReason::ClickhouseClusterDisabled,
                    )?;
                }

                // Similarly, expunge any singlenode clickhouse if the policy
                // says they should exist.
                if !self.input.clickhouse_single_node_enabled() {
                    self.blueprint.expunge_all_singlenode_clickhouse(
                        sled_id,
                        ZoneExpungeReason::ClickhouseSingleNodeDisabled,
                    )?;
                }

                // Are there any expunged zones that haven't yet been marked as
                // ready for cleanup? If so, check whether we can mark them,
                // which requires us to check the inventory collection for two
                // properties:
                //
                // 1. The zone is not running
                // 2. The zone config generation from the sled is at least as
                //    high as the generation in which the zone was expunged
                self.check_zones_eligible_for_cleanup(sled_id)?;
            }
            // Has the sled been expunged? If so, expunge everything on this
            // sled from the blueprint.
            SledPolicy::Expunged => {
                match self.blueprint.current_sled_state(sled_id)? {
                    SledState::Active => {
                        self.blueprint.expunge_sled(sled_id)?;
                    }
                    // If the sled is decommissioned, we've already expunged it
                    // in a prior planning run.
                    SledState::Decommissioned => (),
                }
            }
        }

        Ok(())
    }

    fn check_zones_eligible_for_cleanup(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let Some(sled_inv) = self.inventory.sled_agents.get(&sled_id) else {
            warn!(
                self.log,
                "skipping zones eligible for cleanup check \
                (sled not present in latest inventory collection)";
                "sled_id" => %sled_id,
            );
            return Ok(());
        };

        let mut zones_ready_for_cleanup = Vec::new();
        for zone in self
            .blueprint
            .current_sled_zones(sled_id, BlueprintZoneDisposition::is_expunged)
        {
            // If this is a zone still waiting for cleanup, grab the generation
            // in which it was expunged. Otherwise, move on.
            let as_of_generation = match zone.disposition {
                BlueprintZoneDisposition::Expunged {
                    as_of_generation,
                    ready_for_cleanup,
                } if !ready_for_cleanup => as_of_generation,
                BlueprintZoneDisposition::InService
                | BlueprintZoneDisposition::Expunged { .. } => continue,
            };

            // If the sled hasn't done any reconciliation, wait until it has.
            let Some(reconciliation) = &sled_inv.last_reconciliation else {
                continue;
            };

            // If the sled hasn't reconciled a new-enough generation, wait until
            // it has.
            if reconciliation.last_reconciled_config.generation
                < as_of_generation
            {
                continue;
            }

            // If the sled hasn't shut down the zone, wait until it has.
            if reconciliation.zones.contains_key(&zone.id) {
                continue;
            }

            // All checks passed: we can mark this zone as ready for cleanup.
            zones_ready_for_cleanup.push(zone.id);
        }

        if !zones_ready_for_cleanup.is_empty() {
            self.blueprint.mark_expunged_zones_ready_for_cleanup(
                sled_id,
                &zones_ready_for_cleanup,
            )?;
        }

        Ok(())
    }

    fn do_plan_noop_image_source(
        &mut self,
        noop_info: NoopConvertInfo,
    ) -> Result<PlanningNoopImageSourceStepReport, Error> {
        let mut report = PlanningNoopImageSourceStepReport::new();

        let sleds = match noop_info {
            NoopConvertInfo::GlobalEligible { sleds } => sleds,
            NoopConvertInfo::GlobalIneligible { .. } => return Ok(report),
        };
        for sled in sleds {
            let eligible = match &sled.status {
                NoopConvertSledStatus::Ineligible(_) => continue,
                NoopConvertSledStatus::Eligible(eligible) => eligible,
            };

            let zone_counts = eligible.zone_counts();
            let skipped_zones = if zone_counts.num_install_dataset() == 0 {
                report.sled_zones_all_already_artifact(sled.sled_id);
                true
            } else {
                false
            };

            let skipped_host_phase_2 =
                if eligible.host_phase_2.both_already_artifact() {
                    report
                        .sled_host_phase_2_both_already_artifact(sled.sled_id);
                    true
                } else {
                    false
                };

            if skipped_zones && skipped_host_phase_2 {
                // Nothing to do, continue to the next sled.
                continue;
            }

            if zone_counts.num_eligible > 0
                || eligible.host_phase_2.slot_a.is_eligible()
                || eligible.host_phase_2.slot_b.is_eligible()
            {
                report.converted(
                    sled.sled_id,
                    zone_counts.num_eligible,
                    zone_counts.num_install_dataset(),
                    eligible.host_phase_2.slot_a.is_eligible(),
                    eligible.host_phase_2.slot_b.is_eligible(),
                );
            }

            for zone in &eligible.zones {
                match &zone.status {
                    NoopConvertZoneStatus::Eligible(new_image_source) => {
                        self.blueprint.sled_set_zone_source(
                            sled.sled_id,
                            zone.zone_id,
                            new_image_source.clone(),
                        )?;
                    }
                    NoopConvertZoneStatus::AlreadyArtifact { .. }
                    | NoopConvertZoneStatus::Ineligible(_) => {}
                }
            }

            if zone_counts.num_eligible > 0 {
                self.blueprint.record_operation(
                    Operation::SledNoopZoneImageSourcesUpdated {
                        sled_id: sled.sled_id,
                        count: zone_counts.num_eligible,
                    },
                );
            }

            // Now convert over host phase 2 contents.
            match &eligible.host_phase_2.slot_a {
                NoopConvertHostPhase2Contents::Eligible(new_contents) => {
                    self.blueprint.sled_set_host_phase_2_slot(
                        sled.sled_id,
                        M2Slot::A,
                        new_contents.clone(),
                    )?;
                }
                NoopConvertHostPhase2Contents::AlreadyArtifact { .. }
                | NoopConvertHostPhase2Contents::Ineligible(_) => {}
            }

            match &eligible.host_phase_2.slot_b {
                NoopConvertHostPhase2Contents::Eligible(new_contents) => {
                    self.blueprint.sled_set_host_phase_2_slot(
                        sled.sled_id,
                        M2Slot::B,
                        new_contents.clone(),
                    )?;
                }
                NoopConvertHostPhase2Contents::AlreadyArtifact { .. }
                | NoopConvertHostPhase2Contents::Ineligible(_) => {}
            }

            if eligible.host_phase_2.slot_a.is_eligible()
                || eligible.host_phase_2.slot_b.is_eligible()
            {
                self.blueprint.record_operation(
                    Operation::SledNoopHostPhase2Updated {
                        sled_id: sled.sled_id,
                        slot_a_updated: eligible
                            .host_phase_2
                            .slot_a
                            .is_eligible(),
                        slot_b_updated: eligible
                            .host_phase_2
                            .slot_b
                            .is_eligible(),
                    },
                );
            }
        }

        Ok(report)
    }

    fn do_plan_add(
        &mut self,
        mgs_updates: &PlanningMgsUpdatesStepReport,
    ) -> Result<PlanningAddStepReport, Error> {
        let mut report = PlanningAddStepReport::new();

        // Internal DNS is a prerequisite for bringing up all other zones.  At
        // this point, we assume that internal DNS (as a service) is already
        // functioning.

        // After we make our initial pass through the sleds below to check for
        // zones every sled should have (NTP, Crucible), we'll start making
        // decisions about placing other service zones. We need to _exclude_ any
        // sleds for which we just added an NTP zone, as we won't be able to add
        // additional services to them until that NTP zone has been brought up.
        //
        // We will not mark sleds getting Crucible zones as ineligible; other
        // control plane service zones starting concurrently with Crucible zones
        // is fine.
        for (sled_id, sled_resources) in
            self.input.all_sled_resources(SledFilter::InService)
        {
            // First, we need to ensure that sleds are using their expected
            // disks. This is necessary before we can allocate any zones.
            let sled_edits =
                self.blueprint.sled_add_disks(sled_id, &sled_resources)?;

            if let EnsureMultiple::Changed {
                added,
                updated,
                expunged: _,
                removed,
            } = sled_edits.disks.into()
            {
                info!(
                    &self.log,
                    "altered physical disks";
                    "sled_id" => %sled_id,
                    "sled_edits" => ?sled_edits,
                );
                self.blueprint.record_operation(Operation::UpdateDisks {
                    sled_id,
                    added,
                    updated,
                    removed,
                });
            }

            // The first thing we want to do with any sled is ensure it has an
            // NTP zone. However, this requires the sled to have at least one
            // available zpool on which we could place a zone. There are at
            // least two expected cases where we'll see a sled here with no
            // in-service zpools:
            //
            // 1. A sled was just added and its disks have not yet been adopted
            //    by the control plane.
            // 2. A sled has had all of its disks expunged.
            //
            // In either case, we can't do anything with the sled, but we don't
            // want to fail planning entirely. Just skip sleds in this state.
            if sled_resources
                .all_zpools(ZpoolFilter::InService)
                .next()
                .is_none()
            {
                report.sleds_without_zpools_for_ntp_zones.insert(sled_id);
                report.sleds_waiting_for_ntp_zone.insert(sled_id);
                continue;
            }

            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_ntp(
                sled_id,
                self.image_source_for_new_zone(
                    ZoneKind::InternalNtp,
                    mgs_updates,
                )?,
            )? == Ensure::Added
            {
                report.sleds_missing_ntp_zone.insert(sled_id);
                self.blueprint.record_operation(Operation::AddZone {
                    sled_id,
                    kind: ZoneKind::InternalNtp,
                });

                // If we're setting up a new sled (the typical reason to add a
                // new NTP zone), we don't want make any other changes to this
                // sled.  However, this change is compatible with any other
                // changes to other sleds, so we can "continue" here rather than
                // "break".
                //
                // However, we could be _replacing_ an NTP zone (e.g., if the
                // disk on which a previous NTP zone was running was expunged).
                // If the sled is still running some other control plane
                // services (which is evidence it previously had an NTP zone!),
                // we can go ahead and consider it eligible for new ones.
                if self
                    .blueprint
                    .current_sled_zones(
                        sled_id,
                        BlueprintZoneDisposition::is_in_service,
                    )
                    .any(|z| {
                        OmicronZoneType::from(z.zone_type.clone())
                            .requires_timesync()
                    })
                {
                    report
                        .sleds_getting_ntp_and_discretionary_zones
                        .insert(sled_id);
                } else {
                    report.sleds_waiting_for_ntp_zone.insert(sled_id);
                    continue;
                }
            }

            // Now we've established that the current blueprint _says_ there's
            // an NTP zone on this system.  But we must wait for it to actually
            // be there before we can proceed to add anything else.  Otherwise,
            // we may wind up trying to provision this zone at the same time as
            // other zones, and Sled Agent will reject requests to provision
            // other zones before the clock is synchronized.
            //
            // Note that it's always possible that the NTP zone was added after
            // this inventory was collected (in which case we'll erroneously
            // choose to bail out here, but we'll pick it up again next time
            // we're invoked).  It's conceivable that the NTP zone was removed
            // after this inventory was collected (in which case we'd be making
            // a wrong decision here).  However, we don't ever do this today.
            // If we were to do something like that (maybe as part of upgrading
            // the NTP zone or switching between an internal NTP vs. boundary
            // NTP zone), we'll need to be careful how we do it to avoid a
            // problem here.
            //
            // TODO-cleanup The above comment is now overly conservative;
            // sled-agent won't reject configs just because time isn't sync'd
            // yet. We may be able to remove this check entirely, but we'd need
            // to do some testing to confirm no surprises. (It's probably also
            // fine to keep it for now; removing it just saves us an extra
            // planning iteration when adding a new sled.)
            let has_ntp_inventory = self
                .inventory
                .sled_agents
                .get(&sled_id)
                .map(|sled_agent| {
                    sled_agent.last_reconciliation.as_ref().map_or(
                        false,
                        |reconciliation| {
                            reconciliation
                                .running_omicron_zones()
                                .any(|z| z.zone_type.is_ntp())
                        },
                    )
                })
                .unwrap_or(false);
            if !has_ntp_inventory {
                report.sleds_without_ntp_zones_in_inventory.insert(sled_id);
                continue;
            }

            // Every provisionable zpool on the sled should have a Crucible zone
            // on it.
            let mut ncrucibles_added = 0;
            for zpool_id in sled_resources.all_zpools(ZpoolFilter::InService) {
                if self.blueprint.sled_ensure_zone_crucible(
                    sled_id,
                    *zpool_id,
                    self.image_source_for_new_zone(
                        ZoneKind::Crucible,
                        mgs_updates,
                    )?,
                )? == Ensure::Added
                {
                    report.missing_crucible_zone(sled_id, *zpool_id);
                    ncrucibles_added += 1;
                }
            }

            if ncrucibles_added > 0 {
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                // (Yes, it's currently the last thing in the loop, but being
                // explicit here means we won't forget to do this when more code
                // is added below.)
                self.blueprint.record_operation(Operation::AddZone {
                    sled_id,
                    kind: ZoneKind::Crucible,
                });
                continue;
            }
        }

        self.do_plan_add_discretionary_zones(mgs_updates, &mut report)?;

        // Now that we've added all the disks and zones we plan on adding,
        // ensure that all sleds have the datasets they need to have.
        self.do_plan_datasets(&mut report)?;

        Ok(report)
    }

    fn do_plan_datasets(
        &mut self,
        _report: &mut PlanningAddStepReport,
    ) -> Result<(), Error> {
        for sled_id in self.input.all_sled_ids(SledFilter::InService) {
            if let EnsureMultiple::Changed {
                added,
                updated,
                expunged,
                removed,
            } = self.blueprint.sled_ensure_zone_datasets(sled_id)?
            {
                info!(
                    &self.log,
                    "altered datasets";
                    "sled_id" => %sled_id,
                    "added" => added,
                    "updated" => updated,
                    "expunged" => expunged,
                    "removed" => removed,
                );
                self.blueprint.record_operation(Operation::UpdateDatasets {
                    sled_id,
                    added,
                    updated,
                    expunged,
                    removed,
                });
            }
        }
        Ok(())
    }

    fn do_plan_add_discretionary_zones(
        &mut self,
        mgs_updates: &PlanningMgsUpdatesStepReport,
        report: &mut PlanningAddStepReport,
    ) -> Result<(), Error> {
        // We usually don't need to construct an `OmicronZonePlacement` to add
        // discretionary zones, so defer its creation until it's needed.
        let mut zone_placement = None;

        // Likewise, we usually don't need to create an
        // `ExternalNetworkingAllocator` to add discretionary zones, so defer
        // its creation until it's needed.
        let mut external_networking_alloc = None;

        for zone_kind in [
            DiscretionaryOmicronZone::BoundaryNtp,
            DiscretionaryOmicronZone::Clickhouse,
            DiscretionaryOmicronZone::ClickhouseKeeper,
            DiscretionaryOmicronZone::ClickhouseServer,
            DiscretionaryOmicronZone::CockroachDb,
            DiscretionaryOmicronZone::CruciblePantry,
            DiscretionaryOmicronZone::InternalDns,
            DiscretionaryOmicronZone::ExternalDns,
            DiscretionaryOmicronZone::Oximeter,
            // Nexus only wants placement if no other zones are pending - leave
            // it last in this list so it has visibility into the placement of
            // other discretionary zones.
            DiscretionaryOmicronZone::Nexus,
        ] {
            // Our goal here is to make sure that if we have less redundancy for
            // discretionary zones than needed, we deploy additional zones.
            //
            // For most zone types, we only care about the total count of that
            // kind of zone, regardless of image. In contrast, for Nexus, we may
            // need to reach a minimum redundancy count for multiple zone images
            // (new and old) during a handoff.
            let image_sources = match zone_kind {
                DiscretionaryOmicronZone::Nexus => {
                    let new_repo = self.input.tuf_repo().description();
                    let new_image =
                        new_repo.zone_image_source(zone_kind.into())?;

                    let mut images = vec![];
                    let nexus_in_charge_image =
                        self.lookup_current_nexus_image()?;

                    // Verify that all zones other than Nexus are updated and
                    // using the new image. Note that this is considering both
                    // zones from the parent blueprint, as well as zones
                    // that we might be adding in this current planning pass.
                    let all_non_nexus_zones_updated =
                        report.discretionary_zones_placed.is_empty()
                            && self.all_non_nexus_zones_using_new_image()?;
                    let new_image_is_different =
                        nexus_in_charge_image != new_image;

                    // We may still want to deploy the old image alongside
                    // the new image: if we're running the "old version of a
                    // Nexus" currently, we need to ensure we have
                    // redundancy before the handoff completes.
                    images.push(nexus_in_charge_image);

                    // If all other zones are using their new images, ensure we
                    // start Nexus zones from their new image.
                    //
                    // NOTE: Checking `all_non_nexus_zones_updated` shouldn't be
                    // strictly necessary! It should be fine to launch the new
                    // Nexus before other zone updates; due to Nexus handoff
                    // implementation, it should boot and remain idle.
                    if all_non_nexus_zones_updated && new_image_is_different {
                        images.push(new_image);
                    }
                    images
                }
                _ => {
                    vec![self.image_source_for_new_zone(
                        zone_kind.into(),
                        mgs_updates,
                    )?]
                }
            };

            for image_source in image_sources {
                let num_zones_to_add = self.num_additional_zones_needed(
                    zone_kind,
                    &image_source,
                    report,
                );
                if num_zones_to_add == 0 {
                    continue;
                }
                // We need to add at least one zone; construct our
                // `zone_placement` (or reuse the existing one if a previous
                // loop iteration already created it)...
                let zone_placement = zone_placement.get_or_insert_with(|| {
                    // This constructs a picture of the sleds as we currently
                    // understand them, as far as which sleds have discretionary
                    // zones. This will remain valid as we loop through the
                    // `zone_kind`s in this function, as any zone additions will
                    // update the `zone_placement` heap in-place.
                    let current_discretionary_zones = self
                        .input
                        .all_sled_resources(SledFilter::Discretionary)
                        .filter(|(sled_id, _)| {
                            !report.sleds_waiting_for_ntp_zone.contains(&sled_id)
                        })
                        .map(|(sled_id, sled_resources)| {
                            OmicronZonePlacementSledState {
                                sled_id,
                                num_zpools: sled_resources
                                    .all_zpools(ZpoolFilter::InService)
                                    .count(),
                                discretionary_zones: self
                                    .blueprint
                                    .current_sled_zones(
                                        sled_id,
                                        BlueprintZoneDisposition::is_in_service,
                                    )
                                    .filter_map(|zone| {
                                        DiscretionaryOmicronZone::from_zone_type(
                                            &zone.zone_type,
                                        )
                                    })
                                    .collect(),
                            }
                        });
                    OmicronZonePlacement::new(current_discretionary_zones)
                });
                // ...and our external networking allocator. (We can't use
                // `get_or_insert_with()` because we can't bubble out the error,
                // so recreate the same effect manually.)
                let external_networking_alloc =
                    match external_networking_alloc.as_mut() {
                        Some(allocator) => allocator,
                        None => {
                            let allocator =
                            ExternalNetworkingAllocator::from_current_zones(
                                &self.blueprint,
                                self.input.external_ip_policy(),
                            )
                            .map_err(Error::ExternalNetworkingAllocator)?;
                            external_networking_alloc = Some(allocator);
                            external_networking_alloc.as_mut().unwrap()
                        }
                    };

                self.add_discretionary_zones(
                    zone_placement,
                    external_networking_alloc,
                    zone_kind,
                    num_zones_to_add,
                    image_source,
                    report,
                )?;
            }
        }

        Ok(())
    }

    /// Given the current blueprint state and policy, returns the number of
    /// additional zones needed of the given `zone_kind` to satisfy the policy.
    fn num_additional_zones_needed(
        &mut self,
        discretionary_zone_kind: DiscretionaryOmicronZone,
        image_source: &BlueprintZoneImageSource,
        report: &mut PlanningAddStepReport,
    ) -> usize {
        // Count the number of `kind` zones on all in-service sleds. This
        // will include sleds that are in service but not eligible for new
        // services, but will not include sleds that have been expunged or
        // decommissioned.
        let mut num_existing_kind_zones = 0;
        for sled_id in self.input.all_sled_ids(SledFilter::InService) {
            let zone_kind = ZoneKind::from(discretionary_zone_kind);

            // Internal DNS is special: if we have an expunged internal DNS zone
            // that might still be running, we want to count it here: we can't
            // reuse its subnet until it's ready for cleanup. For all other
            // services, we want to go ahead and replace them if they're below
            // the desired count based on purely "in service vs expunged".
            let disposition_filter = if zone_kind == ZoneKind::InternalDns {
                BlueprintZoneDisposition::could_be_running
            } else {
                BlueprintZoneDisposition::is_in_service
            };
            num_existing_kind_zones += self
                .blueprint
                .current_sled_zones(sled_id, disposition_filter)
                .filter(|z| {
                    let matches_kind = z.zone_type.kind() == zone_kind;
                    match discretionary_zone_kind {
                        DiscretionaryOmicronZone::Nexus => {
                            let matches_image = z.image_source == *image_source;
                            matches_kind && matches_image
                        }
                        _ => matches_kind,
                    }
                })
                .count();
        }

        let target_count = match discretionary_zone_kind {
            DiscretionaryOmicronZone::BoundaryNtp => {
                self.input.target_boundary_ntp_zone_count()
            }
            DiscretionaryOmicronZone::Clickhouse => {
                self.input.target_clickhouse_zone_count()
            }
            DiscretionaryOmicronZone::ClickhouseKeeper => {
                self.input.target_clickhouse_keeper_zone_count()
            }
            DiscretionaryOmicronZone::ClickhouseServer => {
                self.input.target_clickhouse_server_zone_count()
            }
            DiscretionaryOmicronZone::CockroachDb => {
                self.input.target_cockroachdb_zone_count()
            }
            DiscretionaryOmicronZone::CruciblePantry => {
                self.input.target_crucible_pantry_zone_count()
            }
            DiscretionaryOmicronZone::InternalDns => {
                self.input.target_internal_dns_zone_count()
            }
            DiscretionaryOmicronZone::ExternalDns => {
                // TODO-cleanup: When external DNS addresses are
                // in the policy, this can use the input, too.
                self.blueprint.count_parent_external_dns_zones()
            }
            DiscretionaryOmicronZone::Nexus => {
                self.input.target_nexus_zone_count()
            }
            DiscretionaryOmicronZone::Oximeter => {
                self.input.target_oximeter_zone_count()
            }
        };

        // TODO-correctness What should we do if we have _too many_
        // `zone_kind` zones? For now, just report the number of zones
        // any time we have at least the minimum number.
        let num_zones_to_add =
            target_count.saturating_sub(num_existing_kind_zones);
        if num_zones_to_add == 0 {
            report.sufficient_zones_exist(
                ZoneKind::from(discretionary_zone_kind).report_str(),
                target_count,
                num_existing_kind_zones,
            );
        }
        num_zones_to_add
    }

    /// Attempts to place `num_zones_to_add` new zones of `kind`.
    ///
    /// It is not an error if there are too few eligible sleds to start a
    /// sufficient number of zones; instead, we'll report it and start as
    /// many as we can (up to `num_zones_to_add`).
    fn add_discretionary_zones(
        &mut self,
        zone_placement: &mut OmicronZonePlacement,
        external_networking_alloc: &mut ExternalNetworkingAllocator,
        kind: DiscretionaryOmicronZone,
        num_zones_to_add: usize,
        image_source: BlueprintZoneImageSource,
        report: &mut PlanningAddStepReport,
    ) -> Result<(), Error> {
        // If `kind` is "internal DNS", we'll need to pick subnets, but
        // computing the available subnets isn't free. We could do something
        // fancy with lazy construction, but that gets a little messy. Instead,
        // always construct an iterator, and create an empty iterator for any
        // `kind` that isn't "internal DNS".
        let mut available_internal_dns_subnets = match kind {
            DiscretionaryOmicronZone::InternalDns => {
                Either::Left(self.blueprint.available_internal_dns_subnets()?)
            }
            _ => Either::Right(iter::empty()),
        };

        for i in 0..num_zones_to_add {
            let sled_id = match zone_placement.place_zone(kind) {
                Ok(sled_id) => sled_id,
                Err(PlacementError::NoSledsEligible { .. }) => {
                    // We won't treat this as a hard error; it's possible
                    // (albeit unlikely?) we're in a weird state where we need
                    // more sleds or disks to come online, and we may need to be
                    // able to produce blueprints to achieve that status.
                    report.out_of_eligible_sleds(
                        ZoneKind::from(kind).report_str(),
                        i,
                        num_zones_to_add,
                    );
                    break;
                }
            };

            let image = image_source.clone();
            match kind {
                DiscretionaryOmicronZone::BoundaryNtp => {
                    let external_ip =
                        external_networking_alloc.for_new_boundary_ntp()?;
                    self.blueprint.sled_promote_internal_ntp_to_boundary_ntp(
                        sled_id,
                        image,
                        external_ip,
                    )?
                }
                DiscretionaryOmicronZone::Clickhouse => {
                    self.blueprint.sled_add_zone_clickhouse(sled_id, image)?
                }
                DiscretionaryOmicronZone::ClickhouseKeeper => self
                    .blueprint
                    .sled_add_zone_clickhouse_keeper(sled_id, image)?,
                DiscretionaryOmicronZone::ClickhouseServer => self
                    .blueprint
                    .sled_add_zone_clickhouse_server(sled_id, image)?,
                DiscretionaryOmicronZone::CockroachDb => {
                    self.blueprint.sled_add_zone_cockroachdb(sled_id, image)?
                }
                DiscretionaryOmicronZone::CruciblePantry => self
                    .blueprint
                    .sled_add_zone_crucible_pantry(sled_id, image)?,
                DiscretionaryOmicronZone::InternalDns => {
                    let dns_subnet = available_internal_dns_subnets
                        .next()
                        .ok_or(Error::NoAvailableDnsSubnets)?;
                    self.blueprint.sled_add_zone_internal_dns(
                        sled_id, image, dns_subnet,
                    )?
                }
                DiscretionaryOmicronZone::ExternalDns => {
                    let external_ip =
                        external_networking_alloc.for_new_external_dns()?;
                    self.blueprint.sled_add_zone_external_dns(
                        sled_id,
                        image,
                        external_ip,
                    )?
                }
                DiscretionaryOmicronZone::Nexus => {
                    let external_ip =
                        external_networking_alloc.for_new_nexus()?;
                    let nexus_generation =
                        self.determine_nexus_generation(&image)?;
                    self.blueprint.sled_add_zone_nexus(
                        sled_id,
                        image,
                        external_ip,
                        nexus_generation,
                    )?
                }
                DiscretionaryOmicronZone::Oximeter => {
                    self.blueprint.sled_add_zone_oximeter(sled_id, image)?
                }
            };
            report.discretionary_zone_placed(
                sled_id,
                ZoneKind::from(kind).report_str(),
                &image_source,
            );
        }

        Ok(())
    }

    // Determines the appropriate generation number for a new Nexus zone.
    // This generation is based on the generation number used by existing
    // Nexus zones.
    //
    // The logic is:
    // - If any existing Nexus zone has the same image source, reuse its generation
    // - Otherwise, use the highest existing generation + 1
    // - If no existing zones exist, return an error
    //
    // This function also validates that the determined generation matches the
    // top-level current blueprint generation.
    fn determine_nexus_generation(
        &self,
        image_source: &BlueprintZoneImageSource,
    ) -> Result<Generation, Error> {
        // If any other Nexus in the blueprint has the same image source,
        // use it. Otherwise, use the highest generation number + 1.
        let mut highest_seen_generation = None;
        let mut same_image_nexus_generation = None;

        // Iterate over both existing zones and ones that are actively being placed.
        for (zone, nexus) in self
            .blueprint
            .current_zones(BlueprintZoneDisposition::any)
            .filter_map(|(_sled_id, z)| match &z.zone_type {
                BlueprintZoneType::Nexus(nexus) => Some((z, nexus)),
                _ => None,
            })
        {
            if zone.image_source == *image_source {
                // If the image matches exactly, use it.
                same_image_nexus_generation = Some(nexus.nexus_generation);
                break;
            } else if let Some(generation) = highest_seen_generation {
                // Otherwise, use the generation number if it's the highest
                // we've seen
                if nexus.nexus_generation > generation {
                    highest_seen_generation = Some(nexus.nexus_generation);
                }
            } else {
                // Use it regardless if it's the first generation number we've
                // seen
                highest_seen_generation = Some(nexus.nexus_generation);
            }
        }

        let determined_generation = match same_image_nexus_generation {
            Some(generation) => Some(generation),
            None => highest_seen_generation.map(|r#gen| r#gen.next()),
        };

        let Some(r#gen) = determined_generation else {
            return Err(Error::NoNexusZonesInParentBlueprint);
        };

        Ok(r#gen)
    }

    /// Update at most one MGS-managed device (SP, RoT, etc.), if any are out of
    /// date.
    fn do_plan_mgs_updates(
        &mut self,
        zone_safety_checks: &ZoneSafetyChecks,
    ) -> Result<PlanningMgsUpdatesStepReport, Error> {
        let mut report = PlanningMgsUpdatesStepReport::new();
        // Determine which baseboards we will consider updating.
        //
        // Sleds may be present but not adopted as part of the control plane.
        // In deployed systems, this would probably only happen if a sled was
        // about to be added.  In dev/test environments, it's common to leave
        // some number of sleds out of the control plane for various reasons.
        // Inventory will still report them, but we don't want to touch them.
        let included_sled_baseboards = self
            .input
            .all_sleds(SledFilter::SpsUpdatedByReconfigurator)
            .map(|(sled_id, details)| {
                UpdateableBoard::Sled(
                    Arc::new(details.baseboard_id.clone()),
                    sled_id,
                )
            });

        // For better or worse, switches and PSCs do not have the same idea of
        // being adopted into the control plane.  If they're present, they're
        // part of the system, and we will update them.  Chain them onto the end
        // of all the sled baseboards we're supposed to update.
        let included_baseboards = included_sled_baseboards
            .chain(self.inventory.sps.iter().filter_map(
                |(baseboard_id, sp_state)| match sp_state.sp_type {
                    SpType::Sled => {
                        // Sleds are only updated if they're part of
                        // `included_sled_baseboards`; skip them here.
                        None
                    }
                    SpType::Power => {
                        Some(UpdateableBoard::Power(baseboard_id.clone()))
                    }
                    SpType::Switch => {
                        Some(UpdateableBoard::Switch(baseboard_id.clone()))
                    }
                },
            ))
            .collect();

        // Compute the new set of PendingMgsUpdates.
        let current_updates =
            &self.blueprint.parent_blueprint().pending_mgs_updates;
        let current_artifacts = self.input.tuf_repo().description();
        let impossible_update_policy =
            if self.blueprint.parent_blueprint().time_created
                >= self.input.ignore_impossible_mgs_updates_since()
            {
                ImpossibleUpdatePolicy::Keep
            } else {
                ImpossibleUpdatePolicy::Reevaluate
            };
        let PlannedMgsUpdates {
            pending_updates,
            pending_host_phase_2_changes,
            blocked_mgs_updates,
        } = MgsUpdatePlanner {
            log: &self.log,
            inventory: &self.inventory,
            current_boards: &included_baseboards,
            zone_safety_checks,
            current_updates,
            current_artifacts,
            nmax_updates: NUM_CONCURRENT_MGS_UPDATES,
            impossible_update_policy,
        }
        .plan();
        if pending_updates != *current_updates {
            // This will only add comments if our set of updates changed _and_
            // we have at least one update. If we went from "some updates" to
            // "no updates", that's not really comment-worthy; presumably we'll
            // do something else comment-worthy in a subsequent step.
            for update in pending_updates.iter() {
                self.blueprint.comment(update.description());
            }
        }
        self.blueprint
            .apply_pending_host_phase_2_changes(pending_host_phase_2_changes)?;

        self.blueprint.pending_mgs_updates_replace_all(pending_updates.clone());

        report.pending_mgs_updates = pending_updates;
        report.blocked_mgs_updates = blocked_mgs_updates;
        Ok(report)
    }

    // Returns the zones which appear in the blueprint on commissioned sleds,
    // but which have not been reported by the latest reconciliation result from
    // inventory.
    fn get_zones_not_yet_propagated_to_inventory(
        &self,
    ) -> Vec<ZonePropagationIncomplete<'_>> {
        // We are only interested in non-decommissioned sleds.
        let sleds = self
            .input
            .all_sleds(SledFilter::Commissioned)
            .map(|(id, _details)| id)
            .collect::<Vec<_>>();

        // Wait for zones to appear up-to-date in the inventory.
        let inventory_zones = self
            .inventory
            .all_reconciled_omicron_zones()
            .map(|(z, sa_result)| (z.id, (&z.image_source, sa_result)))
            .collect::<BTreeMap<_, _>>();

        let mut updating = vec![];
        for &sled_id in &sleds {
            // Build a list of zones currently in the blueprint but where
            // inventory has a mismatch or does not know about the zone.
            //
            // What about the case where a zone is in inventory but not in the
            // blueprint? See
            // https://github.com/oxidecomputer/omicron/issues/8589.
            let mut zones_currently_updating = self
                .blueprint
                .current_sled_zones(
                    sled_id,
                    BlueprintZoneDisposition::is_in_service,
                )
                .filter_map(|zone| {
                    let bp_image_source =
                        OmicronZoneImageSource::from(zone.image_source.clone());
                    match inventory_zones.get(&zone.id) {
                        Some((
                            inv_image_source,
                            ConfigReconcilerInventoryResult::Ok,
                        )) if *inv_image_source == &bp_image_source => {
                            // The inventory and blueprint image sources match
                            // -- this means that the zone is up-to-date.
                            None
                        }
                        Some((
                            inv_image_source,
                            ConfigReconcilerInventoryResult::Ok,
                        )) => {
                            // The inventory and blueprint image sources differ.
                            Some(ZonePropagationIncomplete {
                                zone_id: zone.id,
                                zone_kind: zone.kind(),
                                reason:
                                    ZonePropagationStatus::ImageSourceMismatch {
                                        bp_image_source: &zone.image_source,
                                        inv_image_source,
                                    },
                            })
                        }
                        Some((
                            inv_image_source,
                            ConfigReconcilerInventoryResult::Err { message },
                        )) => {
                            // The inventory reports this zone but there was an
                            // error reconciling it (most likely an error
                            // starting the zone).
                            Some(ZonePropagationIncomplete {
                                zone_id: zone.id,
                                zone_kind: zone.kind(),
                                reason:
                                    ZonePropagationStatus::ReconciliationError {
                                        bp_image_source: &zone.image_source,
                                        inv_image_source,
                                        message,
                                    },
                            })
                        }
                        None => {
                            // The blueprint has a zone that inventory does not have.
                            Some(ZonePropagationIncomplete {
                                zone_id: zone.id,
                                zone_kind: zone.kind(),
                                reason:
                                    ZonePropagationStatus::MissingInInventory {
                                        bp_image_source: &zone.image_source,
                                    },
                            })
                        }
                    }
                })
                .collect::<Vec<_>>();
            updating.append(&mut zones_currently_updating);
        }
        updating
    }

    /// Update at most one existing zone to use a new image source.
    fn do_plan_zone_updates(
        &mut self,
        mgs_updates: &PlanningMgsUpdatesStepReport,
        zone_safety_checks: &ZoneSafetyChecks,
    ) -> Result<PlanningZoneUpdatesStepReport, Error> {
        let zones_currently_updating =
            self.get_zones_not_yet_propagated_to_inventory();
        if !zones_currently_updating.is_empty() {
            info!(
                self.log, "some zones not yet up-to-date";
                "zones_currently_updating" => ?zones_currently_updating,
            );
            return Ok(PlanningZoneUpdatesStepReport::waiting_on(
                ZoneUpdatesWaitingOn::InventoryPropagation,
            ));
        }

        let mut report = PlanningZoneUpdatesStepReport::new();

        // Find the zones with out-of-date images
        let out_of_date_zones = self.get_out_of_date_zones();
        for (sled_id, zone, desired_image) in out_of_date_zones.iter() {
            report.out_of_date_zone(*sled_id, zone, desired_image.clone());
        }

        // Of the out-of-date zones, filter out zones that can't be updated yet,
        // either because they're not ready or because it wouldn't be safe to
        // bounce them.
        let (nexus_updateable_zones, non_nexus_updateable_zones): (
            Vec<_>,
            Vec<_>,
        ) = out_of_date_zones
            .into_iter()
            .filter(|(_, zone, _)| {
                if let Some(reason) =
                    zone_safety_checks.zone_unsafe_shutdown_reason(&zone.id)
                {
                    report.unsafe_zones.insert(zone.id, reason.clone());
                    false
                } else {
                    self.are_zones_ready_for_updates(mgs_updates)
                }
            })
            .partition(|(_, zone, _)| zone.zone_type.is_nexus());

        // Try to update the first non-Nexus zone
        if let Some((sled_id, zone, new_image_source)) =
            non_nexus_updateable_zones.first()
        {
            return self.update_or_expunge_zone(
                *sled_id,
                zone,
                new_image_source.clone(),
                report,
            );
        }

        // If the only remaining out-of-date zones are Nexus, verify that
        // handoff has occurred before attempting to expunge them.
        //
        // We intentionally order this after other zone updates to minimize
        // the window where we might report "waiting to update Nexus" if
        // handoff has not occurred yet, and we iterate over all Nexus
        // zones with out-of-date images to fill out the planning report.
        let nexus_updateable_zones = nexus_updateable_zones
            .into_iter()
            .filter_map(|(sled, zone, image)| {
                match self.should_nexus_zone_be_expunged(&zone, &mut report) {
                    Ok(true) => Some(Ok((sled, zone, image))),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;

        if let Some((sled_id, zone, new_image_source)) =
            nexus_updateable_zones.first()
        {
            return self.update_or_expunge_zone(
                *sled_id,
                zone,
                new_image_source.clone(),
                report,
            );
        }

        // No zones to update.
        Ok(report)
    }

    // Returns zones that should (eventually) be updated because their image
    // appears different in the target release.
    //
    // Does not consider whether or not it's safe to shut down these zones.
    fn get_out_of_date_zones(
        &self,
    ) -> Vec<(SledUuid, BlueprintZoneConfig, BlueprintZoneImageSource)> {
        // We are only interested in non-decommissioned sleds.
        let sleds = self
            .input
            .all_sleds(SledFilter::Commissioned)
            .map(|(id, _details)| id)
            .collect::<Vec<_>>();
        let target_release = self.input.tuf_repo().description();

        // Find out of date zones, as defined by zones whose image source does
        // not match what it should be based on our current target release.
        sleds
            .into_iter()
            .flat_map(|sled_id| {
                let log = &self.log;
                self.blueprint
                    .current_sled_zones(
                        sled_id,
                        BlueprintZoneDisposition::is_in_service,
                    )
                    .filter_map(move |zone| {
                        let desired_image_source = match target_release
                            .zone_image_source(zone.zone_type.kind())
                        {
                            Ok(source) => source,
                            Err(err) => {
                                // If we can't tell whether a zone is out of
                                // date, assume it isn't.
                                warn!(
                                    log,
                                    "cannot determine whether zone is \
                                     out of date";
                                    "zone" => ?zone,
                                    InlineErrorChain::new(&err),
                                );
                                return None;
                            }
                        };
                        if zone.image_source != desired_image_source {
                            Some((sled_id, zone.clone(), desired_image_source))
                        } else {
                            None
                        }
                    })
            })
            .collect::<Vec<_>>()
    }

    /// Update a zone to use a new image source, either in-place or by
    /// expunging it and letting it be replaced in a future iteration.
    fn update_or_expunge_zone(
        &mut self,
        sled_id: SledUuid,
        zone: &BlueprintZoneConfig,
        new_image_source: BlueprintZoneImageSource,
        mut report: PlanningZoneUpdatesStepReport,
    ) -> Result<PlanningZoneUpdatesStepReport, Error> {
        let zone_kind = zone.zone_type.kind();

        // We're called by `do_plan_zone_updates()`, which guarantees the
        // `new_image_source` is different from the current image source.
        debug_assert_ne!(zone.image_source, new_image_source);

        match zone_kind {
            ZoneKind::Crucible
            | ZoneKind::Clickhouse
            | ZoneKind::ClickhouseKeeper
            | ZoneKind::ClickhouseServer
            | ZoneKind::CockroachDb => {
                self.blueprint.comment(format!(
                    "updating {:?} zone {} in-place",
                    zone.zone_type.kind(),
                    zone.id
                ));
                report.updated_zone(sled_id, &zone);
                self.blueprint.sled_set_zone_source(
                    sled_id,
                    zone.id,
                    new_image_source,
                )?;
            }
            ZoneKind::BoundaryNtp
            | ZoneKind::CruciblePantry
            | ZoneKind::ExternalDns
            | ZoneKind::InternalDns
            | ZoneKind::InternalNtp
            | ZoneKind::Nexus
            | ZoneKind::Oximeter => {
                self.blueprint.comment(format!(
                    "expunge {:?} zone {} for update",
                    zone.zone_type.kind(),
                    zone.id
                ));
                report.expunged_zone(sled_id, zone);
                self.blueprint.sled_expunge_zone(sled_id, zone.id)?;
            }
        }

        Ok(report)
    }

    /// Perform planning for mupdate overrides, returning a map of sleds to
    /// actions taken.
    fn do_plan_mupdate_override(
        &mut self,
        noop_info: &mut NoopConvertInfo,
    ) -> Result<BTreeMap<SledUuid, EnsureMupdateOverrideAction>, Error> {
        // For each sled, compare what's in the inventory to what's in the
        // blueprint.
        let mut actions_by_sled = BTreeMap::new();
        let log = self.log.new(o!("phase" => "do_plan_mupdate_override"));

        // We use the list of in-service sleds here -- we don't want to alter
        // expunged or decommissioned sleds.
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::InService)
        {
            let log = log.new(o!("sled_id" => sled_id.to_string()));
            let Some(inv_sled) = self.inventory.sled_agents.get(&sled_id)
            else {
                warn!(log, "no inventory found for in-service sled");
                continue;
            };
            let action = self.blueprint.sled_ensure_mupdate_override(
                sled_id,
                &sled_details.baseboard_id,
                inv_sled
                    .zone_image_resolver
                    .mupdate_override
                    .boot_override
                    .as_ref(),
                noop_info,
            )?;
            action.log_to(&log);
            actions_by_sled.insert(sled_id, action);
        }

        // As a result of the action above, did any sleds get a new mupdate
        // override in the blueprint? In that case, halt consideration of
        // updates by setting the target_release_minimum_generation.
        //
        // Note that this is edge-triggered, not level-triggered. This is a
        // domain requirement. Consider what happens if:
        //
        // 1. Let's say the target release generation is 5.
        // 2. A sled is mupdated.
        // 3. As a result of the mupdate, we update the target release minimum
        //    generation to 6.
        // 4. Then, an operator sets the target release generation to 6.
        //
        // At this point, we *do not* want to set the blueprint's minimum
        // generation to 7. We only want to do it if we acknowledged a new sled
        // getting mupdated.
        //
        // Some notes:
        //
        // * We only process sleds that are currently in the inventory. This
        //   means that if some sleds take longer to come back up than others
        //   and the target release is updated in the middle, we'll potentially
        //   bump the minimum generation multiple times, asking the operator to
        //   intervene each time.
        //
        //   It's worth considering ways to mitigate this in the future: for
        //   example, we could ensure that for a particular TUF repo a shared
        //   mupdate override ID is assigned by wicketd, and track the override
        //   IDs that are currently in flight.
        //
        // * We aren't handling errors while fetching the mupdate override here.
        //   We don't have a history of state transitions for the mupdate
        //   override, so we can't do edge-triggered logic. We probably need
        //   another channel to report errors. (But in general, errors should be
        //   rare.)
        if actions_by_sled.values().any(|action| {
            matches!(action, EnsureMupdateOverrideAction::BpSetOverride { .. })
        }) {
            let current = self.blueprint.target_release_minimum_generation();
            let new = self.input.tuf_repo().target_release_generation.next();
            if current == new {
                // No change needed.
                info!(
                    log,
                    "would have updated target release minimum generation, but \
                     it was already set to the desired value, so no change was \
                     needed";
                    "generation" => %current,
                );
            } else {
                if current < new {
                    info!(
                        log,
                        "updating target release minimum generation based on \
                         new set-override actions";
                        "current_generation" => %current,
                        "new_generation" => %new,
                    );
                } else {
                    // It would be very strange for the current value to be
                    // greater than the new value. That would indicate something
                    // like a row being removed from the target release
                    // generation table -- one of the invariants of the target
                    // release generation is that it only moves forward.
                    //
                    // In this case we bail out of planning entirely.
                    return Err(
                        Error::TargetReleaseMinimumGenerationRollback {
                            current,
                            new,
                        },
                    );
                }
                self.blueprint
                    .set_target_release_minimum_generation(current, new)
                    .expect("current value passed in => can't fail");
            }
        }

        Ok(actions_by_sled)
    }

    fn should_plan_add_or_update(
        &self,
        actions_by_sled: &BTreeMap<SledUuid, EnsureMupdateOverrideAction>,
    ) -> Result<Vec<String>, Error> {
        // We need to determine whether to also perform other actions like
        // updating or adding zones. We have to be careful here:
        //
        // * We may have moved existing zones with an Artifact source to using
        //   the install dataset via the BpSetOverride action, but we don't want
        //   to use the install dataset on sleds that weren't MUPdated (because
        //   the install dataset might be ancient).
        //
        // * While any overrides are in place according to inventory, we wait
        //   for the system to recover and don't start new zones on *any* sleds,
        //   or perform any further updates.
        //
        // This decision is level-triggered on the following conditions:
        //
        // 1. If the planning input's target release generation is less than the
        //    minimum generation set in the blueprint, the operator hasn't set a
        //    new generation in the blueprint -- we should wait to decide what
        //    to do until the operator provides an indication.
        //
        // 2. If any sleds have the `remove_mupdate_override` field set in the
        //    blueprint (which is downstream of the inventory reporting the
        //    presence of a mupdate override), then we need to wait until the
        //    `remove_mupdate_override` field is cleared. Until that happens,
        //    we don't want to add zones on *any* sled.
        //
        //    This might seem overly conservative (why block zone additions on
        //    *all* sleds if *any* are currently recovering from a MUPdate?),
        //    but is probably correct for the medium term: we want to minimize
        //    the number of different versions of services running at any time.
        //
        // 3. If any sleds had errors obtaining mupdate override info, the
        //    system is in a corrupt state. The planner will not take any
        //    actions until the error is resolved.
        //
        // 4. If any sleds' deployment units aren't at known versions, then we
        //    shouldn't proceed with adding zones or updating deployment units.
        //    Again, this is driven primarily by the desire to minimize the
        //    number of versions of system software running at any time.
        //
        // What does "any sleds" mean in this context? We don't need to care
        // about decommissioned or expunged sleds, so we consider in-service
        // sleds.
        let mut reasons = Vec::new();

        // Condition 1 above.
        if self.blueprint.target_release_minimum_generation()
            > self.input.tuf_repo().target_release_generation
        {
            reasons.push(format!(
                "current target release generation ({}) is lower than \
                 minimum required by blueprint ({})",
                self.input.tuf_repo().target_release_generation,
                self.blueprint.target_release_minimum_generation(),
            ));
        }

        // Condition 2 above.
        {
            let mut sleds_with_override = BTreeSet::new();
            for sled_id in self.input.all_sled_ids(SledFilter::InService) {
                if self
                    .blueprint
                    .sled_get_remove_mupdate_override(sled_id)?
                    .is_some()
                {
                    sleds_with_override.insert(sled_id);
                }
            }

            if !sleds_with_override.is_empty() {
                reasons.push(format!(
                    "sleds have remove mupdate override set in blueprint: {}",
                    sleds_with_override.iter().join(", ")
                ));
            }
        }

        // Condition 3 above.
        {
            // Note: actions_by_sled doesn't consider sleds that aren't present
            // in this inventory run. This can in very rare cases cause issues
            // where both (a) a server has an error fetching override
            // information (should never happen in normal operation) and (b)
            // there's a blip in inventory data. We consider this an acceptable
            // risk.
            //
            // In the future, we could potentially address this by not
            // proceeding with steps if any sleds are missing from inventory.
            let sleds_with_mupdate_override_errors: BTreeSet<_> =
                actions_by_sled
                    .iter()
                    .filter_map(|(sled_id, action)| {
                        matches!(
                            action,
                            EnsureMupdateOverrideAction::GetOverrideError { .. }
                        )
                        .then_some(*sled_id)
                    })
                    .collect();
            if !sleds_with_mupdate_override_errors.is_empty() {
                reasons.push(format!(
                    "sleds have mupdate override errors: {}",
                    sleds_with_mupdate_override_errors.iter().join(", ")
                ));
            }
        }

        // Condition 4 above.
        {
            let mut sleds_with_non_artifact = BTreeMap::new();
            for sled_id in self.input.all_sled_ids(SledFilter::InService) {
                let mut zones_with_non_artifact = IdOrdMap::new();
                // Are all zone image sources set to Artifact?
                for z in self.blueprint.current_sled_zones(
                    sled_id,
                    BlueprintZoneDisposition::is_in_service,
                ) {
                    match &z.image_source {
                        BlueprintZoneImageSource::InstallDataset => {
                            zones_with_non_artifact.insert_overwrite(z);
                        }
                        BlueprintZoneImageSource::Artifact { .. } => {}
                    }
                }

                // TODO: (https://github.com/oxidecomputer/omicron/issues/8918)
                // We should also check that the boot disk's host phase 2
                // image is a known version.
                //
                // Currently, the blueprint doesn't currently cache information
                // about which disk is the boot disk.
                //
                // * Inventory does have this information, but if a sled isn't
                //   in inventory (due to, say, a transient network error), we
                //   won't be able to make that determination.
                //
                // * So we skip this check under the assumption that if all zone
                //   image sources are at known versions, the host phase 2 image
                //   is also most likely at a known version.
                //
                // We really should explicitly check that the host phase 2 image
                // is known, though!

                if !zones_with_non_artifact.is_empty() {
                    sleds_with_non_artifact
                        .insert(sled_id, zones_with_non_artifact);
                }
            }

            if !sleds_with_non_artifact.is_empty() {
                let mut reason =
                    "sleds have deployment units with image sources \
                     not set to Artifact:\n"
                        .to_owned();
                for (sled_id, zones_with_non_artifact) in
                    &sleds_with_non_artifact
                {
                    swriteln!(
                        reason,
                        "- sled {sled_id}: {} {}",
                        zones_with_non_artifact.len(),
                        if zones_with_non_artifact.len() == 1 {
                            "zone"
                        } else {
                            "zones"
                        }
                    );
                }

                reasons.push(reason);
            }
        }

        Ok(reasons)
    }

    // Determines whether or not the top-level "nexus_generation"
    // value should be increased.
    //
    // Doing so will be a signal for all running Nexus instances at
    // lower versions to start quiescing, and to perform handoff.
    fn do_plan_nexus_generation_update(
        &mut self,
    ) -> Result<PlanningNexusGenerationBumpReport, Error> {
        let mut report = PlanningNexusGenerationBumpReport::new();

        // Nexus can only be updated if all non-Nexus zones have been
        // updated, i.e., their image source is an artifact from the new
        // repo.
        let new_repo = self.input.tuf_repo().description();

        // If we don't actually have a TUF repo here, we can't do
        // updates anyway; any return value is fine.
        if new_repo.tuf_repo().is_none() {
            return Ok(report);
        }

        // Check that all in-service zones (other than Nexus) on all
        // sleds have an image source consistent with `new_repo`.
        if !self.all_non_nexus_zones_using_new_image()? {
            report.set_waiting_on(
                NexusGenerationBumpWaitingOn::FoundOldNonNexusZones,
            );
            return Ok(report);
        }

        // In order to do a handoff, there must be Nexus instances at the
        // proposed generation number. These Nexuses must also have records in
        // "db_metadata_nexus" (this is verified by checking that new Nexuses
        // have entries in "self.input.not_yet_nexus_zones").
        let current_generation = self.blueprint.nexus_generation();
        let proposed_generation = self.blueprint.nexus_generation().next();
        let mut old_nexuses_at_current_gen = 0;
        let mut nexuses_at_proposed_gen = 0;
        let mut nexuses_at_proposed_gen_missing_metadata_record = 0;
        for sled_id in self.blueprint.sled_ids_with_zones() {
            for z in self.blueprint.current_sled_zones(
                sled_id,
                BlueprintZoneDisposition::is_in_service,
            ) {
                if let BlueprintZoneType::Nexus(nexus_zone) = &z.zone_type {
                    if nexus_zone.nexus_generation == proposed_generation {
                        nexuses_at_proposed_gen += 1;
                        if !self.input.not_yet_nexus_zones().contains(&z.id) {
                            nexuses_at_proposed_gen_missing_metadata_record +=
                                1;
                        }
                    }

                    if nexus_zone.nexus_generation == current_generation
                        && z.image_source
                            != new_repo.zone_image_source(z.zone_type.kind())?
                    {
                        old_nexuses_at_current_gen += 1;
                    }
                }
            }
        }

        if old_nexuses_at_current_gen == 0 {
            // If all the current-generation Nexuses are "up-to-date", then we
            // have completed a handoff successfully.
            //
            // In this case, there's nothing to report.
            //
            // Note that we'll continue to hit this case until the next update
            // starts (in other words, then `new_repo` changes).
            return Ok(report);
        }

        if nexuses_at_proposed_gen < self.input.target_nexus_zone_count() {
            // If there aren't enough Nexuses at the next generation, quiescing
            // could be a dangerous operation. Blueprint execution should be
            // able to continue even if the new Nexuses haven't started, but to
            // be conservative, we'll wait for the target count.
            report.set_waiting_on(
                NexusGenerationBumpWaitingOn::MissingNewNexusInBlueprint,
            );
            return Ok(report);
        }

        if nexuses_at_proposed_gen_missing_metadata_record > 0 {
            // There are enough Nexuses at the target generation, but not all of
            // them have records yet. Blueprint execution should fix this, by
            // creating these records.
            report.set_waiting_on(
                NexusGenerationBumpWaitingOn::MissingNexusDatabaseAccessRecords,
            );
            return Ok(report);
        }

        // Confirm that all blueprint zones have propagated to inventory
        let zones_currently_updating =
            self.get_zones_not_yet_propagated_to_inventory();
        if !zones_currently_updating.is_empty() {
            info!(
                self.log, "some zones not yet up-to-date";
                "zones_currently_updating" => ?zones_currently_updating,
            );
            report.set_waiting_on(
                NexusGenerationBumpWaitingOn::MissingNewNexusInInventory,
            );
            return Ok(report);
        }

        // If we're here:
        // - There's a new repo
        // - The current generation of Nexuses are not running an image from the
        // new repo (which means they are "older" from the perspective of the
        // update system)
        // - There are Nexuses running with "current generation + 1"
        // - Those new Nexuses have database metadata records that will let
        // them boot successfully
        // - All non-Nexus zones have updated (i.e., are running images from the
        // new repo)
        // - All other blueprint zones have propagated to inventory
        //
        // If all of these are true, the "zone update" portion of the planner
        // has completed, aside from Nexus, and we're ready for old Nexuses
        // to start quiescing.
        //
        // Blueprint planning and execution will be able to continue past this
        // point, for the purposes of restoring redundancy, expunging sleds,
        // etc. However, making this committment will also halt the creation of
        // new sagas temporarily, as handoff from old to new Nexuses occurs.
        self.blueprint.set_nexus_generation(proposed_generation);
        report.set_next_generation(proposed_generation);

        Ok(report)
    }

    fn do_plan_cockroachdb_settings(
        &mut self,
    ) -> PlanningCockroachdbSettingsStepReport {
        let mut report = PlanningCockroachdbSettingsStepReport::new();

        // Figure out what we should set the CockroachDB "preserve downgrade
        // option" setting to based on the planning input.
        //
        // CockroachDB version numbers look like SemVer but are not. Major
        // version numbers consist of the first *two* components, which
        // represent the year and the Nth release that year. So the major
        // version in "22.2.7" is "22.2".
        //
        // A given major version of CockroachDB is backward compatible with the
        // storage format of the previous major version of CockroachDB. This is
        // shown by the `version` setting, which displays the current storage
        // format version. When `version` is '22.2', versions v22.2.x or v23.1.x
        // can be used to run a node. This allows for rolling upgrades of nodes
        // within the cluster and also preserves the ability to rollback until
        // the new software version can be validated.
        //
        // By default, when all nodes of a cluster are upgraded to a new major
        // version, the upgrade is "auto-finalized"; `version` is changed to the
        // new major version, and rolling back to a previous major version of
        // CockroachDB is no longer possible.
        //
        // The `cluster.preserve_downgrade_option` setting can be used to
        // control this. This setting can only be set to the current value
        // of the `version` setting, and when it is set, CockroachDB will not
        // perform auto-finalization. To perform finalization and finish the
        // upgrade, a client must reset the "preserve downgrade option" setting.
        // Finalization occurs in the background, and the "preserve downgrade
        // option" setting should not be changed again until finalization
        // completes.
        //
        // We determine the appropriate value for `preserve_downgrade_option`
        // based on:
        //
        // 1. the _target_ cluster version from the `Policy` (what we want to
        //    be running)
        // 2. the `version` setting reported by CockroachDB (what we're
        //    currently running)
        //
        // by saying:
        //
        // - If we don't recognize the `version` CockroachDB reports, we will
        //   do nothing.
        // - If our target version is _equal to_ what CockroachDB reports,
        //   we will ensure `preserve_downgrade_option` is set to the current
        //   `version`. This prevents auto-finalization when we deploy the next
        //   major version of CockroachDB as part of an update.
        // - If our target version is _older than_ what CockroachDB reports, we
        //   will also ensure `preserve_downgrade_option` is set to the current
        //   `version`. (This will happen on newly-initialized clusters when
        //   we deploy a version of CockroachDB that is newer than our current
        //   policy.)
        // - If our target version is _newer than_ what CockroachDB reports, we
        //   will ensure `preserve_downgrade_option` is set to the default value
        //   (the empty string). This will trigger finalization.

        let policy = self.input.target_cockroachdb_cluster_version();
        let CockroachDbSettings { version, .. } =
            self.input.cockroachdb_settings();
        let value = match CockroachDbClusterVersion::from_str(version) {
            // The current version is known to us.
            Ok(version) => {
                if policy > version {
                    // Ensure `cluster.preserve_downgrade_option` is reset so we
                    // can upgrade.
                    CockroachDbPreserveDowngrade::AllowUpgrade
                } else {
                    // The cluster version is equal to or newer than the
                    // version we want by policy. In either case, ensure
                    // `cluster.preserve_downgrade_option` is set.
                    CockroachDbPreserveDowngrade::Set(version)
                }
            }
            // The current version is unknown to us; we are likely in the middle
            // of an cluster upgrade.
            Err(_) => CockroachDbPreserveDowngrade::DoNotModify,
        };
        self.blueprint.cockroachdb_preserve_downgrade(value);
        report.preserve_downgrade = value;
        report

        // Hey! Listen!
        //
        // If we need to manage more CockroachDB settings, we should ensure
        // that no settings will be modified if we don't recognize the current
        // cluster version -- we're likely in the middle of an upgrade!
        //
        // https://www.cockroachlabs.com/docs/stable/cluster-settings#change-a-cluster-setting
    }

    fn do_plan_clickhouse_cluster_settings(&mut self) {
        let new_config = self.generate_current_clickhouse_cluster_config();
        self.blueprint.set_clickhouse_cluster_config(new_config);
    }

    fn generate_current_clickhouse_cluster_config(
        &mut self,
    ) -> Option<ClickhouseClusterConfig> {
        if !self.input.clickhouse_cluster_enabled() {
            if self.blueprint.clickhouse_cluster_config().is_some() {
                info!(
                    self.log,
                    "clickhouse cluster disabled via policy: \
                     discarding existing 'ClickhouseAllocator' and \
                     the resulting generated 'ClickhouseClusterConfig"
                );
            }
            return None;
        }

        // If we have the clickhouse cluster setup enabled via policy and we
        // don't yet have a `ClickhouseClusterConfiguration`, then we must
        // create one and feed it to our `ClickhouseAllocator`.
        let parent_config =
            self.blueprint.clickhouse_cluster_config().cloned().unwrap_or_else(
                || {
                    info!(
                        self.log,
                        "Clickhouse cluster enabled by policy: \
                         generating initial ClickhouseClusterConfig"
                    );
                    self.blueprint.make_empty_clickhouse_cluster_config()
                },
            );
        let allocator = ClickhouseAllocator::new(
            self.log.clone(),
            parent_config,
            self.inventory.latest_clickhouse_keeper_membership(),
        );
        let should_be_running =
            ClickhouseZonesThatShouldBeRunning::new(&self.blueprint);

        match allocator.plan(&should_be_running) {
            Ok(config) => Some(config),
            Err(err) => {
                error!(
                    self.log, "clickhouse allocator planning failed";
                    InlineErrorChain::new(&err),
                );

                // If planning the new config failed, carry forward our
                // parent's config.
                Some(allocator.into_parent_config())
            }
        }
    }

    /// Return the image source for zones that we need to add.
    fn image_source_for_new_zone(
        &self,
        zone_kind: ZoneKind,
        mgs_updates: &PlanningMgsUpdatesStepReport,
    ) -> Result<BlueprintZoneImageSource, TufRepoContentsError> {
        let source_repo = if self.are_zones_ready_for_updates(mgs_updates) {
            self.input.tuf_repo().description()
        } else {
            self.input.old_repo().description()
        };
        source_repo.zone_image_source(zone_kind)
    }

    /// Return `true` iff a zone is ready to be updated; i.e., its dependencies
    /// have been updated.
    fn are_zones_ready_for_updates(
        &self,
        mgs_updates: &PlanningMgsUpdatesStepReport,
    ) -> bool {
        // We return false for all zone kinds if there are still
        // pending updates for components earlier in the update ordering
        // than zones: RoT bootloader / RoT / SP / Host OS.
        mgs_updates.is_empty()
    }

    fn all_non_nexus_zones_using_new_image(&self) -> Result<bool, Error> {
        let new_repo = self.input.tuf_repo().description();
        for sled_id in self.blueprint.sled_ids_with_zones() {
            for z in self.blueprint.current_sled_zones(
                sled_id,
                BlueprintZoneDisposition::is_in_service,
            ) {
                let kind = z.zone_type.kind();
                if kind != ZoneKind::Nexus
                    && z.image_source != new_repo.zone_image_source(kind)?
                {
                    return Ok(false);
                }
            }
        }
        return Ok(true);
    }

    fn lookup_current_nexus_image(
        &self,
    ) -> Result<BlueprintZoneImageSource, Error> {
        // Look up the active Nexus zone in the blueprint to get its image.
        //
        // Use the *current* blueprint, not the *parent* blueprint, because zone
        // image sources are mutable. In particular, mupdate overrides and noop
        // conversions can change the image source.
        let mut image_source = None;
        for sled_id in self.input.all_sled_ids(SledFilter::InService) {
            for zone in self.blueprint.current_sled_zones(
                sled_id,
                BlueprintZoneDisposition::is_in_service,
            ) {
                if self.input.active_nexus_zones().contains(&zone.id) {
                    image_source = Some(zone.image_source.clone());
                    break;
                }
            }
        }

        if let Some(image) = image_source {
            Ok(image)
        } else {
            Err(Error::NoActiveNexusZonesInBlueprint)
        }
    }

    fn lookup_current_nexus_generation(&self) -> Result<Generation, Error> {
        // Look up the active Nexus zone in the blueprint to get its generation.
        //
        // The Nexus generation is immutable, so it's fine (and easier in this
        // case) to look at the parent blueprint.
        self.blueprint
            .parent_blueprint()
            .find_generation_for_nexus(self.input.active_nexus_zones())
            .map_err(|_| Error::NoActiveNexusZonesInBlueprint)?
            .ok_or(Error::NoActiveNexusZonesInBlueprint)
    }

    // Returns whether the out-of-date Nexus zone is ready to be updated.
    //
    // For reporting purposes, we assume that we want the supplied
    // zone to be expunged or updated because it is out-of-date.
    //
    // If the zone should not be updated yet, updates the planner report to
    // identify why it is not ready for update.
    //
    // Precondition: zone must be a Nexus zone and be running an out-of-date
    // image
    fn should_nexus_zone_be_expunged(
        &self,
        zone: &BlueprintZoneConfig,
        report: &mut PlanningZoneUpdatesStepReport,
    ) -> Result<bool, Error> {
        let zone_nexus_generation = match &zone.zone_type {
            // For Nexus, we're only ready to "update" this zone once control
            // has been handed off to a newer generation of Nexus zones.  (Once
            // that happens, we're not really going to update this zone, just
            // expunge it.)
            BlueprintZoneType::Nexus(nexus_zone) => {
                // Get the nexus_generation of the zone being considered for shutdown
                nexus_zone.nexus_generation
            }
            _ => panic!("Not a Nexus zone"),
        };

        // Get the generation of the currently-executing Nexus zones.
        //
        // This presumably includes the currently-executing Nexus where
        // this logic is being considered.
        let current_gen = self.lookup_current_nexus_generation()?;

        // We need to prevent old Nexus zones from shutting themselves
        // down. In other words: it's only safe to shut down if handoff
        // has occurred.
        //
        // That only happens when the current generation of Nexus (the
        // one running right now) does not match the zone we're
        // considering expunging.
        if current_gen == zone_nexus_generation {
            report.waiting_zone(
                zone,
                ZoneWaitingToExpunge::Nexus {
                    zone_generation: zone_nexus_generation,
                },
            );
            return Ok(false);
        }

        Ok(true)
    }
}

/// The reason a sled's zones need to be expunged.
///
/// This is used only for introspection and logging -- it's not part of the
/// logical flow.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ZoneExpungeReason {
    ClickhouseClusterDisabled,
    ClickhouseSingleNodeDisabled,
}
