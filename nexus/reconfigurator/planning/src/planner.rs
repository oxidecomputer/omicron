// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Ensure;
use crate::blueprint_builder::EnsureMultiple;
use crate::blueprint_builder::EnsureMupdateOverrideAction;
use crate::blueprint_builder::Error;
use crate::blueprint_builder::Operation;
use crate::blueprint_editor::DisksEditError;
use crate::blueprint_editor::SledEditError;
use crate::mgs_updates::plan_mgs_updates;
use crate::planner::image_source::NoopConvertZoneStatus;
use crate::planner::omicron_zone_placement::PlacementError;
use gateway_client::types::SpType;
use itertools::Itertools;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::TufRepoContentsError;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::external_api::views::PhysicalDiskPolicy;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_common::policy::BOUNDARY_NTP_REDUNDANCY;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use slog::debug;
use slog::error;
use slog::o;
use slog::{Logger, info, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;

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

mod image_source;
mod omicron_zone_placement;
pub(crate) mod rng;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpdateStepResult {
    ContinueToNextStep,
    Waiting,
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
    ) -> anyhow::Result<Planner<'a>> {
        let blueprint = BlueprintBuilder::new_based_on(
            &log,
            parent_blueprint,
            input,
            inventory,
            creator,
        )?;
        Ok(Planner { log, input, blueprint, inventory })
    }

    /// Within tests, set a seeded RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn with_rng(mut self, rng: PlannerRng) -> Self {
        // This is an owned builder (self rather than &mut self) because it is
        // almost never going to be conditional.
        self.blueprint.set_rng(rng);
        self
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        debug!(
            self.log,
            "running planner with chicken switches";
            self.input.chicken_switches(),
        );
        self.check_input_validity()?;
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn check_input_validity(&self) -> Result<(), Error> {
        if self.input.target_internal_dns_zone_count() > INTERNAL_DNS_REDUNDANCY
        {
            return Err(Error::PolicySpecifiesTooManyInternalDnsServers);
        }
        Ok(())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        self.do_plan_expunge()?;
        self.do_plan_decommission()?;

        let mut noop_info =
            NoopConvertInfo::new(self.input, self.inventory, &self.blueprint)?;

        let plan_mupdate_override_res =
            self.do_plan_mupdate_override(&mut noop_info)?;

        // Log noop-convert results after do_plan_mupdate_override, because this
        // step might alter noop_info.
        noop_info.log_to(&self.log);

        // Within `do_plan_noop_image_source`, we plan noop image sources on
        // sleds other than those currently affected by mupdate overrides. This
        // means that we don't have to wait for the `plan_mupdate_override_res`
        // result for that step.
        self.do_plan_noop_image_source(noop_info)?;

        // Perform do_plan_add either if plan_mupdate_override_res says to
        // continue, or if the chicken switch is true.
        match (
            plan_mupdate_override_res,
            self.input.chicken_switches().add_zones_with_mupdate_override,
        ) {
            (UpdateStepResult::ContinueToNextStep, _) => {
                self.do_plan_add()?;
            }
            (UpdateStepResult::Waiting, true) => {
                debug!(
                    self.log,
                    "add_zones_with_mupdate_override chicken switch \
                     is true, so running do_plan_add even though \
                     plan_mupdate_override returned Waiting",
                );
                self.do_plan_add()?;
            }
            (UpdateStepResult::Waiting, false) => {
                debug!(
                    self.log,
                    "plan_mupdate_override returned Waiting, and \
                     add_zones_with_mupdate_override chicken switch \
                     is false, so skipping do_plan_add",
                );
            }
        }

        // Perform other steps only if plan_mupdate_override says to continue.
        if let UpdateStepResult::ContinueToNextStep = plan_mupdate_override_res
        {
            // If do_plan_mupdate_override returns Waiting, we don't plan *any*
            // additional steps until the system has recovered.
            if let UpdateStepResult::ContinueToNextStep =
                self.do_plan_mgs_updates()
            {
                self.do_plan_zone_updates()?;
            }
        }

        // CockroachDB settings aren't dependent on zones, so they can be
        // planned independently of the rest of the system.
        self.do_plan_cockroachdb_settings();
        Ok(())
    }

    fn do_plan_decommission(&mut self) -> Result<(), Error> {
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
                // up when we ask for commissioned sleds? Warn, but don't try to
                // decommission it again.
                (SledPolicy::Expunged, SledState::Decommissioned) => {
                    error!(
                        self.log,
                        "decommissioned sled returned by \
                         SledFilter::Commissioned";
                        "sled_id" => %sled_id,
                    );
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

        Ok(())
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

    fn do_plan_expunge(&mut self) -> Result<(), Error> {
        let mut commissioned_sled_ids = BTreeSet::new();

        // Remove services from sleds marked expunged. We use
        // `SledFilter::Commissioned` and have a custom `needs_zone_expungement`
        // function that allows us to produce better errors.
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            commissioned_sled_ids.insert(sled_id);
            self.do_plan_expunge_for_commissioned_sled(sled_id, sled_details)?;
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

        Ok(())
    }

    fn do_plan_expunge_for_commissioned_sled(
        &mut self,
        sled_id: SledUuid,
        sled_details: &SledDetails,
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
                            // fail in this case, but will issue a warning.
                            warn!(
                                self.log,
                                "planning input contained expunged disk not \
                                 present in parent blueprint";
                                "sled_id" => %sled_id,
                                "disk" => ?disk,
                            );
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
    ) -> Result<(), Error> {
        let sleds = match noop_info {
            NoopConvertInfo::GlobalEligible { sleds } => sleds,
            NoopConvertInfo::GlobalIneligible { .. } => return Ok(()),
        };
        for sled in sleds {
            let eligible = match &sled.status {
                NoopConvertSledStatus::Ineligible(_) => continue,
                NoopConvertSledStatus::Eligible(eligible) => eligible,
            };

            let zone_counts = eligible.zone_counts();
            if zone_counts.num_install_dataset() == 0 {
                debug!(
                    self.log,
                    "all zones are already Artifact, so \
                     no noop image source action required";
                    "num_total" => zone_counts.num_total,
                );
                continue;
            }
            if zone_counts.num_eligible > 0 {
                info!(
                    self.log,
                    "noop converting {}/{} install-dataset zones to artifact store",
                    zone_counts.num_eligible,
                    zone_counts.num_install_dataset();
                    "sled_id" => %sled.sled_id,
                    "num_total" => zone_counts.num_total,
                    "num_already_artifact" => zone_counts.num_already_artifact,
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
        }

        Ok(())
    }

    fn do_plan_add(&mut self) -> Result<(), Error> {
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
        let mut sleds_waiting_for_ntp_zone = BTreeSet::new();

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
                info!(
                    self.log,
                    "skipping sled (no zpools in service)";
                    "sled_id" => %sled_id,
                );
                sleds_waiting_for_ntp_zone.insert(sled_id);
                continue;
            }

            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_ntp(
                sled_id,
                self.image_source_for_new_zone(ZoneKind::InternalNtp)?,
            )? == Ensure::Added
            {
                info!(
                    &self.log,
                    "found sled missing NTP zone (will add one)";
                    "sled_id" => %sled_id
                );
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
                    info!(
                        &self.log,
                        "sled getting NTP zone has other services already; \
                         considering it eligible for discretionary zones";
                        "sled_id" => %sled_id,
                    );
                } else {
                    sleds_waiting_for_ntp_zone.insert(sled_id);
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
                info!(
                    &self.log,
                    "parent blueprint contains NTP zone, but it's not in \
                    inventory yet";
                    "sled_id" => %sled_id,
                );
                continue;
            }

            // Every provisionable zpool on the sled should have a Crucible zone
            // on it.
            let mut ncrucibles_added = 0;
            for zpool_id in sled_resources.all_zpools(ZpoolFilter::InService) {
                if self.blueprint.sled_ensure_zone_crucible(
                    sled_id,
                    *zpool_id,
                    self.image_source_for_new_zone(ZoneKind::Crucible)?,
                )? == Ensure::Added
                {
                    info!(
                        &self.log,
                        "found sled zpool missing Crucible zone (will add one)";
                        "sled_id" => ?sled_id,
                        "zpool_id" => ?zpool_id,
                    );
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

        self.do_plan_add_discretionary_zones(&sleds_waiting_for_ntp_zone)?;

        // Now that we've added all the disks and zones we plan on adding,
        // ensure that all sleds have the datasets they need to have.
        self.do_plan_datasets()?;

        Ok(())
    }

    fn do_plan_datasets(&mut self) -> Result<(), Error> {
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
        sleds_waiting_for_ntp_zone: &BTreeSet<SledUuid>,
    ) -> Result<(), Error> {
        // We usually don't need to construct an `OmicronZonePlacement` to add
        // discretionary zones, so defer its creation until it's needed.
        let mut zone_placement = None;

        for zone_kind in [
            DiscretionaryOmicronZone::BoundaryNtp,
            DiscretionaryOmicronZone::Clickhouse,
            DiscretionaryOmicronZone::ClickhouseKeeper,
            DiscretionaryOmicronZone::ClickhouseServer,
            DiscretionaryOmicronZone::CockroachDb,
            DiscretionaryOmicronZone::CruciblePantry,
            DiscretionaryOmicronZone::InternalDns,
            DiscretionaryOmicronZone::ExternalDns,
            DiscretionaryOmicronZone::Nexus,
            DiscretionaryOmicronZone::Oximeter,
        ] {
            let num_zones_to_add = self.num_additional_zones_needed(zone_kind);
            if num_zones_to_add == 0 {
                continue;
            }
            // We need to add at least one zone; construct our `zone_placement`
            // (or reuse the existing one if a previous loop iteration already
            // created it).
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
                        !sleds_waiting_for_ntp_zone.contains(&sled_id)
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
            self.add_discretionary_zones(
                zone_placement,
                zone_kind,
                num_zones_to_add,
            )?;
        }

        Ok(())
    }

    // Given the current blueprint state and policy, returns the number of
    // additional zones needed of the given `zone_kind` to satisfy the policy.
    fn num_additional_zones_needed(
        &mut self,
        zone_kind: DiscretionaryOmicronZone,
    ) -> usize {
        // Count the number of `kind` zones on all in-service sleds. This
        // will include sleds that are in service but not eligible for new
        // services, but will not include sleds that have been expunged or
        // decommissioned.
        let mut num_existing_kind_zones = 0;
        for sled_id in self.input.all_sled_ids(SledFilter::InService) {
            let zone_kind = ZoneKind::from(zone_kind);

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
                .filter(|z| z.zone_type.kind() == zone_kind)
                .count();
        }

        let target_count = match zone_kind {
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
        // `zone_kind` zones? For now, just log it the number of zones any
        // time we have at least the minimum number.
        let num_zones_to_add =
            target_count.saturating_sub(num_existing_kind_zones);
        if num_zones_to_add == 0 {
            info!(
                self.log, "sufficient {zone_kind:?} zones exist in plan";
                "desired_count" => target_count,
                "current_count" => num_existing_kind_zones,
            );
        }
        num_zones_to_add
    }

    // Attempts to place `num_zones_to_add` new zones of `kind`.
    //
    // It is not an error if there are too few eligible sleds to start a
    // sufficient number of zones; instead, we'll log a warning and start as
    // many as we can (up to `num_zones_to_add`).
    fn add_discretionary_zones(
        &mut self,
        zone_placement: &mut OmicronZonePlacement,
        kind: DiscretionaryOmicronZone,
        num_zones_to_add: usize,
    ) -> Result<(), Error> {
        for i in 0..num_zones_to_add {
            let sled_id = match zone_placement.place_zone(kind) {
                Ok(sled_id) => sled_id,
                Err(PlacementError::NoSledsEligible { .. }) => {
                    // We won't treat this as a hard error; it's possible
                    // (albeit unlikely?) we're in a weird state where we need
                    // more sleds or disks to come online, and we may need to be
                    // able to produce blueprints to achieve that status.
                    warn!(
                        self.log,
                        "failed to place all new desired {kind:?} zones";
                        "placed" => i,
                        "wanted_to_place" => num_zones_to_add,
                    );

                    break;
                }
            };

            let image_source = self.image_source_for_new_zone(kind.into())?;
            match kind {
                DiscretionaryOmicronZone::BoundaryNtp => {
                    self.blueprint.sled_promote_internal_ntp_to_boundary_ntp(
                        sled_id,
                        image_source,
                    )?
                }
                DiscretionaryOmicronZone::Clickhouse => self
                    .blueprint
                    .sled_add_zone_clickhouse(sled_id, image_source)?,
                DiscretionaryOmicronZone::ClickhouseKeeper => self
                    .blueprint
                    .sled_add_zone_clickhouse_keeper(sled_id, image_source)?,
                DiscretionaryOmicronZone::ClickhouseServer => self
                    .blueprint
                    .sled_add_zone_clickhouse_server(sled_id, image_source)?,
                DiscretionaryOmicronZone::CockroachDb => self
                    .blueprint
                    .sled_add_zone_cockroachdb(sled_id, image_source)?,
                DiscretionaryOmicronZone::CruciblePantry => self
                    .blueprint
                    .sled_add_zone_crucible_pantry(sled_id, image_source)?,
                DiscretionaryOmicronZone::InternalDns => self
                    .blueprint
                    .sled_add_zone_internal_dns(sled_id, image_source)?,
                DiscretionaryOmicronZone::ExternalDns => self
                    .blueprint
                    .sled_add_zone_external_dns(sled_id, image_source)?,
                DiscretionaryOmicronZone::Nexus => {
                    self.blueprint.sled_add_zone_nexus(sled_id, image_source)?
                }
                DiscretionaryOmicronZone::Oximeter => self
                    .blueprint
                    .sled_add_zone_oximeter(sled_id, image_source)?,
            };
            info!(
                self.log, "added zone to sled";
                "sled_id" => %sled_id,
                "kind" => ?kind,
            );
        }

        Ok(())
    }

    /// Update at most one MGS-managed device (SP, RoT, etc.), if any are out of
    /// date.
    fn do_plan_mgs_updates(&mut self) -> UpdateStepResult {
        // Determine which baseboards we will consider updating.
        //
        // Sleds may be present but not adopted as part of the control plane.
        // In deployed systems, this would probably only happen if a sled was
        // about to be added.  In dev/test environments, it's common to leave
        // some number of sleds out of the control plane for various reasons.
        // Inventory will still report them, but we don't want to touch them.
        //
        // For better or worse, switches and PSCs do not have the same idea of
        // being adopted into the control plane.  If they're present, they're
        // part of the system, and we will update them.
        let included_sled_baseboards: BTreeSet<_> = self
            .input
            .all_sleds(SledFilter::SpsUpdatedByReconfigurator)
            .map(|(_sled_id, details)| &details.baseboard_id)
            .collect();
        let included_baseboards =
            self.inventory
                .sps
                .iter()
                .filter_map(|(baseboard_id, sp_state)| {
                    let do_include = match sp_state.sp_type {
                        SpType::Sled => included_sled_baseboards
                            .contains(baseboard_id.as_ref()),
                        SpType::Power => true,
                        SpType::Switch => true,
                    };
                    do_include.then_some(baseboard_id.clone())
                })
                .collect();

        // Compute the new set of PendingMgsUpdates.
        let current_updates =
            &self.blueprint.parent_blueprint().pending_mgs_updates;
        let current_artifacts = self.input.tuf_repo().description();
        let next = plan_mgs_updates(
            &self.log,
            &self.inventory,
            &included_baseboards,
            &current_updates,
            current_artifacts,
            NUM_CONCURRENT_MGS_UPDATES,
        );

        // TODO This is not quite right.  See oxidecomputer/omicron#8285.
        let rv = if next.is_empty() {
            UpdateStepResult::ContinueToNextStep
        } else {
            UpdateStepResult::Waiting
        };
        self.blueprint.pending_mgs_updates_replace_all(next);
        rv
    }

    /// Update at most one existing zone to use a new image source.
    fn do_plan_zone_updates(&mut self) -> Result<(), Error> {
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

        #[derive(Debug)]
        #[expect(dead_code)]
        struct ZoneCurrentlyUpdating<'a> {
            zone_id: OmicronZoneUuid,
            zone_kind: ZoneKind,
            reason: UpdatingReason<'a>,
        }

        #[derive(Debug)]
        #[expect(dead_code)]
        enum UpdatingReason<'a> {
            ImageSourceMismatch {
                bp_image_source: &'a BlueprintZoneImageSource,
                inv_image_source: &'a OmicronZoneImageSource,
            },
            MissingInInventory {
                bp_image_source: &'a BlueprintZoneImageSource,
            },
            ReconciliationError {
                bp_image_source: &'a BlueprintZoneImageSource,
                inv_image_source: &'a OmicronZoneImageSource,
                message: &'a str,
            },
        }

        for &sled_id in &sleds {
            // Build a list of zones currently in the blueprint but where
            // inventory has a mismatch or does not know about the zone.
            //
            // What about the case where a zone is in inventory but not in the
            // blueprint? See
            // https://github.com/oxidecomputer/omicron/issues/8589.
            let zones_currently_updating = self
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
                            Some(ZoneCurrentlyUpdating {
                                zone_id: zone.id,
                                zone_kind: zone.kind(),
                                reason: UpdatingReason::ImageSourceMismatch {
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
                            Some(ZoneCurrentlyUpdating {
                                zone_id: zone.id,
                                zone_kind: zone.kind(),
                                reason: UpdatingReason::ReconciliationError {
                                    bp_image_source: &zone.image_source,
                                    inv_image_source,
                                    message,
                                },
                            })
                        }
                        None => {
                            // The blueprint has a zone that inventory does not have.
                            Some(ZoneCurrentlyUpdating {
                                zone_id: zone.id,
                                zone_kind: zone.kind(),
                                reason: UpdatingReason::MissingInInventory {
                                    bp_image_source: &zone.image_source,
                                },
                            })
                        }
                    }
                })
                .collect::<Vec<_>>();

            if !zones_currently_updating.is_empty() {
                info!(
                    self.log, "some zones not yet up-to-date";
                    "sled_id" => %sled_id,
                    "zones_currently_updating" => ?zones_currently_updating,
                );
                return Ok(());
            }
        }

        // Find out of date zones, as defined by zones whose image source does
        // not match what it should be based on our current target release.
        let target_release = self.input.tuf_repo().description();
        let mut out_of_date_zones = sleds
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
                            Some((sled_id, zone, desired_image_source))
                        } else {
                            None
                        }
                    })
            })
            .peekable();

        // Before we filter out zones that can't be updated, do we have any out
        // of date zones at all? We need this to explain why we didn't update
        // any zones below, if we don't.
        let have_out_of_date_zones = out_of_date_zones.peek().is_some();

        // Of the out-of-date zones, filter out zones that can't be updated yet,
        // either because they're not ready or because it wouldn't be safe to
        // bounce them.
        let mut updateable_zones =
            out_of_date_zones.filter(|(_sled_id, zone, _new_image_source)| {
                if !self.can_zone_be_shut_down_safely(zone) {
                    return false;
                }
                match self.is_zone_ready_for_update(zone.zone_type.kind()) {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(err) => {
                        // If we can't tell whether a zone is ready for update,
                        // assume it can't be.
                        warn!(
                            self.log,
                            "cannot determine whether zone is ready for update";
                            "zone" => ?zone,
                            InlineErrorChain::new(&err),
                        );
                        false
                    }
                }
            });

        // Update the first out-of-date zone.
        if let Some((sled_id, zone, new_image_source)) = updateable_zones.next()
        {
            // Borrow check workaround: `self.update_or_expunge_zone` needs
            // `&mut self`, but `self` is borrowed in the `updateable_zones`
            // iterator. Clone the one zone we want to update, then drop the
            // iterator; now we can call `&mut self` methods.
            let zone = zone.clone();
            std::mem::drop(updateable_zones);

            return self.update_or_expunge_zone(
                sled_id,
                &zone,
                new_image_source,
            );
        }

        if have_out_of_date_zones {
            info!(
                self.log,
                "not all zones up-to-date, but no zones can be updated now"
            );
        } else {
            info!(self.log, "all zones up-to-date");
        }

        Ok(())
    }

    /// Update a zone to use a new image source, either in-place or by
    /// expunging it and letting it be replaced in a future iteration.
    fn update_or_expunge_zone(
        &mut self,
        sled_id: SledUuid,
        zone: &BlueprintZoneConfig,
        new_image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
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
                info!(
                    self.log, "updating zone image source in-place";
                    "sled_id" => %sled_id,
                    "zone_id" => %zone.id,
                    "kind" => ?zone.zone_type.kind(),
                    "image_source" => %new_image_source,
                );
                self.blueprint.comment(format!(
                    "updating {:?} zone {} in-place",
                    zone.zone_type.kind(),
                    zone.id
                ));
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
                info!(
                    self.log, "expunging out-of-date zone";
                    "sled_id" => %sled_id,
                    "zone_id" => %zone.id,
                    "kind" => ?zone.zone_type.kind(),
                );
                self.blueprint.comment(format!(
                    "expunge {:?} zone {} for update",
                    zone.zone_type.kind(),
                    zone.id
                ));
                self.blueprint.sled_expunge_zone(sled_id, zone.id)?;
            }
        }

        Ok(())
    }

    fn do_plan_mupdate_override(
        &mut self,
        noop_info: &mut NoopConvertInfo,
    ) -> Result<UpdateStepResult, Error> {
        // For each sled, compare what's in the inventory to what's in the
        // blueprint.
        let mut actions_by_sled = BTreeMap::new();
        let log = self.log.new(o!("phase" => "do_plan_mupdate_override"));

        // We use the list of in-service sleds here -- we don't want to alter
        // expunged or decommissioned sleds.
        for sled_id in self.input.all_sled_ids(SledFilter::InService) {
            let log = log.new(o!("sled_id" => sled_id.to_string()));
            let Some(inv_sled) = self.inventory.sled_agents.get(&sled_id)
            else {
                warn!(log, "no inventory found for in-service sled");
                continue;
            };
            let action = self.blueprint.sled_ensure_mupdate_override(
                sled_id,
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

        // Now we need to determine whether to also perform other actions like
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
        // 4. (TODO: https://github.com/oxidecomputer/omicron/issues/8726)
        //    If any sleds' deployment units aren't at known versions after
        //    noop image source changes have been considered, then we shouldn't
        //    proceed with adding or updating zones. Again, this is driven
        //    primarily by the desire to minimize the number of versions of
        //    system software running at any time.
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
            // XXX: This doesn't consider sleds that aren't present in this
            // inventory run. It probably should. Would likely require storing
            // some state on the blueprint.
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

        // TODO: implement condition 4 above.

        if !reasons.is_empty() {
            let reasons = reasons.join("; ");
            info!(
                log,
                "not ready to add or update new zones yet";
                "reasons" => reasons,
            );
            Ok(UpdateStepResult::Waiting)
        } else {
            Ok(UpdateStepResult::ContinueToNextStep)
        }
    }

    fn do_plan_cockroachdb_settings(&mut self) {
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
        info!(
            &self.log,
            "will ensure cockroachdb setting";
            "setting" => "cluster.preserve_downgrade_option",
            "value" => ?value,
        );

        // Hey! Listen!
        //
        // If we need to manage more CockroachDB settings, we should ensure
        // that no settings will be modified if we don't recognize the current
        // cluster version -- we're likely in the middle of an upgrade!
        //
        // https://www.cockroachlabs.com/docs/stable/cluster-settings#change-a-cluster-setting
    }

    /// Return the image source for zones that we need to add.
    fn image_source_for_new_zone(
        &self,
        zone_kind: ZoneKind,
    ) -> Result<BlueprintZoneImageSource, TufRepoContentsError> {
        let source_repo = if self.is_zone_ready_for_update(zone_kind)? {
            self.input.tuf_repo().description()
        } else {
            self.input.old_repo().description()
        };
        source_repo.zone_image_source(zone_kind)
    }

    /// Return `true` iff a zone of the given kind is ready to be updated;
    /// i.e., its dependencies have been updated.
    fn is_zone_ready_for_update(
        &self,
        zone_kind: ZoneKind,
    ) -> Result<bool, TufRepoContentsError> {
        // TODO-correctness: We should return false regardless of `zone_kind` if
        // there are still pending updates for components earlier in the update
        // ordering than zones: RoT bootloader / RoT / SP / Host OS.

        match zone_kind {
            ZoneKind::Nexus => {
                // Nexus can only be updated if all non-Nexus zones have been
                // updated, i.e., their image source is an artifact from the new
                // repo.
                let new_repo = self.input.tuf_repo().description();

                // If we don't actually have a TUF repo here, we can't do
                // updates anyway; any return value is fine.
                if new_repo.tuf_repo().is_none() {
                    return Ok(false);
                }

                // Check that all in-service zones (other than Nexus) on all
                // sleds have an image source consistent with `new_repo`.
                for sled_id in self.blueprint.sled_ids_with_zones() {
                    for z in self.blueprint.current_sled_zones(
                        sled_id,
                        BlueprintZoneDisposition::is_in_service,
                    ) {
                        let kind = z.zone_type.kind();
                        if kind != ZoneKind::Nexus
                            && z.image_source
                                != new_repo.zone_image_source(kind)?
                        {
                            return Ok(false);
                        }
                    }
                }

                Ok(true)
            }
            _ => Ok(true), // other zone kinds have no special dependencies
        }
    }

    /// Return `true` iff we believe a zone can safely be shut down; e.g., any
    /// data it's responsible for is sufficiently persisted or replicated.
    ///
    /// "shut down" includes both "discretionary expunge" (e.g., if we're
    /// dealing with a zone that is updated via expunge -> replace) or "shut
    /// down and restart" (e.g., if we're upgrading a zone in place).
    ///
    /// This function is not (and cannot!) be called in the "expunge a zone
    /// because the underlying disk / sled has been expunged" case. In this
    /// case, we have no choice but to reconcile with the fact that the zone is
    /// now gone.
    fn can_zone_be_shut_down_safely(&self, zone: &BlueprintZoneConfig) -> bool {
        match zone.zone_type.kind() {
            ZoneKind::CockroachDb => {
                debug!(self.log, "Checking if Cockroach node can shut down");
                // We must hear from all nodes
                let all_statuses = &self.inventory.cockroach_status;
                if all_statuses.len() < COCKROACHDB_REDUNDANCY {
                    warn!(self.log, "Not enough nodes");
                    return false;
                }

                // All nodes must report: "We have the necessary redundancy, and
                // have observed no underreplicated ranges".
                for (node_id, status) in all_statuses {
                    let log = self.log.new(slog::o!(
                        "operation" => "Checking Cockroach node status for shutdown safety",
                        "node_id" => node_id.to_string()
                    ));
                    let Some(ranges_underreplicated) =
                        status.ranges_underreplicated
                    else {
                        warn!(log, "Missing underreplicated stat");
                        return false;
                    };
                    if ranges_underreplicated != 0 {
                        warn!(log, "Underreplicated ranges != 0"; "ranges_underreplicated" => ranges_underreplicated);
                        return false;
                    }
                    let Some(live_nodes) = status.liveness_live_nodes else {
                        warn!(log, "Missing live_nodes");
                        return false;
                    };
                    if live_nodes < COCKROACHDB_REDUNDANCY as u64 {
                        warn!(log, "Live nodes < COCKROACHDB_REDUNDANCY"; "live_nodes" => live_nodes);
                        return false;
                    }
                    info!(
                        log,
                        "CockroachDB Node status looks ready for shutdown"
                    );
                }
                true
            }
            ZoneKind::BoundaryNtp => {
                debug!(
                    self.log,
                    "Checking if boundary NTP zone can be shut down"
                );

                // Find all boundary NTP zones expected to be in-service by our
                // blueprint.
                let mut boundary_ntp_zones = std::collections::HashSet::new();
                for sled_id in self.blueprint.sled_ids_with_zones() {
                    for zone in self.blueprint.current_sled_zones(
                        sled_id,
                        BlueprintZoneDisposition::is_in_service,
                    ) {
                        if zone.zone_type.kind() == ZoneKind::BoundaryNtp {
                            boundary_ntp_zones.insert(zone.id);
                        }
                    }
                }

                // Count synchronized boundary NTP zones by checking timesync data.
                let mut synchronized_boundary_ntp_count = 0;
                for timesync in self.inventory.ntp_timesync.iter() {
                    // We only consider zones which we expect to be in-service
                    // from our blueprint - this means that old inventory
                    // collections including data for expunged zones will not be
                    // considered in the total count of synchronized boundary
                    // NTP zones.
                    if boundary_ntp_zones.contains(&timesync.zone_id)
                        && timesync.synced
                    {
                        synchronized_boundary_ntp_count += 1;
                    }
                }

                let can_shutdown =
                    synchronized_boundary_ntp_count >= BOUNDARY_NTP_REDUNDANCY;
                info!(
                    self.log,
                    "Boundary NTP zone shutdown check";
                    "total_boundary_ntp_zones" => boundary_ntp_zones.len(),
                    "synchronized_count" => synchronized_boundary_ntp_count,
                    "can_shutdown" => can_shutdown
                );
                can_shutdown
            }
            _ => true, // other zone kinds have no special safety checks
        }
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

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::example::ExampleSystem;
    use crate::example::ExampleSystemBuilder;
    use crate::example::SimRngState;
    use crate::example::example;
    use crate::system::SledBuilder;
    use chrono::DateTime;
    use chrono::Utc;
    use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
    use clickhouse_admin_types::KeeperId;
    use expectorate::assert_contents;
    use iddqd::IdOrdMap;
    use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
    use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
    use nexus_types::deployment::BlueprintArtifactVersion;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::BlueprintDiffSummary;
    use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::ClickhouseMode;
    use nexus_types::deployment::ClickhousePolicy;
    use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use nexus_types::deployment::SledDisk;
    use nexus_types::deployment::TargetReleaseDescription;
    use nexus_types::deployment::TufRepoPolicy;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::blueprint_zone_type::InternalDns;
    use nexus_types::external_api::views::PhysicalDiskState;
    use nexus_types::external_api::views::SledProvisionPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::CockroachStatus;
    use nexus_types::inventory::TimeSync;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::TufArtifactMeta;
    use omicron_common::api::external::TufRepoDescription;
    use omicron_common::api::external::TufRepoMeta;
    use omicron_common::api::external::Vni;
    use omicron_common::api::internal::shared::NetworkInterface;
    use omicron_common::api::internal::shared::NetworkInterfaceKind;
    use omicron_common::api::internal::shared::SourceNatConfig;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DiskIdentity;
    use omicron_common::policy::COCKROACHDB_REDUNDANCY;
    use omicron_common::policy::CRUCIBLE_PANTRY_REDUNDANCY;
    use omicron_common::policy::NEXUS_REDUNDANCY;
    use omicron_common::update::ArtifactId;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use semver::Version;
    use slog_error_chain::InlineErrorChain;
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactKind;
    use tufaceous_artifact::ArtifactVersion;
    use tufaceous_artifact::KnownArtifactKind;
    use typed_rng::TypedUuidRng;
    use uuid::Uuid;

    // Generate a ClickhousePolicy ignoring fields we don't care about for
    /// planner tests
    fn clickhouse_policy(mode: ClickhouseMode) -> ClickhousePolicy {
        ClickhousePolicy { version: 0, mode, time_created: Utc::now() }
    }

    pub(crate) fn assert_planning_makes_no_changes(
        log: &Logger,
        blueprint: &Blueprint,
        input: &PlanningInput,
        collection: &Collection,
        test_name: &'static str,
    ) {
        let planner = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &input,
            test_name,
            &collection,
        )
        .expect("created planner");
        let child_blueprint = planner.plan().expect("planning succeeded");
        verify_blueprint(&child_blueprint);
        let summary = child_blueprint.diff_since_blueprint(&blueprint);
        eprintln!(
            "diff between blueprints (expected no changes):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);
    }

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        static TEST_NAME: &str = "planner_basic_add_sled";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system.
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut example, blueprint1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        verify_blueprint(&blueprint1);

        println!("{}", blueprint1.display());

        // Now run the planner.  It should do nothing because our initial
        // system didn't have any issues that the planner currently knows how to
        // fix.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &example.input,
            "no-op?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expected no changes):\n{}", summary.display());
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);
        assert_eq!(summary.diff.sleds.unchanged().count(), 3);
        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 0);
        assert_eq!(summary.total_disks_added(), 0);
        assert_eq!(summary.total_disks_removed(), 0);
        assert_eq!(summary.total_disks_modified(), 0);
        assert_eq!(summary.total_datasets_added(), 0);
        assert_eq!(summary.total_datasets_removed(), 0);
        assert_eq!(summary.total_datasets_modified(), 0);
        verify_blueprint(&blueprint2);

        // Now add a new sled.
        let mut sled_id_rng = rng.next_sled_id_rng();
        let new_sled_id = sled_id_rng.next();
        let _ =
            example.system.sled(SledBuilder::new().id(new_sled_id)).unwrap();
        let input = example.system.to_planning_input_builder().unwrap().build();

        // Check that the first step is to add an NTP zone
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test: add NTP?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint3.diff_since_blueprint(&blueprint2);
        println!(
            "2 -> 3 (expect new NTP zone on new sled):\n{}",
            summary.display()
        );
        assert_contents(
            "tests/output/planner_basic_add_sled_2_3.txt",
            &summary.display().to_string(),
        );

        assert_eq!(summary.diff.sleds.added.len(), 1);
        assert_eq!(summary.total_disks_added(), 10);
        assert_eq!(summary.total_datasets_added(), 21);
        let (&sled_id, sled_added) =
            summary.diff.sleds.added.first_key_value().unwrap();
        // We have defined elsewhere that the first generation contains no
        // zones.  So the first one with zones must be newer.  See
        // OmicronZonesConfig::INITIAL_GENERATION.
        assert!(sled_added.sled_agent_generation > Generation::new());
        assert_eq!(*sled_id, new_sled_id);
        assert_eq!(sled_added.zones.len(), 1);
        assert!(matches!(
            sled_added.zones.first().unwrap().kind(),
            ZoneKind::InternalNtp
        ));
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);
        verify_blueprint(&blueprint3);

        // Check that with no change in inventory, the planner makes no changes.
        // It needs to wait for inventory to reflect the new NTP zone before
        // proceeding.
        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test: add nothing more",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("failed to plan");
        let summary = blueprint4.diff_since_blueprint(&blueprint3);
        println!("3 -> 4 (expected no changes):\n{}", summary.display());
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);
        verify_blueprint(&blueprint4);

        // Now update the inventory to have the requested NTP zone.
        //
        // TODO: mutating example.system doesn't automatically update
        // example.collection -- this should be addressed via API improvements.
        example
            .system
            .sled_set_omicron_config(
                new_sled_id,
                blueprint4
                    .sleds
                    .get(&new_sled_id)
                    .expect("blueprint should contain zones for new sled")
                    .clone()
                    .into_in_service_sled_config(),
            )
            .unwrap();
        let collection =
            example.system.to_collection_builder().unwrap().build();

        // Check that the next step is to add Crucible zones
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test: add Crucible zones?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp5")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint5.diff_since_blueprint(&blueprint3);
        println!("3 -> 5 (expect Crucible zones):\n{}", summary.display());
        assert_contents(
            "tests/output/planner_basic_add_sled_3_5.txt",
            &summary.display().to_string(),
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 1);
        let sled_id = summary.diff.sleds.modified_keys().next().unwrap();
        assert_eq!(*sled_id, new_sled_id);
        // No removed or modified zones on this sled
        let sled_cfg_diff = &summary.diff.sleds.get_modified(sled_id).unwrap();
        let zones_diff = sled_cfg_diff.diff_pair().zones;
        assert!(zones_diff.removed.is_empty());
        assert_eq!(zones_diff.modified().count(), 0);
        // 10 crucible zones addeed
        assert_eq!(
            sled_cfg_diff.after.sled_agent_generation,
            sled_cfg_diff.before.sled_agent_generation.next()
        );

        assert_eq!(zones_diff.added.len(), 10);
        for zone in zones_diff.added.values() {
            if zone.kind() != ZoneKind::Crucible {
                panic!("unexpectedly added a non-Crucible zone: {zone:?}");
            }
        }
        verify_blueprint(&blueprint5);

        // Check that there are no more steps.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint5,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner will add more Nexus zones to a single sled, if
    /// needed
    #[test]
    fn test_add_multiple_nexus_to_one_sled() {
        static TEST_NAME: &str = "planner_add_multiple_nexus_to_one_sled";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system with one sled and one Nexus instance as a
        // starting point.
        let (example, blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .nsleds(1)
                .nexus_count(1)
                .build();
        let sled_id = example
            .collection
            .sled_agents
            .iter()
            .next()
            .map(|sa| sa.sled_id)
            .unwrap();
        let input = example.input;
        let collection = example.collection;

        // This blueprint should only have 1 Nexus instance on the one sled we
        // kept.
        assert_eq!(blueprint1.sleds.len(), 1);
        assert_eq!(
            blueprint1
                .sleds
                .get(&sled_id)
                .expect("missing kept sled")
                .zones
                .iter()
                .filter(|z| z.zone_type.is_nexus())
                .count(),
            1
        );

        // Now run the planner.  It should add additional Nexus zones to the
        // one sled we have.
        let mut builder = input.into_builder();
        builder.policy_mut().target_nexus_zone_count = 5;

        // But we don't want it to add any more internal DNS zones,
        // which it would by default (because we have only one sled).
        builder.policy_mut().target_internal_dns_zone_count = 1;

        let input = builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "1 -> 2 (added additional Nexus zones):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 1);
        let (changed_sled_id, changed_sled) =
            summary.diff.sleds.modified().next().unwrap();

        assert_eq!(*changed_sled_id, sled_id);
        assert_eq!(changed_sled.diff_pair().datasets.added.len(), 4);

        let zones_added = &summary
            .diff
            .sleds
            .get_modified(&sled_id)
            .unwrap()
            .diff_pair()
            .zones
            .added;
        assert_eq!(zones_added.len(), input.target_nexus_zone_count() - 1);
        for (_, zone) in zones_added {
            if zone.kind() != ZoneKind::Nexus {
                panic!("unexpectedly added a non-Nexus zone: {zone:?}");
            }
        }

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner will spread additional Nexus zones out across
    /// sleds as it adds them
    #[test]
    fn test_spread_additional_nexus_zones_across_sleds() {
        static TEST_NAME: &str =
            "planner_spread_additional_nexus_zones_across_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) = example(&logctx.log, TEST_NAME);

        // This blueprint should only have 3 Nexus zones: one on each sled.
        assert_eq!(blueprint1.sleds.len(), 3);
        for sled_config in blueprint1.sleds.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.zone_type.is_nexus())
                    .count(),
                1
            );
        }

        // Now run the planner with a high number of target Nexus zones.
        let mut builder = input.into_builder();
        builder.policy_mut().target_nexus_zone_count = 14;
        let input = builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "1 -> 2 (added additional Nexus zones):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 3);

        // All 3 sleds should get additional Nexus zones. We expect a total of
        // 11 new Nexus zones, which should be spread evenly across the three
        // sleds (two should get 4 and one should get 3).
        let mut total_new_nexus_zones = 0;
        for (sled_id, modified_sled) in summary.diff.sleds.modified() {
            let zones_diff = &modified_sled.diff_pair().zones;
            assert!(zones_diff.removed.is_empty());
            assert_eq!(zones_diff.modified().count(), 0);
            let zones_added = &zones_diff.added;
            match zones_added.len() {
                n @ (3 | 4) => {
                    total_new_nexus_zones += n;
                }
                n => {
                    panic!("unexpected number of zones added to {sled_id}: {n}")
                }
            }
            for (_, zone) in zones_added {
                if zone.kind() != ZoneKind::Nexus {
                    panic!("unexpectedly added a non-Nexus zone: {zone:?}");
                }
            }
        }
        assert_eq!(total_new_nexus_zones, 11);

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner will spread additional internal DNS zones out across
    /// sleds as it adds them
    #[test]
    fn test_spread_internal_dns_zones_across_sleds() {
        static TEST_NAME: &str =
            "planner_spread_internal_dns_zones_across_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, mut blueprint1) =
            example(&logctx.log, TEST_NAME);

        // This blueprint should have exactly 3 internal DNS zones: one on each
        // sled.
        assert_eq!(blueprint1.sleds.len(), 3);
        for sled_config in blueprint1.sleds.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.zone_type.is_internal_dns())
                    .count(),
                1
            );
        }

        // Try to run the planner with a high number of internal DNS zones;
        // it will fail because the target is > MAX_DNS_REDUNDANCY.
        let mut builder = input.clone().into_builder();
        builder.policy_mut().target_internal_dns_zone_count = 14;
        match Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &builder.build(),
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .plan()
        {
            Ok(_) => panic!("unexpected success"),
            Err(err) => {
                let err = InlineErrorChain::new(&err).to_string();
                assert!(
                    err.contains("can only have ")
                        && err.contains(" internal DNS servers"),
                    "unexpected error: {err}"
                );
            }
        }

        // Remove two of the internal DNS zones; the planner should put new
        // zones back in their places.
        for (_sled_id, sled) in blueprint1.sleds.iter_mut().take(2) {
            sled.zones.retain(|z| !z.zone_type.is_internal_dns());
        }
        for (_, sled_config) in blueprint1.sleds.iter_mut().take(2) {
            sled_config.datasets.retain(|dataset| {
                // This is gross; once zone configs know explicit dataset IDs,
                // we should retain by ID instead.
                match &dataset.kind {
                    DatasetKind::InternalDns => false,
                    DatasetKind::TransientZone { name } => {
                        !name.starts_with("oxz_internal_dns")
                    }
                    _ => true,
                }
            });
        }

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "1 -> 2 (added additional internal DNS zones):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 2);

        // 2 sleds should each get 1 additional internal DNS zone.
        let mut total_new_zones = 0;
        for (sled_id, modified_sled) in summary.diff.sleds.modified() {
            let zones_diff = &modified_sled.diff_pair().zones;
            assert!(zones_diff.removed.is_empty());
            assert_eq!(zones_diff.modified().count(), 0);
            let zones_added = &zones_diff.added;
            match zones_added.len() {
                0 => {}
                n @ 1 => {
                    total_new_zones += n;
                }
                n => {
                    panic!("unexpected number of zones added to {sled_id}: {n}")
                }
            }
            for (_, zone) in zones_added {
                assert_eq!(
                    zone.kind(),
                    ZoneKind::InternalDns,
                    "unexpectedly added a non-internal-DNS zone: {zone:?}"
                );
            }
        }
        assert_eq!(total_new_zones, 2);

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner will reuse external IPs that were previously
    /// assigned to expunged zones
    #[test]
    fn test_reuse_external_ips_from_expunged_zones() {
        static TEST_NAME: &str =
            "planner_reuse_external_ips_from_expunged_zones";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) = example(&logctx.log, TEST_NAME);

        // Expunge the first sled we see, which will result in a Nexus external
        // IP no longer being associated with a running zone, and a new Nexus
        // zone being added to one of the two remaining sleds.
        let mut builder = input.into_builder();
        let (sled_id, _) =
            builder.sleds_mut().iter_mut().next().expect("no sleds");
        let sled_id = *sled_id;
        builder.expunge_sled(&sled_id).unwrap();
        let input = builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunged sled):\n{}", diff.display());

        // The expunged sled should have an expunged Nexus zone.
        let zone = blueprint2.sleds[&sled_id]
            .zones
            .iter()
            .find(|zone| matches!(zone.zone_type, BlueprintZoneType::Nexus(_)))
            .expect("no nexus zone found");
        assert_eq!(
            zone.disposition,
            BlueprintZoneDisposition::Expunged {
                as_of_generation: blueprint2.sleds[&sled_id]
                    .sled_agent_generation,
                ready_for_cleanup: true,
            }
        );

        // Set the target Nexus zone count to one that will completely exhaust
        // the service IP pool. This will force reuse of the IP that was
        // allocated to the expunged Nexus zone.
        let mut builder = input.into_builder();
        assert_eq!(builder.policy_mut().service_ip_pool_ranges.len(), 1);
        builder.policy_mut().target_nexus_zone_count =
            builder.policy_mut().service_ip_pool_ranges[0]
                .len()
                .try_into()
                .unwrap();
        let input = builder.build();
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!("2 -> 3 (maximum Nexus):\n{}", diff.display());

        // Planning succeeded, but let's prove that we reused the IP address!
        let expunged_ip = zone.zone_type.external_networking().unwrap().0.ip();
        let new_zone = blueprint3
            .sleds
            .values()
            .flat_map(|c| c.zones.iter())
            .find(|zone| {
                zone.disposition == BlueprintZoneDisposition::InService
                    && zone
                        .zone_type
                        .external_networking()
                        .map_or(false, |(ip, _)| expunged_ip == ip.ip())
            })
            .expect("couldn't find that the external IP was reused");
        println!(
            "zone {} reused external IP {} from expunged zone {}",
            new_zone.id, expunged_ip, zone.id
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner will reuse external DNS IPs that were
    /// previously assigned to expunged zones
    #[test]
    fn test_reuse_external_dns_ips_from_expunged_zones() {
        static TEST_NAME: &str =
            "planner_reuse_external_dns_ips_from_expunged_zones";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) = example(&logctx.log, TEST_NAME);

        // We should not be able to add any external DNS zones yet,
        // because we haven't give it any addresses (which currently
        // come only from RSS). This is not an error, though.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &input,
            &collection,
            TEST_NAME,
        )
        .expect("failed to build blueprint builder");
        let sled_id = builder.sled_ids_with_zones().next().expect("no sleds");
        builder
            .sled_add_zone_external_dns(
                sled_id,
                BlueprintZoneImageSource::InstallDataset,
            )
            .expect_err("can't add external DNS zones");

        // Build a builder for a modfied blueprint that will include
        // some external DNS addresses.
        let mut blueprint_builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &input,
            &collection,
            TEST_NAME,
        )
        .expect("failed to build blueprint builder");

        // Manually reach into the external networking allocator and "find"
        // some external IP addresses (maybe they fell off a truck).
        // TODO-cleanup: Remove when external DNS addresses are in the policy.
        let external_dns_ips =
            ["10.0.0.1", "10.0.0.2", "10.0.0.3"].map(|addr| {
                addr.parse::<IpAddr>()
                    .expect("can't parse external DNS IP address")
            });
        for addr in external_dns_ips {
            blueprint_builder.inject_untracked_external_dns_ip(addr).unwrap();
        }

        // Now we can add external DNS zones. We'll add two to the first
        // sled and one to the second.
        let (sled_1, sled_2) = {
            let mut sleds = blueprint_builder.sled_ids_with_zones();
            (
                sleds.next().expect("no first sled"),
                sleds.next().expect("no second sled"),
            )
        };
        blueprint_builder
            .sled_add_zone_external_dns(
                sled_1,
                BlueprintZoneImageSource::InstallDataset,
            )
            .expect("added external DNS zone");
        blueprint_builder
            .sled_add_zone_external_dns(
                sled_1,
                BlueprintZoneImageSource::InstallDataset,
            )
            .expect("added external DNS zone");
        blueprint_builder
            .sled_add_zone_external_dns(
                sled_2,
                BlueprintZoneImageSource::InstallDataset,
            )
            .expect("added external DNS zone");

        let blueprint1a = blueprint_builder.build();
        assert_eq!(
            blueprint1a
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, zone)| zone.zone_type.is_external_dns())
                .count(),
            3,
            "can't find external DNS zones in new blueprint"
        );

        // Plan with external DNS.
        let input_builder = input.clone().into_builder();
        let input = input_builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1a,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (added external DNS zones):\n{}", diff.display());

        // The first sled should have three external DNS zones.
        assert_eq!(
            blueprint2
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, zone)| zone.zone_type.is_external_dns())
                .count(),
            3,
            "can't find external DNS zones in planned blueprint"
        );

        // Expunge the first sled and re-plan. That gets us two expunged
        // external DNS zones; two external DNS zones should then be added to
        // the remaining sleds.
        let mut input_builder = input.into_builder();
        input_builder.expunge_sled(&sled_1).expect("found sled 1 again");
        let input = input_builder.build();
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("failed to re-plan");

        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!("2 -> 3 (expunged sled):\n{}", diff.display());
        assert_eq!(
            blueprint3.sleds[&sled_id]
                .zones
                .iter()
                .filter(|zone| {
                    zone.disposition
                        == BlueprintZoneDisposition::Expunged {
                            as_of_generation: blueprint3.sleds[&sled_id]
                                .sled_agent_generation,
                            ready_for_cleanup: true,
                        }
                        && zone.zone_type.is_external_dns()
                })
                .count(),
            2
        );

        // The IP addresses of the new external DNS zones should be the
        // same as the original set that we "found".
        let mut ips = blueprint3
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_id, zone)| {
                zone.zone_type.is_external_dns().then(|| {
                    zone.zone_type.external_networking().unwrap().0.ip()
                })
            })
            .collect::<Vec<IpAddr>>();
        ips.sort();
        assert_eq!(
            ips, external_dns_ips,
            "wrong addresses for new external DNS zones"
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_crucible_allocation_skips_nonprovisionable_disks() {
        static TEST_NAME: &str =
            "planner_crucible_allocation_skips_nonprovisionable_disks";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (example, blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(1).build();
        let collection = example.collection;
        let input = example.input;

        let mut builder = input.into_builder();

        // Avoid churning on the quantity of Nexus and internal DNS zones -
        // we're okay staying at one each.
        builder.policy_mut().target_nexus_zone_count = 1;
        builder.policy_mut().target_internal_dns_zone_count = 1;

        // Make generated disk ids deterministic
        let mut disk_rng =
            TypedUuidRng::from_seed(TEST_NAME, "NewPhysicalDisks");
        let mut new_sled_disk = |policy| SledDisk {
            disk_identity: DiskIdentity {
                vendor: "test-vendor".to_string(),
                serial: "test-serial".to_string(),
                model: "test-model".to_string(),
            },
            disk_id: PhysicalDiskUuid::from(disk_rng.next()),
            policy,
            state: PhysicalDiskState::Active,
        };

        let (_, sled_details) = builder.sleds_mut().iter_mut().next().unwrap();

        // Inject some new disks into the input.
        //
        // These counts are arbitrary, as long as they're non-zero
        // for the sake of the test.

        const NEW_IN_SERVICE_DISKS: usize = 2;
        const NEW_EXPUNGED_DISKS: usize = 1;

        let mut zpool_rng = TypedUuidRng::from_seed(TEST_NAME, "NewZpools");
        for _ in 0..NEW_IN_SERVICE_DISKS {
            sled_details.resources.zpools.insert(
                ZpoolUuid::from(zpool_rng.next()),
                new_sled_disk(PhysicalDiskPolicy::InService),
            );
        }
        for _ in 0..NEW_EXPUNGED_DISKS {
            sled_details.resources.zpools.insert(
                ZpoolUuid::from(zpool_rng.next()),
                new_sled_disk(PhysicalDiskPolicy::Expunged),
            );
        }

        let input = builder.build();

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test: some new disks",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "1 -> 2 (some new disks, one expunged):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.modified().count(), 1);

        // We should be adding a Crucible zone for each new in-service disk.
        assert_eq!(summary.total_zones_added(), NEW_IN_SERVICE_DISKS);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_disks_added(), NEW_IN_SERVICE_DISKS);
        assert_eq!(summary.total_disks_removed(), 0);

        // 1 Zone, Crucible, Transient Crucible Zone, and Debug dataset created
        // per disk.
        assert_eq!(summary.total_datasets_added(), NEW_IN_SERVICE_DISKS * 4);
        assert_eq!(summary.total_datasets_removed(), 0);
        assert_eq!(summary.total_datasets_modified(), 0);

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_dataset_settings_modified_in_place() {
        static TEST_NAME: &str = "planner_dataset_settings_modified_in_place";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (example, mut blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(1).build();
        let collection = example.collection;
        let input = example.input;

        let mut builder = input.into_builder();

        // Avoid churning on the quantity of Nexus and internal DNS zones -
        // we're okay staying at one each.
        builder.policy_mut().target_nexus_zone_count = 1;
        builder.policy_mut().target_internal_dns_zone_count = 1;

        // Manually update the blueprint to report an abnormal "Debug dataset"
        {
            let (_sled_id, sled_config) =
                blueprint1.sleds.iter_mut().next().unwrap();
            let mut dataset_config = sled_config
                .datasets
                .iter_mut()
                .find(|config| {
                    matches!(
                        config.kind,
                        omicron_common::disk::DatasetKind::Debug
                    )
                })
                .expect("No debug dataset found");

            // These values are out-of-sync with what the blueprint will typically
            // enforce.
            dataset_config.quota = None;
            dataset_config.reservation = Some(
                omicron_common::api::external::ByteCount::from_gibibytes_u32(1),
            );
        }

        let input = builder.build();

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test: fix a dataset",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (modify a dataset):\n{}", summary.display());
        assert_contents(
            "tests/output/planner_dataset_settings_modified_in_place_1_2.txt",
            &summary.display().to_string(),
        );

        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 1);

        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 0);
        assert_eq!(summary.total_disks_added(), 0);
        assert_eq!(summary.total_disks_removed(), 0);
        assert_eq!(summary.total_disks_modified(), 0);
        assert_eq!(summary.total_datasets_added(), 0);
        assert_eq!(summary.total_datasets_removed(), 0);
        assert_eq!(summary.total_datasets_modified(), 1);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_disk_add_expunge_decommission() {
        static TEST_NAME: &str = "planner_disk_add_expunge_decommission";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (example, blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(1).build();
        let mut collection = example.collection;
        let input = example.input;

        // The initial blueprint configuration has generation 2
        let (sled_id, sled_config) =
            blueprint1.sleds.first_key_value().unwrap();
        assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(2));

        // All disks should have an `InService` disposition and `Active` state
        for disk in &sled_config.disks {
            assert_eq!(
                disk.disposition,
                BlueprintPhysicalDiskDisposition::InService
            );
        }

        let mut builder = input.into_builder();

        // Let's expunge a disk. Its disposition should change to `Expunged`
        // but its state should remain active.
        let expunged_disk_id = {
            let expunged_disk = &mut builder
                .sleds_mut()
                .get_mut(&sled_id)
                .unwrap()
                .resources
                .zpools
                .iter_mut()
                .next()
                .unwrap()
                .1;
            expunged_disk.policy = PhysicalDiskPolicy::Expunged;
            expunged_disk.disk_id
        };

        let input = builder.build();

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test: expunge a disk",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunge a disk):\n{}", diff.display());

        let sled_config = &blueprint2.sleds.first_key_value().unwrap().1;

        // The generation goes from 2 -> 3
        assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(3));
        // One disk should have it's disposition set to
        // `Expunged{ready_for_cleanup: false, ..}`.
        for disk in &sled_config.disks {
            if disk.id == expunged_disk_id {
                assert!(matches!(
                    disk.disposition,
                    BlueprintPhysicalDiskDisposition::Expunged {
                        ready_for_cleanup: false,
                        ..
                    }
                ));
            } else {
                assert_eq!(
                    disk.disposition,
                    BlueprintPhysicalDiskDisposition::InService
                );
            }
            println!("{disk:?}");
        }

        // We haven't updated the inventory, so no changes should be made
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        // Let's update the inventory to reflect that the sled-agent
        // has learned about the expungement.
        collection
            .sled_agents
            .get_mut(sled_id)
            .unwrap()
            .last_reconciliation
            .as_mut()
            .unwrap()
            .last_reconciled_config
            .generation = Generation::from_u32(3);

        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test: decommission a disk",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!("2 -> 3 (decommission a disk):\n{}", diff.display());

        let sled_config = &blueprint3.sleds.first_key_value().unwrap().1;

        // The config generation does not change, as decommissioning doesn't
        // bump the generation.
        //
        // The reason for this is because the generation is there primarily to
        // inform the sled-agent that it has work to do, but decommissioning
        // doesn't trigger any sled-agent changes.
        assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(3));
        // One disk should have its disposition set to
        // `Expunged{ready_for_cleanup: true, ..}`.
        for disk in &sled_config.disks {
            if disk.id == expunged_disk_id {
                assert!(matches!(
                    disk.disposition,
                    BlueprintPhysicalDiskDisposition::Expunged {
                        ready_for_cleanup: true,
                        ..
                    }
                ));
            } else {
                assert_eq!(
                    disk.disposition,
                    BlueprintPhysicalDiskDisposition::InService
                );
            }
            println!("{disk:?}");
        }

        // Now let's expunge a sled via the planning input. All disks should get
        // expunged and decommissioned in the same planning round. We also have
        // to manually expunge all the disks via policy, which would happen in a
        // database transaction when an operator expunges a sled.
        //
        // We don't rely on the sled-agents learning about expungement to
        // decommission because by definition expunging a sled means it's
        // already gone.
        let mut builder = input.into_builder();
        builder.expunge_sled(sled_id).unwrap();
        let input = builder.build();

        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test: expunge and decommission all disks",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!(
            "3 -> 4 (expunge and decommission all disks):\n{}",
            diff.display()
        );

        let sled_config = &blueprint4.sleds.first_key_value().unwrap().1;

        // The config generation goes from 3 -> 4
        assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(4));
        // We should still have 10 disks
        assert_eq!(sled_config.disks.len(), 10);
        // All disks should have their disposition set to
        // `Expunged{ready_for_cleanup: true, ..}`.
        for disk in &sled_config.disks {
            assert!(matches!(
                disk.disposition,
                BlueprintPhysicalDiskDisposition::Expunged {
                    ready_for_cleanup: true,
                    ..
                }
            ));
            println!("{disk:?}");
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_disk_expungement_removes_zones_durable_zpool() {
        static TEST_NAME: &str =
            "planner_disk_expungement_removes_zones_durable_zpool";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (example, blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(1).build();
        let collection = example.collection;
        let input = example.input;

        let mut builder = input.into_builder();

        // Avoid churning on the quantity of Nexus and internal DNS zones -
        // we're okay staying at one each.
        builder.policy_mut().target_nexus_zone_count = 1;
        builder.policy_mut().target_internal_dns_zone_count = 1;

        // The example system should be assigning crucible zones to each
        // in-service disk. When we expunge one of these disks, the planner
        // should remove the associated zone.
        //
        // Find a disk which is only used by a single zone, if one exists.
        //
        // If we don't do this, we might select a physical disk supporting
        // multiple zones of distinct types.
        let mut zpool_by_zone_usage = HashMap::new();
        for sled in blueprint1.sleds.values() {
            for zone in &sled.zones {
                let pool = &zone.filesystem_pool;
                zpool_by_zone_usage
                    .entry(pool.id())
                    .and_modify(|count| *count += 1)
                    .or_insert_with(|| 1);
            }
        }
        let (_, sled_details) = builder.sleds_mut().iter_mut().next().unwrap();
        let (_, disk) = sled_details
            .resources
            .zpools
            .iter_mut()
            .find(|(zpool_id, _disk)| {
                *zpool_by_zone_usage.get(*zpool_id).unwrap() == 1
            })
            .expect("Couldn't find zpool only used by a single zone");
        disk.policy = PhysicalDiskPolicy::Expunged;

        let input = builder.build();

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test: expunge a disk",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunge a disk):\n{}", summary.display());
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 1);

        // We should be removing a single zone, associated with the Crucible
        // using that device.
        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 1);
        assert_eq!(summary.total_disks_added(), 0);
        assert_eq!(summary.total_disks_removed(), 0);
        assert_eq!(summary.total_datasets_added(), 0);
        // NOTE: Expunging a disk doesn't immediately delete datasets; see the
        // "decommissioned_disk_cleaner" background task for more context.
        assert_eq!(summary.total_datasets_removed(), 0);

        // The disposition has changed from `InService` to `Expunged` for the 4
        // datasets on this sled.
        assert_eq!(summary.total_datasets_modified(), 4);
        // We don't know the expected name, other than the fact it's a crucible zone
        let test_transient_zone_kind = DatasetKind::TransientZone {
            name: "some-crucible-zone-name".to_string(),
        };
        let mut expected_kinds = BTreeSet::from_iter([
            DatasetKind::Crucible,
            DatasetKind::Debug,
            DatasetKind::TransientZoneRoot,
            test_transient_zone_kind.clone(),
        ]);
        let mut modified_sled_configs = Vec::new();
        for modified_sled in summary.diff.sleds.modified_values_diff() {
            for modified in modified_sled.datasets.modified_values_diff() {
                assert_eq!(
                    *modified.disposition.before,
                    BlueprintDatasetDisposition::InService
                );
                assert_eq!(
                    *modified.disposition.after,
                    BlueprintDatasetDisposition::Expunged
                );
                if let DatasetKind::TransientZone { name } =
                    &modified.kind.before
                {
                    assert!(name.starts_with("oxz_crucible"));
                    assert!(expected_kinds.remove(&test_transient_zone_kind));
                } else {
                    assert!(expected_kinds.remove(&modified.kind.before));
                }
            }
            modified_sled_configs.push(modified_sled);
        }
        assert!(expected_kinds.is_empty());

        assert_eq!(modified_sled_configs.len(), 1);
        let modified_sled_config = modified_sled_configs.pop().unwrap();
        assert!(modified_sled_config.zones.added.is_empty());
        assert!(modified_sled_config.zones.removed.is_empty());
        let mut modified_zones = modified_sled_config
            .zones
            .modified_values_diff()
            .collect::<Vec<_>>();
        assert_eq!(modified_zones.len(), 1);
        let modified_zone = modified_zones.pop().unwrap();
        assert!(
            modified_zone.zone_type.before.is_crucible(),
            "Expected the modified zone to be a Crucible zone, \
             but it was: {:?}",
            modified_zone.zone_type.before.kind()
        );
        assert_eq!(
            *modified_zone.disposition.after,
            BlueprintZoneDisposition::Expunged {
                as_of_generation: modified_sled_config
                    .sled_agent_generation
                    .before
                    .next(),
                ready_for_cleanup: false,
            },
            "Should have expunged this zone"
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_disk_expungement_removes_zones_transient_filesystem() {
        static TEST_NAME: &str =
            "planner_disk_expungement_removes_zones_transient_filesystem";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (example, blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(1).build();
        let collection = example.collection;
        let input = example.input;

        let mut builder = input.into_builder();

        // Aside: Avoid churning on the quantity of Nexus zones - we're okay
        // staying at one.
        builder.policy_mut().target_nexus_zone_count = 1;

        // Find whatever pool NTP was using
        let pool_to_expunge = blueprint1
            .sleds
            .iter()
            .find_map(|(_, sled_config)| {
                for zone_config in &sled_config.zones {
                    if zone_config.zone_type.is_ntp() {
                        return Some(zone_config.filesystem_pool);
                    }
                }
                None
            })
            .expect("No NTP zone pool?");

        // This is mostly for test stability across "example system" changes:
        // Find all the zones using this same zpool.
        let mut zones_on_pool = BTreeSet::new();
        let mut zone_kinds_on_pool = BTreeMap::<_, usize>::new();
        for (_, zone_config) in blueprint1
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        {
            if pool_to_expunge == zone_config.filesystem_pool {
                zones_on_pool.insert(zone_config.id);
                *zone_kinds_on_pool
                    .entry(zone_config.zone_type.kind())
                    .or_default() += 1;
            }
        }
        assert!(
            !zones_on_pool.is_empty(),
            "We should be expunging at least one zone using this zpool"
        );

        // For that pool, find the physical disk behind it, and mark it
        // expunged.
        let (_, sled_details) = builder.sleds_mut().iter_mut().next().unwrap();
        let disk = sled_details
            .resources
            .zpools
            .get_mut(&pool_to_expunge.id())
            .unwrap();
        disk.policy = PhysicalDiskPolicy::Expunged;

        let input = builder.build();

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test: expunge a disk with a zone on top",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunge a disk):\n{}", summary.display());
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 1);

        // No zones should have been removed from the blueprint entirely.
        assert_eq!(summary.total_zones_removed(), 0);

        // We should have expunged all the zones on this pool.
        let mut zones_expunged = BTreeSet::new();
        for sled in summary.diff.sleds.modified_values_diff() {
            let expected_generation =
                sled.sled_agent_generation.diff_pair().before.next();
            for (_, z) in sled.zones.modified() {
                assert_eq!(
                    z.after.disposition,
                    BlueprintZoneDisposition::Expunged {
                        as_of_generation: expected_generation,
                        ready_for_cleanup: false,
                    },
                    "Should have expunged this zone"
                );
                zones_expunged.insert(z.after.id);
            }
        }
        assert_eq!(zones_on_pool, zones_expunged);

        // We also should have added back a new zone for each kind that was
        // removed, except the Crucible zone (which is specific to the disk) and
        // the internal DNS zone (which can't be replaced until the original
        // zone is ready for cleanup per inventory). Remove these from our
        // original counts, then check against the added zones count.
        assert_eq!(zone_kinds_on_pool.remove(&ZoneKind::Crucible), Some(1));
        assert_eq!(zone_kinds_on_pool.remove(&ZoneKind::InternalDns), Some(1));
        let mut zone_kinds_added = BTreeMap::new();
        for sled in summary.diff.sleds.modified_values_diff() {
            for z in sled.zones.added.values() {
                *zone_kinds_added.entry(z.zone_type.kind()).or_default() +=
                    1_usize;
            }
        }
        assert_eq!(zone_kinds_on_pool, zone_kinds_added);

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner will skip non-provisionable sleds when allocating
    /// extra Nexus zones
    #[test]
    fn test_nexus_allocation_skips_nonprovisionable_sleds() {
        static TEST_NAME: &str =
            "planner_nexus_allocation_skips_nonprovisionable_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        //
        // Request two extra sleds here so we test non-provisionable, expunged,
        // and decommissioned sleds. (When we add more kinds of
        // non-provisionable states in the future, we'll have to add more
        // sleds.)
        let (example, mut blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(5).build();
        let collection = example.collection;
        let input = example.input;

        // This blueprint should only have 5 Nexus zones: one on each sled.
        assert_eq!(blueprint1.sleds.len(), 5);
        for sled_config in blueprint1.sleds.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.zone_type.is_nexus())
                    .count(),
                1
            );
        }

        // Arbitrarily choose some of the sleds and mark them non-provisionable
        // in various ways.
        let mut builder = input.into_builder();
        let mut sleds_iter = builder.sleds_mut().iter_mut();

        let nonprovisionable_sled_id = {
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.policy = SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            };
            *sled_id
        };
        println!("1 -> 2: marked non-provisionable {nonprovisionable_sled_id}");
        let expunged_sled_id = {
            let (sled_id, _) = sleds_iter.next().expect("no sleds");
            // We need to call builder.expunge_sled(), but can't while
            // iterating; we defer that work until after we're done with
            // `sleds_iter`.
            *sled_id
        };
        println!("1 -> 2: expunged {expunged_sled_id}");
        let decommissioned_sled_id = {
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.state = SledState::Decommissioned;

            // Drop the mutable borrow on the builder so we can call
            // `builder.expunge_sled()`
            let sled_id = *sled_id;
            // Let's also properly expunge the sled and its disks. We can't have
            // a decommissioned sled that is not expunged also.
            builder.expunge_sled(&sled_id).unwrap();

            // Decommissioned sleds can only occur if their zones have been
            // expunged, so lie and pretend like that already happened
            // (otherwise the planner will rightfully fail to generate a new
            // blueprint, because we're feeding it invalid inputs).
            for mut zone in
                &mut blueprint1.sleds.get_mut(&sled_id).unwrap().zones
            {
                zone.disposition = BlueprintZoneDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: false,
                };
            }

            // Similarly, a sled can only have gotten into the `Decommissioned`
            // state via blueprints. If the database says the sled is
            // decommissioned but the parent blueprint says it's still active,
            // that's an invalid state that the planner will reject.
            blueprint1
                .sleds
                .get_mut(&sled_id)
                .expect("found state in parent blueprint")
                .state = SledState::Decommissioned;

            sled_id
        };
        // Actually expunge the sled (the work that was deferred during iteration above)
        builder.expunge_sled(&expunged_sled_id).unwrap();
        println!("1 -> 2: decommissioned {decommissioned_sled_id}");

        // Now run the planner with a high number of target Nexus zones. The
        // number (9) is chosen such that:
        //
        // * we start with 5 sleds with 1 Nexus each
        // * we take two sleds out of service (one expunged, one
        //   decommissioned), so we're down to 3 in-service Nexuses: we need to
        //   add 6 to get to the new policy target of 9
        // * of the remaining 3 sleds, only 2 are eligible for provisioning
        // * each of those 2 sleds should get exactly 3 new Nexuses
        builder.policy_mut().target_nexus_zone_count = 9;

        // Disable addition of zone types we're not checking for below.
        builder.policy_mut().target_internal_dns_zone_count = 0;
        builder.policy_mut().target_crucible_pantry_zone_count = 0;

        let input = builder.build();
        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        // Define a time_created for consistent output across runs.
        blueprint2.time_created = DateTime::<Utc>::UNIX_EPOCH;

        assert_contents(
            "tests/output/planner_nonprovisionable_bp2.txt",
            &blueprint2.display().to_string(),
        );

        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "1 -> 2 (added additional Nexus zones, take 2 sleds out of service):\n{}",
            summary.display()
        );
        assert_contents(
            "tests/output/planner_nonprovisionable_1_2.txt",
            &summary.display().to_string(),
        );

        // The expunged and decommissioned sleds should have had all zones be
        // marked as expunged. (Not removed! Just marked as expunged.)
        //
        // Note that at this point we're neither removing zones from the
        // blueprint nor marking sleds as decommissioned -- we still need to do
        // cleanup, and we aren't performing garbage collection on zones or
        // sleds at the moment.

        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 3);
        assert_eq!(summary.diff.sleds.unchanged().count(), 2);

        assert_all_zones_expunged(&summary, expunged_sled_id, "expunged sled");

        // Only 2 of the 3 remaining sleds (not the non-provisionable sled)
        // should get additional Nexus zones. We expect a total of 6 new Nexus
        // zones, which should be split evenly between the two sleds, while the
        // non-provisionable sled should be unchanged.
        let remaining_modified_sleds = summary
            .diff
            .sleds
            .modified()
            .filter_map(|(&sled_id, sled)| {
                (sled_id != expunged_sled_id).then_some((sled_id, sled))
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(remaining_modified_sleds.len(), 2);
        let mut total_new_nexus_zones = 0;
        for (sled_id, modified_sled) in remaining_modified_sleds {
            assert!(sled_id != nonprovisionable_sled_id);
            assert!(sled_id != expunged_sled_id);
            assert!(sled_id != decommissioned_sled_id);
            let zones_on_modified_sled = &modified_sled.diff_pair().zones;
            assert!(zones_on_modified_sled.removed.is_empty());
            let zones = &zones_on_modified_sled.added;
            for (_, zone) in zones {
                if ZoneKind::Nexus != zone.kind() {
                    panic!("unexpectedly added a non-Nexus zone: {zone:?}");
                };
            }
            if zones.len() == 3 {
                total_new_nexus_zones += 3;
            } else {
                panic!(
                    "unexpected number of zones added to {sled_id}: {}",
                    zones.len()
                );
            }
        }
        assert_eq!(total_new_nexus_zones, 6);

        // ---

        // Also poke at some of the config by hand; we'll use this to test out
        // diff output. This isn't a real blueprint, just one that we're
        // creating to test diff output.
        //
        // Some of the things we're testing here:
        //
        // * modifying zones
        // * removing zones
        // * removing sleds
        // * for modified sleds' zone config generation, both a bump and the
        //   generation staying the same (the latter should produce a warning)
        let mut blueprint2a = blueprint2.clone();

        enum NextCrucibleMutate {
            Modify,
            Remove,
            Done,
        }
        let mut next = NextCrucibleMutate::Modify;

        // Leave the non-provisionable sled's generation alone.
        let zones = &mut blueprint2a
            .sleds
            .get_mut(&nonprovisionable_sled_id)
            .unwrap()
            .zones;

        zones.retain(|zone| {
            if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                ..
            }) = &mut zone.zone_type
            {
                // Change the internal address.
                let mut segments = internal_address.ip().segments();
                segments[0] = segments[0].wrapping_add(1);
                internal_address.set_ip(segments.into());
                true
            } else if let BlueprintZoneType::Crucible(_) = zone.zone_type {
                match next {
                    NextCrucibleMutate::Modify => {
                        zone.disposition = BlueprintZoneDisposition::Expunged {
                            as_of_generation: Generation::new(),
                            ready_for_cleanup: false,
                        };
                        next = NextCrucibleMutate::Remove;
                        true
                    }
                    NextCrucibleMutate::Remove => {
                        next = NextCrucibleMutate::Done;
                        false
                    }
                    NextCrucibleMutate::Done => true,
                }
            } else if let BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp { address },
            ) = &mut zone.zone_type
            {
                // Change the underlay IP.
                let mut segments = address.ip().segments();
                segments[0] += 1;
                address.set_ip(segments.into());
                true
            } else {
                true
            }
        });

        let expunged_sled =
            &mut blueprint2a.sleds.get_mut(&expunged_sled_id).unwrap();
        expunged_sled.zones.clear();
        expunged_sled.sled_agent_generation =
            expunged_sled.sled_agent_generation.next();

        blueprint2a.sleds.remove(&decommissioned_sled_id);

        blueprint2a.external_dns_version =
            blueprint2a.external_dns_version.next();

        let diff = blueprint2a.diff_since_blueprint(&blueprint2);
        println!("2 -> 2a (manually modified zones):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_nonprovisionable_2_2a.txt",
            &diff.display().to_string(),
        );

        // ---

        logctx.cleanup_successful();
    }

    #[track_caller]
    fn assert_all_zones_expunged<'a>(
        summary: &BlueprintDiffSummary<'a>,
        expunged_sled_id: SledUuid,
        desc: &str,
    ) {
        assert!(
            summary.added_zones(&expunged_sled_id).is_none(),
            "for {desc}, no zones should have been added to blueprint"
        );

        // A zone disposition going to expunged *does not* mean that the
        // zone is actually removed, i.e. `zones_removed` is still 0. Any
        // zone removal will be part of some future garbage collection
        // process that isn't currently defined.

        assert!(
            summary.removed_zones(&expunged_sled_id).is_none(),
            "for {desc}, no zones should have been removed from blueprint"
        );

        // Run through all the common zones and ensure that all of them
        // have been marked expunged.
        let modified_sled = &summary
            .diff
            .sleds
            .get_modified(&expunged_sled_id)
            .unwrap()
            .diff_pair();
        assert_eq!(
            modified_sled.sled_agent_generation.before.next(),
            *modified_sled.sled_agent_generation.after,
            "for {desc}, generation should have been bumped"
        );

        for modified_zone in modified_sled.zones.modified_values_diff() {
            assert_eq!(
                *modified_zone.disposition.after,
                BlueprintZoneDisposition::Expunged {
                    as_of_generation: *modified_sled
                        .sled_agent_generation
                        .after,
                    ready_for_cleanup: true,
                },
                "for {desc}, zone {} should have been marked expunged",
                modified_zone.id.after
            );
        }
    }

    #[test]
    fn planner_decommissions_sleds() {
        static TEST_NAME: &str = "planner_decommissions_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) = example(&logctx.log, TEST_NAME);

        // Expunge one of the sleds.
        //
        // We expunge a sled via planning input using the builder so that disks
        // are properly taken into account.
        let mut builder = input.into_builder();
        let expunged_sled_id = {
            let mut iter = builder.sleds_mut().iter_mut();
            let (sled_id, _) = iter.next().expect("at least one sled");
            *sled_id
        };
        builder.expunge_sled(&expunged_sled_id).expect("sled is expungable");
        let input = builder.build();

        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");

        // Define a time_created for consistent output across runs.
        blueprint2.time_created = DateTime::<Utc>::UNIX_EPOCH;

        assert_contents(
            "tests/output/planner_decommissions_sleds_bp2.txt",
            &blueprint2.display().to_string(),
        );
        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunged {expunged_sled_id}):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_decommissions_sleds_1_2.txt",
            &diff.display().to_string(),
        );

        // All the zones of the expunged sled should be expunged, and the sled
        // itself should be decommissioned.
        assert!(blueprint2.sleds[&expunged_sled_id].are_all_zones_expunged());
        assert_eq!(
            blueprint2.sleds[&expunged_sled_id].state,
            SledState::Decommissioned
        );

        // Set the state of the expunged sled to decommissioned, and run the
        // planner again.
        let mut builder = input.into_builder();
        let expunged = builder
            .sleds_mut()
            .get_mut(&expunged_sled_id)
            .expect("expunged sled is present in input");
        expunged.state = SledState::Decommissioned;
        let input = builder.build();

        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("succeeded in planner");

        // There should be no changes to the blueprint; we don't yet garbage
        // collect zones, so we should still have the sled's expunged zones
        // (even though the sled itself is no longer present in the list of
        // commissioned sleds).
        let summary = blueprint3.diff_since_blueprint(&blueprint2);
        println!(
            "2 -> 3 (decommissioned {expunged_sled_id}):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);
        assert_eq!(
            summary.diff.sleds.unchanged().count(),
            ExampleSystemBuilder::DEFAULT_N_SLEDS
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            &collection,
            TEST_NAME,
        );

        // Now remove the decommissioned sled from the input entirely. (This
        // should not happen in practice at the moment -- entries in the sled
        // table are kept forever -- but we need to test it.)
        //
        // Eventually, once zone and sled garbage collection is implemented,
        // we'll expect that the diff's `sleds.removed` will become
        // non-empty. At some point we may also want to remove entries from the
        // sled table, but that's a future concern that would come after
        // blueprint cleanup is implemented.
        let mut builder = input.into_builder();
        builder.sleds_mut().remove(&expunged_sled_id);
        let input = builder.build();

        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test_blueprint4",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("succeeded in planner");

        let summary = blueprint4.diff_since_blueprint(&blueprint3);
        println!(
            "3 -> 4 (removed from input {expunged_sled_id}):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);
        assert_eq!(
            summary.diff.sleds.unchanged().count(),
            ExampleSystemBuilder::DEFAULT_N_SLEDS
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint4,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_ensure_preserve_downgrade_option() {
        static TEST_NAME: &str = "planner_ensure_preserve_downgrade_option";
        let logctx = test_setup_log(TEST_NAME);

        let (example, bp1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(0).build();
        let collection = example.collection;
        let input = example.input;
        let mut builder = input.into_builder();
        assert!(bp1.cockroachdb_fingerprint.is_empty());
        assert_eq!(
            bp1.cockroachdb_setting_preserve_downgrade,
            CockroachDbPreserveDowngrade::DoNotModify
        );

        // If `preserve_downgrade_option` is unset and the current cluster
        // version matches `POLICY`, we ensure it is set.
        builder.set_cockroachdb_settings(CockroachDbSettings {
            state_fingerprint: "bp2".to_owned(),
            version: CockroachDbClusterVersion::POLICY.to_string(),
            preserve_downgrade: String::new(),
        });
        let bp2 = Planner::new_based_on(
            logctx.log.clone(),
            &bp1,
            &builder.clone().build(),
            "initial settings",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to plan");
        assert_eq!(bp2.cockroachdb_fingerprint, "bp2");
        assert_eq!(
            bp2.cockroachdb_setting_preserve_downgrade,
            CockroachDbClusterVersion::POLICY.into()
        );

        // If `preserve_downgrade_option` is unset and the current cluster
        // version is known to us and _newer_ than `POLICY`, we still ensure
        // it is set. (During a "tock" release, `POLICY == NEWLY_INITIALIZED`
        // and this won't be materially different than the above test, but it
        // shouldn't need to change when moving to a "tick" release.)
        builder.set_cockroachdb_settings(CockroachDbSettings {
            state_fingerprint: "bp3".to_owned(),
            version: CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
            preserve_downgrade: String::new(),
        });
        let bp3 = Planner::new_based_on(
            logctx.log.clone(),
            &bp1,
            &builder.clone().build(),
            "initial settings",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("failed to plan");
        assert_eq!(bp3.cockroachdb_fingerprint, "bp3");
        assert_eq!(
            bp3.cockroachdb_setting_preserve_downgrade,
            CockroachDbClusterVersion::NEWLY_INITIALIZED.into()
        );

        // When we run the planner again after setting the setting, the inputs
        // will change; we should still be ensuring the setting.
        builder.set_cockroachdb_settings(CockroachDbSettings {
            state_fingerprint: "bp4".to_owned(),
            version: CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
            preserve_downgrade: CockroachDbClusterVersion::NEWLY_INITIALIZED
                .to_string(),
        });
        let bp4 = Planner::new_based_on(
            logctx.log.clone(),
            &bp1,
            &builder.clone().build(),
            "after ensure",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("failed to plan");
        assert_eq!(bp4.cockroachdb_fingerprint, "bp4");
        assert_eq!(
            bp4.cockroachdb_setting_preserve_downgrade,
            CockroachDbClusterVersion::NEWLY_INITIALIZED.into()
        );

        // When `version` isn't recognized, do nothing regardless of the value
        // of `preserve_downgrade`.
        for preserve_downgrade in [
            String::new(),
            CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
            "definitely not a real cluster version".to_owned(),
        ] {
            builder.set_cockroachdb_settings(CockroachDbSettings {
                state_fingerprint: "bp5".to_owned(),
                version: "definitely not a real cluster version".to_owned(),
                preserve_downgrade: preserve_downgrade.clone(),
            });
            let bp5 = Planner::new_based_on(
                logctx.log.clone(),
                &bp1,
                &builder.clone().build(),
                "unknown version",
                &collection,
            )
            .expect("failed to create planner")
            .with_rng(PlannerRng::from_seed((
                TEST_NAME,
                format!("bp5-{}", preserve_downgrade),
            )))
            .plan()
            .expect("failed to plan");
            assert_eq!(bp5.cockroachdb_fingerprint, "bp5");
            assert_eq!(
                bp5.cockroachdb_setting_preserve_downgrade,
                CockroachDbPreserveDowngrade::DoNotModify
            );
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_crucible_pantry() {
        static TEST_NAME: &str = "test_crucible_pantry";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) = example(&logctx.log, TEST_NAME);

        // We should start with CRUCIBLE_PANTRY_REDUNDANCY pantries spread out
        // to at most 1 per sled. Find one of the sleds running one.
        let pantry_sleds = blueprint1
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(sled_id, zone)| {
                zone.zone_type.is_crucible_pantry().then_some(sled_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            pantry_sleds.len(),
            CRUCIBLE_PANTRY_REDUNDANCY,
            "expected {CRUCIBLE_PANTRY_REDUNDANCY} pantries, but found {}",
            pantry_sleds.len(),
        );

        // Expunge one of the pantry-hosting sleds and re-plan. The planner
        // should immediately replace the zone with one on another
        // (non-expunged) sled.
        let expunged_sled_id = pantry_sleds[0];

        let mut input_builder = input.into_builder();
        input_builder
            .sleds_mut()
            .get_mut(&expunged_sled_id)
            .expect("can't find sled")
            .policy = SledPolicy::Expunged;
        let input = input_builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to re-plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunged sled):\n{}", diff.display());
        assert_eq!(
            blueprint2
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(sled_id, zone)| *sled_id != expunged_sled_id
                    && zone.zone_type.is_crucible_pantry())
                .count(),
            CRUCIBLE_PANTRY_REDUNDANCY,
            "can't find replacement pantry zone"
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Check that the planner can replace a single-node ClickHouse zone.
    /// This is completely distinct from (and much simpler than) the replicated
    /// (multi-node) case.
    #[test]
    fn test_single_node_clickhouse() {
        static TEST_NAME: &str = "test_single_node_clickhouse";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) = example(&logctx.log, TEST_NAME);

        // We should start with one ClickHouse zone. Find out which sled it's on.
        let clickhouse_sleds = blueprint1
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .filter_map(|(sled, zone)| {
                zone.zone_type.is_clickhouse().then(|| Some(sled))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            clickhouse_sleds.len(),
            1,
            "can't find ClickHouse zone in initial blueprint"
        );
        let clickhouse_sled = clickhouse_sleds[0].expect("missing sled id");

        // Expunge the sled hosting ClickHouse and re-plan. The planner should
        // immediately replace the zone with one on another (non-expunged) sled.
        let mut input_builder = input.into_builder();
        input_builder
            .sleds_mut()
            .get_mut(&clickhouse_sled)
            .expect("can't find sled")
            .policy = SledPolicy::Expunged;
        let input = input_builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("failed to re-plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunged sled):\n{}", diff.display());
        assert_eq!(
            blueprint2
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(sled, zone)| *sled != clickhouse_sled
                    && zone.zone_type.is_clickhouse())
                .count(),
            1,
            "can't find replacement ClickHouse zone"
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Deploy all keeper nodes server nodes at once for a new cluster.
    /// Then add keeper nodes 1 at a time.
    #[test]
    fn test_plan_deploy_all_clickhouse_cluster_nodes() {
        static TEST_NAME: &str = "planner_deploy_all_keeper_nodes";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let (mut collection, input, blueprint1) = example(&log, TEST_NAME);
        verify_blueprint(&blueprint1);

        // We shouldn't have a clickhouse cluster config, as we don't have a
        // clickhouse policy set yet
        assert!(blueprint1.clickhouse_cluster_config.is_none());
        let target_keepers = 3;
        let target_servers = 2;

        // Enable clickhouse clusters via policy
        let mut input_builder = input.into_builder();
        input_builder.policy_mut().clickhouse_policy =
            Some(clickhouse_policy(ClickhouseMode::Both {
                target_servers,
                target_keepers,
            }));

        // Creating a new blueprint should deploy all the new clickhouse zones
        let input = input_builder.build();
        let blueprint2 = Planner::new_based_on(
            log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        assert_contents(
            "tests/output/planner_deploy_all_keeper_nodes_1_2.txt",
            &diff.display().to_string(),
        );

        // We should see zones for 3 clickhouse keepers, and 2 servers created
        let active_zones: Vec<_> = blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .map(|(_, z)| z.clone())
            .collect();

        let keeper_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_keeper())
            .map(|z| z.id)
            .collect();
        let server_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_server())
            .map(|z| z.id)
            .collect();

        assert_eq!(keeper_zone_ids.len(), target_keepers as usize);
        assert_eq!(server_zone_ids.len(), target_servers as usize);

        // We should be attempting to allocate all servers and keepers since
        // this the initial configuration
        {
            let clickhouse_cluster_config =
                blueprint2.clickhouse_cluster_config.as_ref().unwrap();
            assert_eq!(clickhouse_cluster_config.generation, 2.into());
            assert_eq!(
                clickhouse_cluster_config.max_used_keeper_id,
                (u64::from(target_keepers)).into()
            );
            assert_eq!(
                clickhouse_cluster_config.max_used_server_id,
                (u64::from(target_servers)).into()
            );
            assert_eq!(
                clickhouse_cluster_config.keepers.len(),
                target_keepers as usize
            );
            assert_eq!(
                clickhouse_cluster_config.servers.len(),
                target_servers as usize
            );

            // Ensure that the added keepers are in server zones
            for zone_id in clickhouse_cluster_config.keepers.keys() {
                assert!(keeper_zone_ids.contains(zone_id));
            }

            // Ensure that the added servers are in server zones
            for zone_id in clickhouse_cluster_config.servers.keys() {
                assert!(server_zone_ids.contains(zone_id));
            }
        }

        // Planning again without changing inventory should result in the same
        // state
        let blueprint3 = Planner::new_based_on(
            log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("plan");

        assert_eq!(
            blueprint2.clickhouse_cluster_config,
            blueprint3.clickhouse_cluster_config
        );

        // Updating the inventory to reflect the keepers
        // should result in the same state, except for the
        // `highest_seen_keeper_leader_committed_log_index`
        let (_, keeper_id) = blueprint3
            .clickhouse_cluster_config
            .as_ref()
            .unwrap()
            .keepers
            .first_key_value()
            .unwrap();
        let membership = ClickhouseKeeperClusterMembership {
            queried_keeper: *keeper_id,
            leader_committed_log_index: 1,
            raft_config: blueprint3
                .clickhouse_cluster_config
                .as_ref()
                .unwrap()
                .keepers
                .values()
                .cloned()
                .collect(),
        };
        collection.clickhouse_keeper_cluster_membership.insert(membership);

        let blueprint4 = Planner::new_based_on(
            log.clone(),
            &blueprint3,
            &input,
            "test_blueprint4",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("plan");

        let diff = blueprint4.diff_since_blueprint(&blueprint3);
        assert_contents(
            "tests/output/planner_deploy_all_keeper_nodes_3_4.txt",
            &diff.display().to_string(),
        );

        let bp3_config = blueprint3.clickhouse_cluster_config.as_ref().unwrap();
        let bp4_config = blueprint4.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(bp4_config.generation, bp3_config.generation);
        assert_eq!(
            bp4_config.max_used_keeper_id,
            bp3_config.max_used_keeper_id
        );
        assert_eq!(
            bp4_config.max_used_server_id,
            bp3_config.max_used_server_id
        );
        assert_eq!(bp4_config.keepers, bp3_config.keepers);
        assert_eq!(bp4_config.servers, bp3_config.servers);
        assert_eq!(
            bp4_config.highest_seen_keeper_leader_committed_log_index,
            1
        );

        // Let's bump the clickhouse target to 5 via policy so that we can add
        // more nodes one at a time. Initial configuration deploys all nodes,
        // but reconfigurations may only add or remove one node at a time.
        // Enable clickhouse clusters via policy
        let target_keepers = 5;
        let mut input_builder = input.into_builder();
        input_builder.policy_mut().clickhouse_policy =
            Some(clickhouse_policy(ClickhouseMode::Both {
                target_servers,
                target_keepers,
            }));
        let input = input_builder.build();
        let blueprint5 = Planner::new_based_on(
            log.clone(),
            &blueprint4,
            &input,
            "test_blueprint5",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp5")))
        .plan()
        .expect("plan");

        let diff = blueprint5.diff_since_blueprint(&blueprint4);
        assert_contents(
            "tests/output/planner_deploy_all_keeper_nodes_4_5.txt",
            &diff.display().to_string(),
        );

        let active_zones: Vec<_> = blueprint5
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .map(|(_, z)| z.clone())
            .collect();

        let new_keeper_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_keeper())
            .map(|z| z.id)
            .collect();

        // We should have allocated 2 new keeper zones
        assert_eq!(new_keeper_zone_ids.len(), target_keepers as usize);
        assert!(keeper_zone_ids.is_subset(&new_keeper_zone_ids));

        // We should be trying to provision one new keeper for a keeper zone
        let bp4_config = blueprint4.clickhouse_cluster_config.as_ref().unwrap();
        let bp5_config = blueprint5.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(bp5_config.generation, bp4_config.generation.next());
        assert_eq!(
            bp5_config.max_used_keeper_id,
            bp4_config.max_used_keeper_id + 1.into()
        );
        assert_eq!(
            bp5_config.keepers.len(),
            bp5_config.max_used_keeper_id.0 as usize
        );

        // Planning again without updating inventory results in the same `ClickhouseClusterConfig`
        let blueprint6 = Planner::new_based_on(
            log.clone(),
            &blueprint5,
            &input,
            "test_blueprint6",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp6")))
        .plan()
        .expect("plan");

        let diff = blueprint6.diff_since_blueprint(&blueprint5);
        assert_contents(
            "tests/output/planner_deploy_all_keeper_nodes_5_6.txt",
            &diff.display().to_string(),
        );

        let bp6_config = blueprint6.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(bp5_config, bp6_config);

        // Updating the inventory to include the 4th node should add another
        // keeper node
        let membership = ClickhouseKeeperClusterMembership {
            queried_keeper: *keeper_id,
            leader_committed_log_index: 2,
            raft_config: blueprint6
                .clickhouse_cluster_config
                .as_ref()
                .unwrap()
                .keepers
                .values()
                .cloned()
                .collect(),
        };
        collection.clickhouse_keeper_cluster_membership.insert(membership);

        let blueprint7 = Planner::new_based_on(
            log.clone(),
            &blueprint6,
            &input,
            "test_blueprint7",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp7")))
        .plan()
        .expect("plan");

        let bp7_config = blueprint7.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(bp7_config.generation, bp6_config.generation.next());
        assert_eq!(
            bp7_config.max_used_keeper_id,
            bp6_config.max_used_keeper_id + 1.into()
        );
        assert_eq!(
            bp7_config.keepers.len(),
            bp7_config.max_used_keeper_id.0 as usize
        );
        assert_eq!(bp7_config.keepers.len(), target_keepers as usize);
        assert_eq!(
            bp7_config.highest_seen_keeper_leader_committed_log_index,
            2
        );

        // Updating the inventory to reflect the newest keeper node should not
        // increase the cluster size since we have reached the target.
        let membership = ClickhouseKeeperClusterMembership {
            queried_keeper: *keeper_id,
            leader_committed_log_index: 3,
            raft_config: blueprint7
                .clickhouse_cluster_config
                .as_ref()
                .unwrap()
                .keepers
                .values()
                .cloned()
                .collect(),
        };
        collection.clickhouse_keeper_cluster_membership.insert(membership);
        let blueprint8 = Planner::new_based_on(
            log.clone(),
            &blueprint7,
            &input,
            "test_blueprint8",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp8")))
        .plan()
        .expect("plan");

        let bp8_config = blueprint8.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(bp8_config.generation, bp7_config.generation);
        assert_eq!(
            bp8_config.max_used_keeper_id,
            bp7_config.max_used_keeper_id
        );
        assert_eq!(bp8_config.keepers, bp7_config.keepers);
        assert_eq!(bp7_config.keepers.len(), target_keepers as usize);
        assert_eq!(
            bp8_config.highest_seen_keeper_leader_committed_log_index,
            3
        );

        logctx.cleanup_successful();
    }

    // Start with an existing clickhouse cluster and expunge a keeper. This
    // models what will happen after an RSS deployment with clickhouse policy
    // enabled or an existing system already running a clickhouse cluster.
    #[test]
    fn test_expunge_clickhouse_clusters() {
        static TEST_NAME: &str = "planner_expunge_clickhouse_clusters";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let (mut collection, input, blueprint1) = example(&log, TEST_NAME);

        let target_keepers = 3;
        let target_servers = 2;

        // Enable clickhouse clusters via policy
        let mut input_builder = input.into_builder();
        input_builder.policy_mut().clickhouse_policy =
            Some(clickhouse_policy(ClickhouseMode::Both {
                target_servers,
                target_keepers,
            }));
        let input = input_builder.build();

        // Create a new blueprint to deploy all our clickhouse zones
        let mut blueprint2 = Planner::new_based_on(
            log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("plan");

        // We should see zones for 3 clickhouse keepers, and 2 servers created
        let active_zones: Vec<_> = blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .map(|(_, z)| z.clone())
            .collect();

        let keeper_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_keeper())
            .map(|z| z.id)
            .collect();
        let server_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_server())
            .map(|z| z.id)
            .collect();

        assert_eq!(keeper_zone_ids.len(), target_keepers as usize);
        assert_eq!(server_zone_ids.len(), target_servers as usize);

        // Directly manipulate the blueprint and inventory so that the
        // clickhouse clusters are stable
        let config = blueprint2.clickhouse_cluster_config.as_mut().unwrap();
        config.max_used_keeper_id = (u64::from(target_keepers)).into();
        config.keepers = keeper_zone_ids
            .iter()
            .enumerate()
            .map(|(i, zone_id)| (*zone_id, KeeperId(i as u64)))
            .collect();
        config.highest_seen_keeper_leader_committed_log_index = 1;

        let raft_config: BTreeSet<_> =
            config.keepers.values().cloned().collect();

        collection.clickhouse_keeper_cluster_membership = config
            .keepers
            .values()
            .map(|keeper_id| ClickhouseKeeperClusterMembership {
                queried_keeper: *keeper_id,
                leader_committed_log_index: 1,
                raft_config: raft_config.clone(),
            })
            .collect();

        // Let's run the planner. The blueprint shouldn't change with regards to
        // clickhouse as our inventory reflects our desired state.
        let blueprint3 = Planner::new_based_on(
            log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("plan");

        assert_eq!(
            blueprint2.clickhouse_cluster_config,
            blueprint3.clickhouse_cluster_config
        );

        // Find the sled containing one of the keeper zones and expunge it
        let (sled_id, bp_zone_config) = blueprint3
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .find(|(_, z)| z.zone_type.is_clickhouse_keeper())
            .unwrap();

        // What's the keeper id for this expunged zone?
        let expunged_keeper_id = blueprint3
            .clickhouse_cluster_config
            .as_ref()
            .unwrap()
            .keepers
            .get(&bp_zone_config.id)
            .unwrap();

        // Expunge a keeper zone
        let mut builder = input.into_builder();
        builder.expunge_sled(&sled_id).unwrap();
        let input = builder.build();

        let blueprint4 = Planner::new_based_on(
            log.clone(),
            &blueprint3,
            &input,
            "test_blueprint4",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("plan");

        let diff = blueprint4.diff_since_blueprint(&blueprint3);
        assert_contents(
            "tests/output/planner_expunge_clickhouse_clusters_3_4.txt",
            &diff.display().to_string(),
        );

        // The planner should expunge a zone based on the sled being expunged. Since this
        // is a clickhouse keeper zone, the clickhouse keeper configuration should change
        // to reflect this.
        let old_config = blueprint3.clickhouse_cluster_config.as_ref().unwrap();
        let config = blueprint4.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(config.generation, old_config.generation.next());
        assert!(!config.keepers.contains_key(&bp_zone_config.id));
        // We've only removed one keeper from our desired state
        assert_eq!(config.keepers.len() + 1, old_config.keepers.len());
        // We haven't allocated any new keepers
        assert_eq!(config.max_used_keeper_id, old_config.max_used_keeper_id);

        // Planning again will not change the keeper state because we haven't updated the inventory
        let blueprint5 = Planner::new_based_on(
            log.clone(),
            &blueprint4,
            &input,
            "test_blueprint5",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp5")))
        .plan()
        .expect("plan");

        assert_eq!(
            blueprint4.clickhouse_cluster_config,
            blueprint5.clickhouse_cluster_config
        );

        // Updating the inventory to reflect the removed keeper results in a new one being added

        // Remove the keeper for the expunged zone
        collection
            .clickhouse_keeper_cluster_membership
            .retain(|m| m.queried_keeper != *expunged_keeper_id);

        // Update the inventory on at least one of the remaining nodes.
        let mut existing = collection
            .clickhouse_keeper_cluster_membership
            .pop_first()
            .unwrap();
        existing.leader_committed_log_index = 3;
        existing.raft_config = config.keepers.values().cloned().collect();
        collection.clickhouse_keeper_cluster_membership.insert(existing);

        let blueprint6 = Planner::new_based_on(
            log.clone(),
            &blueprint5,
            &input,
            "test_blueprint6",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp6")))
        .plan()
        .expect("plan");

        let diff = blueprint6.diff_since_blueprint(&blueprint5);
        assert_contents(
            "tests/output/planner_expunge_clickhouse_clusters_5_6.txt",
            &diff.display().to_string(),
        );

        let old_config = blueprint5.clickhouse_cluster_config.as_ref().unwrap();
        let config = blueprint6.clickhouse_cluster_config.as_ref().unwrap();

        // Our generation has changed to reflect the added keeper
        assert_eq!(config.generation, old_config.generation.next());
        assert!(!config.keepers.contains_key(&bp_zone_config.id));
        // We've only added one keeper from our desired state
        // This brings us back up to our target count
        assert_eq!(config.keepers.len(), old_config.keepers.len() + 1);
        assert_eq!(config.keepers.len(), target_keepers as usize);
        // We've allocated one new keeper
        assert_eq!(
            config.max_used_keeper_id,
            old_config.max_used_keeper_id + 1.into()
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_expunge_clickhouse_zones_after_policy_is_changed() {
        static TEST_NAME: &str =
            "planner_expunge_clickhouse_zones_after_policy_is_changed";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let (collection, input, blueprint1) = example(&log, TEST_NAME);

        let target_keepers = 3;
        let target_servers = 2;

        // Enable clickhouse clusters via policy
        let mut input_builder = input.into_builder();
        input_builder.policy_mut().clickhouse_policy =
            Some(clickhouse_policy(ClickhouseMode::Both {
                target_servers,
                target_keepers,
            }));
        let input = input_builder.build();

        // Create a new blueprint to deploy all our clickhouse zones
        let blueprint2 = Planner::new_based_on(
            log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("plan");

        // We should see zones for 3 clickhouse keepers, and 2 servers created
        let active_zones: Vec<_> = blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .map(|(_, z)| z.clone())
            .collect();

        let keeper_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_keeper())
            .map(|z| z.id)
            .collect();
        let server_zone_ids: BTreeSet<_> = active_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_server())
            .map(|z| z.id)
            .collect();

        assert_eq!(keeper_zone_ids.len(), target_keepers as usize);
        assert_eq!(server_zone_ids.len(), target_servers as usize);

        // Disable clickhouse single node via policy, and ensure the zone goes
        // away. First ensure we have one.
        assert_eq!(
            1,
            active_zones.iter().filter(|z| z.zone_type.is_clickhouse()).count()
        );
        let mut input_builder = input.into_builder();
        input_builder.policy_mut().clickhouse_policy =
            Some(clickhouse_policy(ClickhouseMode::ClusterOnly {
                target_servers,
                target_keepers,
            }));
        let input = input_builder.build();

        // Create a new blueprint with `ClickhouseMode::ClusterOnly`
        let blueprint3 = Planner::new_based_on(
            log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("plan");

        // We should have expunged our single-node clickhouse zone
        let expunged_zones: Vec<_> = blueprint3
            .all_omicron_zones(BlueprintZoneDisposition::is_expunged)
            .map(|(_, z)| z.clone())
            .collect();

        assert_eq!(1, expunged_zones.len());
        assert!(expunged_zones.first().unwrap().zone_type.is_clickhouse());

        // Disable clickhouse clusters via policy and restart single node
        let mut input_builder = input.into_builder();
        input_builder.policy_mut().clickhouse_policy =
            Some(clickhouse_policy(ClickhouseMode::SingleNodeOnly));
        let input = input_builder.build();

        // Create a new blueprint for `ClickhouseMode::SingleNodeOnly`
        let blueprint4 = Planner::new_based_on(
            log.clone(),
            &blueprint3,
            &input,
            "test_blueprint4",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp4")))
        .plan()
        .expect("plan");

        let diff = blueprint4.diff_since_blueprint(&blueprint3);
        assert_contents(
            "tests/output/planner_expunge_clickhouse_zones_after_policy_is_changed_3_4.txt",
            &diff.display().to_string(),
        );

        // All our clickhouse keeper and server zones that we created when we
        // enabled our clickhouse policy should be expunged when we disable it.
        let expunged_zones: Vec<_> = blueprint4
            .all_omicron_zones(BlueprintZoneDisposition::is_expunged)
            .map(|(_, z)| z.clone())
            .collect();

        let expunged_keeper_zone_ids: BTreeSet<_> = expunged_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_keeper())
            .map(|z| z.id)
            .collect();
        let expunged_server_zone_ids: BTreeSet<_> = expunged_zones
            .iter()
            .filter(|z| z.zone_type.is_clickhouse_server())
            .map(|z| z.id)
            .collect();

        assert_eq!(keeper_zone_ids, expunged_keeper_zone_ids);
        assert_eq!(server_zone_ids, expunged_server_zone_ids);

        // We should have a new single-node clickhouse zone
        assert_eq!(
            1,
            blueprint4
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| z.zone_type.is_clickhouse())
                .count()
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_zones_marked_ready_for_cleanup_based_on_inventory() {
        static TEST_NAME: &str =
            "planner_zones_marked_ready_for_cleanup_based_on_inventory";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let (mut collection, input, blueprint1) = example(&log, TEST_NAME);

        // Don't start more internal DNS zones (which the planner would, as a
        // side effect of our test details).
        let input = {
            let mut builder = input.into_builder();
            builder.policy_mut().target_internal_dns_zone_count = 0;
            builder.build()
        };

        // Find a Nexus zone we'll use for our test.
        let (sled_id, nexus_config) = blueprint1
            .sleds
            .iter()
            .find_map(|(sled_id, sled_config)| {
                let nexus = sled_config
                    .zones
                    .iter()
                    .find(|z| z.zone_type.is_nexus())?;
                Some((*sled_id, nexus.clone()))
            })
            .expect("found a Nexus zone");

        // Expunge the disk used by the Nexus zone.
        let input = {
            let nexus_zpool = &nexus_config.filesystem_pool;
            let mut builder = input.into_builder();
            builder
                .sleds_mut()
                .get_mut(&sled_id)
                .expect("input has all sleds")
                .resources
                .zpools
                .get_mut(&nexus_zpool.id())
                .expect("input has Nexus disk")
                .policy = PhysicalDiskPolicy::Expunged;
            builder.build()
        };

        // Run the planner. It should expunge all zones on the disk we just
        // expunged, including our Nexus zone, but not mark them as ready for
        // cleanup yet.
        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "expunge disk",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("planned");

        // Mark the disk we expected as "ready for cleanup"; this isn't what
        // we're testing, and failing to do this will interfere with some of the
        // checks we do below.
        for mut disk in
            blueprint2.sleds.get_mut(&sled_id).unwrap().disks.iter_mut()
        {
            match disk.disposition {
                BlueprintPhysicalDiskDisposition::InService => (),
                BlueprintPhysicalDiskDisposition::Expunged {
                    as_of_generation,
                    ..
                } => {
                    disk.disposition =
                        BlueprintPhysicalDiskDisposition::Expunged {
                            as_of_generation,
                            ready_for_cleanup: true,
                        };
                }
            }
        }

        // Helper to extract the Nexus zone's disposition in a blueprint.
        let get_nexus_disposition = |bp: &Blueprint| {
            bp.sleds.get(&sled_id).unwrap().zones.iter().find_map(|z| {
                if z.id == nexus_config.id { Some(z.disposition) } else { None }
            })
        };

        // This sled's config generation should have been bumped...
        let bp2_config = blueprint2.sleds.get(&sled_id).unwrap().clone();
        let bp2_sled_config = bp2_config.clone().into_in_service_sled_config();
        assert_eq!(
            blueprint1
                .sleds
                .get(&sled_id)
                .unwrap()
                .sled_agent_generation
                .next(),
            bp2_sled_config.generation
        );
        // ... and the Nexus should should have the disposition we expect.
        assert_eq!(
            get_nexus_disposition(&blueprint2),
            Some(BlueprintZoneDisposition::Expunged {
                as_of_generation: bp2_sled_config.generation,
                ready_for_cleanup: false,
            })
        );

        // Running the planner again should make no changes until the inventory
        // reports that the zone is not running and that the sled has reconciled
        // a new-enough generation. Try these variants:
        //
        // * same inventory as above (expect no changes)
        // * new config is ledgered but not reconciled (expect no changes)
        // * new config is reconciled, but zone is in an error state (expect
        //   no changes)
        eprintln!("planning with no inventory change...");
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );
        eprintln!("planning with config ledgered but not reconciled...");
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &{
                let mut collection = collection.clone();
                collection
                    .sled_agents
                    .get_mut(&sled_id)
                    .unwrap()
                    .ledgered_sled_config = Some(bp2_sled_config.clone());
                collection
            },
            TEST_NAME,
        );
        eprintln!(
            "planning with config ledgered but \
             zones failed to shut down..."
        );
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &{
                let mut collection = collection.clone();
                collection
                    .sled_agents
                    .get_mut(&sled_id)
                    .unwrap()
                    .ledgered_sled_config = Some(bp2_sled_config.clone());
                let mut reconciliation =
                    ConfigReconcilerInventory::debug_assume_success(
                        bp2_sled_config.clone(),
                    );
                // For all the zones that are in bp2_config but not
                // bp2_sled_config (i.e., zones that should have been shut
                // down), insert an error result in the reconciliation.
                for zone_id in bp2_config.zones.keys() {
                    if !reconciliation.zones.contains_key(zone_id) {
                        reconciliation.zones.insert(
                            *zone_id,
                            ConfigReconcilerInventoryResult::Err {
                                message: "failed to shut down".to_string(),
                            },
                        );
                    }
                }
                collection
                    .sled_agents
                    .get_mut(&sled_id)
                    .unwrap()
                    .last_reconciliation = Some(reconciliation);
                collection
            },
            TEST_NAME,
        );

        // Now make both changes to the inventory.
        {
            let mut config = collection.sled_agents.get_mut(&sled_id).unwrap();
            config.ledgered_sled_config = Some(bp2_sled_config.clone());
            config.last_reconciliation =
                Some(ConfigReconcilerInventory::debug_assume_success(
                    bp2_sled_config.clone(),
                ));
        }

        // Run the planner. It mark our Nexus zone as ready for cleanup now that
        // the inventory conditions are satisfied.
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "removed Nexus zone from inventory",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("planned");

        assert_eq!(
            get_nexus_disposition(&blueprint3),
            Some(BlueprintZoneDisposition::Expunged {
                as_of_generation: bp2_sled_config.generation,
                ready_for_cleanup: true,
            })
        );

        // ready_for_cleanup changes should not bump the config generation,
        // since it doesn't affect what's sent to sled-agent.
        assert_eq!(
            blueprint3.sleds.get(&sled_id).unwrap().sled_agent_generation,
            bp2_sled_config.generation
        );

        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_internal_dns_zone_replaced_after_marked_for_cleanup() {
        static TEST_NAME: &str =
            "planner_internal_dns_zone_replaced_after_marked_for_cleanup";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let (mut collection, input, blueprint1) = example(&log, TEST_NAME);

        // Find a internal DNS zone we'll use for our test.
        let (sled_id, internal_dns_config) = blueprint1
            .sleds
            .iter()
            .find_map(|(sled_id, sled_config)| {
                let config = sled_config
                    .zones
                    .iter()
                    .find(|z| z.zone_type.is_internal_dns())?;
                Some((*sled_id, config.clone()))
            })
            .expect("found an Internal DNS zone");

        // Expunge the disk used by the internal DNS zone.
        let input = {
            let internal_dns_zpool = &internal_dns_config.filesystem_pool;
            let mut builder = input.into_builder();
            builder
                .sleds_mut()
                .get_mut(&sled_id)
                .expect("input has all sleds")
                .resources
                .zpools
                .get_mut(&internal_dns_zpool.id())
                .expect("input has internal DNS disk")
                .policy = PhysicalDiskPolicy::Expunged;
            builder.build()
        };

        // Run the planner. It should expunge all zones on the disk we just
        // expunged, including our DNS zone, but not mark them as ready for
        // cleanup yet.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "expunge disk",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp2")))
        .plan()
        .expect("planned");

        // Helper to extract the DNS zone's disposition in a blueprint.
        let get_dns_disposition = |bp: &Blueprint| {
            bp.sleds.get(&sled_id).unwrap().zones.iter().find_map(|z| {
                if z.id == internal_dns_config.id {
                    Some(z.disposition)
                } else {
                    None
                }
            })
        };

        // This sled's config generation should have been bumped...
        let bp2_config = blueprint2
            .sleds
            .get(&sled_id)
            .unwrap()
            .clone()
            .into_in_service_sled_config();
        assert_eq!(
            blueprint1
                .sleds
                .get(&sled_id)
                .unwrap()
                .sled_agent_generation
                .next(),
            bp2_config.generation
        );
        // ... and the DNS zone should should have the disposition we expect.
        assert_eq!(
            get_dns_disposition(&blueprint2),
            Some(BlueprintZoneDisposition::Expunged {
                as_of_generation: bp2_config.generation,
                ready_for_cleanup: false,
            })
        );

        // Running the planner again should make no changes until the inventory
        // reports that the zone is not running and that the sled has seen a
        // new-enough generation.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            TEST_NAME,
        );

        // Make the inventory changes necessary for cleanup to proceed.
        {
            let config = &mut collection.sled_agents.get_mut(&sled_id).unwrap();
            config.ledgered_sled_config = Some(bp2_config.clone());
            config.last_reconciliation =
                Some(ConfigReconcilerInventory::debug_assume_success(
                    bp2_config.clone(),
                ));
        }

        // Run the planner. It should mark our internal DNS zone as ready for
        // cleanup now that the inventory conditions are satisfied, and also
        // place a new internal DNS zone now that the original subnet is free to
        // reuse.
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "removed Nexus zone from inventory",
            &collection,
        )
        .expect("created planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("planned");

        assert_eq!(
            get_dns_disposition(&blueprint3),
            Some(BlueprintZoneDisposition::Expunged {
                as_of_generation: bp2_config.generation,
                ready_for_cleanup: true,
            })
        );

        let summary = blueprint3.diff_since_blueprint(&blueprint2);
        eprintln!("{}", summary.display());

        let mut added_count = 0;
        for sled_cfg in summary.diff.sleds.modified_values_diff() {
            added_count += sled_cfg.zones.added.len();
            for z in sled_cfg.zones.added.values() {
                match &z.zone_type {
                    BlueprintZoneType::InternalDns(internal_dns) => {
                        let BlueprintZoneType::InternalDns(InternalDns {
                            dns_address: orig_dns_address,
                            ..
                        }) = &internal_dns_config.zone_type
                        else {
                            unreachable!();
                        };

                        assert_eq!(internal_dns.dns_address, *orig_dns_address);
                    }
                    _ => panic!("unexpected added zone {z:?}"),
                }
            }
        }
        assert_eq!(added_count, 1);

        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            &collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Manually update the example system's inventory collection's zones
    /// from a blueprint.
    fn update_collection_from_blueprint(
        example: &mut ExampleSystem,
        blueprint: &Blueprint,
    ) {
        for (&sled_id, config) in blueprint.sleds.iter() {
            example
                .system
                .sled_set_omicron_config(
                    sled_id,
                    config.clone().into_in_service_sled_config(),
                )
                .expect("can't set sled config");
        }
        example.collection =
            example.system.to_collection_builder().unwrap().build();
    }

    macro_rules! fake_zone_artifact {
        ($kind: ident, $version: expr) => {
            TufArtifactMeta {
                id: ArtifactId {
                    name: ZoneKind::$kind.artifact_id_name().to_string(),
                    version: $version,
                    kind: ArtifactKind::from_known(KnownArtifactKind::Zone),
                },
                hash: ArtifactHash([0; 32]),
                size: 0,
            }
        };
    }

    fn create_artifacts_at_version(
        version: &ArtifactVersion,
    ) -> Vec<TufArtifactMeta> {
        vec![
            // Omit `BoundaryNtp` because it has the same artifact name as
            // `InternalNtp`.
            fake_zone_artifact!(Clickhouse, version.clone()),
            fake_zone_artifact!(ClickhouseKeeper, version.clone()),
            fake_zone_artifact!(ClickhouseServer, version.clone()),
            fake_zone_artifact!(CockroachDb, version.clone()),
            fake_zone_artifact!(Crucible, version.clone()),
            fake_zone_artifact!(CruciblePantry, version.clone()),
            fake_zone_artifact!(ExternalDns, version.clone()),
            fake_zone_artifact!(InternalDns, version.clone()),
            fake_zone_artifact!(InternalNtp, version.clone()),
            fake_zone_artifact!(Nexus, version.clone()),
            fake_zone_artifact!(Oximeter, version.clone()),
        ]
    }

    /// Ensure that dependent zones (here just Crucible Pantry) are updated
    /// before Nexus.
    #[test]
    fn test_update_crucible_pantry() {
        static TEST_NAME: &str = "update_crucible_pantry";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut example, mut blueprint1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        verify_blueprint(&blueprint1);

        // We should start with no specified TUF repo and nothing to do.
        assert!(example.input.tuf_repo().description().tuf_repo().is_none());
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint1,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // All zones should be sourced from the install dataset by default.
        assert!(
            blueprint1
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .all(|(_, z)| matches!(
                    z.image_source,
                    BlueprintZoneImageSource::InstallDataset
                ))
        );

        // This generation is successively incremented for each TUF repo. We use
        // generation 2 to represent the first generation with a TUF repo
        // attached.
        let target_release_generation = Generation::from_u32(2);

        // Manually specify a TUF repo with fake zone images.
        let mut input_builder = example.input.clone().into_builder();
        let version = ArtifactVersion::new_static("1.0.0-freeform")
            .expect("can't parse artifact version");
        let fake_hash = ArtifactHash([0; 32]);
        let image_source = BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: version.clone(),
            },
            hash: fake_hash,
        };
        let artifacts = create_artifacts_at_version(&version);
        let target_release_generation = target_release_generation.next();
        input_builder.policy_mut().tuf_repo = TufRepoPolicy {
            target_release_generation,
            description: TargetReleaseDescription::TufRepo(
                TufRepoDescription {
                    repo: TufRepoMeta {
                        hash: fake_hash,
                        targets_role_version: 0,
                        valid_until: Utc::now(),
                        system_version: Version::new(1, 0, 0),
                        file_name: String::from(""),
                    },
                    artifacts,
                },
            ),
        };

        // Some helper predicates for the assertions below.
        let is_old_nexus = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_nexus()
                && matches!(
                    zone.image_source,
                    BlueprintZoneImageSource::InstallDataset
                )
        };
        let is_up_to_date_nexus = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_nexus() && zone.image_source == image_source
        };
        let is_old_pantry = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_crucible_pantry()
                && matches!(
                    zone.image_source,
                    BlueprintZoneImageSource::InstallDataset
                )
        };
        let is_up_to_date_pantry = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_crucible_pantry()
                && zone.image_source == image_source
        };

        // Manually update all zones except CruciblePantry and Nexus.
        for mut zone in blueprint1
            .sleds
            .values_mut()
            .flat_map(|config| config.zones.iter_mut())
            .filter(|z| {
                !z.zone_type.is_nexus() && !z.zone_type.is_crucible_pantry()
            })
        {
            zone.image_source = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: version.clone(),
                },
                hash: fake_hash,
            };
        }

        // Request another Nexus zone.
        input_builder.policy_mut().target_nexus_zone_count =
            input_builder.policy_mut().target_nexus_zone_count + 1;
        let input = input_builder.build();

        // Check that there is a new nexus zone that does *not* use the new
        // artifact (since not all of its dependencies are updated yet).
        update_collection_from_blueprint(&mut example, &blueprint1);
        let blueprint2 = Planner::new_based_on(
            log.clone(),
            &blueprint1,
            &input,
            "test_blueprint3",
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp3")))
        .plan()
        .expect("can't re-plan for new Nexus zone");
        {
            let summary = blueprint2.diff_since_blueprint(&blueprint1);
            for sled in summary.diff.sleds.modified_values_diff() {
                assert!(sled.zones.removed.is_empty());
                assert_eq!(sled.zones.added.len(), 1);
                let added = sled.zones.added.values().next().unwrap();
                assert!(matches!(
                    &added.zone_type,
                    BlueprintZoneType::Nexus(_)
                ));
                assert!(matches!(
                    &added.image_source,
                    BlueprintZoneImageSource::InstallDataset
                ));
            }
        }

        // We should now have three sets of expunge/add iterations for the
        // Crucible Pantry zones.
        let mut parent = blueprint2;
        for i in 3..=8 {
            let blueprint_name = format!("blueprint_{i}");
            update_collection_from_blueprint(&mut example, &parent);
            let blueprint = Planner::new_based_on(
                log.clone(),
                &parent,
                &input,
                &blueprint_name,
                &example.collection,
            )
            .expect("can't create planner")
            .with_rng(PlannerRng::from_seed((TEST_NAME, &blueprint_name)))
            .plan()
            .unwrap_or_else(|_| panic!("can't re-plan after {i} iterations"));

            {
                let summary = blueprint.diff_since_blueprint(&parent);
                eprintln!("diff to {blueprint_name}: {}", summary.display());
                for sled in summary.diff.sleds.modified_values_diff() {
                    if i % 2 == 1 {
                        assert!(sled.zones.added.is_empty());
                        assert!(sled.zones.removed.is_empty());
                        assert_eq!(
                            sled.zones
                                .common
                                .iter()
                                .filter(|(_, z)| matches!(
                                    z.after.zone_type,
                                    BlueprintZoneType::CruciblePantry(_)
                                ) && matches!(
                                    z.after.disposition,
                                    BlueprintZoneDisposition::Expunged { .. }
                                ))
                                .count(),
                            1
                        );
                    } else {
                        assert!(sled.zones.removed.is_empty());
                        assert_eq!(sled.zones.added.len(), 1);
                        let added = sled.zones.added.values().next().unwrap();
                        assert!(matches!(
                            &added.zone_type,
                            BlueprintZoneType::CruciblePantry(_)
                        ));
                        assert_eq!(added.image_source, image_source);
                    }
                }
            }

            parent = blueprint;
        }
        let blueprint8 = parent;

        // All Crucible Pantries should now be updated.
        assert_eq!(
            blueprint8
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_up_to_date_pantry(z))
                .count(),
            CRUCIBLE_PANTRY_REDUNDANCY
        );

        // All old Pantry zones should now be expunged.
        assert_eq!(
            blueprint8
                .all_omicron_zones(BlueprintZoneDisposition::is_expunged)
                .filter(|(_, z)| is_old_pantry(z))
                .count(),
            CRUCIBLE_PANTRY_REDUNDANCY
        );

        // Now we can update Nexus, because all of its dependent zones
        // are up-to-date w/r/t the new repo.
        assert_eq!(
            blueprint8
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_old_nexus(z))
                .count(),
            NEXUS_REDUNDANCY + 1,
        );
        let mut parent = blueprint8;
        for i in 9..=16 {
            update_collection_from_blueprint(&mut example, &parent);

            let blueprint_name = format!("blueprint{i}");
            let blueprint = Planner::new_based_on(
                log.clone(),
                &parent,
                &input,
                &blueprint_name,
                &example.collection,
            )
            .expect("can't create planner")
            .with_rng(PlannerRng::from_seed((TEST_NAME, &blueprint_name)))
            .plan()
            .unwrap_or_else(|_| panic!("can't re-plan after {i} iterations"));

            {
                let summary = blueprint.diff_since_blueprint(&parent);
                for sled in summary.diff.sleds.modified_values_diff() {
                    if i % 2 == 1 {
                        assert!(sled.zones.added.is_empty());
                        assert!(sled.zones.removed.is_empty());
                    } else {
                        assert!(sled.zones.removed.is_empty());
                        assert_eq!(sled.zones.added.len(), 1);
                        let added = sled.zones.added.values().next().unwrap();
                        assert!(matches!(
                            &added.zone_type,
                            BlueprintZoneType::Nexus(_)
                        ));
                        assert_eq!(added.image_source, image_source);
                    }
                }
            }

            parent = blueprint;
        }

        // Everything's up-to-date in Kansas City!
        let blueprint16 = parent;
        assert_eq!(
            blueprint16
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_up_to_date_nexus(z))
                .count(),
            NEXUS_REDUNDANCY + 1,
        );

        update_collection_from_blueprint(&mut example, &blueprint16);
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint16,
            &input,
            &example.collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_update_cockroach() {
        static TEST_NAME: &str = "update_cockroach";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut example, mut blueprint) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        verify_blueprint(&blueprint);

        // Update the example system and blueprint, as a part of test set-up.
        //
        // Ask for COCKROACHDB_REDUNDANCY cockroach nodes

        let mut input_builder = example.input.clone().into_builder();
        input_builder.policy_mut().target_cockroachdb_zone_count =
            COCKROACHDB_REDUNDANCY;
        example.input = input_builder.build();

        let blueprint_name = "blueprint_with_cockroach";
        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            &blueprint_name,
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, &blueprint_name)))
        .plan()
        .unwrap_or_else(|_| panic!("can't plan to include Cockroach nodes"));

        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            assert_eq!(summary.total_zones_added(), COCKROACHDB_REDUNDANCY);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 0);
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);

        // We should have started with no specified TUF repo and nothing to do.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // All zones should be sourced from the install dataset by default.
        assert!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .all(|(_, z)| matches!(
                    z.image_source,
                    BlueprintZoneImageSource::InstallDataset
                ))
        );

        // This test "starts" here -- we specify a new TUF repo with an updated
        // CockroachDB image. We create a new TUF repo where version of
        // CockroachDB has been updated out of the install dataset.
        //
        // The planner should avoid doing this update until it has confirmation
        // from inventory that the cluster is healthy.

        let mut input_builder = example.input.clone().into_builder();
        let version = ArtifactVersion::new_static("1.0.0-freeform")
            .expect("can't parse artifact version");
        let fake_hash = ArtifactHash([0; 32]);
        let image_source = BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: version.clone(),
            },
            hash: fake_hash,
        };
        let artifacts = create_artifacts_at_version(&version);
        let target_release_generation = Generation::from_u32(2);
        input_builder.policy_mut().tuf_repo = TufRepoPolicy {
            target_release_generation,
            description: TargetReleaseDescription::TufRepo(
                TufRepoDescription {
                    repo: TufRepoMeta {
                        hash: fake_hash,
                        targets_role_version: 0,
                        valid_until: Utc::now(),
                        system_version: Version::new(1, 0, 0),
                        file_name: String::from(""),
                    },
                    artifacts,
                },
            ),
        };
        example.input = input_builder.build();

        // Manually update all zones except Cockroach
        //
        // We just specified a new TUF repo, everything is going to shift from
        // the install dataset to this new repo.
        for mut zone in blueprint
            .sleds
            .values_mut()
            .flat_map(|config| config.zones.iter_mut())
            .filter(|z| !z.zone_type.is_cockroach())
        {
            zone.image_source = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: version.clone(),
                },
                hash: fake_hash,
            };
        }
        update_collection_from_blueprint(&mut example, &blueprint);

        // Some helper predicates for the assertions below.
        let is_old_cockroach = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_cockroach()
                && matches!(
                    zone.image_source,
                    BlueprintZoneImageSource::InstallDataset
                )
        };
        let is_up_to_date_cockroach = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_cockroach() && zone.image_source == image_source
        };
        let create_valid_looking_status = || {
            let mut result = BTreeMap::new();
            for i in 1..=COCKROACHDB_REDUNDANCY {
                result.insert(
                    omicron_cockroach_metrics::NodeId(i.to_string()),
                    CockroachStatus {
                        ranges_underreplicated: Some(0),
                        liveness_live_nodes: Some(GOAL_REDUNDANCY),
                    },
                );
            }
            result
        };

        // If we have missing info in our inventory, the
        // planner will not update any Cockroach zones.
        example.collection.cockroach_status = BTreeMap::new();
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // If we're missing info from even a single node, we
        // will still refuse to update.
        example.collection.cockroach_status = create_valid_looking_status();
        example.collection.cockroach_status.pop_first();
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        const GOAL_REDUNDANCY: u64 = COCKROACHDB_REDUNDANCY as u64;

        // If we have any non-zero "ranges_underreplicated" in in our inventory,
        // the planner will not update any Cockroach zones.
        example.collection.cockroach_status = create_valid_looking_status();
        *example
            .collection
            .cockroach_status
            .get_mut(&omicron_cockroach_metrics::NodeId("1".to_string()))
            .unwrap() = CockroachStatus {
            ranges_underreplicated: Some(1),
            liveness_live_nodes: Some(GOAL_REDUNDANCY),
        };
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // If we don't have enough live nodes, we won't update Cockroach zones.
        example.collection.cockroach_status = create_valid_looking_status();
        *example
            .collection
            .cockroach_status
            .get_mut(&omicron_cockroach_metrics::NodeId("1".to_string()))
            .unwrap() = CockroachStatus {
            ranges_underreplicated: Some(0),
            liveness_live_nodes: Some(GOAL_REDUNDANCY - 1),
        };

        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // Once we have zero underreplicated ranges, we can start to update
        // Cockroach zones.
        //
        // We'll update one zone at a time, from the install dataset to the
        // new TUF repo artifact.
        for i in 1..=COCKROACHDB_REDUNDANCY {
            // Keep setting this value in a loop;
            // "update_collection_from_blueprint" resets it.
            example.collection.cockroach_status = create_valid_looking_status();

            println!("Updating cockroach {i} of {COCKROACHDB_REDUNDANCY}");
            let new_blueprint = Planner::new_based_on(
                log.clone(),
                &blueprint,
                &example.input,
                &format!("test_blueprint_cockroach_{i}"),
                &example.collection,
            )
            .expect("can't create planner")
            .with_rng(PlannerRng::from_seed((TEST_NAME, "bp_crdb")))
            .plan()
            .expect("plan for trivial TUF repo");

            blueprint = new_blueprint;

            assert_eq!(
                blueprint
                    .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                    .filter(|(_, z)| is_old_cockroach(z))
                    .count(),
                COCKROACHDB_REDUNDANCY - i
            );
            assert_eq!(
                blueprint
                    .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                    .filter(|(_, z)| is_up_to_date_cockroach(z))
                    .count(),
                i
            );
            update_collection_from_blueprint(&mut example, &blueprint);
        }

        // Validate that we have no further changes to make, once all Cockroach
        // zones have been updated.
        example.collection.cockroach_status = create_valid_looking_status();

        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // Validate that we do not flip back to the install dataset after
        // performing the update.
        example.collection.cockroach_status = create_valid_looking_status();
        example
            .collection
            .cockroach_status
            .values_mut()
            .next()
            .unwrap()
            .ranges_underreplicated = Some(1);

        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_update_boundary_ntp() {
        static TEST_NAME: &str = "update_boundary_ntp";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut example, mut blueprint) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        verify_blueprint(&blueprint);

        // The example system creates three internal NTP zones, and zero
        // boundary NTP zones. This is a little arbitrary, but we're checking it
        // here: the lack of boundary NTP zones means we need to perform some
        // manual promotion of "internal -> boundary NTP", as documented below.

        assert_eq!(
            example
                .collection
                .all_running_omicron_zones()
                .filter(|zone_config| { zone_config.zone_type.is_ntp() })
                .count(),
            3,
        );
        assert_eq!(
            example
                .collection
                .all_running_omicron_zones()
                .filter(|zone_config| {
                    zone_config.zone_type.is_boundary_ntp()
                })
                .count(),
            0,
        );

        // Update the example system and blueprint, as a part of test set-up.
        //
        // Ask for BOUNDARY_NTP_REDUNDANCY boundary NTP zones.
        //
        // To pull this off, we need to have AT LEAST ONE boundary NTP zone
        // that already exists. We'll perform a manual promotion first, then
        // ask for the other boundary NTP zones.

        {
            let mut zone = blueprint
                .sleds
                .values_mut()
                .flat_map(|config| config.zones.iter_mut())
                .find(|z| z.zone_type.is_ntp())
                .unwrap();
            let address = match zone.zone_type {
                BlueprintZoneType::InternalNtp(
                    blueprint_zone_type::InternalNtp { address },
                ) => address,
                _ => panic!("should be internal NTP?"),
            };

            // The contents here are all lies, but it's just stored
            // as plain-old-data for the purposes of this test, so
            // it doesn't need to be real.
            zone.zone_type = BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp {
                    address,
                    ntp_servers: vec![],
                    dns_servers: vec![],
                    domain: None,
                    nic: NetworkInterface {
                        id: Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: Uuid::new_v4(),
                        },
                        name: "ntp-0".parse().unwrap(),
                        ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
                        mac: MacAddr::random_system(),
                        subnet: oxnet::IpNet::new(
                            IpAddr::V6(Ipv6Addr::LOCALHOST),
                            8,
                        )
                        .unwrap(),
                        vni: Vni::SERVICES_VNI,
                        primary: true,
                        slot: 0,
                        transit_ips: vec![],
                    },
                    external_ip: OmicronZoneExternalSnatIp {
                        id: ExternalIpUuid::new_v4(),
                        snat_cfg: SourceNatConfig::new(
                            IpAddr::V6(Ipv6Addr::LOCALHOST),
                            0,
                            0x4000 - 1,
                        )
                        .unwrap(),
                    },
                },
            );
        }
        update_collection_from_blueprint(&mut example, &blueprint);

        // We should have one boundary NTP zone now.
        assert_eq!(
            example
                .collection
                .all_running_omicron_zones()
                .filter(|zone_config| {
                    zone_config.zone_type.is_boundary_ntp()
                })
                .count(),
            1,
        );

        // Use that boundary NTP zone to promote others.
        let mut input_builder = example.input.clone().into_builder();
        input_builder.policy_mut().target_boundary_ntp_zone_count =
            BOUNDARY_NTP_REDUNDANCY;
        example.input = input_builder.build();
        let blueprint_name = "blueprint_with_boundary_ntp";
        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            &blueprint_name,
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, &blueprint_name)))
        .plan()
        .unwrap_or_else(|err| {
            panic!("can't plan to include boundary NTP: {err}")
        });

        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            assert_eq!(
                summary.total_zones_added(),
                BOUNDARY_NTP_REDUNDANCY - 1
            );
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(
                summary.total_zones_modified(),
                BOUNDARY_NTP_REDUNDANCY - 1
            );
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);

        assert_eq!(
            example
                .collection
                .all_running_omicron_zones()
                .filter(|zone_config| {
                    zone_config.zone_type.is_boundary_ntp()
                })
                .count(),
            BOUNDARY_NTP_REDUNDANCY
        );

        let planner = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            TEST_NAME,
            &example.collection,
        )
        .expect("can't create planner");
        let new_blueprint = planner.plan().expect("planning succeeded");
        verify_blueprint(&new_blueprint);
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            assert_eq!(summary.total_zones_added(), 0);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(
                summary.total_zones_modified(),
                BOUNDARY_NTP_REDUNDANCY - 1
            );
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);

        // We should have started with no specified TUF repo and nothing to do.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // All zones should be sourced from the install dataset by default.
        assert!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .all(|(_, z)| matches!(
                    z.image_source,
                    BlueprintZoneImageSource::InstallDataset
                ))
        );

        // This test "starts" here -- we specify a new TUF repo with an updated
        // Boundary NTP image. We create a new TUF repo where version of
        // Boundary NTP has been updated out of the install dataset.
        //
        // The planner should avoid doing this update until it has confirmation
        // from inventory that the cluster is healthy.

        let mut input_builder = example.input.clone().into_builder();
        let version = ArtifactVersion::new_static("1.0.0-freeform")
            .expect("can't parse artifact version");
        let fake_hash = ArtifactHash([0; 32]);
        let image_source = BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: version.clone(),
            },
            hash: fake_hash,
        };
        let artifacts = create_artifacts_at_version(&version);
        let target_release_generation = Generation::from_u32(2);
        input_builder.policy_mut().tuf_repo = TufRepoPolicy {
            target_release_generation,
            description: TargetReleaseDescription::TufRepo(
                TufRepoDescription {
                    repo: TufRepoMeta {
                        hash: fake_hash,
                        targets_role_version: 0,
                        valid_until: Utc::now(),
                        system_version: Version::new(1, 0, 0),
                        file_name: String::from(""),
                    },
                    artifacts,
                },
            ),
        };
        example.input = input_builder.build();

        // Manually update all zones except boundary NTP
        //
        // We just specified a new TUF repo, everything is going to shift from
        // the install dataset to this new repo.
        for mut zone in blueprint
            .sleds
            .values_mut()
            .flat_map(|config| config.zones.iter_mut())
            .filter(|z| !z.zone_type.is_boundary_ntp())
        {
            zone.image_source = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: version.clone(),
                },
                hash: fake_hash,
            };
        }
        update_collection_from_blueprint(&mut example, &blueprint);

        // Some helper predicates for the assertions below.
        let is_old_boundary_ntp = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_boundary_ntp()
                && matches!(
                    zone.image_source,
                    BlueprintZoneImageSource::InstallDataset
                )
        };
        let old_boundary_ntp_count = |blueprint: &Blueprint| -> usize {
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_old_boundary_ntp(z))
                .count()
        };
        let is_up_to_date_boundary_ntp = |zone: &BlueprintZoneConfig| -> bool {
            zone.zone_type.is_boundary_ntp()
                && zone.image_source == image_source
        };
        let up_to_date_boundary_ntp_count = |blueprint: &Blueprint| -> usize {
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_up_to_date_boundary_ntp(z))
                .count()
        };

        let set_valid_looking_timesync = |collection: &mut Collection| {
            let mut ntp_timesync = IdOrdMap::<TimeSync>::new();

            for sled in &collection.sled_agents {
                let config = &sled
                    .last_reconciliation
                    .as_ref()
                    .expect("Sled missing ledger? {sled:?}")
                    .last_reconciled_config;

                let Some(zone_id) =
                    config.zones.iter().find_map(|zone| match zone.zone_type {
                        OmicronZoneType::BoundaryNtp { .. }
                        | OmicronZoneType::InternalNtp { .. } => Some(zone.id),
                        _ => None,
                    })
                else {
                    // Sled without NTP
                    continue;
                };

                ntp_timesync
                    .insert_unique(TimeSync { zone_id, synced: true })
                    .expect("NTP zone with same zone ID seen repeatedly");
            }
            collection.ntp_timesync = ntp_timesync;
        };

        // If we have missing info in our inventory, the
        // planner will not update any boundary NTP zones.
        example.collection.ntp_timesync = IdOrdMap::new();
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // If we don't have enough info from boundary NTP nodes, we'll refuse to
        // update.
        set_valid_looking_timesync(&mut example.collection);
        let boundary_ntp_zone = example
            .collection
            .all_running_omicron_zones()
            .find_map(|z| {
                if let OmicronZoneType::BoundaryNtp { .. } = z.zone_type {
                    Some(z.id)
                } else {
                    None
                }
            })
            .unwrap();
        example.collection.ntp_timesync.remove(&boundary_ntp_zone);
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // If we don't have enough explicitly synced nodes, we'll refuse to
        // update.
        set_valid_looking_timesync(&mut example.collection);
        let boundary_ntp_zone = example
            .collection
            .all_running_omicron_zones()
            .find_map(|z| {
                if let OmicronZoneType::BoundaryNtp { .. } = z.zone_type {
                    Some(z.id)
                } else {
                    None
                }
            })
            .unwrap();
        example
            .collection
            .ntp_timesync
            .get_mut(&boundary_ntp_zone)
            .unwrap()
            .synced = false;
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // Once all nodes are timesync'd, we can start to update boundary NTP
        // zones.
        //
        // We'll update one zone at a time, from the install dataset to the
        // new TUF repo artifact.
        set_valid_looking_timesync(&mut example.collection);

        //
        // Step 1:
        //
        // * Expunge old boundary NTP. This is treated as a "modified zone", here and below.
        //
        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            "test_blueprint_expunge_old_boundary_ntp",
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp_ntp")))
        .plan()
        .expect("plan for trivial TUF repo");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            eprintln!(
                "diff between blueprints (should be expunging boundary NTP using install dataset):\n{}",
                summary.display()
            );

            assert_eq!(summary.total_zones_added(), 0);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 1);
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);
        set_valid_looking_timesync(&mut example.collection);

        // NOTE: This is a choice! The current planner is opting to reduce the
        // redundancy count of boundary NTP zones for the duration of the
        // upgrade.
        assert_eq!(
            old_boundary_ntp_count(&blueprint),
            BOUNDARY_NTP_REDUNDANCY - 1
        );
        assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 0);

        //
        // Step 2:
        //
        // On one sled:
        // * Finish expunging the boundary NTP zone (started in prior step)
        // + Add an internal NTP zone on the sled where that boundary NTP zone was expunged.
        // Since NTP is a non-discretionary zone, this is the default behavior.
        //
        // On another sled, do promotion to try to restore boundary NTP redundancy:
        // * Expunge an internal NTP zone
        // + Add it back as a boundary NTP zone
        //

        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            "test_blueprint_boundary_ntp_add_internal_and_promote_one",
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp_ntp")))
        .plan()
        .expect("plan for trivial TUF repo");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            eprintln!(
                "diff between blueprints (should be adding one internal NTP and promoting another to boundary):\n{}",
                summary.display()
            );

            assert_eq!(summary.total_zones_added(), 2);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 2);
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);
        set_valid_looking_timesync(&mut example.collection);

        assert_eq!(old_boundary_ntp_count(&blueprint), 1);
        assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 1);

        //
        // Step 3:
        //
        // Now that the sum of "old + new" boundary NTP zones == BOUNDARY_NTP_REDUNDANCY,
        // we can finish the upgrade process.
        //
        // * Start expunging the remaining old boundary NTP zone
        // * Finish expunging the internal NTP zone (started in prior step)
        //

        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            "test_blueprint_boundary_ntp_expunge_the_other_one",
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp_ntp")))
        .plan()
        .expect("plan for trivial TUF repo");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            eprintln!(
                "diff between blueprints (should be expunging another boundary NTP):\n{}",
                summary.display()
            );

            assert_eq!(summary.total_zones_added(), 0);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 2);
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);
        set_valid_looking_timesync(&mut example.collection);

        assert_eq!(old_boundary_ntp_count(&blueprint), 0);
        assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 1);

        //
        // Step 4:
        //
        // Promotion:
        // + Add a boundary NTP on a sled where there was an internal NTP
        // + Start expunging an internal NTP
        //
        // Cleanup:
        // * Finish expunging the boundary NTP on the install dataset
        //

        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            "test_blueprint_boundary_ntp_promotion",
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp_ntp")))
        .plan()
        .expect("plan for trivial TUF repo");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            eprintln!(
                "diff between blueprints (should be adding promoting internal -> boundary NTP):\n{}",
                summary.display()
            );

            assert_eq!(summary.total_zones_added(), 2);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 1);
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);
        set_valid_looking_timesync(&mut example.collection);

        assert_eq!(old_boundary_ntp_count(&blueprint), 0);
        assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 2);

        //
        // Step 5:
        //
        // Cleanup:
        // * Finish clearing out expunged internal NTP zones (added in prior step)
        //

        let new_blueprint = Planner::new_based_on(
            log.clone(),
            &blueprint,
            &example.input,
            "test_blueprint_boundary_ntp_finish_expunging",
            &example.collection,
        )
        .expect("can't create planner")
        .with_rng(PlannerRng::from_seed((TEST_NAME, "bp_ntp")))
        .plan()
        .expect("plan for trivial TUF repo");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            eprintln!(
                "diff between blueprints (should be adding wrapping up internal NTP expungement):\n{}",
                summary.display()
            );

            assert_eq!(summary.total_zones_added(), 0);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 1);
        }
        blueprint = new_blueprint;
        update_collection_from_blueprint(&mut example, &blueprint);
        set_valid_looking_timesync(&mut example.collection);

        assert_eq!(old_boundary_ntp_count(&blueprint), 0);
        assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 2);

        // Validate that we have no further changes to make, once all Boundary
        // NTP zones have been updated.
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        // Validate that we do not flip back to the install dataset after
        // performing the update, even if we lose timesync data.
        example.collection.ntp_timesync = IdOrdMap::new();
        assert_planning_makes_no_changes(
            &log,
            &blueprint,
            &example.input,
            &example.collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    /// Ensure that planning to update all zones terminates.
    #[test]
    fn test_update_all_zones() {
        static TEST_NAME: &str = "update_all_zones";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();

        // Use our example system.
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut example, blueprint1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        verify_blueprint(&blueprint1);

        // All zones should be sourced from the install dataset by default.
        assert!(
            blueprint1
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .all(|(_, z)| matches!(
                    z.image_source,
                    BlueprintZoneImageSource::InstallDataset
                ))
        );

        // Manually specify a TUF repo with fake images for all zones.
        // Only the name and kind of the artifacts matter.
        let mut input_builder = example.input.clone().into_builder();
        let version = ArtifactVersion::new_static("2.0.0-freeform")
            .expect("can't parse artifact version");
        let fake_hash = ArtifactHash([0; 32]);
        let image_source = BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: version.clone(),
            },
            hash: fake_hash,
        };
        // We use generation 2 to represent the first generation with a TUF repo
        // attached.
        let target_release_generation = Generation::new().next();
        let tuf_repo = TufRepoPolicy {
            target_release_generation,
            description: TargetReleaseDescription::TufRepo(
                TufRepoDescription {
                    repo: TufRepoMeta {
                        hash: fake_hash,
                        targets_role_version: 0,
                        valid_until: Utc::now(),
                        system_version: Version::new(1, 0, 0),
                        file_name: String::from(""),
                    },
                    artifacts: vec![
                        // Omit `BoundaryNtp` because it has the same artifact
                        // name as `InternalNtp`.
                        fake_zone_artifact!(Clickhouse, version.clone()),
                        fake_zone_artifact!(ClickhouseKeeper, version.clone()),
                        fake_zone_artifact!(ClickhouseServer, version.clone()),
                        fake_zone_artifact!(CockroachDb, version.clone()),
                        fake_zone_artifact!(Crucible, version.clone()),
                        fake_zone_artifact!(CruciblePantry, version.clone()),
                        fake_zone_artifact!(ExternalDns, version.clone()),
                        fake_zone_artifact!(InternalDns, version.clone()),
                        fake_zone_artifact!(InternalNtp, version.clone()),
                        fake_zone_artifact!(Nexus, version.clone()),
                        fake_zone_artifact!(Oximeter, version.clone()),
                    ],
                },
            ),
        };
        input_builder.policy_mut().tuf_repo = tuf_repo;
        let input = input_builder.build();

        /// Expected number of planner iterations required to converge.
        /// If incidental planner work changes this value occasionally,
        /// that's fine; but if we find we're changing it all the time,
        /// we should probably drop it and keep just the maximum below.
        const EXP_PLANNING_ITERATIONS: usize = 57;

        /// Planning must not take more than this number of iterations.
        const MAX_PLANNING_ITERATIONS: usize = 100;
        assert!(EXP_PLANNING_ITERATIONS < MAX_PLANNING_ITERATIONS);

        let mut parent = blueprint1;
        for i in 2..=MAX_PLANNING_ITERATIONS {
            update_collection_from_blueprint(&mut example, &parent);

            let blueprint_name = format!("blueprint{i}");
            let blueprint = Planner::new_based_on(
                log.clone(),
                &parent,
                &input,
                &blueprint_name,
                &example.collection,
            )
            .expect("can't create planner")
            .with_rng(PlannerRng::from_seed((TEST_NAME, &blueprint_name)))
            .plan()
            .unwrap_or_else(|_| panic!("can't re-plan after {i} iterations"));

            {
                let summary = blueprint.diff_since_blueprint(&parent);
                if summary.total_zones_added() == 0
                    && summary.total_zones_removed() == 0
                    && summary.total_zones_modified() == 0
                {
                    assert!(
                        blueprint
                            .all_omicron_zones(
                                BlueprintZoneDisposition::is_in_service
                            )
                            .all(|(_, zone)| zone.image_source == image_source),
                        "failed to update all zones"
                    );

                    assert_eq!(
                        i, EXP_PLANNING_ITERATIONS,
                        "expected {EXP_PLANNING_ITERATIONS} iterations but converged in {i}"
                    );
                    println!("planning converged after {i} iterations");

                    logctx.cleanup_successful();
                    return;
                }
            }

            parent = blueprint;
        }

        panic!("did not converge after {MAX_PLANNING_ITERATIONS} iterations");
    }
}
