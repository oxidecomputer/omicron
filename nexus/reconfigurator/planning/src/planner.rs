// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Ensure;
use crate::blueprint_builder::EnsureMultiple;
use crate::blueprint_builder::Error;
use crate::blueprint_builder::Operation;
use crate::planner::omicron_zone_placement::PlacementError;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::SledUuid;
use slog::error;
use slog::{info, warn, Logger};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;
use std::str::FromStr;

pub(crate) use self::omicron_zone_placement::DiscretionaryOmicronZone;
use self::omicron_zone_placement::OmicronZonePlacement;
use self::omicron_zone_placement::OmicronZonePlacementSledState;

mod omicron_zone_placement;

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
            creator,
        )?;
        Ok(Planner { log, input, blueprint, inventory })
    }

    /// Within tests, set a seeded RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn with_rng_seed<H: Hash>(mut self, seed: H) -> Self {
        // This is an owned builder because it is almost never going to be
        // conditional.
        self.blueprint.set_rng_seed(seed);
        self
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        // We perform planning in two loops: the first one turns expunged sleds
        // into expunged zones, and the second one adds services.

        self.do_plan_expunge()?;
        self.do_plan_add()?;
        self.do_plan_decommission()?;
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
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            // Check 1: look for sleds that are expunged.
            match (sled_details.policy, sled_details.state) {
                // If the sled is still in service, don't decommission it.
                (SledPolicy::InService { .. }, _) => continue,
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

            // Check 2: have all this sled's zones been expunged? It's possible
            // we ourselves have made this change, which is fine.
            let all_zones_expunged = self
                .blueprint
                .current_sled_zones(sled_id, BlueprintZoneFilter::All)
                .all(|zone| {
                    zone.disposition == BlueprintZoneDisposition::Expunged
                });

            // Check 3: Are there any instances assigned to this sled? See
            // comment above; while we wait for omicron#4872, we just assume
            // there are no instances running.
            let num_instances_assigned = 0;

            if all_zones_expunged && num_instances_assigned == 0 {
                self.blueprint
                    .set_sled_state(sled_id, SledState::Decommissioned);
            }
        }

        Ok(())
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

            // Perform the expungement, for any zones that might need it.
            self.blueprint.expunge_zones_for_sled(sled_id, sled_details)?;
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
                    .current_sled_zones(sled_id, BlueprintZoneFilter::All)
                    .filter(|zone| {
                        zone.disposition != BlueprintZoneDisposition::Expunged
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

    fn do_plan_add(&mut self) -> Result<(), Error> {
        // Internal DNS is a prerequisite for bringing up all other zones.  At
        // this point, we assume that internal DNS (as a service) is already
        // functioning.  At some point, this function will have to grow the
        // ability to determine whether more internal DNS zones need to be
        // added and where they should go.  And the blueprint builder will need
        // to grow the ability to provision one.

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
            if let EnsureMultiple::Changed {
                added,
                updated,
                expunged: _,
                removed,
            } = self.blueprint.sled_ensure_disks(sled_id, &sled_resources)?
            {
                info!(
                    &self.log,
                    "altered physical disks";
                    "sled_id" => %sled_id
                );
                self.blueprint.record_operation(Operation::UpdateDisks {
                    sled_id,
                    added,
                    updated,
                    removed,
                });

                // Note that this doesn't actually need to short-circuit the
                // rest of the blueprint planning, as long as during execution
                // we send this request first.
            }

            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_ntp(sled_id)? == Ensure::Added {
                info!(
                    &self.log,
                    "found sled missing NTP zone (will add one)";
                    "sled_id" => %sled_id
                );
                self.blueprint.record_operation(Operation::AddZone {
                    sled_id,
                    kind: ZoneKind::BoundaryNtp,
                });
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                sleds_waiting_for_ntp_zone.insert(sled_id);
                continue;
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
            let has_ntp_inventory = self
                .inventory
                .omicron_zones
                .get(&sled_id)
                .map(|sled_zones| {
                    sled_zones.zones.zones.iter().any(|z| z.zone_type.is_ntp())
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
                if self
                    .blueprint
                    .sled_ensure_zone_crucible(sled_id, *zpool_id)?
                    == Ensure::Added
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
        for (sled_id, sled_resources) in
            self.input.all_sled_resources(SledFilter::InService)
        {
            if let EnsureMultiple::Changed {
                added,
                updated,
                expunged,
                removed,
            } =
                self.blueprint.sled_ensure_datasets(sled_id, &sled_resources)?
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
            DiscretionaryOmicronZone::CockroachDb,
            DiscretionaryOmicronZone::Nexus,
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
                                    BlueprintZoneFilter::ShouldBeRunning,
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
            let num_zones_of_kind = self
                .blueprint
                .sled_num_running_zones_of_kind(sled_id, zone_kind.into());
            num_existing_kind_zones += num_zones_of_kind;
        }

        let target_count = match zone_kind {
            DiscretionaryOmicronZone::BoundaryNtp => {
                self.input.target_boundary_ntp_zone_count()
            }
            DiscretionaryOmicronZone::CockroachDb => {
                self.input.target_cockroachdb_zone_count()
            }
            DiscretionaryOmicronZone::Nexus => {
                self.input.target_nexus_zone_count()
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
        mut num_zones_to_add: usize,
    ) -> Result<(), Error> {
        // Build a map of sled -> new zones to add.
        let mut sleds_to_change: BTreeMap<SledUuid, usize> = BTreeMap::new();

        for i in 0..num_zones_to_add {
            match zone_placement.place_zone(kind) {
                Ok(sled_id) => {
                    *sleds_to_change.entry(sled_id).or_default() += 1;
                }
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

                    // Adjust `num_zones_to_add` downward so it's consistent
                    // with the number of zones we're actually adding.
                    num_zones_to_add = i;

                    break;
                }
            }
        }

        // For each sled we need to change, actually do so.
        let mut new_zones_added = 0;
        for (sled_id, additional_zone_count) in sleds_to_change {
            // TODO-cleanup This is awkward: the builder wants to know how many
            // total zones go on a given sled, but we have a count of how many
            // we want to add. Construct a new target count. Maybe the builder
            // should provide a different interface here?
            let new_total_zone_count = self
                .blueprint
                .sled_num_running_zones_of_kind(sled_id, kind.into())
                + additional_zone_count;

            let result = match kind {
                DiscretionaryOmicronZone::BoundaryNtp => self
                    .blueprint
                    .sled_promote_internal_ntp_to_boundary_ntp(sled_id)?,
                DiscretionaryOmicronZone::CockroachDb => {
                    self.blueprint.sled_ensure_zone_multiple_cockroachdb(
                        sled_id,
                        new_total_zone_count,
                    )?
                }
                DiscretionaryOmicronZone::Nexus => {
                    self.blueprint.sled_ensure_zone_multiple_nexus(
                        sled_id,
                        new_total_zone_count,
                    )?
                }
            };
            match result {
                EnsureMultiple::Changed {
                    added,
                    updated,
                    expunged,
                    removed,
                } => {
                    info!(
                        self.log, "modified zones on sled";
                        "sled_id" => %sled_id,
                        "kind" => ?kind,
                        "added" => added,
                        "updated" => updated,
                        "expunged" => expunged,
                        "removed" => removed,
                    );
                    new_zones_added += added;
                }
                // This is only possible if we asked the sled to ensure the same
                // number of zones it already has, but that's impossible based
                // on the way we built up `sleds_to_change`.
                EnsureMultiple::NotNeeded => unreachable!(
                    "sled on which we added {kind:?} zones did not add any"
                ),
            }
        }

        // Double check that we didn't make any arithmetic mistakes. If we've
        // arrived here, we think we've added the number of Nexus zones we
        // needed to.
        assert_eq!(
            new_zones_added, num_zones_to_add,
            "internal error counting {kind:?} zones"
        );

        Ok(())
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
}

/// Returns `Some(reason)` if the sled needs its zones to be expunged,
/// based on the policy and state.
fn sled_needs_all_zones_expunged(
    state: SledState,
    policy: SledPolicy,
) -> Option<ZoneExpungeReason> {
    match state {
        SledState::Active => {}
        SledState::Decommissioned => {
            // A decommissioned sled that still has resources attached to it is
            // an illegal state, but representable. If we see a sled in this
            // state, we should still expunge all zones in it, but parent code
            // should warn on it.
            return Some(ZoneExpungeReason::SledDecommissioned);
        }
    }

    match policy {
        SledPolicy::InService { .. } => None,
        SledPolicy::Expunged => Some(ZoneExpungeReason::SledExpunged),
    }
}

pub(crate) fn zone_needs_expungement(
    sled_details: &SledDetails,
    zone_config: &BlueprintZoneConfig,
) -> Option<ZoneExpungeReason> {
    // Should we expunge the zone because the sled is gone?
    if let Some(reason) =
        sled_needs_all_zones_expunged(sled_details.state, sled_details.policy)
    {
        return Some(reason);
    }

    // Should we expunge the zone because durable storage is gone?
    if let Some(durable_storage_zpool) = zone_config.zone_type.durable_zpool() {
        let zpool_id = durable_storage_zpool.id();
        if !sled_details.resources.zpool_is_provisionable(&zpool_id) {
            return Some(ZoneExpungeReason::DiskExpunged);
        }
    };

    // Should we expunge the zone because transient storage is gone?
    if let Some(ref filesystem_zpool) = zone_config.filesystem_pool {
        let zpool_id = filesystem_zpool.id();
        if !sled_details.resources.zpool_is_provisionable(&zpool_id) {
            return Some(ZoneExpungeReason::DiskExpunged);
        }
    };

    None
}

/// The reason a sled's zones need to be expunged.
///
/// This is used only for introspection and logging -- it's not part of the
/// logical flow.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ZoneExpungeReason {
    DiskExpunged,
    SledDecommissioned,
    SledExpunged,
}

#[cfg(test)]
mod test {
    use super::Planner;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::blueprint_builder::test::DEFAULT_N_SLEDS;
    use crate::example::example;
    use crate::example::ExampleSystem;
    use crate::system::SledBuilder;
    use chrono::NaiveDateTime;
    use chrono::TimeZone;
    use chrono::Utc;
    use expectorate::assert_contents;
    use nexus_inventory::now_db_precision;
    use nexus_sled_agent_shared::inventory::ZoneKind;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintDiff;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::CockroachDbClusterVersion;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use nexus_types::deployment::CockroachDbSettings;
    use nexus_types::deployment::OmicronZoneNetworkResources;
    use nexus_types::deployment::SledDisk;
    use nexus_types::external_api::views::PhysicalDiskPolicy;
    use nexus_types::external_api::views::PhysicalDiskState;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledProvisionPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::OmicronZonesFound;
    use omicron_common::api::external::Generation;
    use omicron_common::disk::DiskIdentity;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::HashMap;
    use std::mem;
    use typed_rng::TypedUuidRng;

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        static TEST_NAME: &str = "planner_basic_add_sled";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system.
        let mut example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint1 = &example.blueprint;
        verify_blueprint(blueprint1);

        println!("{}", blueprint1.display());

        // Now run the planner.  It should do nothing because our initial
        // system didn't have any issues that the planner currently knows how to
        // fix.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            blueprint1,
            &example.input,
            "no-op?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(blueprint1);
        println!("1 -> 2 (expected no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);
        assert_eq!(diff.zones.added.len(), 0);
        assert_eq!(diff.zones.removed.len(), 0);
        assert_eq!(diff.zones.modified.len(), 0);
        assert_eq!(diff.zones.errors.len(), 0);
        assert_eq!(diff.physical_disks.added.len(), 0);
        assert_eq!(diff.physical_disks.removed.len(), 0);
        verify_blueprint(&blueprint2);

        // Now add a new sled.
        let new_sled_id = example.sled_rng.next();
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
        .with_rng_seed((TEST_NAME, "bp3"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!(
            "2 -> 3 (expect new NTP zone on new sled):\n{}",
            diff.display()
        );
        assert_contents(
            "tests/output/planner_basic_add_sled_2_3.txt",
            &diff.display().to_string(),
        );
        assert_eq!(diff.sleds_added.len(), 1);
        let sled_id = *diff.sleds_added.first().unwrap();
        let sled_zones = diff.zones.added.get(&sled_id).unwrap();
        // We have defined elsewhere that the first generation contains no
        // zones.  So the first one with zones must be newer.  See
        // OmicronZonesConfig::INITIAL_GENERATION.
        assert!(sled_zones.generation_after.unwrap() > Generation::new());
        assert_eq!(sled_id, new_sled_id);
        assert_eq!(sled_zones.zones.len(), 1);
        assert!(matches!(sled_zones.zones[0].kind(), ZoneKind::InternalNtp));
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);
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
        .with_rng_seed((TEST_NAME, "bp4"))
        .plan()
        .expect("failed to plan");
        let diff = blueprint4.diff_since_blueprint(&blueprint3);
        println!("3 -> 4 (expected no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);
        verify_blueprint(&blueprint4);

        // Now update the inventory to have the requested NTP zone.
        let mut collection = example.collection.clone();
        assert!(collection
            .omicron_zones
            .insert(
                new_sled_id,
                OmicronZonesFound {
                    time_collected: now_db_precision(),
                    source: String::from("test suite"),
                    sled_id: new_sled_id,
                    zones: blueprint4
                        .blueprint_zones
                        .get(&new_sled_id)
                        .expect("blueprint should contain zones for new sled")
                        .to_omicron_zones_config(
                            BlueprintZoneFilter::ShouldBeRunning
                        )
                }
            )
            .is_none());

        // Check that the next step is to add Crucible zones
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test: add Crucible zones?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp5"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint5.diff_since_blueprint(&blueprint3);
        println!("3 -> 5 (expect Crucible zones):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_basic_add_sled_3_5.txt",
            &diff.display().to_string(),
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 1);
        let sled_id = diff.sleds_modified.first().unwrap();
        assert_eq!(*sled_id, new_sled_id);
        // No removed or modified zones on this sled
        assert!(!diff.zones.removed.contains_key(sled_id));
        assert!(!diff.zones.modified.contains_key(sled_id));
        // 10 crucible zones addeed
        let zones_added = diff.zones.added.get(sled_id).unwrap();
        assert_eq!(
            zones_added.generation_after.unwrap(),
            zones_added.generation_before.unwrap().next()
        );

        assert_eq!(zones_added.zones.len(), 10);
        for zone in &zones_added.zones {
            if zone.kind() != ZoneKind::Crucible {
                panic!("unexpectedly added a non-Crucible zone: {zone:?}");
            }
        }
        verify_blueprint(&blueprint5);

        // Check that there are no more steps.
        let blueprint6 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint5,
            &input,
            "test: no-op?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp6"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint6.diff_since_blueprint(&blueprint5);
        println!("5 -> 6 (expect no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);
        verify_blueprint(&blueprint6);

        logctx.cleanup_successful();
    }

    /// Check that the planner will add more Nexus zones to a single sled, if
    /// needed
    #[test]
    fn test_add_multiple_nexus_to_one_sled() {
        static TEST_NAME: &str = "planner_add_multiple_nexus_to_one_sled";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point, but strip it down to just
        // one sled.
        let (sled_id, blueprint1, collection, input) = {
            let (mut collection, input, mut blueprint) =
                example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

            // Pick one sled ID to keep and remove the rest.
            let mut builder = input.into_builder();
            let keep_sled_id =
                builder.sleds().keys().next().copied().expect("no sleds");
            builder.sleds_mut().retain(|&k, _v| keep_sled_id == k);
            collection.sled_agents.retain(|&k, _v| keep_sled_id == k);
            collection.omicron_zones.retain(|&k, _v| keep_sled_id == k);

            assert_eq!(collection.sled_agents.len(), 1);
            assert_eq!(collection.omicron_zones.len(), 1);
            blueprint.blueprint_zones.retain(|k, _v| keep_sled_id == *k);
            blueprint.blueprint_disks.retain(|k, _v| keep_sled_id == *k);

            // Also remove all the networking resources for the zones we just
            // stripped out; i.e., only keep those for `keep_sled_id`.
            let mut new_network_resources = OmicronZoneNetworkResources::new();
            let old_network_resources = builder.network_resources_mut();
            for old_ip in old_network_resources.omicron_zone_external_ips() {
                if blueprint.all_omicron_zones(BlueprintZoneFilter::All).any(
                    |(_, zone)| {
                        zone.zone_type
                            .external_networking()
                            .map(|(ip, _nic)| ip.id() == old_ip.ip.id())
                            .unwrap_or(false)
                    },
                ) {
                    new_network_resources
                        .add_external_ip(old_ip.zone_id, old_ip.ip)
                        .expect("copied IP to new input");
                }
            }
            for old_nic in old_network_resources.omicron_zone_nics() {
                if blueprint.all_omicron_zones(BlueprintZoneFilter::All).any(
                    |(_, zone)| {
                        zone.zone_type
                            .external_networking()
                            .map(|(_ip, nic)| {
                                nic.id == old_nic.nic.id.into_untyped_uuid()
                            })
                            .unwrap_or(false)
                    },
                ) {
                    new_network_resources
                        .add_nic(old_nic.zone_id, old_nic.nic)
                        .expect("copied NIC to new input");
                }
            }
            mem::swap(old_network_resources, &mut &mut new_network_resources);

            (keep_sled_id, blueprint, collection, builder.build())
        };

        // This blueprint should only have 1 Nexus instance on the one sled we
        // kept.
        assert_eq!(blueprint1.blueprint_zones.len(), 1);
        assert_eq!(
            blueprint1
                .blueprint_zones
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
        let input = builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 1);
        let changed_sled_id = diff.sleds_modified.first().unwrap();

        // TODO-cleanup use `TypedUuid` everywhere
        assert_eq!(*changed_sled_id, sled_id);
        assert_eq!(diff.zones.removed.len(), 0);
        assert_eq!(diff.zones.modified.len(), 0);
        let zones_added = diff.zones.added.get(changed_sled_id).unwrap();
        assert_eq!(
            zones_added.zones.len(),
            input.target_nexus_zone_count() - 1
        );
        for zone in &zones_added.zones {
            if zone.kind() != ZoneKind::Nexus {
                panic!("unexpectedly added a non-Nexus zone: {zone:?}");
            }
        }

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
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // This blueprint should only have 3 Nexus zones: one on each sled.
        assert_eq!(blueprint1.blueprint_zones.len(), 3);
        for sled_config in blueprint1.blueprint_zones.values() {
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
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 3);

        // All 3 sleds should get additional Nexus zones. We expect a total of
        // 11 new Nexus zones, which should be spread evenly across the three
        // sleds (two should get 4 and one should get 3).
        let mut total_new_nexus_zones = 0;
        for sled_id in diff.sleds_modified {
            assert!(!diff.zones.removed.contains_key(&sled_id));
            assert!(!diff.zones.modified.contains_key(&sled_id));
            let zones_added = &diff.zones.added.get(&sled_id).unwrap().zones;
            match zones_added.len() {
                n @ (3 | 4) => {
                    total_new_nexus_zones += n;
                }
                n => {
                    panic!("unexpected number of zones added to {sled_id}: {n}")
                }
            }
            for zone in zones_added {
                if zone.kind() != ZoneKind::Nexus {
                    panic!("unexpectedly added a non-Nexus zone: {zone:?}");
                }
            }
        }
        assert_eq!(total_new_nexus_zones, 11);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_crucible_allocation_skips_nonprovisionable_disks() {
        static TEST_NAME: &str =
            "planner_crucible_allocation_skips_nonprovisionable_disks";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, 1);

        let mut builder = input.into_builder();

        // Avoid churning on the quantity of Nexus zones - we're okay staying at
        // one.
        builder.policy_mut().target_nexus_zone_count = 1;

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
                (new_sled_disk(PhysicalDiskPolicy::InService), vec![]),
            );
        }
        for _ in 0..NEW_EXPUNGED_DISKS {
            sled_details.resources.zpools.insert(
                ZpoolUuid::from(zpool_rng.next()),
                (new_sled_disk(PhysicalDiskPolicy::Expunged), vec![]),
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
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (some new disks, one expunged):\n{}", diff.display());
        assert_eq!(diff.sleds_modified.len(), 1);
        let sled_id = diff.sleds_modified.first().unwrap();

        // We should be adding a Crucible zone for each new in-service disk.
        assert_eq!(
            diff.zones.added.get(sled_id).unwrap().zones.len(),
            NEW_IN_SERVICE_DISKS
        );
        assert!(!diff.zones.removed.contains_key(sled_id));

        logctx.cleanup_successful();
    }

    #[test]
    fn test_disk_expungement_removes_zones_durable_zpool() {
        static TEST_NAME: &str =
            "planner_disk_expungement_removes_zones_durable_zpool";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, 1);

        let mut builder = input.into_builder();

        // Aside: Avoid churning on the quantity of Nexus zones - we're okay
        // staying at one.
        builder.policy_mut().target_nexus_zone_count = 1;

        // The example system should be assigning crucible zones to each
        // in-service disk. When we expunge one of these disks, the planner
        // should remove the associated zone.
        //
        // Find a disk which is only used by a single zone, if one exists.
        //
        // If we don't do this, we might select a physical disk supporting
        // multiple zones of distinct types.
        let mut zpool_by_zone_usage = HashMap::new();
        for zones in blueprint1.blueprint_zones.values() {
            for zone in &zones.zones {
                let pool = zone.filesystem_pool.as_ref().unwrap();
                zpool_by_zone_usage
                    .entry(pool.id())
                    .and_modify(|count| *count += 1)
                    .or_insert_with(|| 1);
            }
        }
        let (_, sled_details) = builder.sleds_mut().iter_mut().next().unwrap();
        let (_, (disk, _datasets)) = sled_details
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
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunge a disk):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 1);

        // We should be removing a single zone, associated with the Crucible
        // using that device.
        assert_eq!(diff.zones.added.len(), 0);
        assert_eq!(diff.zones.removed.len(), 0);
        assert_eq!(diff.zones.modified.len(), 1);

        let (_zone_id, modified_zones) =
            diff.zones.modified.iter().next().unwrap();
        assert_eq!(modified_zones.zones.len(), 1);
        let modified_zone = &modified_zones.zones.first().unwrap().zone;
        assert!(
            matches!(modified_zone.kind(), ZoneKind::Crucible),
            "Expected the modified zone to be a Crucible zone, but it was: {:?}",
            modified_zone.kind()
        );
        assert_eq!(
            modified_zone.disposition(),
            BlueprintZoneDisposition::Expunged,
            "Should have expunged this zone"
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_disk_expungement_removes_zones_transient_filesystem() {
        static TEST_NAME: &str =
            "planner_disk_expungement_removes_zones_transient_filesystem";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, 1);

        let mut builder = input.into_builder();

        // Aside: Avoid churning on the quantity of Nexus zones - we're okay
        // staying at one.
        builder.policy_mut().target_nexus_zone_count = 1;

        // Find whatever pool NTP was using
        let pool_to_expunge = blueprint1
            .blueprint_zones
            .iter()
            .find_map(|(_, zones_config)| {
                for zone_config in &zones_config.zones {
                    if zone_config.zone_type.is_ntp() {
                        return zone_config.filesystem_pool.clone();
                    }
                }
                None
            })
            .expect("No NTP zone pool?");

        // This is mostly for test stability across "example system" changes:
        // Find how many other zones are using this same zpool.
        let zones_using_zpool = blueprint1.blueprint_zones.iter().fold(
            0,
            |acc, (_, zones_config)| {
                let mut zones_using_zpool = 0;
                for zone_config in &zones_config.zones {
                    if let Some(pool) = &zone_config.filesystem_pool {
                        if pool == &pool_to_expunge {
                            zones_using_zpool += 1;
                            continue;
                        }
                    }
                    if let Some(pool) = zone_config.zone_type.durable_zpool() {
                        if pool == &pool_to_expunge {
                            zones_using_zpool += 1;
                            continue;
                        }
                    }
                }
                acc + zones_using_zpool
            },
        );
        assert!(
            zones_using_zpool > 0,
            "We should be expunging at least one zone using this zpool"
        );

        // For that pool, find the physical disk behind it, and mark it
        // expunged.
        let (_, sled_details) = builder.sleds_mut().iter_mut().next().unwrap();
        let (disk, _datasets) = sled_details
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
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("1 -> 2 (expunge a disk):\n{}", diff.display());
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 1);

        // We should be removing all zones using this zpool. Because we're
        // removing the NTP zone, we should add a new one.
        assert_eq!(diff.zones.added.len(), 1);
        assert_eq!(diff.zones.removed.len(), 0);
        assert_eq!(diff.zones.modified.len(), 1);

        let (_zone_id, added_zones) = diff.zones.added.iter().next().unwrap();
        assert_eq!(added_zones.zones.len(), 1);
        for zone in &added_zones.zones {
            assert_eq!(zone.kind(), ZoneKind::InternalNtp);
        }

        let (_zone_id, modified_zones) =
            diff.zones.modified.iter().next().unwrap();
        assert_eq!(modified_zones.zones.len(), zones_using_zpool);
        for modified_zone in &modified_zones.zones {
            assert_eq!(
                modified_zone.zone.disposition(),
                BlueprintZoneDisposition::Expunged,
                "Should have expunged this zone"
            );
        }

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
        let (collection, input, mut blueprint1) =
            example(&logctx.log, TEST_NAME, 5);

        // This blueprint should only have 5 Nexus zones: one on each sled.
        assert_eq!(blueprint1.blueprint_zones.len(), 5);
        for sled_config in blueprint1.blueprint_zones.values() {
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
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.policy = SledPolicy::Expunged;
            *sled_id
        };
        println!("1 -> 2: expunged {expunged_sled_id}");
        let decommissioned_sled_id = {
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.state = SledState::Decommissioned;

            // Decommissioned sleds can only occur if their zones have been
            // expunged, so lie and pretend like that already happened
            // (otherwise the planner will rightfully fail to generate a new
            // blueprint, because we're feeding it invalid inputs).
            for zone in
                &mut blueprint1.blueprint_zones.get_mut(sled_id).unwrap().zones
            {
                zone.disposition = BlueprintZoneDisposition::Expunged;
            }

            *sled_id
        };
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
        let input = builder.build();
        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        // Define a time_created for consistent output across runs.
        blueprint2.time_created =
            Utc.from_utc_datetime(&NaiveDateTime::UNIX_EPOCH);

        assert_contents(
            "tests/output/planner_nonprovisionable_bp2.txt",
            &blueprint2.display().to_string(),
        );

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "1 -> 2 (added additional Nexus zones, take 2 sleds out of service):\n{}",
            diff.display()
        );
        assert_contents(
            "tests/output/planner_nonprovisionable_1_2.txt",
            &diff.display().to_string(),
        );

        // The expunged and decommissioned sleds should have had all zones be
        // marked as expunged. (Not removed! Just marked as expunged.)
        //
        // Note that at this point we're neither removing zones from the
        // blueprint nor marking sleds as decommissioned -- we still need to do
        // cleanup, and we aren't performing garbage collection on zones or
        // sleds at the moment.

        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);

        assert_all_zones_expunged(&diff, expunged_sled_id, "expunged sled");

        // Only 2 of the 3 remaining sleds (not the non-provisionable sled)
        // should get additional Nexus zones. We expect a total of 6 new Nexus
        // zones, which should be split evenly between the two sleds, while the
        // non-provisionable sled should be unchanged.
        let mut remaining_modified_sleds = diff.sleds_modified.clone();
        remaining_modified_sleds.remove(&expunged_sled_id);
        remaining_modified_sleds.remove(&decommissioned_sled_id);

        assert_eq!(remaining_modified_sleds.len(), 2);
        let mut total_new_nexus_zones = 0;
        for sled_id in remaining_modified_sleds {
            assert!(sled_id != nonprovisionable_sled_id);
            assert!(sled_id != expunged_sled_id);
            assert!(sled_id != decommissioned_sled_id);
            assert!(!diff.zones.removed.contains_key(&sled_id));
            assert!(!diff.zones.modified.contains_key(&sled_id));
            let zones = &diff.zones.added.get(&sled_id).unwrap().zones;
            for zone in zones {
                if ZoneKind::Nexus != zone.kind() {
                    panic!("unexpectedly added a non-Crucible zone: {zone:?}");
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
            .blueprint_zones
            .get_mut(&nonprovisionable_sled_id)
            .unwrap()
            .zones;

        zones.retain_mut(|zone| {
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
                        zone.disposition = BlueprintZoneDisposition::Quiesced;
                        next = NextCrucibleMutate::Remove;
                        true
                    }
                    NextCrucibleMutate::Remove => {
                        next = NextCrucibleMutate::Done;
                        false
                    }
                    NextCrucibleMutate::Done => true,
                }
            } else if let BlueprintZoneType::InternalNtp(_) =
                &mut zone.zone_type
            {
                // Change the underlay IP.
                let mut segments = zone.underlay_address.segments();
                segments[0] += 1;
                zone.underlay_address = segments.into();
                true
            } else {
                true
            }
        });

        let expunged_zones =
            blueprint2a.blueprint_zones.get_mut(&expunged_sled_id).unwrap();
        expunged_zones.zones.clear();
        expunged_zones.generation = expunged_zones.generation.next();

        blueprint2a.blueprint_zones.remove(&decommissioned_sled_id);

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

    fn assert_all_zones_expunged(
        diff: &BlueprintDiff,
        expunged_sled_id: SledUuid,
        desc: &str,
    ) {
        assert!(
            !diff.zones.added.contains_key(&expunged_sled_id),
            "for {desc}, no zones should have been added to blueprint"
        );

        // A zone disposition going to expunged *does not* mean that the
        // zone is actually removed, i.e. `zones_removed` is still 0. Any
        // zone removal will be part of some future garbage collection
        // process that isn't currently defined.

        assert!(
            !diff.zones.removed.contains_key(&expunged_sled_id),
            "for {desc}, no zones should have been removed from blueprint"
        );

        // Run through all the common zones and ensure that all of them
        // have been marked expunged.
        let modified_zones =
            diff.zones.modified.get(&expunged_sled_id).unwrap();
        assert_eq!(
            modified_zones.generation_before.next(),
            modified_zones.generation_after,
            "for {desc}, generation should have been bumped"
        );

        for modified_zone in &modified_zones.zones {
            assert_eq!(
                modified_zone.zone.disposition(),
                BlueprintZoneDisposition::Expunged,
                "for {desc}, zone {} should have been marked expunged",
                modified_zone.zone.id()
            );
        }
    }

    #[test]
    fn planner_decommissions_sleds() {
        static TEST_NAME: &str = "planner_decommissions_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // Expunge one of the sleds.
        let mut builder = input.into_builder();
        let expunged_sled_id = {
            let mut iter = builder.sleds_mut().iter_mut();
            let (sled_id, details) = iter.next().expect("at least one sled");
            details.policy = SledPolicy::Expunged;
            *sled_id
        };

        let input = builder.build();
        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        // Define a time_created for consistent output across runs.
        blueprint2.time_created =
            Utc.from_utc_datetime(&NaiveDateTime::UNIX_EPOCH);

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
        assert!(blueprint2.blueprint_zones[&expunged_sled_id]
            .are_all_zones_expunged());
        assert_eq!(
            blueprint2.sled_state[&expunged_sled_id],
            SledState::Decommissioned
        );

        // Remove the now-decommissioned sled from the planning input.
        let mut builder = input.into_builder();
        builder.sleds_mut().remove(&expunged_sled_id);
        let input = builder.build();

        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("created planner")
        .with_rng_seed((TEST_NAME, "bp3"))
        .plan()
        .expect("failed to plan");

        // There should be no changes to the blueprint; we don't yet garbage
        // collect zones, so we should still have the sled's expunged zones
        // (even though the sled itself is no longer present in the list of
        // commissioned sleds).
        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!(
            "2 -> 3 (decommissioned {expunged_sled_id}):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);
        assert_eq!(diff.sleds_unchanged.len(), DEFAULT_N_SLEDS);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_ensure_preserve_downgrade_option() {
        static TEST_NAME: &str = "planner_ensure_preserve_downgrade_option";
        let logctx = test_setup_log(TEST_NAME);

        let (collection, input, bp1) = example(&logctx.log, TEST_NAME, 0);
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
        .with_rng_seed((TEST_NAME, "bp2"))
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
        .with_rng_seed((TEST_NAME, "bp3"))
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
        .with_rng_seed((TEST_NAME, "bp4"))
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
            .with_rng_seed((TEST_NAME, format!("bp5-{}", preserve_downgrade)))
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
}
