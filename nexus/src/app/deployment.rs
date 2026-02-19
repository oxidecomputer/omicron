// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use nexus_db_model::TargetReleaseSource;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::params;
use nexus_types::internal_api::views::UpdateStatus;
use nexus_types::inventory::Collection;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    planning_input: PlanningInput,
    creator: String,
    inventory: Option<Collection>,
}

/// Enum describing the two operator intentions behind asking for a change to
/// the current system target release version.
pub(crate) enum SetTargetReleaseIntent {
    /// Set a new target release to induce a system update.
    Update,

    /// Recover from a mupdate, which requires setting the current target
    /// release to the version we mupdated to.
    RecoverFromMupdate,
}

impl super::Nexus {
    pub async fn blueprint_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BlueprintMetadata> {
        self.db_datastore.blueprints_list(opctx, pagparams).await
    }

    pub async fn blueprint_view(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> LookupResult<Blueprint> {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_read(opctx, &blueprint).await
    }

    pub async fn blueprint_delete(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> DeleteResult {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_delete(opctx, &blueprint).await
    }

    pub async fn blueprint_target_view(
        &self,
        opctx: &OpContext,
    ) -> Result<BlueprintTarget, Error> {
        self.db_datastore.blueprint_target_get_current(opctx).await
    }

    pub async fn blueprint_target_set(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current(opctx, new_target)
            .await?;

        // We have a new target: trigger the background task to load this
        // blueprint.
        self.background_tasks
            .activate(&self.background_tasks.task_blueprint_loader);

        Ok(new_target)
    }

    pub async fn blueprint_target_set_enabled(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current_enabled(opctx, new_target)
            .await?;

        // We don't know whether this actually changed the enabled bit; activate
        // the background task to load this blueprint which does know.
        self.background_tasks
            .activate(&self.background_tasks.task_blueprint_loader);

        Ok(new_target)
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        let creator = self.id.to_string();
        let datastore = self.datastore();

        let (_, parent_blueprint) =
            self.db_datastore.blueprint_target_get_current_full(opctx).await?;

        // Load up the planner config from the db directly (rather than from,
        // say, the background task) to ensure we get the latest state.
        let planner_config = self
            .db_datastore
            .reconfigurator_config_get_latest(opctx)
            .await?
            .map_or_else(PlannerConfig::default, |c| c.config.planner_config);

        let planning_input = PlanningInputFromDb::assemble(
            opctx,
            datastore,
            planner_config,
            Arc::new(parent_blueprint),
        )
        .await?;

        // The choice of which inventory collection to use here is not
        // necessarily trivial.  Inventory collections may be incomplete due to
        // transient (or even persistent) errors.  It's not yet clear what
        // general criteria we'll want to use in picking a collection here.  But
        // as of this writing, this is only used for one specific case, which is
        // to implement a gate that prevents the planner from provisioning
        // non-NTP zones on a sled unless we know there's an NTP zone already on
        // that sled.  For that purpose, it's okay if this collection is
        // incomplete due to a transient error -- that would just prevent
        // forward progress in the planner until the next time we try this.
        // (Critically, it won't cause the planner to do anything wrong.)
        let inventory = datastore
            .inventory_get_latest_collection(opctx)
            .await
            .internal_context(
                "fetching latest inventory collection for blueprint planner",
            )?;

        Ok(PlanningContext { planning_input, creator, inventory })
    }

    async fn blueprint_add(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        self.db_datastore.blueprint_insert(opctx, blueprint).await
    }

    pub async fn blueprint_create_regenerate(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &planning_context.planning_input,
            &planning_context.creator,
            &inventory,
            PlannerRng::from_entropy(),
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error creating blueprint planner: {error:#}",
            ))
        })?;
        let blueprint = planner.plan().map_err(|error| {
            Error::internal_error(&format!(
                "error generating blueprint: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, &blueprint).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_import(
        &self,
        opctx: &OpContext,
        blueprint: Blueprint,
    ) -> Result<(), Error> {
        let _ = self.blueprint_add(&opctx, &blueprint).await?;
        Ok(())
    }

    pub async fn update_status(
        &self,
        opctx: &OpContext,
    ) -> Result<UpdateStatus, Error> {
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;

        // Build a map of sleds we want to consider in the update status. This
        // may be different from what's available in inventory in either
        // direction:
        //
        // * We might have a sled we expect to be present but that is physically
        //   missing (or otherwise missing from inventory).
        // * We might have sleds present from which we can collect inventory but
        //   which are not members of the control plane.
        let sleds_by_baseboard: BTreeMap<_, _> = planning_context
            .planning_input
            .all_sleds(SledFilter::SpsUpdatedByReconfigurator)
            .map(|(sled_id, details)| (details.baseboard_id.clone(), sled_id))
            .collect();

        let new = planning_context.planning_input.tuf_repo().description();
        let old = planning_context.planning_input.old_repo().description();
        let status =
            UpdateStatus::new(old, new, &sleds_by_baseboard, &inventory);

        Ok(status)
    }

    pub(crate) async fn target_release_update(
        &self,
        opctx: &OpContext,
        params: params::SetTargetReleaseParams,
        intent: SetTargetReleaseIntent,
    ) -> Result<(), Error> {
        let new_system_version = params.system_version;

        // We don't need a transaction for the following queries because
        // (1) the generation numbers provide optimistic concurrency control:
        // if another request were to successfully update the target release
        // between when we fetch it here and when we try to update it below,
        // our update would fail because the next generation number would
        // would already be taken; and
        // (2) we assume that TUF repo depot records are immutable, i.e.,
        // system version X.Y.Z won't designate different repos over time.
        let current_target_release =
            self.datastore().target_release_get_current(&opctx).await?;
        let current_target_release_source = current_target_release
            .release_source()
            .map_err(|err| Error::internal_error(&format!("{err:#}")))?;

        match current_target_release_source {
            TargetReleaseSource::Unspecified => {
                // There is no current target release; it's always fine to
                // set the first one.
            }
            TargetReleaseSource::SystemVersion(tuf_repo_id) => {
                let (_, current_blueprint) = self
                    .datastore()
                    .blueprint_target_get_current_full(opctx)
                    .await?;

                // We already have a target release, and an operator is
                // attempting to change it. We have very different rules to
                // enforce depending on _why_ they're trying to change it.
                //
                // If they're attempting to start a new system update, we have
                // several system-level requirements we must enforce (e.g.,
                // there must not be any sleds waiting for mupdate recovery, and
                // the version cannot be downgraded). These are enforced by
                // `validate_can_set_target_release_for_update()`.
                //
                // If they're attempting to recover from a mupdate, the only
                // requirement we enforce is that there is at least one sled
                // that is waiting for mupdate recovery. If there isn't, there's
                // no reason to attempt to recover from a mupdate. If there is,
                // we don't do any further version checking, because a mupdate
                // is by design going outside the bounds of the update system.
                // It's possible support has intentionally mupdated the system
                // to a version that wouldn't normally be allowed by the update
                // system, and we have to provide a way to notify the system of
                // that change. (A benign example that has come up in practice
                // is: system is mupdated to version N. Operator sets the target
                // release with `RecoverFromMupdate` intent, but accidentally
                // sets it to version N+1. The sleds remain in the "waiting for
                // mupdate recovery" state, because N+1 doesn't match the
                // software deployed, but the thing the operator needs to do now
                // is set the target release with `RecoverFromMupdate` intent to
                // version N. From Nexus's point of view this looks like a
                // version downgrade, but it's not: we're still trying to
                // recover from a mupdate.)
                let validation_result = match intent {
                    SetTargetReleaseIntent::Update => {
                        let current_version = self
                            .datastore()
                            .tuf_repo_get_version(&opctx, &tuf_repo_id)
                            .await?;
                        validate_can_set_target_release_for_update(
                            &current_blueprint,
                            &current_version,
                            &new_system_version,
                            &self.log,
                        )
                    }
                    SetTargetReleaseIntent::RecoverFromMupdate => {
                        validate_can_set_target_release_for_mupdate_recovery(
                            &current_blueprint,
                        )
                    }
                };

                // Unpack the result and convert the error, if any.
                let () = validation_result.map_err(|err| {
                    Error::invalid_request(format!(
                        "Target release cannot be changed: {}",
                        InlineErrorChain::new(&err),
                    ))
                })?;
            }
        }

        // Fetch the TUF repo metadata and update the target release.
        let tuf_repo_id = self
            .datastore()
            .tuf_repo_get_by_version(&opctx, new_system_version.into())
            .await?
            .id;
        let next_target_release =
            nexus_db_model::TargetRelease::new_system_version(
                &current_target_release,
                tuf_repo_id,
            );
        self.datastore()
            .target_release_insert(&opctx, next_target_release)
            .await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum TargetReleaseChangeError {
    #[error("no evidence a mupdate has occurred - recovery not needed")]
    NoMupdateRecoveryNeeded,
    #[error(
        "a support-driven recovery (mupdate) has occurred and \
         must be cleared first"
    )]
    WaitingForMupdateToBeCleared,
    #[error("a previous update is still in progress")]
    PreviousUpdateInProgress,
    #[error(
        "cannot update to target release {0} (already targeting that version)"
    )]
    UpdateToIdenticalVersion(semver::Version),
    #[error(
        "cannot skip from major version {current} to major version {proposed}"
    )]
    CannotSkipMajorVersion { current: u64, proposed: u64 },
    #[error(
        "cannot downgrade: requested target release version {proposed} \
         is older than current target release version {current}"
    )]
    CannotDowngrade { current: semver::Version, proposed: semver::Version },
}

// Check whether we should allow an operator to change the current target
// release to recover from a mupdate.
//
// We must be very generous here, as discussed at our call site in
// `target_release_update()` above. We only reject this request if there are no
// sleds waiting for recovery from a mupdate. Because of this, this function
// does not take any arguments about the current or proposed system
// version (unlike `validate_can_set_target_release_for_update()`). Mupdate can
// bypass all our typical version ordering requirements, so we have to allow
// recovery to the _actual_ version it installed, regardless of what we
// currently have on the system.
fn validate_can_set_target_release_for_mupdate_recovery(
    current_blueprint: &Blueprint,
) -> Result<(), TargetReleaseChangeError> {
    // Check sled configs first: if any sled still has a mupdate override in
    // place, we're waiting for mupdate recovery.
    for (_, sled_config) in current_blueprint.active_sled_configs() {
        if sled_config.remove_mupdate_override.is_some() {
            return Ok(());
        }

        // Be paranoid: also check the host OS slots for `CurrentContents`.
        // Mupdate writes both slots, so both slots set to `CurrentContents` is
        // also a sign that we're waiting for a mupdate to be cleared.
        if sled_config.host_phase_2.slot_a
            == BlueprintHostPhase2DesiredContents::CurrentContents
            && sled_config.host_phase_2.slot_b
                == BlueprintHostPhase2DesiredContents::CurrentContents
        {
            return Ok(());
        }
    }

    // Confirm all zones have converted to running out of known artifacts. If
    // any are still running from the install dataset, we haven't recovered from
    // the mupdate.
    // Now check zone configs.
    for (_, zone_config) in current_blueprint.in_service_zones() {
        match &zone_config.image_source {
            BlueprintZoneImageSource::InstallDataset => {
                return Ok(());
            }
            BlueprintZoneImageSource::Artifact { .. } => continue,
        }
    }

    // No sleds have a mupdate override and all zones are configured to use
    // artifact sources - there hasn't been a mupdate.
    //
    // This check is inherently racy: a sled could have just been mupdated but
    // we haven't yet noticed. There isn't much we can do about that?
    Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
}

// Helper for `validate_target_release_change_allowed_for_update()` below that
// only performs the checks to enforce our update version ordering.
fn validate_update_version_number_ordering(
    current_version: &semver::Version,
    proposed_new_version: &semver::Version,
    log: &Logger,
) -> Result<(), TargetReleaseChangeError> {
    // We cannot update to the _identical_ version we're already at.
    if proposed_new_version == current_version {
        warn!(log, "cannot start update: attempt to update to current version");
        return Err(TargetReleaseChangeError::UpdateToIdenticalVersion(
            current_version.clone(),
        ));
    }

    // We cannot skip major versions.
    if proposed_new_version.major > current_version.major + 1 {
        warn!(
            log,
            "cannot start update: attempt to update past next major version"
        );
        return Err(TargetReleaseChangeError::CannotSkipMajorVersion {
            current: current_version.major,
            proposed: proposed_new_version.major,
        });
    }

    // We cannot downgrade; however, we do need to be able to allow updates to
    // "same version, different build info" to allow for dev/test systems that
    // want to update from one commit inside a release to a subsequent commit in
    // the same release (dogfood, racklettes). We implement this check by
    // stripping out the build info and then comparing the versions.
    //
    // This is not entirely correct - it allows updating to _any_ commit with
    // the same release, even older ones - but we don't have enough information
    // in the version strings today to determine commit ordering. See
    // <https://github.com/oxidecomputer/omicron/issues/9071>.
    let is_downgrade = {
        let mut current_version = current_version.clone();
        let mut proposed_new_version = proposed_new_version.clone();

        current_version.build = semver::BuildMetadata::EMPTY;
        proposed_new_version.build = semver::BuildMetadata::EMPTY;

        proposed_new_version < current_version
    };
    if is_downgrade {
        warn!(log, "cannot start update: attempt to downgrade");
        return Err(TargetReleaseChangeError::CannotDowngrade {
            current: current_version.clone(),
            proposed: proposed_new_version.clone(),
        });
    }

    Ok(())
}

// Check whether we should allow an operator to change the current target
// release to start a new system update.
//
// We must reject target release changes if:
//
// * A mupdate has occurred (they must use the "set target releaes for recovery"
//   endpoint instead)
// * Another update is in progress
// * The new version doesn't satisfy our requirements for upgrade ordering (no
//   downgrades; cannot skip major releases)
//
// The latter restriction is due to the implementation of the blueprint planner,
// which means it's sufficient to look at the current target blueprint; we don't
// have to look at inventory. As long as we've been able to decide how to finish
// a previous update, we can allow starting the next one, even if we haven't
// finished executing the previous one.
fn validate_can_set_target_release_for_update(
    current_blueprint: &Blueprint,
    current_target_version: &semver::Version,
    proposed_new_version: &semver::Version,
    log: &Logger,
) -> Result<(), TargetReleaseChangeError> {
    let log = log.new(slog::o!(
        "current_version" => current_target_version.to_string(),
        "proposed_version" => proposed_new_version.to_string(),
    ));
    validate_update_version_number_ordering(
        current_target_version,
        proposed_new_version,
        &log,
    )?;

    // Convert this to a string; comparing "system version as a string" to
    // "artifact version as a string" below feels bad, but is maybe fine? The
    // artifact versions are loaded from the DB by joining against a table that
    // populates them as the system version that contained them.
    let current_target_version = current_target_version.to_string();

    // Check sled configs first.
    for (sled_id, sled_config) in current_blueprint.active_sled_configs() {
        if sled_config.remove_mupdate_override.is_some() {
            // A mupdate has occurred; we must not allow an update.
            warn!(
                log,
                "cannot start update: mupdate override in place";
                "sled_id" => %sled_id,
            );
            return Err(TargetReleaseChangeError::WaitingForMupdateToBeCleared);
        }

        // Blueprints don't check which slot is supposed to be the boot disk
        // (maybe they should?), so checking the host OS is a little funky. We
        // consider the system to be updateable (as far as this check is
        // concerned) if _either_ slot is set to an artifact with a version that
        // matches the current system target version, with the expectation that
        // seeing that means we updated this sled to that version.
        //
        // If neither slot contains the current version, we're going to reject
        // this update request, but we try to give an accurate error message:
        //
        // * If both slots contain `CurrentContents`, this sled has been
        //   mupdated and we need to recover from that.
        // * Otherwise, we assume we're on an old version and are still waiting
        //   to update to the current target release.
        let mut found_current_version_in_either_slot = false;
        let mut num_slots_with_current_contents = 0;
        for phase2 in
            [&sled_config.host_phase_2.slot_a, &sled_config.host_phase_2.slot_b]
        {
            match phase2 {
                BlueprintHostPhase2DesiredContents::CurrentContents => {
                    num_slots_with_current_contents += 1;
                }
                BlueprintHostPhase2DesiredContents::Artifact {
                    version,
                    ..
                } => match version {
                    BlueprintArtifactVersion::Available { version } => {
                        if version.as_str() == current_target_version {
                            found_current_version_in_either_slot = true;
                            break;
                        }
                    }
                    BlueprintArtifactVersion::Unknown => {
                        // This shouldn't happen; it means we have an artifact
                        // source in the blueprint that doesn't match a known
                        // artifact in the database. Should we instead load all
                        // the artifacts in the current target release and check
                        // hashes?
                        //
                        // For now, treat this as "not the current version".
                    }
                },
            }
        }

        // If neither slot contains the current version, we're not done
        // upgrading to it.
        if !found_current_version_in_either_slot {
            // Mupdate writes both slots - if they're both `CurrentContents`,
            // inform the operator that we're waiting for a mupdate to be
            // cleared.
            //
            // Otherwise, one or both slots have old or unknown artifact
            // versions, which indicate an incomplete update.
            if num_slots_with_current_contents == 2 {
                warn!(
                    log,
                    "cannot start update: host OS has been mupdated";
                    "sled_id" => %sled_id,
                );
                return Err(
                    TargetReleaseChangeError::WaitingForMupdateToBeCleared,
                );
            } else {
                warn!(
                    log,
                    "cannot start update: host OS update not complete";
                    "sled_id" => %sled_id,
                );
                return Err(TargetReleaseChangeError::PreviousUpdateInProgress);
            }
        }
    }

    // Now check zone configs.
    for (sled_id, zone_config) in current_blueprint.in_service_zones() {
        match &zone_config.image_source {
            BlueprintZoneImageSource::InstallDataset => {
                // A mupdate has occurred; we must not allow an update.
                warn!(
                    log,
                    "cannot start update: zone image source is install dataset";
                    "sled_id" => %sled_id,
                    "zone_id" => %zone_config.id,
                );
                return Err(
                    TargetReleaseChangeError::WaitingForMupdateToBeCleared,
                );
            }
            BlueprintZoneImageSource::Artifact { version, .. } => {
                match version {
                    BlueprintArtifactVersion::Available { version } => {
                        if version.as_str() != current_target_version {
                            // We found a zone not yet on the current target
                            // version; the previous upgrade is not yet
                            // complete.
                            warn!(
                                log,
                                "cannot start update: \
                                 zone image source is out of date";
                                "sled_id" => %sled_id,
                                "zone_id" => %zone_config.id,
                                "zone_version" => %version,
                            );
                            return Err(TargetReleaseChangeError::PreviousUpdateInProgress);
                        }
                    }
                    BlueprintArtifactVersion::Unknown => {
                        // This shouldn't happen; it means we have an artifact
                        // source in the blueprint that doesn't match a known
                        // artifact in the database. Should we instead load all
                        // the artifacts in the current target release and check
                        // hashes?
                        //
                        // For now, treat this as "not the current version".
                        warn!(
                            log,
                            "cannot start update: \
                             zone image source version is unknown";
                            "sled_id" => %sled_id,
                            "zone_id" => %zone_config.id,
                        );
                        return Err(
                            TargetReleaseChangeError::PreviousUpdateInProgress,
                        );
                    }
                }
            }
        }
    }

    // All the sled and zone configs match the current target version; it's okay
    // to proceed with an update. We don't attempt to check Hubris components:
    //
    // * They don't have the same API versioning restrictions that require
    //   strict single-stepped upgrades.
    // * We don't keep the desired state of all Hubris components in the
    //   blueprint anyway.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_reconfigurator_planning::example::example;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::MupdateOverrideUuid;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    fn make_os_artifact(
        version: &semver::Version,
    ) -> BlueprintHostPhase2DesiredContents {
        BlueprintHostPhase2DesiredContents::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: ArtifactVersion::new(version.to_string())
                    .expect("valid version"),
            },
            hash: ArtifactHash([0; 32]),
        }
    }

    fn make_zone_artifact(
        version: &semver::Version,
    ) -> BlueprintZoneImageSource {
        BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: ArtifactVersion::new(version.to_string())
                    .expect("valid version"),
            },
            hash: ArtifactHash([0; 32]),
        }
    }

    fn make_blueprint_matching_system_version(
        log: &Logger,
        test_name: &str,
        version: &semver::Version,
    ) -> Blueprint {
        let (_, _, mut bp) = example(log, test_name);
        for sled_config in bp.sleds.values_mut() {
            sled_config.remove_mupdate_override = None;
            sled_config.host_phase_2 = BlueprintHostPhase2DesiredSlots {
                slot_a: make_os_artifact(&version),
                slot_b: make_os_artifact(&version),
            };
            for mut zone_config in sled_config.zones.iter_mut() {
                zone_config.image_source = make_zone_artifact(&version);
            }
        }
        bp
    }

    #[test]
    fn test_version_number_ordering_requirements_for_update() {
        static TEST_NAME: &str =
            "test_version_number_ordering_requirements_for_update";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // Setup: start with an arbitrary system version and a blueprint where
        // all components are on that version.
        let current_version: semver::Version =
            "16.2.0-0.ci+git544f608e05a".parse().unwrap();
        let blueprint = make_blueprint_matching_system_version(
            log,
            TEST_NAME,
            &current_version,
        );

        // Versions we should allow as new targets for an update: different
        // commit in the same release, and any version from the next major
        // version (but not above "major + 1").
        for valid_update_version in [
            "16.2.0-0.ci+git11111111111",
            "16.2.0-0.ci+git22222222222",
            "16.2.0-0.ci+gitfffffffffff",
            "16.2.1-0.ci+git11111111111",
            "16.3.0-0.ci+git22222222222",
            "17.0.0-0.ci+git11111111111",
            "17.0.1-0.ci+git22222222222",
            "17.1.0-0.ci+gitfffffffffff",
            "17.100.99-0.ci+git123456789ab",
        ] {
            let v: semver::Version = valid_update_version.parse().unwrap();
            assert_eq!(
                validate_can_set_target_release_for_update(
                    &blueprint,
                    &current_version,
                    &v,
                    log,
                ),
                Ok(()),
                "should be able to update from {current_version} to {v}"
            );
        }

        // Versions we should not allow as new update targets.
        let same_version = current_version.clone();
        let older_minor: semver::Version =
            "16.1.0-0.ci+git123456789ab".parse().unwrap();
        let older_major: semver::Version =
            "15.3.0-0.ci+git123456789ab".parse().unwrap();
        let skip_major: semver::Version =
            "18.0.0-0.ci+git123456789ab".parse().unwrap();
        for (v, expected_err) in [
            (
                &same_version,
                TargetReleaseChangeError::UpdateToIdenticalVersion(
                    same_version.clone(),
                ),
            ),
            (
                &older_minor,
                TargetReleaseChangeError::CannotDowngrade {
                    current: current_version.clone(),
                    proposed: older_minor.clone(),
                },
            ),
            (
                &older_major,
                TargetReleaseChangeError::CannotDowngrade {
                    current: current_version.clone(),
                    proposed: older_major.clone(),
                },
            ),
            (
                &skip_major,
                TargetReleaseChangeError::CannotSkipMajorVersion {
                    current: 16,
                    proposed: 18,
                },
            ),
        ] {
            match validate_can_set_target_release_for_update(
                &blueprint,
                &current_version,
                v,
                log,
            ) {
                Ok(()) => panic!(
                    "unexpected success updating from {current_version} to {v}"
                ),
                Err(err) => assert_eq!(err, expected_err),
            }
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_reject_update_requests_if_system_is_not_updateable() {
        static TEST_NAME: &str =
            "test_reject_update_requests_if_system_is_not_updateable";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // Setup: start with an arbitrary system version and a blueprint where
        // all components are on that version.
        let current_version: semver::Version =
            "16.2.0-0.ci+git544f608e05a".parse().unwrap();
        let blueprint = make_blueprint_matching_system_version(
            log,
            TEST_NAME,
            &current_version,
        );

        // Pick a next version that's legal in terms of ordering. (Version
        // number ordering checks are covered by
        // `test_version_number_ordering_requirements_for_update()` above.)
        let next_version: semver::Version =
            "17.0.0-0.ci+git123456789ab".parse().unwrap();

        // From our base blueprint, we should be allowed to start an update.
        assert_eq!(
            validate_can_set_target_release_for_update(
                &blueprint,
                &current_version,
                &next_version,
                log,
            ),
            Ok(()),
            "should be able to update from {current_version} to {next_version}"
        );

        // Also pick an old version; one of our tests below is that some
        // component is still on this older version (i.e., we're still trying to
        // update to `current_version`).
        let prev_version: semver::Version =
            "15.0.0-0.ci+git123456789ab".parse().unwrap();

        // Create some blueprints that are modified in various ways to put the
        // system in a non-updateable state.

        // 1. Can't update if any sled has a mupdate override in place (evidence
        //    of a mupdate).
        let bp_sled_mupdate_override = {
            let mut bp = blueprint.clone();
            bp.sleds.values_mut().next().unwrap().remove_mupdate_override =
                Some(MupdateOverrideUuid::new_v4());
            bp
        };

        // 2. Can't update if any sled has both OS slots set to "current
        //    contents" (evidence of a mupdate).
        let bp_os_mupdate = {
            let mut bp = blueprint.clone();
            let sled = bp.sleds.values_mut().next().unwrap();
            sled.host_phase_2.slot_a =
                BlueprintHostPhase2DesiredContents::CurrentContents;
            sled.host_phase_2.slot_b =
                BlueprintHostPhase2DesiredContents::CurrentContents;
            bp
        };

        // 3. Can't update if any zone has an image source of InstallDataset
        //    (evidence of a mupdate).
        let bp_zone_mupdate = {
            let mut bp = blueprint.clone();
            bp.sleds
                .values_mut()
                .next()
                .unwrap()
                .zones
                .iter_mut()
                .next()
                .unwrap()
                .image_source = BlueprintZoneImageSource::InstallDataset;
            bp
        };

        // 4. Can't update if any zone is still running from an old version
        //    (evidence that an update is still in progress).
        let bp_zone_old_version = {
            let mut bp = blueprint.clone();
            bp.sleds
                .values_mut()
                .next()
                .unwrap()
                .zones
                .iter_mut()
                .next()
                .unwrap()
                .image_source = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: ArtifactVersion::new(prev_version.to_string())
                        .unwrap(),
                },
                hash: ArtifactHash([0; 32]),
            };
            bp
        };

        // 5. Can't update if any zone is running from an unknown version
        //    (evidence that an update is still in progress, although today this
        //    shouldn't be possible since we require strictly stepping from one
        //    version to the next).
        let bp_zone_unknown_version = {
            let mut bp = blueprint.clone();
            bp.sleds
                .values_mut()
                .next()
                .unwrap()
                .zones
                .iter_mut()
                .next()
                .unwrap()
                .image_source = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Unknown,
                hash: ArtifactHash([0; 32]),
            };
            bp
        };

        // 6. Can't update if neither host OS slot is on the current version.
        //    (This check should be more precise: we really only care about the
        //    current active slot, but that isn't tracked by the config.)
        let bp_os_old_version = {
            let mut bp = blueprint.clone();
            let sled = bp.sleds.values_mut().next().unwrap();
            let prev_artifact = BlueprintHostPhase2DesiredContents::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: ArtifactVersion::new(prev_version.to_string())
                        .unwrap(),
                },
                hash: ArtifactHash([0; 32]),
            };
            sled.host_phase_2.slot_a = prev_artifact.clone();
            sled.host_phase_2.slot_b = prev_artifact;
            bp
        };

        for (description, blueprint, expected_err) in [
            (
                "sled mupdate override",
                bp_sled_mupdate_override,
                TargetReleaseChangeError::WaitingForMupdateToBeCleared,
            ),
            (
                "OS mupdate",
                bp_os_mupdate,
                TargetReleaseChangeError::WaitingForMupdateToBeCleared,
            ),
            (
                "zone mupdate",
                bp_zone_mupdate,
                TargetReleaseChangeError::WaitingForMupdateToBeCleared,
            ),
            (
                "zone old version",
                bp_zone_old_version,
                TargetReleaseChangeError::PreviousUpdateInProgress,
            ),
            (
                "zone unknown version",
                bp_zone_unknown_version,
                TargetReleaseChangeError::PreviousUpdateInProgress,
            ),
            (
                "OS old version",
                bp_os_old_version,
                TargetReleaseChangeError::PreviousUpdateInProgress,
            ),
        ] {
            match validate_can_set_target_release_for_update(
                &blueprint,
                &current_version,
                &next_version,
                log,
            ) {
                Ok(()) => {
                    panic!("unexpected success with blueprint: {description}")
                }
                Err(err) => assert_eq!(
                    err, expected_err,
                    "unexpected error attempting to update from \
                     blueprint: {description}"
                ),
            }
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_validate_can_set_target_release_for_mupdate_recovery() {
        static TEST_NAME: &str =
            "test_validate_can_set_target_release_for_mupdate_recovery";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // Setup: start with an arbitrary system version and a blueprint where
        // all components are on that version.
        let current_version: semver::Version =
            "16.2.0-0.ci+git544f608e05a".parse().unwrap();
        let blueprint = make_blueprint_matching_system_version(
            log,
            TEST_NAME,
            &current_version,
        );

        // The blueprint described a system on a known version; i.e., no
        // evidence of a mupdate. We should not be able to set the target
        // release for mupdate recovery.
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(&blueprint),
            Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
        );

        // Confirm that blueprints with different kinds of evidence a mupdate
        // has occurred allow setting a new target release for mupdate recovery.

        // sled with a mupdate override in place
        let bp_sled_mupdate_override = {
            let mut bp = blueprint.clone();
            bp.sleds.values_mut().next().unwrap().remove_mupdate_override =
                Some(MupdateOverrideUuid::new_v4());
            bp
        };

        // sled with a mupdate'd host OS
        let bp_os_mupdate = {
            let mut bp = blueprint.clone();
            let sled = bp.sleds.values_mut().next().unwrap();
            sled.host_phase_2.slot_a =
                BlueprintHostPhase2DesiredContents::CurrentContents;
            sled.host_phase_2.slot_b =
                BlueprintHostPhase2DesiredContents::CurrentContents;
            bp
        };

        // zone with an artifact source set to the install dataset
        let bp_zone_mupdate = {
            let mut bp = blueprint.clone();
            bp.sleds
                .values_mut()
                .next()
                .unwrap()
                .zones
                .iter_mut()
                .next()
                .unwrap()
                .image_source = BlueprintZoneImageSource::InstallDataset;
            bp
        };

        for (description, blueprint) in [
            ("sled with mupdate override", bp_sled_mupdate_override),
            ("sled with OS mupdate", bp_os_mupdate),
            ("zone set to install dataset", bp_zone_mupdate),
        ] {
            assert_eq!(
                validate_can_set_target_release_for_mupdate_recovery(
                    &blueprint
                ),
                Ok(()),
                "should find evidence of mupdate in blueprint: {description}"
            );
        }

        logctx.cleanup_successful();
    }
}
