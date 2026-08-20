// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use anyhow::Context;
use anyhow::bail;
use iddqd::IdOrdMap;
use nexus_db_model::TargetReleaseSource;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_reconfigurator_preparation::reconfigurator_state_assemble;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintSledConfig;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::UnstableReconfiguratorState;
use nexus_types::external_api::update;
use nexus_types::internal_api::views::UpdateStatus;
use nexus_types::inventory::Collection;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_debug_dropbox::DepositError;
use omicron_debug_dropbox::DepositHandle;
use omicron_debug_dropbox::Producer;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    target: BlueprintTarget,
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

    async fn assemble_state_for_new_target(
        &self,
        opctx: &OpContext,
        new_target: BlueprintTarget,
    ) -> Result<UnstableReconfiguratorState, Error> {
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let datastore = self.datastore();
        let blueprint = self
            .blueprint_view(opctx, *new_target.target_id.as_untyped_uuid())
            .await?;
        let debug_intent = reconfigurator_state_assemble(
            opctx,
            datastore,
            planning_context.planning_input,
            IdOrdMap::from_iter([inventory]),
            IdOrdMap::from_iter([blueprint.clone()]),
            planning_context.target,
            Some(blueprint.id),
        )
        .await
        .map_err(|error| {
            Error::internal_error(&format!(
                "error assembling Reconfigurator state: {}",
                InlineErrorChain::new(&*error),
            ))
        })?;

        Ok(debug_intent)
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

        // Assemble a Reconfigurator state file so that we have a record of the
        // new target blueprint.
        //
        // We make this best-effort because in an emergency, support should be
        // able to set a new target even if we can't assemble this.
        let step1 = self.assemble_state_for_new_target(opctx, new_target).await;
        let maybe_debug_files = step1.and_then(|debug_intent| {
            SetTargetDebugFile::new(
                &opctx.log,
                &self.debug_dropbox_reconfigurator,
                debug_intent,
            )
            .map_err(|error| {
                Error::internal_error(&format!(
                    "error assembling intended Reconfigurator state: {}",
                    InlineErrorChain::new(&*error),
                ))
            })
        });

        let maybe_debug_files = match maybe_debug_files {
            Ok(debug_files) => debug_files
                .write_intent(BlueprintDebugAction::TargetIntent)
                .await
                .context("saving intent debug file")
                .map_err(|error| {
                    Error::internal_error(&format!(
                        "error saving intended Reconfigurator state: {}",
                        InlineErrorChain::new(&*error),
                    ))
                }),
            Err(error) => Err(error),
        };

        if let Err(error) = self
            .db_datastore
            .blueprint_target_set_current(opctx, new_target)
            .await
        {
            // Try to cancel the dropbox deposit.  This information is
            // useless now.  It's not a problem if this doesn't work.
            if let Ok(debug_files) = maybe_debug_files {
                debug_files.cancel().await;
            }
            return Err(error);
        }

        // We've got a new target.
        //
        // There's no point in failing after this, whatever happens.
        //
        // Write a second Reconfigurator state file reflecting the new target.
        if let Ok(debug_files) = maybe_debug_files {
            debug_files
                .write_committed(new_target, BlueprintDebugAction::Target)
                .await;
        }

        // Trigger the background task to load this blueprint.
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

        // We don't need to create or archive a Reconfigurator state file here
        // because one would have been created when this blueprint was made the
        // target in the first place.
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
        let creator = format!("nexus {}", self.id);
        let datastore = self.datastore();

        let (target, parent_blueprint) =
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

        Ok(PlanningContext { target, planning_input, creator, inventory })
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

        // Assemble a Reconfigurator state file that we can archive for future
        // debugging.
        let parent = Blueprint::clone(
            planning_context.planning_input.parent_blueprint(),
        );
        let debug = reconfigurator_state_assemble(
            opctx,
            self.datastore(),
            planning_context.planning_input,
            IdOrdMap::from_iter([inventory]),
            IdOrdMap::from_iter([parent, blueprint.clone()]),
            planning_context.target,
            None,
        )
        .await
        .and_then(|s| {
            serde_json::to_string(&s)
                .context("serializing Reconfigurator state file")
        })
        .map_err(|error| {
            Error::internal_error(&format!(
                "error assembling Reconfigurator state: {}",
                InlineErrorChain::new(&*error),
            ))
        })?;

        // Archive the Reconfigurator state file.
        let debug_name =
            blueprint_debug_filename(&blueprint, BlueprintDebugAction::Plan);
        let deposit = self
            .debug_dropbox_reconfigurator
            .deposit_file(&debug_name, &debug)
            .await
            .map_err(|error| {
                Error::internal_error(&format!(
                    "error saving Reconfigurator state: {}",
                    InlineErrorChain::new(&error),
                ))
            })?;

        if let Err(error) = self.blueprint_add(&opctx, &blueprint).await {
            // Try to cancel the dropbox deposit.  This information is
            // useless now.  It's not a problem if this doesn't work.
            deposit.cancel_and_attempt_delete().await;
            return Err(error);
        }

        Ok(blueprint)
    }

    pub async fn blueprint_import(
        &self,
        opctx: &OpContext,
        blueprint: Blueprint,
    ) -> Result<(), Error> {
        // We do not save a Reconfigurator state file for import.  The only
        // reason to do so is for the historical record to contain this specific
        // blueprint.  If it's made the target, the record will contain another
        // state file for that operation and that will contain the blueprint.
        // If not, it's not that important.  (We could generate one anyway, but
        // most of the state in the state file would be useless: most of it is
        // oriented around understanding a planning decision, but the state we
        // would construct would not be associated with planning this
        // blueprint.)
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
        params: update::SetTargetReleaseParams,
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
                // If they're attempting to recover from a mupdate, we enforce
                // that this looks like a reasonable thing to do, but it's hard
                // for us to check this exactly. We allow this operation to
                // proceed in two cases:
                //
                // 1. Any sled is waiting for mupdate recovery - we allow the
                //    operation to proceed with no checks on the proposed new
                //    version - we have to trust the operator that it matches
                //    whatever those sled(s) are waiting for. If they get it
                //    wrong, the sleds will still stay in the "waiting for
                //    recovery" state, and the operator can try this again with
                //    the correct version.
                // 2. All sleds are already running the proposed new version,
                //    but the blueprint minimum target release generation is
                //    ahead of the current target release generation. This
                //    happens when a mupdate _to the current version_ on any
                //    sled has occurred: Reconfigurator notices the mupdate,
                //    bumping the minimum target release generation, and does a
                //    no-op conversion to artifacts from the current target
                //    release.
                //
                // An alternative implementation to handle the case of 2 would
                // be to check: is the blueprint minimum target release
                // generation ahead of the current target release generation AND
                // is the operator attempting to recover to the same version
                // that's already the target release. In a "correctly mupdated"
                // system (i.e., one or more sleds have been mupdated to the
                // same version as the rest of the rack, not during a live
                // update), this should be equivalent to the check we do. It
                // would behave differently in "incorrectly mupdated" cases,
                // though. For example:
                //
                // 1. System is in the middle of a live update from release A to
                //    release B.
                // 2. A sled is mupdated to release B. (This is incorrect and
                //    potentially wildly dangerous! But mupdates are an escape
                //    hatch and we have no way of preventing it other than
                //    documentation and processes.)
                // 3. Reconfigurator notices the mupdate and bumps the minimum
                //    target release generation, and pauses the update as a
                //    result.
                // 4. Reconfigurator performs a no-op conversion of this sled,
                //    updating all its components to artifacts sourced from
                //    release B. (No-op conversion is _not_ paused by the
                //    blueprint's minimum target release generation being ahead
                //    of the current target release.)
                //
                // If the operator attempts to use this endpoint to set the
                // target release to B, what should we do? In the current
                // implementation, we'll check all artifact versions in the
                // blueprint, find some on A and some on B, and therefore reject
                // the request. In the alternative implementation proposed
                // above, we would allow the request, which would unpause the
                // update. It seems safer to _reject_ the request, even though
                // that leaves the update wedged without support intervention,
                // because the system is in an illegal state that required
                // support intervention in the first place (mupdating a single
                // sled in the middle of a live update).
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
                            *current_target_release.generation,
                            &new_system_version,
                            &self.log,
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
        "mupdate recovery required, but specified version \
         {proposed_new_version} does not match the version of \
         components deployed on sled {sled_id} ({version_found})"
    )]
    MupdateRecoveryToWrongVersion {
        sled_id: SledUuid,
        version_found: BlueprintArtifactVersion,
        proposed_new_version: semver::Version,
    },
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
        "cannot skip from scheduled release {current} \
         to scheduled release {proposed}"
    )]
    CannotSkipScheduledRelease { current: u64, proposed: u64 },
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
// `target_release_update()` above. We only reject this request if there is no
// evidence that we need to recover from a mupdate, or if we have enough
// information to know the operator is trying to recover to an incorrect
// version. Evidence we consider:
//
// 1. Are any sleds waiting for mupdate recovery (either they have a mupdate
//    override in place, or they have components sourced to the install
//    dataset / both OS slots set to "current contents")?
// 2. If there are no sleds waiting for mupdate recovery, we still need to allow
//    recovery to proceed if (a) the blueprint's minimum target release
//    generation is higher than the current target release's generation and (b)
//    all current zones / OS images have sources matching the proposed target
//    version. This case coincides with an individual sled being mupdated _to
//    the same target release as the rest of the system_; see
//    <https://github.com/oxidecomputer/omicron/issues/10917> for more details.
//
// This check is inherently racy: a sled could have just been mupdated but we
// haven't yet noticed. There isn't much we can do about that, but it seems
// quite unlikely (the same human would generally be doing both of these
// operations) and the operator should be able to retry this operation and have
// it work the second time.
fn validate_can_set_target_release_for_mupdate_recovery(
    current_blueprint: &Blueprint,
    current_target_release_gen: Generation,
    proposed_new_version: &semver::Version,
    log: &Logger,
) -> Result<(), TargetReleaseChangeError> {
    let min_target_release_gen_is_ahead_of_actual_target_release_gen =
        current_blueprint.target_release_minimum_generation
            > current_target_release_gen;

    // Check whether all components already match `proposed_new_version`.
    match BlueprintTargetReleaseStatus::new(
        current_blueprint,
        proposed_new_version,
    ) {
        BlueprintTargetReleaseStatus::AllComponentsMatchTargetRelease => {
            if min_target_release_gen_is_ahead_of_actual_target_release_gen {
                // All components are on the proposed new version, but we need
                // to allow recovery to catch up to the min target release
                // generation specified in the blueprint.
                info!(
                    log,
                    "allowing target release to be set for mupdate recovery: \
                     all components are on the proposed new version, but the \
                     blueprint minimum target release generation is ahead of \
                     the current target release generation";
                    "proposed_version" => %proposed_new_version,
                    "blueprint_min_target_release_gen" =>
                        %current_blueprint.target_release_minimum_generation,
                    "current_target_release_gen" => %current_target_release_gen,
                );
                Ok(())
            } else {
                // All components are on the proposed new version and there is
                // no need to bump the target release generation - we don't need
                // to recover.
                Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
            }
        }
        BlueprintTargetReleaseStatus::WaitingForMupdateToBeCleared {
            how,
            sled_id,
        } => {
            // At least one sled is waiting for a mupdate to be cleared;
            // recovery is allowed.
            info!(
                log,
                "allowing target release to be set for mupdate recovery: \
                 found a sled that is waiting for a mupdate to be cleared";
                "mupdate_detected_how" => ?how,
                "sled_id" => %sled_id,
            );
            Ok(())
        }
        BlueprintTargetReleaseStatus::FoundDifferentVersion {
            sled_id,
            version_found,
        } => {
            // There are two obvious ways to get here:
            //
            // 1. No mupdate has happened, and the operator has called this
            //    endpoint erroneously
            // 2. A mupdate to the current target release has happened, but the
            //    operator has called this endpoint with the wrong version
            //
            // We'll key off of
            // `min_target_release_gen_is_ahead_of_actual_target_release_gen` to
            // try to guess which case we're in: if it does look like a mupdate
            // has happened that needs to be recovered from, we'll return an
            // error noting that we think we're in case 2. Otherwise, it looks
            // like we're in case 1 and no mupdate recovery is needed.
            if min_target_release_gen_is_ahead_of_actual_target_release_gen {
                Err(TargetReleaseChangeError::MupdateRecoveryToWrongVersion {
                    sled_id,
                    version_found,
                    proposed_new_version: proposed_new_version.clone(),
                })
            } else {
                Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
            }
        }
    }
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

    // We cannot skip scheduled releases.
    if proposed_new_version.major > current_version.major + 1 {
        warn!(
            log,
            "cannot start update: attempt to update past next scheduled release"
        );
        return Err(TargetReleaseChangeError::CannotSkipScheduledRelease {
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

/// Status of the blueprint relative to a specified target release version
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BlueprintTargetReleaseStatus {
    /// All sled and zone configs match the specified target release version; no
    /// evidence of a mupdate.
    AllComponentsMatchTargetRelease,
    /// At least one sled or zone shows evidence of a mupdate that must be
    /// cleared.
    WaitingForMupdateToBeCleared {
        how: SledMupdateDetectedHow,
        sled_id: SledUuid,
    },
    /// At least one sled or zone is not on the specified target release
    /// version (and no mupdate evidence was found).
    FoundDifferentVersion {
        sled_id: SledUuid,
        version_found: BlueprintArtifactVersion,
    },
}

impl BlueprintTargetReleaseStatus {
    // Check the blueprint against a specified target release version.
    //
    // Returned statuses are prioritized:
    //
    // 1. If any sleds are waiting for a mupdate to be cleared,
    //    `WaitingForMupdateToBeCleared { .. }` will be returned
    // 2. Otherwise, if we find any components at a version other than
    //    `version_to_check`, `FoundDifferentVersion { .. }` will be returned
    // 3. Otherwise, `AllComponentsMatchTargetRelease` will be returned.
    //
    // We don't attempt to check Hubris components:
    //
    // * They don't have the same API versioning restrictions that require
    //   strict single-stepped upgrades.
    // * We don't keep the desired state of all Hubris components in the
    //   blueprint anyway.
    pub(super) fn new(
        current_blueprint: &Blueprint,
        version_to_check: &semver::Version,
    ) -> Self {
        let mut found_mupdate = None;
        let mut found_different_version = None;

        // Blueprint artifact versions are stored as strings, not
        // `semver::Version`s. Here we're only looking at zone and OS versions,
        // which are guaranteed to match the system version, but we still need
        // to convert the `semver::Version` we got to a string for comparison.
        let version_to_check = version_to_check.to_string();

        // Check sled configs first.
        for (sled_id, sled_config) in current_blueprint.active_sled_configs() {
            match SledUpdateStatus::new(sled_config, &version_to_check) {
                SledUpdateStatus::HasUnresolvedMupdate(how) => {
                    found_mupdate.get_or_insert((how, sled_id));
                }
                SledUpdateStatus::FoundDifferentVersion { os_version } => {
                    found_different_version
                        .get_or_insert((sled_id, os_version));
                }
                SledUpdateStatus::VersionMatches => {
                    // This sled is okay; move on to the next.
                }
            }
        }

        // Now check zone configs.
        for (sled_id, zone_config) in current_blueprint.in_service_zones() {
            match &zone_config.image_source {
                // When a zone's image source is the install dataset, the sled
                // has never been updated by reconfigurator and is still in the
                // initial state left by the manufacturing mupdate.
                BlueprintZoneImageSource::InstallDataset => {
                    found_mupdate.get_or_insert_with(|| {
                        (
                            SledMupdateDetectedHow::VersionIsInstallDataset,
                            sled_id,
                        )
                    });
                }
                BlueprintZoneImageSource::Artifact { version, .. } => {
                    match version {
                        BlueprintArtifactVersion::Available { version: v } => {
                            if v.as_str() != version_to_check {
                                found_different_version.get_or_insert_with(
                                    || (sled_id, version.clone()),
                                );
                            }
                        }
                        // This shouldn't happen; it means we have an artifact
                        // source in the blueprint that doesn't match a known
                        // artifact in the database. Should we instead load all
                        // the artifacts in the current target release and check
                        // hashes?
                        //
                        // For now, record this as "not the version we're
                        // checking for".
                        BlueprintArtifactVersion::Unknown => {
                            found_different_version.get_or_insert_with(|| {
                                (sled_id, version.clone())
                            });
                        }
                    }
                }
            }
        }

        // Prioritize "found a mupdate" > "found a wrong version" > "ok"
        match (found_mupdate, found_different_version) {
            (Some((how, sled_id)), _) => {
                BlueprintTargetReleaseStatus::WaitingForMupdateToBeCleared {
                    how,
                    sled_id,
                }
            }
            (None, Some((sled_id, version_found))) => {
                BlueprintTargetReleaseStatus::FoundDifferentVersion {
                    sled_id,
                    version_found,
                }
            }
            (None, None) => {
                BlueprintTargetReleaseStatus::AllComponentsMatchTargetRelease
            }
        }
    }
}

// Check whether we should allow an operator to change the current target
// release to start a new system update.
//
// We must reject target release changes if:
//
// * A mupdate has occurred (they must use the "set target release for recovery"
//   endpoint instead)
// * Another update is in progress
// * The new version doesn't satisfy our requirements for upgrade ordering (no
//   downgrades; cannot skip scheduled releases)
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

    match BlueprintTargetReleaseStatus::new(
        current_blueprint,
        current_target_version,
    ) {
        // When all components are on the current target release it means no
        // mupdate is detected
        BlueprintTargetReleaseStatus::AllComponentsMatchTargetRelease => Ok(()),
        BlueprintTargetReleaseStatus::WaitingForMupdateToBeCleared {
            how,
            sled_id,
        } => {
            warn!(
                log,
                "cannot start update: mupdate detected";
                "sled_id" => %sled_id,
                "mupdated_detected_how" => ?how,
            );
            Err(TargetReleaseChangeError::WaitingForMupdateToBeCleared)
        }
        BlueprintTargetReleaseStatus::FoundDifferentVersion {
            sled_id,
            version_found,
        } => {
            warn!(
                log,
                "cannot start update: previous update not complete";
                "sled_id" => %sled_id,
                "version_found" => %version_found,
            );
            Err(TargetReleaseChangeError::PreviousUpdateInProgress)
        }
    }
}

// Ways in which a [`BlueprintSledConfig`] can indicate the sled has been
// mupdated.
//
// (Does not count the zones _within_ a `BlueprintSledConfig`; those are checked
// elsewhere.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SledMupdateDetectedHow {
    RemoveMupdateOverridePresent,
    BootDiskContents,
    VersionIsInstallDataset,
}

// Status of any update or mupdate on a sled, not considering its zones, based
// on a provided version to check against.
enum SledUpdateStatus {
    // The sled has been mupdated and is waiting on mupdate recovery.
    HasUnresolvedMupdate(SledMupdateDetectedHow),

    // The sled is not waiting on mupdate recovery, but is not running the
    // specified target version.
    FoundDifferentVersion { os_version: BlueprintArtifactVersion },

    // The sled is not waiting on mupdate recovery and is running the specified
    // target version.
    VersionMatches,
}

impl SledUpdateStatus {
    // Determine the [`SledUpdateStatus`] of the given sled based on its config
    // and a version to check.
    fn new(sled_config: &BlueprintSledConfig, version_to_check: &str) -> Self {
        // Is the planner currently trying to remove a mupdate override from
        // this sled?
        if sled_config.remove_mupdate_override.is_some() {
            return Self::HasUnresolvedMupdate(
                SledMupdateDetectedHow::RemoveMupdateOverridePresent,
            );
        }

        // This sled does not have a `remove_mupdate_override` set; now check
        // the OS slots.
        //
        // After the blueprint planner detects a mupdate override, it sets
        // `remove_mupdate_override` to `Some(_)` _and_ sets all OS and zone
        // sources to `CurrentContents` (OS) and `InstallDataset` (zones). Once
        // sled-agent has received that new config, it will remove its mupdate
        // override, and then the planner will set `remove_mupdate_override`
        // back to `None`. But we're still waiting for mupdate recovery if our
        // OS slots are set to `CurrentContents` or any zone is sourced from
        // `InstallDataset`. (Zone checks are performed elsewhere.)

        // If both OS slots are set to `CurrentContents`, we've been mupdated.
        // Otherwise, we consider this sled to be running `version_to_check` if
        // either slot matches. Ideally we'd only check the slot that is
        // supposed to be booting, but blueprints don't currently track that
        // (maybe they should?).
        //
        // This is not as precise as we'd like, but in practice is unlikely to
        // be a problem: OS slots are updated before zones, so even if we
        // erroneously claim the OS is up to date when it isn't (e.g., if it's
        // still booting out of an old slot), there will be many zones present
        // that are not yet updated.
        let mut num_slots_with_current_contents = 0;
        let mut found_version_to_check_in_either_slot = false;
        let mut found_different_version = BlueprintArtifactVersion::Unknown;
        for slot in
            [&sled_config.host_phase_2.slot_a, &sled_config.host_phase_2.slot_b]
        {
            match slot {
                BlueprintHostPhase2DesiredContents::CurrentContents => {
                    num_slots_with_current_contents += 1;
                }
                BlueprintHostPhase2DesiredContents::Artifact {
                    version,
                    ..
                } => match version {
                    BlueprintArtifactVersion::Available { version: v } => {
                        if v.as_str() == version_to_check {
                            found_version_to_check_in_either_slot = true;
                            break;
                        } else {
                            found_different_version = version.clone();
                        }
                    }
                    BlueprintArtifactVersion::Unknown => {
                        // This shouldn't happen; it means we have an artifact
                        // source in the blueprint that doesn't match a known
                        // artifact in the database. It is certainly true that
                        // it's not the current version and not evidence of a
                        // mupdate, though, so we treat it just like any other
                        // "not current version".
                        //
                        // We initialize `found_different_version` to
                        // `Unknown`; if both slots are unknown, we leave it in
                        // that state. If either slot is known, we'll update it
                        // in the `::Available` branch above.
                    }
                },
            }
        }

        // As noted above, but now condensed: if we found the version to check
        // in either slot, we assume this sled is updated. If we instead found
        // `CurrentContents` in both slots, this sled has been mupdated. Any
        // other combination means the sled has not been mupdated but also isn't
        // running the version we're checking for.
        if found_version_to_check_in_either_slot {
            Self::VersionMatches
        } else if num_slots_with_current_contents == 2 {
            Self::HasUnresolvedMupdate(SledMupdateDetectedHow::BootDiskContents)
        } else {
            Self::FoundDifferentVersion { os_version: found_different_version }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BlueprintDebugAction {
    /// the autoplanner generated this blueprint and will try to make it the
    /// target
    AutoplanIntent,
    /// the autoplanner generated this blueprint and made it the target
    Autoplan,
    /// someone explicitly ran the planner using the Nexus internal API
    /// (likely a person running `omdb`)
    Plan,
    /// someone explicitly requested to set the target blueprint using the Nexus
    /// internal API (likely a person running `omdb`) and the system will try to
    /// make this the new target
    TargetIntent,
    /// someone explicitly set the target blueprint using the Nexus internal API
    /// (likely a person running `omdb`) and the system made it the new target
    Target,
}

/// Returns the filename for a debug drop file related to blueprint planning
pub fn blueprint_debug_filename(
    blueprint: &Blueprint,
    action: BlueprintDebugAction,
) -> String {
    let action_str = match action {
        BlueprintDebugAction::AutoplanIntent => "autoplan-intent",
        BlueprintDebugAction::Autoplan => "autoplan",
        BlueprintDebugAction::Plan => "plan",
        BlueprintDebugAction::TargetIntent => "target-intent",
        BlueprintDebugAction::Target => "target",
    };
    let time_str = blueprint.time_created.format("%Y%m%dT%H%MZ");
    format!("{time_str}-{action_str}-{}.json", blueprint.id)
}

/// Typestate-based helper to manage writing out two Reconfigurator state files
/// as part of setting a new target blueprint: the first is an "intent" file and
/// the second is a "committed" file.
// This is currently used in two places, but the main reason to factor it
// separately is to test the intent file behavior.  This is otherwise difficult
// to orchestrate in either of the consumers.
#[derive(Debug)]
pub struct SetTargetDebugFile<'a> {
    log: &'a Logger,
    producer: &'a Producer,
    intent_state: UnstableReconfiguratorState,
    intent_state_str: String,
    intended_blueprint_id: BlueprintUuid,
}

impl<'a> SetTargetDebugFile<'a> {
    pub fn new(
        log: &'a Logger,
        producer: &'a Producer,
        intent_state: UnstableReconfiguratorState,
    ) -> Result<SetTargetDebugFile<'a>, anyhow::Error> {
        let Some(intended_blueprint_id) =
            intent_state.intended_target_blueprint
        else {
            bail!("intent_state does not have an intended_target_blueprint");
        };

        if !intent_state.blueprints.contains_key(&intended_blueprint_id) {
            bail!(
                "intent_state's intended_target_blueprint is missing \
                 from `blueprints`"
            );
        }

        let intent_state_str = serde_json::to_string(&intent_state)
            .context("serializing intent Reconfigurator state file")?;

        Ok(SetTargetDebugFile {
            log,
            producer,
            intent_state,
            intent_state_str,
            intended_blueprint_id,
        })
    }

    pub async fn write_intent(
        self,
        intent_reason: BlueprintDebugAction,
    ) -> Result<SetTargetDebugFileWroteIntent<'a>, DepositError> {
        // unwrap(): we checked in `new` that this was present.
        let blueprint = self
            .intent_state
            .blueprints
            .get(&self.intended_blueprint_id)
            .unwrap();
        let name = blueprint_debug_filename(blueprint, intent_reason);
        let intent_deposit =
            self.producer.deposit_file(&name, &self.intent_state_str).await?;

        info!(&self.log, "saved intended debug state"; "filename" => name);

        Ok(SetTargetDebugFileWroteIntent {
            log: self.log,
            producer: self.producer,
            intent_state: self.intent_state,
            intent_deposit,
        })
    }
}

#[derive(Debug)]
pub struct SetTargetDebugFileWroteIntent<'a> {
    log: &'a Logger,
    producer: &'a Producer,
    intent_state: UnstableReconfiguratorState,
    intent_deposit: DepositHandle,
}

impl<'a> SetTargetDebugFileWroteIntent<'a> {
    pub async fn cancel(self) {
        debug!(self.log, "attempting to remove intent file after failure");
        self.intent_deposit.cancel_and_attempt_delete().await;
        warn!(self.log, "removed intent file after failure");
    }

    pub async fn write_committed(
        self,
        new_target: BlueprintTarget,
        commit_reason: BlueprintDebugAction,
    ) {
        // Writing the commited state is best-effort, since the action has
        // already been done.

        let committed_state = UnstableReconfiguratorState {
            intended_target_blueprint: None,
            target_blueprint: new_target,
            ..self.intent_state
        };

        // unwrap(): we checked this in `SetTargetDebugFile::new()`.
        let blueprint = committed_state
            .blueprints
            .get(&committed_state.target_blueprint.target_id)
            .unwrap();
        let name = blueprint_debug_filename(&blueprint, commit_reason);

        let committed_str = match serde_json::to_string(&committed_state) {
            Ok(s) => s,
            Err(error) => {
                error!(
                    &self.log,
                    "failed to serialize committed debug state";
                    InlineErrorChain::new(&error),
                    "filename" => name,
                );
                return;
            }
        };

        match self.producer.deposit_file(&name, &committed_str).await {
            Ok(_deposit) => {
                // We successfully deposited the "commit" state.
                // Make a best-effort to cancel the intended state file.
                self.intent_deposit.cancel_and_attempt_delete().await;

                info!(
                    &self.log,
                    "saved committed debug state";
                    "filename" => name,
                );
            }
            Err(error) => {
                // We failed to deposit the "commit" state.  Log the error and
                // keep the intended state around.  There's nothing more to do.
                error!(
                    &self.log,
                    "failed to save committed debug state";
                    InlineErrorChain::new(&error),
                    "filename" => name,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use nexus_reconfigurator_planning::example::example;
    use nexus_reconfigurator_preparation::reconfigurator_state_load;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use omicron_test_utils::dev::dropbox::TestDropbox;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::MupdateOverrideUuid;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

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
                TargetReleaseChangeError::CannotSkipScheduledRelease {
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

        // Construct a blueprint with a bumped
        // `target_release_minimum_generation`, returning that and the previous
        // generation.
        let (current_target_release_gen, blueprint) = {
            let mut bp = make_blueprint_matching_system_version(
                log,
                TEST_NAME,
                &current_version,
            );
            let initial_gen = bp.target_release_minimum_generation;
            bp.target_release_minimum_generation =
                bp.target_release_minimum_generation.next();
            (initial_gen, bp)
        };

        // The version we'll propose for mupdate recovery. This test exercises
        // the "whole system" mupdate paths, not the special case where we're
        // trying to recover to the same version that's already deployed (that's
        // covered by
        // test_target_release_for_mupdate_recovery_after_noop_conversion
        // below), so any version that's different from `current_version` is
        // fine.
        let proposed_recovery_version: semver::Version =
            "17.0.0-0.ci+git0123456789a".parse().unwrap();

        // The blueprint described a system on a known version _and_ the current
        // target release gen matches the blueprint's minimum; i.e., no evidence
        // of a mupdate. We should not be able to set the target release for
        // mupdate recovery.
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                blueprint.target_release_minimum_generation,
                &proposed_recovery_version,
                log,
            ),
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
            // We should detect these sleds that need mupdate recovery whether
            // or not the blueprint min target release generation is ahead of
            // the current target release generation; try both.
            for (gen_description, generation) in [
                (
                    "generation behind blueprint minimum",
                    current_target_release_gen,
                ),
                (
                    "generation caught up to blueprint minimum",
                    blueprint.target_release_minimum_generation,
                ),
            ] {
                assert_eq!(
                    validate_can_set_target_release_for_mupdate_recovery(
                        &blueprint,
                        generation,
                        &proposed_recovery_version,
                        log,
                    ),
                    Ok(()),
                    "should find evidence of mupdate in blueprint: \
                     {description} ({gen_description})"
                );
            }
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_target_release_for_mupdate_recovery_after_noop_conversion() {
        static TEST_NAME: &str =
            "test_target_release_for_mupdate_recovery_after_noop_conversion";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // Setup: start with an arbitrary system version and a blueprint where
        // all components are on that version.
        let current_version: semver::Version =
            "16.2.0-0.ci+git544f608e05a".parse().unwrap();
        let mut blueprint = make_blueprint_matching_system_version(
            log,
            TEST_NAME,
            &current_version,
        );

        let different_version: semver::Version =
            "17.0.0-0.ci+git0123456789a".parse().unwrap();
        let initial_target_release_generation =
            blueprint.target_release_minimum_generation;

        // The blueprint described a system on a known version; i.e., no
        // evidence of a mupdate. We should not be able to set the target
        // release for mupdate recovery, whether to the current version or a
        // different version.
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                initial_target_release_generation,
                &current_version,
                log,
            ),
            Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
        );
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                initial_target_release_generation,
                &different_version,
                log,
            ),
            Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
        );

        // Bump the min target release generation in the blueprint, but do _not_
        // change any of the component versions: this is consistent with one or
        // more sleds being mupdated to `current_version`, the planner clearing
        // that mupdate and no-op converting all components to `current_version`
        // artifacts.
        blueprint.target_release_minimum_generation =
            blueprint.target_release_minimum_generation.next();

        // Attempting to mupdate recover to a _different_ version should fail,
        // regardless of the current target release generation: no sled is
        // showing evidence of a mupdate, so we only allow mupdate recovery to a
        // version that matches all configured component sources.
        let expected_err =
            TargetReleaseChangeError::MupdateRecoveryToWrongVersion {
                // Our checks always return the first sled with a problem, which
                // in this case just means "the first sled".
                sled_id: blueprint.active_sled_configs().next().unwrap().0,
                proposed_new_version: different_version.clone(),
                version_found: BlueprintArtifactVersion::Available {
                    version: ArtifactVersion::new(current_version.to_string())
                        .unwrap(),
                },
            };
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                initial_target_release_generation,
                &different_version,
                log,
            ),
            Err(expected_err),
        );
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                blueprint.target_release_minimum_generation,
                &different_version,
                log,
            ),
            Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
        );

        // But mupdate recovery to the correct current version should succeed if
        // the blueprint is ahead of the current target release generation.
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                initial_target_release_generation,
                &current_version,
                log,
            ),
            Ok(())
        );

        // Correct version, but blueprint target release generation is no
        // longer ahead of the current target release generation: no recovery
        // needed.
        assert_eq!(
            validate_can_set_target_release_for_mupdate_recovery(
                &blueprint,
                blueprint.target_release_minimum_generation,
                &current_version,
                log,
            ),
            Err(TargetReleaseChangeError::NoMupdateRecoveryNeeded)
        );

        logctx.cleanup_successful();
    }

    /// Verifies writing Reconfigurator state in the success path
    #[nexus_test(server = crate::Server)]
    async fn test_debug_files(cptestctx: &ControlPlaneTestContext) {
        let log = &cptestctx.logctx.log;

        // This whole block is just to get our hands on a complete, self-
        // consistent Reconfigurator state and a blueprint consistent with it.
        let (initial_state, blueprint) = {
            let datastore = cptestctx.server.server_context().nexus.datastore();
            let opctx = OpContext::for_tests(log.clone(), datastore.clone());
            let nblueprints = 5;
            let initial_state =
                reconfigurator_state_load(&opctx, &datastore, nblueprints)
                    .await
                    .expect("loading test system state");
            assert!(initial_state.intended_target_blueprint.is_none());

            // Make a new blueprint.
            let nexus_client = cptestctx.lockstep_client();
            let blueprint = nexus_client
                .blueprint_regenerate()
                .await
                .expect("creating new blueprint")
                .into_inner();
            (initial_state, blueprint)
        };

        // Case: it's an error to provide an initial state that doesn't have the
        // intended blueprint field set.
        let test_dropbox = TestDropbox::new(log.clone()).await;
        let intent_state = initial_state.clone();
        let error =
            SetTargetDebugFile::new(log, test_dropbox.producer(), intent_state)
                .unwrap_err();
        println!("found error: {error:#}");
        assert!(format!("{error:#}").contains("intended_target_blueprint"));
        assert!(test_dropbox.new_reader().load_new::<()>().is_empty());

        // Case: it's an error to provide an initial state referencing a
        // blueprint that isn't in `blueprints`.
        let test_dropbox = TestDropbox::new(log.clone()).await;
        let mut intent_state = initial_state.clone();
        intent_state.intended_target_blueprint = Some(blueprint.id);
        let error =
            SetTargetDebugFile::new(log, test_dropbox.producer(), intent_state)
                .unwrap_err();
        println!("found error: {error:#}");
        assert!(format!("{error:#}").contains("intended_target_blueprint"));
        assert!(test_dropbox.new_reader().load_new::<()>().is_empty());
        test_dropbox.cleanup_successful();

        // The remaining test cases assume a valid intended state.
        let mut intent_state = initial_state.clone();
        intent_state.intended_target_blueprint = Some(blueprint.id);
        intent_state
            .blueprints
            .insert_unique(blueprint)
            .expect("new blueprint");

        test_debug_files_success(log, &intent_state).await;
        test_debug_files_cancel(log, &intent_state).await;
        test_debug_files_intent_fail(log, &intent_state).await;
    }

    /// Test case: happy path (writing "intent" file followed by "commit" file)
    async fn test_debug_files_success(
        log: &Logger,
        intent_state: &UnstableReconfiguratorState,
    ) {
        let test_dropbox = TestDropbox::new(log.clone()).await;
        let producer = test_dropbox.producer();
        let mut reader = test_dropbox.new_reader();

        // No files ought to have been created yet.
        let files = reader.load_new::<UnstableReconfiguratorState>();
        assert!(files.is_empty());

        // Write the intent file and verify it.
        let helper =
            SetTargetDebugFile::new(log, producer, intent_state.clone())
                .expect("valid input")
                .write_intent(BlueprintDebugAction::TargetIntent)
                .await
                .expect("write intent file");

        let files = reader.load_new::<UnstableReconfiguratorState>();
        assert_eq!(files.len(), 1);
        let file = files.into_iter().next().expect("non-empty Vec");
        assert_eq!(file, *intent_state);
        assert_eq!(0, reader.count_removed());

        // Write the commit file and verify it.
        let now = Utc::now();
        let new_target = BlueprintTarget {
            target_id: intent_state.intended_target_blueprint.unwrap(),
            enabled: intent_state.target_blueprint.enabled,
            time_made_target: now,
        };
        helper.write_committed(new_target, BlueprintDebugAction::Target).await;

        let files = reader.load_new::<UnstableReconfiguratorState>();
        assert_eq!(files.len(), 1);
        let file = files.into_iter().next().expect("non-empty Vec");

        let expected_state = UnstableReconfiguratorState {
            target_blueprint: new_target,
            intended_target_blueprint: None,
            ..intent_state.clone()
        };
        assert_eq!(file, expected_state);

        // The intent file ought to have been removed.
        assert_eq!(1, reader.count_removed());
        test_dropbox.cleanup_successful();
    }

    /// Test case: cancel writing new "set target" debug file after writing the
    /// intent file
    async fn test_debug_files_cancel(
        log: &Logger,
        intent_state: &UnstableReconfiguratorState,
    ) {
        let test_dropbox = TestDropbox::new(log.clone()).await;
        let producer = test_dropbox.producer();
        let mut reader = test_dropbox.new_reader();

        // No files ought to have been created yet.
        let files = reader.load_new::<UnstableReconfiguratorState>();
        assert!(files.is_empty());

        // Write the intent file and verify it.
        let helper =
            SetTargetDebugFile::new(log, producer, intent_state.clone())
                .expect("valid input")
                .write_intent(BlueprintDebugAction::TargetIntent)
                .await
                .expect("write intent file");

        let files = reader.load_new::<UnstableReconfiguratorState>();
        assert_eq!(files.len(), 1);
        let file = files.into_iter().next().expect("non-empty Vec");
        assert_eq!(file, *intent_state);

        // Case: cancel.  This is what we'd do if we failed to make the new
        // blueprint the target.  There should be nothing new in the dropbox and
        // the previous file ought to have been removed.
        helper.cancel().await;
        let files = reader.load_new::<UnstableReconfiguratorState>();
        assert!(files.is_empty());
        assert_eq!(1, reader.count_removed());

        test_dropbox.cleanup_successful();
    }

    /// Test case: exercise failure to write the intent file
    async fn test_debug_files_intent_fail(
        log: &Logger,
        intent_state: &UnstableReconfiguratorState,
    ) {
        let test_dropbox = TestDropbox::new(log.clone()).await;
        let (dir, producer) = test_dropbox.into_parts();

        // Delete the directory so that the write below will fail.
        dir.cleanup_successful();

        // Attempt to write the intent file and verify the error.
        let error =
            SetTargetDebugFile::new(log, &producer, intent_state.clone())
                .expect("valid input")
                .write_intent(BlueprintDebugAction::TargetIntent)
                .await
                .expect_err("failure to write intent file");
        let message = InlineErrorChain::new(&error);
        println!("found error: {message}");
        assert!(message.contains("I/O error"));
    }

    /// Test case: exercise failure to write the intent file
    async fn test_debug_files_intent_fail(
        log: &Logger,
        intent_state: &UnstableReconfiguratorState,
    ) {
        let test_dropbox = TestDropbox::new(log.clone()).await;
        let (dir, producer) = test_dropbox.into_parts();

        // Delete the directory so that the write below will fail.
        dir.cleanup_successful();

        // Attempt to write the intent file and verify the error.
        let error =
            SetTargetDebugFile::new(log, &producer, intent_state.clone())
                .expect("valid input")
                .write_intent(BlueprintDebugAction::TargetIntent)
                .await
                .expect_err("failure to write intent file");
        let message = InlineErrorChain::new(&error).to_string();
        println!("found error: {message}");
        assert!(message.contains("I/O error"));
    }
}
