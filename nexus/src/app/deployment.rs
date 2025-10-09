// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

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
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::PlanningInput;
use nexus_types::external_api::views::TargetReleaseSource;
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
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    planning_input: PlanningInput,
    creator: String,
    inventory: Option<Collection>,
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
        // Load up the planner config from the db directly (rather than from,
        // say, the background task) to ensure we get the latest state.
        let planner_config = self
            .db_datastore
            .reconfigurator_config_get_latest(opctx)
            .await?
            .map_or_else(PlannerConfig::default, |c| c.config.planner_config);

        let planning_input =
            PlanningInputFromDb::assemble(opctx, datastore, planner_config)
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
        let (_, parent_blueprint) =
            self.db_datastore.blueprint_target_get_current_full(opctx).await?;

        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &parent_blueprint,
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
        let new = planning_context.planning_input.tuf_repo().description();
        let old = planning_context.planning_input.old_repo().description();
        let status = UpdateStatus::new(old, new, &inventory);

        Ok(status)
    }

    pub(crate) async fn validate_target_release_change_allowed(
        &self,
        opctx: &OpContext,
        current_target_release: &TargetReleaseSource,
    ) -> Result<(), Error> {
        let current_target_version = match current_target_release {
            TargetReleaseSource::Unspecified => {
                // We've never set a target release before; it's always fine to
                // set the first one.
                return Ok(());
            }
            TargetReleaseSource::SystemVersion { version } => version,
        };

        let (_, current_blueprint) =
            self.datastore().blueprint_target_get_current_full(opctx).await?;

        if is_target_release_change_allowed(
            &current_blueprint,
            current_target_version,
        ) {
            Ok(())
        } else {
            Err(Error::invalid_request(
                "Target release cannot be changed: \
                 a previous update is still in progress.",
            ))
        }
    }
}

// Check whether we should allow an operator to change the current target
// release. This does not check whether changing to a _particular_ version is
// ok; rather, it checks whether changing it _all all_ is allowed.
//
// We must allow target release changes if there's any evidence a mupdate has
// occurred; setting a new target release is how the operator gives control back
// to Nexus after that's happened.
//
// If there's not evidence a mupdate has occurred, we must _reject_ target
// release changes if another update is still in progress; we don't allow
// upgrading to version N+1 if we're still in the process of upgrading from N-1
// to N.
//
// The latter restriction is due to the implementation of the blueprint planner,
// which means it's sufficient to look at the current target blueprint; we don't
// have to look at inventory. As long as we've been able to decide how to finish
// a previous update, we can allow starting the next one, even if we haven't
// finished executing the previous one.
fn is_target_release_change_allowed(
    current_blueprint: &Blueprint,
    current_target_version: &semver::Version,
) -> bool {
    let current_target_version = current_target_version.to_string();

    // Convert this to a string; comparing "system version as a string" to
    // "artifact version as a string" below feels bad, but is maybe fine? The
    // artifact versions are loaded from the DB by joining
    // As we look over the blueprint, did we find any component not on the
    // current target release? We can't immediately return `Ok(false)` if we do,
    // because we have to keep checking them all for evidence of a mupdate.
    // Instead, we'll set this boolean to `false` and return it at the end.
    let mut all_components_on_current_target_release = true;

    // Check sled configs first.
    for sled_config in current_blueprint.active_sled_configs() {
        if sled_config.remove_mupdate_override.is_some() {
            // A mupdate has occurred; we must allow a new target release.
            return true;
        }

        // This check is a little funky and I'm not sure it's right, so
        // we'll err on the side of being permissive. The blueprint doesn't
        // track which slot is supposed to be the boot disk (maybe it
        // should?), so we'll consider a mupdate in progress if either slot
        // shows evidence of a mupdate, and we'll consider an update done if
        // either slot matches the current target release.
        let mut found_current_version_in_either_slot = false;
        for phase2 in
            [&sled_config.host_phase_2.slot_a, &sled_config.host_phase_2.slot_b]
        {
            match phase2 {
                BlueprintHostPhase2DesiredContents::CurrentContents => {
                    // Weak evidence of a mupdate; err on the side of "allow".
                    // This is weak evidence because we could have already
                    // completed one update and it's the other slot that's still
                    // set to `CurrentContents`.
                    return true;
                }
                BlueprintHostPhase2DesiredContents::Artifact {
                    version,
                    ..
                } => match version {
                    BlueprintArtifactVersion::Available { version } => {
                        if version.as_str() == current_target_version {
                            found_current_version_in_either_slot = true;
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
            all_components_on_current_target_release = false;
        }
    }

    // Now check zone configs.
    for (_, zone_config) in current_blueprint
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
    {
        match &zone_config.image_source {
            BlueprintZoneImageSource::InstallDataset => {
                // A mupdate has occurred; we must allow a new target release.
                return true;
            }
            BlueprintZoneImageSource::Artifact { version, .. } => {
                match version {
                    BlueprintArtifactVersion::Available { version } => {
                        if version.as_str() != current_target_version {
                            // We found a zone not yet on the current target
                            // version; the previous upgrade is not yet
                            // complete.
                            all_components_on_current_target_release = false;
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
                        all_components_on_current_target_release = false;
                    }
                }
            }
        }
    }

    // We don't attempt to check Hubris components:
    //
    // * They don't have the same API versioning restrictions that require
    //   strict single-stepped upgrades.
    // * We don't keep the desired state of all Hubris components in the
    //   blueprint anyway.

    // If we made it here, we found no evidence of a mupdate; only allow a new
    // target release if all components have been upgraded to the current target
    // release.
    all_components_on_current_target_release
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_reconfigurator_planning::example::example;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use nexus_types::external_api::views::SledState;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::MupdateOverrideUuid;
    use std::mem;
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

    #[test]
    fn test_is_target_release_change_allowed() {
        static TEST_NAME: &str = "is_target_release_change_allowed";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // Fake versions for current target release and previous target release.
        let previous_target_version: semver::Version = "1.0.0".parse().unwrap();
        let current_target_version: semver::Version = "2.0.0".parse().unwrap();

        // Build a base blueprint where all the zone image sources and OS images
        // reference artifacts from the current target version.
        let base_blueprint = {
            let (_, _, mut bp) = example(log, TEST_NAME);
            for sled_config in bp.sleds.values_mut() {
                sled_config.remove_mupdate_override = None;
                sled_config.host_phase_2 = BlueprintHostPhase2DesiredSlots {
                    slot_a: make_os_artifact(&current_target_version),
                    slot_b: make_os_artifact(&current_target_version),
                };
                for mut zone_config in sled_config.zones.iter_mut() {
                    zone_config.image_source =
                        make_zone_artifact(&current_target_version);
                }
            }
            bp
        };

        // All components in this blueprint reference current_target_release, so
        // we should be able to set a new target release.
        assert!(is_target_release_change_allowed(
            &base_blueprint,
            &current_target_version
        ));

        {
            // Build a blueprint for which target release changes are rejected
            // due to a sled with no current OS.
            let bp_old_os = {
                let mut bp = base_blueprint.clone();
                let sled_config = bp.sleds.values_mut().next().unwrap();
                sled_config.host_phase_2 = BlueprintHostPhase2DesiredSlots {
                    slot_a: make_os_artifact(&previous_target_version),
                    slot_b: make_os_artifact(&previous_target_version),
                };
                assert!(!is_target_release_change_allowed(
                    &bp,
                    &current_target_version
                ));
                bp
            };

            // Change the sled to be decommissioned; we should ignore it now and
            // allow changing the target release.
            let mut bp = bp_old_os.clone();
            let sled_config = bp.sleds.values_mut().next().unwrap();
            sled_config.state = SledState::Decommissioned;
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));

            // Change one of the OS slots to `CurrentContents`; now changing the
            // target release should be allowed, because this sled might have
            // been mupdated.
            let mut bp = bp_old_os.clone();
            let sled_config = bp.sleds.values_mut().next().unwrap();
            sled_config.host_phase_2 = BlueprintHostPhase2DesiredSlots {
                slot_a: make_os_artifact(&previous_target_version),
                slot_b: BlueprintHostPhase2DesiredContents::CurrentContents,
            };
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));

            // Change a different sled - set a mupdate override.
            let mut bp = bp_old_os.clone();
            let next_sled_config = bp.sleds.values_mut().nth(1).unwrap();
            next_sled_config.remove_mupdate_override =
                Some(MupdateOverrideUuid::new_v4());
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));

            // Change a zone on a different sled to be sourced from the
            // install dataset.
            let mut bp = bp_old_os.clone();
            let next_sled_config = bp.sleds.values_mut().nth(1).unwrap();
            let mut zone_config =
                next_sled_config.zones.iter_mut().next().unwrap();
            zone_config.image_source = BlueprintZoneImageSource::InstallDataset;
            mem::drop(zone_config);
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));
        }

        {
            // Repeat similar tests to the above section, but start by building
            // a blueprint for which target release changes are rejected due to
            // a sled with a zone running an artifact from the previous release.
            let bp_old_zone = {
                let mut bp = base_blueprint.clone();
                let sled_config = bp.sleds.values_mut().next().unwrap();
                let mut zone_config =
                    sled_config.zones.iter_mut().next().unwrap();
                zone_config.image_source =
                    make_zone_artifact(&previous_target_version);
                mem::drop(zone_config);
                assert!(!is_target_release_change_allowed(
                    &bp,
                    &current_target_version
                ));
                bp
            };

            // Expunge the zone; we should ignore it now and allow changing the
            // target release.
            let mut bp = bp_old_zone.clone();
            let sled_config = bp.sleds.values_mut().next().unwrap();
            let mut zone_config = sled_config.zones.iter_mut().next().unwrap();
            zone_config.disposition = BlueprintZoneDisposition::Expunged {
                as_of_generation: Generation::new(),
                ready_for_cleanup: false,
            };
            mem::drop(zone_config);
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));

            // Change one of the OS slots to `CurrentContents`; now changing the
            // target release should be allowed, because this sled might have
            // been mupdated.
            let mut bp = bp_old_zone.clone();
            let sled_config = bp.sleds.values_mut().next().unwrap();
            sled_config.host_phase_2 = BlueprintHostPhase2DesiredSlots {
                slot_a: make_os_artifact(&previous_target_version),
                slot_b: BlueprintHostPhase2DesiredContents::CurrentContents,
            };
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));

            // Change a different sled - set a mupdate override.
            let mut bp = bp_old_zone.clone();
            let next_sled_config = bp.sleds.values_mut().nth(1).unwrap();
            next_sled_config.remove_mupdate_override =
                Some(MupdateOverrideUuid::new_v4());
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));

            // Change a zone on a different sled to be sourced from the
            // install dataset.
            let mut bp = bp_old_zone.clone();
            let next_sled_config = bp.sleds.values_mut().nth(1).unwrap();
            let mut zone_config =
                next_sled_config.zones.iter_mut().next().unwrap();
            zone_config.image_source = BlueprintZoneImageSource::InstallDataset;
            mem::drop(zone_config);
            assert!(is_target_release_change_allowed(
                &bp,
                &current_target_version
            ));
        }
    }
}
