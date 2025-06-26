// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods to get/set the current [`TargetRelease`].

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::{
    Generation, SemverVersion, TargetRelease, TargetReleaseSource,
};
use async_bb8_diesel::AsyncRunQueryDsl as _;
use diesel::insert_into;
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_schema::schema::target_release::dsl;
use nexus_types::external_api::views;
use omicron_common::api::external::{CreateResult, Error, LookupResult};

impl DataStore {
    /// Fetch the current target release, i.e., the row with the largest
    /// generation number.
    pub async fn target_release_get_current(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<TargetRelease> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let current = dsl::target_release
            .select(TargetRelease::as_select())
            .order_by(dsl::generation.desc())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // We expect there to always be a current target release,
        // since the database migration `create-target-release/up3.sql`
        // adds an initial row.
        let current = current
            .ok_or_else(|| Error::internal_error("no target release"))?;

        Ok(current)
    }

    /// Fetch a target release by generation number.
    pub async fn target_release_get_generation(
        &self,
        opctx: &OpContext,
        generation: Generation,
    ) -> LookupResult<Option<TargetRelease>> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        dsl::target_release
            .select(TargetRelease::as_select())
            .filter(dsl::generation.eq(generation))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a new target release row and return it. It will only become
    /// the current target release if its generation is larger than any
    /// existing row.
    pub async fn target_release_insert(
        &self,
        opctx: &OpContext,
        target_release: TargetRelease,
    ) -> CreateResult<TargetRelease> {
        opctx
            .authorize(authz::Action::Modify, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        insert_into(dsl::target_release)
            .values(target_release)
            .returning(TargetRelease::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Convert a model-level target release to an external view.
    /// This method lives here because we have to look up the version
    /// corresponding to the TUF repo.
    pub async fn target_release_view(
        &self,
        opctx: &OpContext,
        target_release: &TargetRelease,
    ) -> LookupResult<views::TargetRelease> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let release_source = match target_release.release_source {
            TargetReleaseSource::Unspecified => {
                views::TargetReleaseSource::Unspecified
            }
            TargetReleaseSource::SystemVersion => {
                use nexus_db_schema::schema::tuf_repo;
                if let Some(tuf_repo_id) = target_release.tuf_repo_id {
                    views::TargetReleaseSource::SystemVersion {
                        version: tuf_repo::table
                            .select(tuf_repo::system_version)
                            .filter(tuf_repo::id.eq(tuf_repo_id))
                            .first_async::<SemverVersion>(&*conn)
                            .await
                            .map_err(|e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            })?
                            .into(),
                    }
                } else {
                    return Err(Error::internal_error(
                        "missing TUF repo ID for specified system version",
                    ));
                }
            }
        };
        // We choose to fetch the blueprint directly from the database rather
        // than relying on the cached blueprint in Nexus because our APIs try to
        // be strongly consistent. This shows up/will show up as a warning in
        // the UI, and we don't want the warning to flicker in and out of
        // existence based on which Nexus is getting hit.
        let min_gen = self.blueprint_target_get_current_min_gen(opctx).await?;
        // The semantics of min_gen mean we use a > sign here, not >=.
        let mupdate_override = min_gen > target_release.generation.0;
        Ok(target_release.into_external(release_source, mupdate_override))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::{Generation, TargetReleaseSource};
    use crate::db::pub_test_utils::TestDatabase;
    use chrono::{TimeDelta, Utc};
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::example::{
        ExampleSystemBuilder, SimRngState,
    };
    use nexus_types::deployment::BlueprintTarget;
    use omicron_common::api::external::{
        TufArtifactMeta, TufRepoDescription, TufRepoMeta,
    };
    use omicron_common::update::ArtifactId;
    use omicron_test_utils::dev;
    use semver::Version;
    use tufaceous_artifact::{ArtifactKind, ArtifactVersion};

    #[tokio::test]
    async fn target_release_datastore() {
        const TEST_NAME: &str = "target_release_datastore";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should always be a target release.
        // This is ensured by the schema migration.
        let initial_target_release = datastore
            .target_release_get_current(opctx)
            .await
            .expect("should be a target release");
        assert_eq!(initial_target_release.generation, Generation(1.into()));
        assert!(initial_target_release.time_requested < Utc::now());
        assert_eq!(
            initial_target_release.release_source,
            TargetReleaseSource::Unspecified
        );
        assert!(initial_target_release.tuf_repo_id.is_none());

        // Set up an initial blueprint and make it the target. This models real
        // systems which always have a target blueprint.
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (system, mut blueprint) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        assert_eq!(
            blueprint.target_release_minimum_generation,
            1.into(),
            "initial blueprint should have minimum generation of 1",
        );
        // Treat this blueprint as the initial one for the system.
        blueprint.parent_blueprint_id = None;

        datastore
            .blueprint_insert(&opctx, &blueprint)
            .await
            .expect("inserted blueprint");
        datastore
            .blueprint_target_set_current(
                opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    // enabled = true or false shouldn't matter for this.
                    enabled: true,
                    time_made_target: now_db_precision(),
                },
            )
            .await
            .expect("set blueprint target");

        // We should always be able to get a view of the target release.
        let initial_target_release_view = datastore
            .target_release_view(opctx, &initial_target_release)
            .await
            .expect("got target release");
        eprintln!(
            "initial target release view: {:#?}",
            initial_target_release_view
        );

        // This target release should not have the mupdate override set, because
        // the generation is <= the minimum generation in the target blueprint.
        assert_eq!(
            initial_target_release_view.mupdate_override, None,
            "mupdate_override should be None for initial target release"
        );

        // We should be able to set a new generation just like the first.
        // We allow some slack in the timestamp comparison because the
        // database only stores timestamps with μsec precision.
        let target_release =
            TargetRelease::new_unspecified(&initial_target_release);
        let target_release = datastore
            .target_release_insert(opctx, target_release)
            .await
            .unwrap();
        assert_eq!(target_release.generation, Generation(2.into()));
        assert!(
            (target_release.time_requested - target_release.time_requested)
                .abs()
                < TimeDelta::new(0, 1_000).expect("1 μsec")
        );
        assert!(target_release.tuf_repo_id.is_none());

        // Trying to reuse a generation should fail.
        assert!(
            datastore
                .target_release_insert(
                    opctx,
                    TargetRelease::new_unspecified(&initial_target_release),
                )
                .await
                .is_err()
        );

        // But inserting a new unspecified target should be fine.
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_unspecified(&target_release),
            )
            .await
            .unwrap();
        assert_eq!(target_release.generation, Generation(3.into()));

        // Now add a new TUF repo and use it as the source.
        let version = Version::new(0, 0, 1);
        const ARTIFACT_VERSION: ArtifactVersion =
            ArtifactVersion::new_const("0.0.1");
        let hash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .expect("SHA256('')");
        let repo = datastore
            .tuf_repo_insert(
                opctx,
                &TufRepoDescription {
                    repo: TufRepoMeta {
                        hash,
                        targets_role_version: 0,
                        valid_until: Utc::now(),
                        system_version: version.clone(),
                        file_name: String::new(),
                    },
                    artifacts: vec![TufArtifactMeta {
                        id: ArtifactId {
                            name: String::new(),
                            version: ARTIFACT_VERSION,
                            kind: ArtifactKind::from_static("empty"),
                        },
                        hash,
                        size: 0,
                    }],
                },
            )
            .await
            .unwrap()
            .recorded
            .repo;
        assert_eq!(repo.system_version, version.into());
        let tuf_repo_id = repo.id;

        let before = Utc::now();
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(&target_release, tuf_repo_id),
            )
            .await
            .unwrap();
        let after = Utc::now();
        assert_eq!(target_release.generation, Generation(4.into()));
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::SystemVersion
        );
        assert_eq!(target_release.tuf_repo_id, Some(tuf_repo_id));

        // Generate a new blueprint with a greater target release generation.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &system.input,
            &system.collection,
            TEST_NAME,
        )
        .expect("created blueprint builder");
        builder.set_rng(rng.next_planner_rng());
        builder
            .set_target_release_minimum_generation(
                blueprint.target_release_minimum_generation,
                5.into(),
            )
            .expect("set target release minimum generation");
        let bp2 = builder.build();

        datastore
            .blueprint_insert(&opctx, &bp2)
            .await
            .expect("inserted blueprint");
        datastore
            .blueprint_target_set_current(
                opctx,
                BlueprintTarget {
                    target_id: bp2.id,
                    // enabled = true or false shouldn't matter for this.
                    enabled: true,
                    time_made_target: now_db_precision(),
                },
            )
            .await
            .expect("set blueprint target");

        // Fetch the target release again.
        let target_release = datastore
            .target_release_get_current(opctx)
            .await
            .expect("got target release");
        let target_release_view_2 = datastore
            .target_release_view(opctx, &target_release)
            .await
            .expect("got target release");

        eprintln!("target release view 2: {target_release_view_2:#?}");

        assert!(
            target_release_view_2.mupdate_override,
            "mupdate override is set",
        );

        // Now set the target release again -- this should cause the mupdate
        // override to disappear.
        let before = Utc::now();
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(&target_release, tuf_repo_id),
            )
            .await
            .unwrap();
        let after = Utc::now();

        assert_eq!(target_release.generation, Generation(5.into()));
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);

        let target_release_view_3 = datastore
            .target_release_view(opctx, &target_release)
            .await
            .expect("got target release");

        eprintln!("target release view 3: {target_release_view_3:#?}");

        assert!(
            !target_release_view_3.mupdate_override,
            "mupdate override is not set",
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
