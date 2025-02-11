// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods to get/set the current [`TargetRelease`].

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use crate::db::model::{TargetRelease, TargetReleaseSource};
use crate::db::schema::target_release::dsl;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl as _;
use diesel::insert_into;
use diesel::prelude::*;
use omicron_common::api::external::{CreateResult, LookupResult};

impl DataStore {
    /// Fetch the current target release, i.e., the row with the largest
    /// generation number.
    pub async fn get_target_release(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<TargetRelease> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        // Fetch the row in the `target_release` table with the largest
        // generation number. The subquery accesses the same table, so we
        // have to make an alias to not confuse diesel.
        let target_release2 = diesel::alias!(
            crate::db::schema::target_release as target_release_2
        );
        dsl::target_release
            .filter(dsl::generation.nullable().eq_any(target_release2.select(
                diesel::dsl::max(target_release2.field(dsl::generation)),
            )))
            .select(TargetRelease::as_select())
            .first_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a new target release row and return it. It will only become
    /// the current target release if its generation is larger than any
    /// existing row.
    pub async fn set_target_release(
        &self,
        opctx: &OpContext,
        target_release: TargetRelease,
    ) -> CreateResult<TargetRelease> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("set_target_release")
            .transaction(&conn, |conn| {
                let target_release = target_release.clone();
                let err = err.clone();
                async move {
                    // Ensure that we have a TUF repo representing a system version.
                    if let TargetReleaseSource::SystemVersion =
                        target_release.release_source
                    {
                        if let Some(system_version) =
                            target_release.system_version.clone()
                        {
                            assert_eq!(
                                system_version.clone(),
                                self.update_tuf_repo_get(opctx, system_version)
                                    .await
                                    .map_err(|e| err.bail(e))?
                                    .repo
                                    .system_version,
                                "inconsistent system version"
                            );
                        }
                    }

                    // Insert the target_release row.
                    insert_into(dsl::target_release)
                        .values(target_release)
                        .returning(TargetRelease::as_returning())
                        .get_result_async(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::{
        ArtifactHash, Generation, SemverVersion, TargetReleaseSource,
        TufArtifact, TufRepo, TufRepoDescription,
    };
    use crate::db::pub_test_utils::TestDatabase;
    use chrono::{TimeDelta, Utc};
    use omicron_common::update::{ArtifactId, ArtifactKind};
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn target_release_datastore() {
        let logctx = dev::test_setup_log("target_release_datastore");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should always be a target release.
        // This is ensured by the schema migration.
        let target_release = datastore
            .get_target_release(opctx)
            .await
            .expect("should be a target release");
        assert_eq!(target_release.generation, Generation(0.into()));
        assert!(target_release.time_requested < Utc::now());
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::InstallDataset
        );
        assert!(target_release.system_version.is_none());

        // We should be able to set a new generation just like the first,
        // with some (very small) fuzz allowed in the timestamp reported
        // by the database.
        let initial_target_release = TargetRelease::new_from_prev(
            target_release,
            TargetReleaseSource::InstallDataset,
            None,
        );
        let target_release = datastore
            .set_target_release(opctx, initial_target_release.clone())
            .await
            .unwrap();
        assert_eq!(
            target_release.generation,
            initial_target_release.generation
        );
        assert!(
            (target_release.time_requested
                - initial_target_release.time_requested)
                .abs()
                < TimeDelta::new(0, 1_000).expect("1 μsec")
        );
        assert!(target_release.system_version.is_none());

        // Trying to reuse a generation should fail.
        assert!(datastore
            .set_target_release(
                opctx,
                TargetRelease::new(
                    target_release.generation,
                    TargetReleaseSource::InstallDataset,
                    None,
                )
            )
            .await
            .is_err());

        // But the next generation should be fine, even with the same source.
        let target_release = datastore
            .set_target_release(
                opctx,
                TargetRelease::new_from_prev(
                    target_release,
                    TargetReleaseSource::InstallDataset,
                    None,
                ),
            )
            .await
            .unwrap();

        // If we specify a system version, it had better exist!
        let _ = datastore
            .set_target_release(
                opctx,
                TargetRelease::new_from_prev(
                    target_release.clone(),
                    TargetReleaseSource::SystemVersion,
                    Some(SemverVersion::new(0, 0, 0)),
                ),
            )
            .await
            .expect_err("unknown system version");

        // Finally, use a new generation number and a valid system version.
        // We assume the queries above will have taken some non-trivial
        // amount of time.
        let version = SemverVersion::new(0, 0, 1);
        let hash = ArtifactHash(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .expect("SHA256('')"),
        );
        let system_version = datastore
            .update_tuf_repo_insert(
                opctx,
                TufRepoDescription {
                    repo: TufRepo::new(
                        hash,
                        0,
                        Utc::now(),
                        version.clone(),
                        String::from(""),
                    ),
                    artifacts: vec![TufArtifact::new(
                        ArtifactId {
                            name: String::from(""),
                            version: version.clone().into(),
                            kind: ArtifactKind::from_static("empty"),
                        },
                        hash,
                        0,
                    )],
                },
            )
            .await
            .unwrap()
            .recorded
            .repo
            .system_version;
        assert_eq!(version, system_version);
        let target_release = datastore
            .set_target_release(
                opctx,
                TargetRelease::new_from_prev(
                    target_release,
                    TargetReleaseSource::SystemVersion,
                    Some(version.clone()),
                ),
            )
            .await
            .unwrap();
        assert_eq!(target_release.generation, Generation(3.into()));
        assert!(
            (target_release.time_requested
                - initial_target_release.time_requested)
                > TimeDelta::new(0, 1_000_000).expect("1 msec")
        );
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::SystemVersion
        );
        assert_eq!(target_release.system_version, Some(version));

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
