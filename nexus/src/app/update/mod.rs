// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use chrono::Utc;
use hex;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::KnownArtifactKind;
use nexus_types::external_api::{params, shared};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, Error, ListResultVec, LookupResult,
    PaginationOrder, UpdateResult,
};
use omicron_common::api::internal::nexus::UpdateArtifactId;
use rand::Rng;
use ring::digest;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

static BASE_ARTIFACT_DIR: &str = "/var/tmp/oxide_artifacts";

impl super::Nexus {
    async fn tuf_base_url(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<String>, Error> {
        let rack = self.rack_lookup(opctx, &self.rack_id).await?;

        Ok(self.updates_config.as_ref().map(|c| {
            rack.tuf_base_url.unwrap_or_else(|| c.default_base_url.clone())
        }))
    }

    pub(crate) async fn updates_refresh_metadata(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let updates_config = self.updates_config.as_ref().ok_or_else(|| {
            Error::InvalidRequest {
                message: "updates system not configured".into(),
            }
        })?;
        let base_url = self.tuf_base_url(opctx).await?.ok_or_else(|| {
            Error::InvalidRequest {
                message: "updates system not configured".into(),
            }
        })?;
        let trusted_root = tokio::fs::read(&updates_config.trusted_root)
            .await
            .map_err(|e| Error::InternalError {
                internal_message: format!(
                    "error trying to read trusted root: {}",
                    e
                ),
            })?;

        let artifacts = tokio::task::spawn_blocking(move || {
            crate::updates::read_artifacts(&trusted_root, base_url)
        })
        .await
        .unwrap()
        .map_err(|e| Error::InternalError {
            internal_message: format!("error trying to refresh updates: {}", e),
        })?;

        // FIXME: if we hit an error in any of these database calls, the
        // available artifact table will be out of sync with the current
        // artifacts.json. can we do a transaction or something?

        let mut current_version = None;
        for artifact in &artifacts {
            current_version = Some(artifact.targets_role_version);
            self.db_datastore
                .update_artifact_upsert(&opctx, artifact.clone())
                .await?;
        }

        // ensure table is in sync with current copy of artifacts.json
        if let Some(current_version) = current_version {
            self.db_datastore
                .update_artifact_hard_delete_outdated(&opctx, current_version)
                .await?;
        }

        // demo-grade update logic: tell all sleds to apply all artifacts
        for sled in self
            .db_datastore
            .sled_list(
                &opctx,
                &DataPageParams {
                    marker: None,
                    direction: PaginationOrder::Ascending,
                    limit: NonZeroU32::new(100).unwrap(),
                },
            )
            .await?
        {
            let client = self.sled_client(&sled.id()).await?;
            for artifact in &artifacts {
                info!(
                    self.log,
                    "telling sled {} to apply {}",
                    sled.id(),
                    artifact.target_name
                );
                client
                    .update_artifact(
                        &sled_agent_client::types::UpdateArtifactId {
                            name: artifact.name.clone(),
                            version: artifact.version.0.clone().into(),
                            kind: artifact.kind.0.into(),
                        },
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Downloads a file from within [`BASE_ARTIFACT_DIR`].
    pub(crate) async fn download_artifact(
        &self,
        opctx: &OpContext,
        artifact: UpdateArtifactId,
    ) -> Result<Vec<u8>, Error> {
        let mut base_url =
            self.tuf_base_url(opctx).await?.ok_or_else(|| {
                Error::InvalidRequest {
                    message: "updates system not configured".into(),
                }
            })?;
        if !base_url.ends_with('/') {
            base_url.push('/');
        }

        // We cache the artifact based on its checksum, so fetch that from the
        // database.
        let (.., artifact_entry) = LookupPath::new(opctx, &self.db_datastore)
            .update_artifact_tuple(
                &artifact.name,
                db::model::SemverVersion(artifact.version.clone()),
                KnownArtifactKind(artifact.kind),
            )
            .fetch()
            .await?;
        let filename = format!(
            "{}.{}.{}-{}",
            artifact_entry.target_sha256,
            artifact.kind,
            artifact.name,
            artifact.version
        );
        let path = Path::new(BASE_ARTIFACT_DIR).join(&filename);

        if !path.exists() {
            // If the artifact doesn't exist, we should download it.
            //
            // TODO: There also exists the question of "when should we *remove*
            // things from BASE_ARTIFACT_DIR", which we should also resolve.
            // Demo-quality solution could be "destroy it on boot" or something?
            // (we aren't doing that yet).
            info!(self.log, "Accessing {} - needs to be downloaded", filename);
            tokio::fs::create_dir_all(BASE_ARTIFACT_DIR).await.map_err(
                |e| {
                    Error::internal_error(&format!(
                        "Failed to create artifacts directory: {}",
                        e
                    ))
                },
            )?;

            let mut response = reqwest::get(format!(
                "{}targets/{}.{}",
                base_url,
                artifact_entry.target_sha256,
                artifact_entry.target_name
            ))
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to fetch artifact: {}",
                    e
                ))
            })?;

            // To ensure another request isn't trying to use this target while we're downloading it
            // or before we've verified it, write to a random path in the same directory, then move
            // it to the correct path after verification.
            let temp_path = path.with_file_name(format!(
                ".{}.{:x}",
                filename,
                rand::thread_rng().gen::<u64>()
            ));
            let mut file =
                tokio::fs::File::create(&temp_path).await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to create file: {}",
                        e
                    ))
                })?;

            let mut context = digest::Context::new(&digest::SHA256);
            let mut length: i64 = 0;
            while let Some(chunk) = response.chunk().await.map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to read HTTP body: {}",
                    e
                ))
            })? {
                file.write_all(&chunk).await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to write to file: {}",
                        e
                    ))
                })?;
                context.update(&chunk);
                length += i64::try_from(chunk.len()).unwrap();

                if length > artifact_entry.target_length {
                    return Err(Error::internal_error(&format!(
                        "target {} is larger than expected",
                        artifact_entry.target_name
                    )));
                }
            }
            drop(file);

            if hex::encode(context.finish()) == artifact_entry.target_sha256
                && length == artifact_entry.target_length
            {
                tokio::fs::rename(temp_path, &path).await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to rename file after verification: {}",
                        e
                    ))
                })?
            } else {
                return Err(Error::internal_error(&format!(
                    "failed to verify target {}",
                    artifact_entry.target_name
                )));
            }

            info!(
                self.log,
                "wrote {} to artifact dir", artifact_entry.target_name
            );
        } else {
            info!(self.log, "Accessing {} - already exists", path.display());
        }

        // TODO: These artifacts could be quite large - we should figure out how to
        // stream this file back instead of holding it entirely in-memory in a
        // Vec<u8>.
        //
        // Options:
        // - RFC 7233 - "Range Requests" (is this HTTP/1.1 only?)
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
        // - "Roll our own". See:
        // https://stackoverflow.com/questions/20969331/standard-method-for-http-partial-upload-resume-upload
        let body = tokio::fs::read(&path).await.map_err(|e| {
            Error::internal_error(&format!(
                "Cannot read artifact from filesystem: {}",
                e
            ))
        })?;
        Ok(body)
    }

    pub async fn upsert_system_update(
        &self,
        opctx: &OpContext,
        create_update: params::SystemUpdateCreate,
    ) -> CreateResult<db::model::SystemUpdate> {
        let update = db::model::SystemUpdate::new(create_update.version)?;
        self.db_datastore.upsert_system_update(opctx, update).await
    }

    pub async fn create_component_update(
        &self,
        opctx: &OpContext,
        create_update: params::ComponentUpdateCreate,
    ) -> CreateResult<db::model::ComponentUpdate> {
        let now = Utc::now();
        let update = db::model::ComponentUpdate {
            identity: db::model::ComponentUpdateIdentity {
                id: Uuid::new_v4(),
                time_created: now,
                time_modified: now,
            },
            version: db::model::SemverVersion(create_update.version),
            component_type: create_update.component_type.into(),
        };

        self.db_datastore
            .create_component_update(
                opctx,
                create_update.system_update_id,
                update,
            )
            .await
    }

    pub(crate) async fn system_update_fetch_by_version(
        &self,
        opctx: &OpContext,
        version: &external::SemverVersion,
    ) -> LookupResult<db::model::SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        self.db_datastore
            .system_update_fetch_by_version(opctx, version.clone().into())
            .await
    }

    pub(crate) async fn system_updates_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.system_updates_list_by_id(opctx, pagparams).await
    }

    pub(crate) async fn system_update_list_components(
        &self,
        opctx: &OpContext,
        version: &external::SemverVersion,
    ) -> ListResultVec<db::model::ComponentUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let system_update = self
            .db_datastore
            .system_update_fetch_by_version(opctx, version.clone().into())
            .await?;

        self.db_datastore
            .system_update_components_list(opctx, system_update.id())
            .await
    }

    pub async fn create_updateable_component(
        &self,
        opctx: &OpContext,
        create_component: params::UpdateableComponentCreate,
    ) -> CreateResult<db::model::UpdateableComponent> {
        let component =
            db::model::UpdateableComponent::try_from(create_component)?;
        self.db_datastore.create_updateable_component(opctx, component).await
    }

    pub(crate) async fn updateable_components_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::UpdateableComponent> {
        self.db_datastore
            .updateable_components_list_by_id(opctx, pagparams)
            .await
    }

    pub(crate) async fn create_update_deployment(
        &self,
        opctx: &OpContext,
        start: params::SystemUpdateStart,
    ) -> CreateResult<db::model::UpdateDeployment> {
        // 404 if specified version doesn't exist
        // TODO: is 404 the right error for starting an update with a nonexistent version?
        self.system_update_fetch_by_version(opctx, &start.version).await?;

        // We only need to look at the latest deployment because it's the only
        // one that could be running

        let latest_deployment = self.latest_update_deployment(opctx).await;
        if let Ok(dep) = latest_deployment {
            if dep.status == db::model::UpdateStatus::Updating {
                // TODO: should "already updating" conflict be a new kind of error?
                return Err(Error::ObjectAlreadyExists {
                    type_name: external::ResourceType::UpdateDeployment,
                    object_name: dep.id().to_string(),
                });
            }
        }

        let deployment = db::model::UpdateDeployment {
            identity: db::model::UpdateDeploymentIdentity::new(Uuid::new_v4()),
            version: db::model::SemverVersion(start.version),
            status: db::model::UpdateStatus::Updating,
        };
        self.db_datastore.create_update_deployment(opctx, deployment).await
    }

    /// If there's a running update, change it to steady. Otherwise do nothing.
    // TODO: codify the state machine around update deployments
    pub(crate) async fn steady_update_deployment(
        &self,
        opctx: &OpContext,
    ) -> UpdateResult<db::model::UpdateDeployment> {
        let latest = self.latest_update_deployment(opctx).await?;
        // already steady. do nothing in order to avoid updating `time_modified`
        if latest.status == db::model::UpdateStatus::Steady {
            return Ok(latest);
        }

        self.db_datastore.steady_update_deployment(opctx, latest.id()).await
    }

    pub(crate) async fn update_deployments_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::UpdateDeployment> {
        self.db_datastore.update_deployments_list_by_id(opctx, pagparams).await
    }

    pub(crate) async fn update_deployment_fetch_by_id(
        &self,
        opctx: &OpContext,
        deployment_id: &Uuid,
    ) -> LookupResult<db::model::UpdateDeployment> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let (.., db_deployment) = LookupPath::new(opctx, &self.db_datastore)
            .update_deployment_id(*deployment_id)
            .fetch()
            .await?;
        Ok(db_deployment)
    }

    pub(crate) async fn latest_update_deployment(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::UpdateDeployment> {
        self.db_datastore.latest_update_deployment(opctx).await
    }

    pub(crate) async fn lowest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::SemverVersion> {
        self.db_datastore.lowest_component_system_version(opctx).await
    }

    pub(crate) async fn highest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::SemverVersion> {
        self.db_datastore.highest_component_system_version(opctx).await
    }

    /// Inner function makes it easier to implement the logic where we ignore
    /// ObjectAlreadyExists errors but let the others pass through
    async fn populate_mock_system_updates_inner(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<()> {
        let types = vec![
            shared::UpdateableComponentType::HubrisForPscRot,
            shared::UpdateableComponentType::HubrisForPscSp,
            shared::UpdateableComponentType::HubrisForSidecarRot,
            shared::UpdateableComponentType::HubrisForSidecarSp,
            shared::UpdateableComponentType::HubrisForGimletRot,
            shared::UpdateableComponentType::HubrisForGimletSp,
            shared::UpdateableComponentType::HeliosHostPhase1,
            shared::UpdateableComponentType::HeliosHostPhase2,
            shared::UpdateableComponentType::HostOmicron,
        ];

        // create system updates and associated component updates
        for v in [1, 2, 3] {
            let version = external::SemverVersion::new(v, 0, 0);
            let su = self
                .upsert_system_update(
                    opctx,
                    params::SystemUpdateCreate { version: version.clone() },
                )
                .await?;

            for component_type in types.clone() {
                self.create_component_update(
                    &opctx,
                    params::ComponentUpdateCreate {
                        version: external::SemverVersion::new(1, v, 0),
                        system_update_id: su.identity.id,
                        component_type,
                    },
                )
                .await?;
            }
        }

        // create deployment for v1.0.0, stop it, then create one for v2.0.0.
        // This makes plausible the state of the components: all v1 except for one v2
        self.create_update_deployment(
            &opctx,
            params::SystemUpdateStart {
                version: external::SemverVersion::new(1, 0, 0),
            },
        )
        .await?;
        self.steady_update_deployment(opctx).await?;

        self.create_update_deployment(
            &opctx,
            params::SystemUpdateStart {
                version: external::SemverVersion::new(2, 0, 0),
            },
        )
        .await?;

        // now create components, with one component on a different system
        // version from the others

        for (i, component_type) in types.iter().enumerate() {
            let version = if i == 0 {
                external::SemverVersion::new(1, 2, 0)
            } else {
                external::SemverVersion::new(1, 1, 0)
            };

            let system_version = if i == 0 {
                external::SemverVersion::new(2, 0, 0)
            } else {
                external::SemverVersion::new(1, 0, 0)
            };

            self.create_updateable_component(
                opctx,
                params::UpdateableComponentCreate {
                    version,
                    system_version,
                    device_id: "a-device".to_string(),
                    component_type: component_type.clone(),
                },
            )
            .await?;
        }

        Ok(())
    }

    /// Populate the DB with update-related data. Data is hard-coded until we
    /// figure out how to pull it from the TUF repo.
    ///
    /// We need this to be idempotent because it can be called arbitrarily many
    /// times. The service functions we call to create these resources will
    /// error on ID or version conflicts, so to remain idempotent we can simply
    /// ignore those errors. We let other errors through.
    pub(crate) async fn populate_mock_system_updates(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<()> {
        self.populate_mock_system_updates_inner(opctx).await.or_else(|error| {
            match error {
                // ignore ObjectAlreadyExists but pass through other errors
                external::Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(error),
            }
        })
    }
}

// TODO: convert system update tests to integration tests now that I know how to
// call nexus functions in those

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use std::num::NonZeroU32;

    use dropshot::PaginationOrder;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::model::UpdateStatus;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::{
        params::{
            ComponentUpdateCreate, SystemUpdateCreate, SystemUpdateStart,
            UpdateableComponentCreate,
        },
        shared::UpdateableComponentType,
    };
    use omicron_common::api::external::{self, DataPageParams};
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx.nexus.datastore().clone(),
        )
    }

    pub fn test_pagparams() -> DataPageParams<'static, Uuid> {
        DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_system_updates(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        // starts out with 3 populated
        let system_updates = nexus
            .system_updates_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(system_updates.len(), 3);

        let su1_create = SystemUpdateCreate {
            version: external::SemverVersion::new(5, 0, 0),
        };
        let su1 = nexus.upsert_system_update(&opctx, su1_create).await.unwrap();

        // weird order is deliberate
        let su3_create = SystemUpdateCreate {
            version: external::SemverVersion::new(10, 0, 0),
        };
        nexus.upsert_system_update(&opctx, su3_create).await.unwrap();

        let su2_create = SystemUpdateCreate {
            version: external::SemverVersion::new(0, 7, 0),
        };
        let su2 = nexus.upsert_system_update(&opctx, su2_create).await.unwrap();

        // now there should be a bunch of system updates, sorted by version descending
        let versions: Vec<String> = nexus
            .system_updates_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap()
            .iter()
            .map(|su| su.version.to_string())
            .collect();

        assert_eq!(versions.len(), 6);
        assert_eq!(versions[0], "10.0.0".to_string());
        assert_eq!(versions[1], "5.0.0".to_string());
        assert_eq!(versions[2], "3.0.0".to_string());
        assert_eq!(versions[3], "2.0.0".to_string());
        assert_eq!(versions[4], "1.0.0".to_string());
        assert_eq!(versions[5], "0.7.0".to_string());

        // let's also make sure we can fetch by version
        let su1_fetched = nexus
            .system_update_fetch_by_version(&opctx, &su1.version)
            .await
            .unwrap();
        assert_eq!(su1.identity.id, su1_fetched.identity.id);

        // now create two component updates for update 1, one at root, and one
        // hanging off the first
        nexus
            .create_component_update(
                &opctx,
                ComponentUpdateCreate {
                    version: external::SemverVersion::new(1, 0, 0),
                    component_type: UpdateableComponentType::BootloaderForRot,
                    system_update_id: su1.identity.id,
                },
            )
            .await
            .expect("Failed to create component update");
        nexus
            .create_component_update(
                &opctx,
                ComponentUpdateCreate {
                    version: external::SemverVersion::new(2, 0, 0),
                    component_type: UpdateableComponentType::HubrisForGimletSp,
                    system_update_id: su1.identity.id,
                },
            )
            .await
            .expect("Failed to create component update");

        // now there should be two component updates
        let cus_for_su1 = nexus
            .system_update_list_components(&opctx, &su1.version)
            .await
            .unwrap();

        assert_eq!(cus_for_su1.len(), 2);

        // other system update should not be associated with any component updates
        let cus_for_su2 = nexus
            .system_update_list_components(&opctx, &su2.version)
            .await
            .unwrap();

        assert_eq!(cus_for_su2.len(), 0);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_semver_max(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        let expected = "Invalid Value: version, Major, minor, and patch version must be less than 99999999";

        // major, minor, and patch are all capped

        let su_create = SystemUpdateCreate {
            version: external::SemverVersion::new(100000000, 0, 0),
        };
        let error =
            nexus.upsert_system_update(&opctx, su_create).await.unwrap_err();
        assert!(error.to_string().contains(expected));

        let su_create = SystemUpdateCreate {
            version: external::SemverVersion::new(0, 100000000, 0),
        };
        let error =
            nexus.upsert_system_update(&opctx, su_create).await.unwrap_err();
        assert!(error.to_string().contains(expected));

        let su_create = SystemUpdateCreate {
            version: external::SemverVersion::new(0, 0, 100000000),
        };
        let error =
            nexus.upsert_system_update(&opctx, su_create).await.unwrap_err();
        assert!(error.to_string().contains(expected));
    }

    #[nexus_test(server = crate::Server)]
    async fn test_updateable_components(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        // starts out populated
        let components = nexus
            .updateable_components_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(components.len(), 9);

        // with no components these should both 500. as discussed in the
        // implementation, this is appropriate because we should never be
        // running the external API without components populated
        //
        // let low =
        //     nexus.lowest_component_system_version(&opctx).await.unwrap_err();
        // assert_matches!(low, external::Error::InternalError { .. });
        // let high =
        //     nexus.highest_component_system_version(&opctx).await.unwrap_err();
        // assert_matches!(high, external::Error::InternalError { .. });

        // creating a component if its system_version doesn't exist is a 404
        let uc_create = UpdateableComponentCreate {
            version: external::SemverVersion::new(0, 4, 1),
            system_version: external::SemverVersion::new(0, 2, 0),
            component_type: UpdateableComponentType::BootloaderForSp,
            device_id: "look-a-device".to_string(),
        };
        let uc_404 = nexus
            .create_updateable_component(&opctx, uc_create.clone())
            .await
            .unwrap_err();
        assert_matches!(uc_404, external::Error::ObjectNotFound { .. });

        // create system updates for the component updates to hang off of
        let v020 = external::SemverVersion::new(0, 2, 0);
        nexus
            .upsert_system_update(&opctx, SystemUpdateCreate { version: v020 })
            .await
            .expect("Failed to create system update");
        let v3 = external::SemverVersion::new(4, 0, 0);
        nexus
            .upsert_system_update(&opctx, SystemUpdateCreate { version: v3 })
            .await
            .expect("Failed to create system update");
        let v10 = external::SemverVersion::new(10, 0, 0);
        nexus
            .upsert_system_update(&opctx, SystemUpdateCreate { version: v10 })
            .await
            .expect("Failed to create system update");

        // now uc_create and friends will work
        nexus
            .create_updateable_component(&opctx, uc_create)
            .await
            .expect("failed to create updateable component");
        nexus
            .create_updateable_component(
                &opctx,
                UpdateableComponentCreate {
                    version: external::SemverVersion::new(0, 4, 1),
                    system_version: external::SemverVersion::new(3, 0, 0),
                    component_type: UpdateableComponentType::HeliosHostPhase2,
                    device_id: "another-device".to_string(),
                },
            )
            .await
            .expect("failed to create updateable component");
        nexus
            .create_updateable_component(
                &opctx,
                UpdateableComponentCreate {
                    version: external::SemverVersion::new(0, 4, 1),
                    system_version: external::SemverVersion::new(10, 0, 0),
                    component_type: UpdateableComponentType::HeliosHostPhase1,
                    device_id: "a-third-device".to_string(),
                },
            )
            .await
            .expect("failed to create updateable component");

        // now there should be 3 more, or 12
        let components = nexus
            .updateable_components_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(components.len(), 12);

        let low = nexus.lowest_component_system_version(&opctx).await.unwrap();
        assert_eq!(&low.to_string(), "0.2.0");
        let high =
            nexus.highest_component_system_version(&opctx).await.unwrap();
        assert_eq!(&high.to_string(), "10.0.0");

        // TODO: update the version of a component
    }

    #[nexus_test(server = crate::Server)]
    async fn test_update_deployments(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        // starts out with one populated
        let deployments = nexus
            .update_deployments_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(deployments.len(), 2);

        // start update fails with nonexistent version
        let not_found = nexus
            .create_update_deployment(
                &opctx,
                SystemUpdateStart {
                    version: external::SemverVersion::new(6, 0, 0),
                },
            )
            .await
            .unwrap_err();

        assert_matches!(not_found, external::Error::ObjectNotFound { .. });

        // starting with existing version fails because there's already an
        // update running
        let start_v3 = SystemUpdateStart {
            version: external::SemverVersion::new(3, 0, 0),
        };
        let already_updating = nexus
            .create_update_deployment(&opctx, start_v3.clone())
            .await
            .unwrap_err();

        assert_matches!(
            already_updating,
            external::Error::ObjectAlreadyExists { .. }
        );

        // stop the running update
        nexus
            .steady_update_deployment(&opctx)
            .await
            .expect("Failed to stop running update");

        // now starting an update succeeds
        let d = nexus
            .create_update_deployment(&opctx, start_v3)
            .await
            .expect("Failed to create deployment");

        let deployment_ids: Vec<Uuid> = nexus
            .update_deployments_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.identity.id)
            .collect();

        assert_eq!(deployment_ids.len(), 3);
        assert!(deployment_ids.contains(&d.identity.id));

        // latest deployment returns the one just created
        let latest_deployment =
            nexus.latest_update_deployment(&opctx).await.unwrap();

        assert_eq!(latest_deployment.identity.id, d.identity.id);
        assert_eq!(latest_deployment.status, UpdateStatus::Updating);
        assert!(
            latest_deployment.identity.time_modified
                == d.identity.time_modified
        );

        // stopping update updates both its status and its time_modified
        nexus
            .steady_update_deployment(&opctx)
            .await
            .expect("Failed to steady running update");

        let latest_deployment =
            nexus.latest_update_deployment(&opctx).await.unwrap();

        assert_eq!(latest_deployment.identity.id, d.identity.id);
        assert_eq!(latest_deployment.status, UpdateStatus::Steady);
        assert!(
            latest_deployment.identity.time_modified > d.identity.time_modified
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_populate_mock_system_updates(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        // starts out with updates because they're populated at rack init
        let su_count = nexus
            .system_updates_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap()
            .len();
        assert!(su_count > 0);

        // additional call doesn't error because the conflict gets eaten
        let result = nexus.populate_mock_system_updates(&opctx).await;
        assert!(result.is_ok());

        // count didn't change
        let system_updates = nexus
            .system_updates_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();
        assert_eq!(system_updates.len(), su_count);
    }
}
