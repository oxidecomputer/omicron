// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Asset;
use crate::db::lookup::LookupPath;
use crate::db::model::UpdateArtifactKind;
use chrono::Utc;
use hex;
use nexus_db_model::{SemverVersion, UpdateableComponentType};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, Error, ListResultVec, LookupResult,
    PaginationOrder,
};
use omicron_common::api::internal::nexus::UpdateArtifact;
use rand::Rng;
use ring::digest;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

static BASE_ARTIFACT_DIR: &str = "/var/tmp/oxide_artifacts";

pub struct CreateSystemUpdate {
    version: external::SemverVersion,
}

// TODO: it's janky to use the external version of SemverVersion (which makes
// sense because this is all coming from outside the db layer) but
// UpdateableComponentType comes from the db model. We probably need to move
// views::UpdateableComponentType out of views and into shared, and then
// important that here and use it.
pub struct CreateComponentUpdate {
    version: external::SemverVersion,
    component_type: UpdateableComponentType,
    parent_id: Option<Uuid>,
    system_update_id: Uuid,
}

pub struct CreateUpdateableComponent {
    version: external::SemverVersion, // TODO: is this weird? do we know the version at create time?
    component_type: UpdateableComponentType,
    parent_id: Option<Uuid>,
    device_id: String,
}

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

    pub async fn updates_refresh_metadata(
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
                .update_available_artifact_upsert(&opctx, artifact.clone())
                .await?;
        }

        // ensure table is in sync with current copy of artifacts.json
        if let Some(current_version) = current_version {
            self.db_datastore
                .update_available_artifact_hard_delete_outdated(
                    &opctx,
                    current_version,
                )
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
                        &sled_agent_client::types::UpdateArtifact {
                            name: artifact.name.clone(),
                            version: artifact.version.clone(),
                            kind: artifact.kind.0.into(),
                        },
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Downloads a file from within [`BASE_ARTIFACT_DIR`].
    pub async fn download_artifact(
        &self,
        opctx: &OpContext,
        artifact: UpdateArtifact,
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
            .update_available_artifact_tuple(
                &artifact.name,
                &artifact.version,
                UpdateArtifactKind(artifact.kind),
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

    pub async fn system_update_create(
        &self,
        opctx: &OpContext,
        create_update: CreateSystemUpdate,
    ) -> CreateResult<db::model::SystemUpdate> {
        let now = Utc::now();
        let update = db::model::SystemUpdate {
            identity: db::model::SystemUpdateIdentity {
                id: Uuid::new_v4(),
                time_created: now,
                time_modified: now,
            },
            version: SemverVersion(create_update.version),
        };
        self.db_datastore.system_update_create(opctx, update).await
    }

    pub async fn component_update_create(
        &self,
        opctx: &OpContext,
        create_update: CreateComponentUpdate,
    ) -> CreateResult<db::model::ComponentUpdate> {
        let now = Utc::now();
        let update = db::model::ComponentUpdate {
            identity: db::model::ComponentUpdateIdentity {
                id: Uuid::new_v4(),
                time_created: now,
                time_modified: now,
            },
            version: SemverVersion(create_update.version),
            component_type: create_update.component_type,
            parent_id: create_update.parent_id,
        };

        // TODO: make sure system update with that ID exists first

        self.db_datastore
            .component_update_create(
                opctx,
                create_update.system_update_id,
                update,
            )
            .await
    }

    pub async fn system_update_fetch_by_version(
        &self,
        opctx: &OpContext,
        version: &external::SemverVersion,
    ) -> LookupResult<db::model::SystemUpdate> {
        // TODO: I don't think this is the right way to do this auth check
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let (.., db_system_update) = LookupPath::new(opctx, &self.db_datastore)
            .system_update_version(version.clone().into())
            .fetch()
            .await?;

        Ok(db_system_update)
    }

    pub async fn system_updates_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.system_updates_list_by_id(opctx, pagparams).await
    }

    pub async fn system_update_list_components(
        &self,
        opctx: &OpContext,
        version: &external::SemverVersion,
    ) -> ListResultVec<db::model::ComponentUpdate> {
        // TODO: I don't think this is the right way to do this auth check
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let (authz_update, ..) = LookupPath::new(opctx, &self.db_datastore)
            .system_update_version(version.clone().into())
            .fetch()
            .await?;

        self.db_datastore
            .system_update_components_list(opctx, &authz_update)
            .await
    }

    pub async fn updateable_component_create(
        &self,
        opctx: &OpContext,
        create_component: CreateUpdateableComponent,
    ) -> CreateResult<db::model::UpdateableComponent> {
        let now = Utc::now();
        let component = db::model::UpdateableComponent {
            identity: db::model::UpdateableComponentIdentity {
                id: Uuid::new_v4(),
                time_created: now,
                time_modified: now,
            },
            version: SemverVersion(create_component.version),
            component_type: create_component.component_type,
            parent_id: create_component.parent_id,
            device_id: create_component.device_id,
            status: db::model::UpdateStatus::Steady,
        };

        self.db_datastore.updateable_component_create(opctx, component).await
    }

    pub async fn updateable_components_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::UpdateableComponent> {
        self.db_datastore
            .updateable_components_list_by_id(opctx, pagparams)
            .await
    }

    pub async fn update_deployments_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::UpdateDeployment> {
        self.db_datastore.update_deployments_list_by_id(opctx, pagparams).await
    }

    pub async fn update_deployment_fetch_by_id(
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

    pub async fn latest_update_deployment(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::UpdateDeployment> {
        self.db_datastore.latest_update_deployment(opctx).await
    }
}

// TODO: should these tests be done as integration tests? the creates would
// still have to be direct calls to the service functions, but the retrievals
// could be HTTP requests, which would cover more code

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use crate::app::update::{
        CreateComponentUpdate, CreateSystemUpdate, CreateUpdateableComponent,
    };
    use crate::context::OpContext;
    use dropshot::PaginationOrder;
    use nexus_db_model::UpdateableComponentType;
    use nexus_test_utils_macros::nexus_test;
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

        // starts out empty
        let system_updates = nexus
            .system_updates_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(system_updates.len(), 0);

        let su1_create = CreateSystemUpdate {
            version: external::SemverVersion::new(1, 0, 0),
        };
        let su1 = nexus.system_update_create(&opctx, su1_create).await.unwrap();
        let su2_create = CreateSystemUpdate {
            version: external::SemverVersion::new(2, 0, 0),
        };
        let su2 = nexus.system_update_create(&opctx, su2_create).await.unwrap();

        // now there should be two system updates
        let system_updates = nexus
            .system_updates_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(system_updates.len(), 2);

        // let's also make sure we can fetch them by version
        let su1_fetched = nexus
            .system_update_fetch_by_version(&opctx, &su1.version)
            .await
            .unwrap();
        assert_eq!(su1.identity.id, su1_fetched.identity.id);

        let su2_fetched = nexus
            .system_update_fetch_by_version(&opctx, &su2.version)
            .await
            .unwrap();
        assert_eq!(su2.identity.id, su2_fetched.identity.id);

        // now create two component updates for update 1, one at root, and one
        // hanging off the first
        let cu1 = nexus
            .component_update_create(
                &opctx,
                CreateComponentUpdate {
                    version: external::SemverVersion::new(1, 0, 0),
                    component_type: UpdateableComponentType::BootloaderForRot,
                    parent_id: None,
                    system_update_id: su1.identity.id,
                },
            )
            .await
            .unwrap();
        let _cu1a = nexus
            .component_update_create(
                &opctx,
                CreateComponentUpdate {
                    version: external::SemverVersion::new(2, 0, 0),
                    component_type: UpdateableComponentType::HubrisForGimletSp,
                    parent_id: Some(cu1.identity.id),
                    system_update_id: su1.identity.id,
                },
            )
            .await
            .unwrap();

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
    async fn test_updateable_components(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        // starts out empty
        let components = nexus
            .updateable_components_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(components.len(), 0);

        let uc1 = nexus
            .updateable_component_create(
                &opctx,
                CreateUpdateableComponent {
                    version: external::SemverVersion::new(1, 0, 0),
                    component_type: UpdateableComponentType::BootloaderForSp,
                    parent_id: None,
                    device_id: "look-a-device".to_string(),
                },
            )
            .await
            .unwrap();
        let _uc2 = nexus
            .updateable_component_create(
                &opctx,
                CreateUpdateableComponent {
                    version: external::SemverVersion::new(2, 0, 0),
                    component_type: UpdateableComponentType::HeliosHostPhase2,
                    parent_id: Some(uc1.identity.id),
                    device_id: "another-device".to_string(),
                },
            )
            .await
            .unwrap();

        // now there should be 2
        let components = nexus
            .updateable_components_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(components.len(), 2);

        // TODO: update the version of a component
    }

    #[nexus_test(server = crate::Server)]
    async fn test_update_deployments(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        // starts out empty
        let deployments = nexus
            .update_deployments_list_by_id(&opctx, &test_pagparams())
            .await
            .unwrap();

        assert_eq!(deployments.len(), 0);
    }
}
