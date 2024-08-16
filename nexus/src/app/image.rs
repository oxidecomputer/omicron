// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Images (both project and silo scoped)

use crate::external_api::params;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::ImageLookup;
use nexus_db_queries::db::lookup::ImageParentLookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use std::sync::Arc;
use uuid::Uuid;

use super::sagas;

impl super::Nexus {
    pub(crate) async fn image_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        image_selector: params::ImageSelector,
    ) -> LookupResult<ImageLookup<'a>> {
        match image_selector {
            params::ImageSelector {
                image: NameOrId::Id(id),
                project: None,
            } => {
                let (.., db_image) = LookupPath::new(opctx, &self.db_datastore)
                    .image_id(id).fetch().await?;
                let lookup = match db_image.project_id {
                    Some(_) => ImageLookup::ProjectImage(LookupPath::new(opctx, &self.db_datastore)
                        .project_image_id(id)),
                    None => {
                        ImageLookup::SiloImage(LookupPath::new(opctx, &self.db_datastore)
                            .silo_image_id(id))
                    },
                };
                Ok(lookup)
            }
            params::ImageSelector {
                image: NameOrId::Name(name),
                project: Some(project),
            } => {
                let image =
                    self.project_lookup(opctx, params::ProjectSelector { project })?.project_image_name_owned(name.into());
                Ok(ImageLookup::ProjectImage(image))
            }
            params::ImageSelector {
                image: NameOrId::Name(name),
                project: None,
            } => {
                let image = self.current_silo_lookup(opctx)?.silo_image_name_owned(name.into());
                Ok(ImageLookup::SiloImage(image))
            }
            params::ImageSelector {
                image: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing image as an ID, project should not be specified",
            )),
            }
    }

    /// Creates an image
    pub(crate) async fn image_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        lookup_parent: &ImageParentLookup<'_>,
        params: &params::ImageCreate,
    ) -> CreateResult<db::model::Image> {
        let (authz_silo, maybe_authz_project) = match lookup_parent {
            ImageParentLookup::Project(project) => {
                let (authz_silo, authz_project) =
                    project.lookup_for(authz::Action::CreateChild).await?;
                (authz_silo, Some(authz_project))
            }
            ImageParentLookup::Silo(silo) => {
                let (.., authz_silo) =
                    silo.lookup_for(authz::Action::CreateChild).await?;
                (authz_silo, None)
            }
        };
        let new_image = match &params.source {
            params::ImageSource::Snapshot { id } => {
                let image_id = Uuid::new_v4();

                // Grab the snapshot to get block size
                let (.., db_snapshot) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .snapshot_id(*id)
                        .fetch()
                        .await?;

                if let Some(authz_project) = &maybe_authz_project {
                    if db_snapshot.project_id != authz_project.id() {
                        return Err(Error::invalid_request(
                            "snapshot does not belong to this project",
                        ));
                    }
                }

                // Copy the Volume data for this snapshot with randomized ids -
                // this is safe because the snapshot is read-only, and even
                // though volume_checkout will bump the gen numbers multiple
                // Upstairs can connect to read-only downstairs without kicking
                // each other out.

                let image_volume = self
                    .db_datastore
                    .volume_checkout_randomize_ids(
                        db_snapshot.volume_id,
                        db::datastore::VolumeCheckoutReason::ReadOnlyCopy,
                    )
                    .await?;

                db::model::Image {
                    identity: db::model::ImageIdentity::new(
                        image_id,
                        params.identity.clone(),
                    ),
                    silo_id: authz_silo.id(),
                    project_id: maybe_authz_project.clone().map(|p| p.id()),
                    volume_id: image_volume.id(),
                    url: None,
                    os: params.os.clone(),
                    version: params.version.clone(),
                    digest: None, // TODO
                    block_size: db_snapshot.block_size,
                    size: db_snapshot.size,
                }
            }

            params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine => {
                // Each Propolis zone ships with an alpine.iso (it's part of the
                // package-manifest.toml blobs), and for development purposes
                // allow users to boot that. This should go away when that blob
                // does.
                let db_block_size = db::model::BlockSize::Traditional;
                let block_size: u64 = u64::from(db_block_size.to_bytes());

                let image_id = Uuid::new_v4();

                let volume_construction_request =
                    sled_agent_client::types::VolumeConstructionRequest::File {
                        id: image_id,
                        block_size,
                        path: "/opt/oxide/propolis-server/blob/alpine.iso"
                            .into(),
                    };

                let volume_data =
                    serde_json::to_string(&volume_construction_request)?;

                // Nexus runs in its own zone so we can't ask the propolis zone
                // image tar file for size of alpine.iso. Conservatively set the
                // size to 100M (at the time of this comment, it's 41M). Any
                // disk created from this image has to be larger than it.
                let size: u64 = 100 * 1024 * 1024;
                let size: external::ByteCount =
                    size.try_into().map_err(|e| {
                        Error::invalid_value(
                            "size",
                            format!("size is invalid: {}", e),
                        )
                    })?;

                let new_image_volume =
                    db::model::Volume::new(Uuid::new_v4(), volume_data);
                let volume =
                    self.db_datastore.volume_create(new_image_volume).await?;

                db::model::Image {
                    identity: db::model::ImageIdentity::new(
                        image_id,
                        params.identity.clone(),
                    ),
                    silo_id: authz_silo.id(),
                    project_id: maybe_authz_project.clone().map(|p| p.id()),
                    volume_id: volume.id(),
                    url: None,
                    os: "alpine".into(),
                    version: "propolis-blob".into(),
                    digest: None,
                    block_size: db_block_size,
                    size: size.into(),
                }
            }
        };

        match maybe_authz_project {
            Some(authz_project) => {
                self.db_datastore
                    .project_image_create(
                        opctx,
                        &authz_project,
                        new_image.try_into()?,
                    )
                    .await
            }
            None => {
                self.db_datastore
                    .silo_image_create(
                        opctx,
                        &authz_silo,
                        new_image.try_into()?,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn image_list(
        &self,
        opctx: &OpContext,
        parent_lookup: &ImageParentLookup<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Image> {
        match parent_lookup {
            ImageParentLookup::Project(project) => {
                let (.., authz_project) =
                    project.lookup_for(authz::Action::ListChildren).await?;
                self.db_datastore
                    .project_image_list(opctx, &authz_project, pagparams)
                    .await
            }
            ImageParentLookup::Silo(silo) => {
                let (.., authz_silo) =
                    silo.lookup_for(authz::Action::ListChildren).await?;
                self.db_datastore
                    .silo_image_list(opctx, &authz_silo, pagparams)
                    .await
            }
        }
    }

    pub(crate) async fn image_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        image_lookup: &ImageLookup<'_>,
    ) -> DeleteResult {
        let image_param: sagas::image_delete::ImageParam = match image_lookup {
            ImageLookup::ProjectImage(lookup) => {
                let (_, _, authz_image, image) =
                    lookup.fetch_for(authz::Action::Delete).await?;
                sagas::image_delete::ImageParam::Project { authz_image, image }
            }
            ImageLookup::SiloImage(lookup) => {
                let (_, authz_image, image) =
                    lookup.fetch_for(authz::Action::Delete).await?;
                sagas::image_delete::ImageParam::Silo { authz_image, image }
            }
        };

        let saga_params = sagas::image_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            image_param,
        };

        self.sagas
            .saga_execute::<sagas::image_delete::SagaImageDelete>(saga_params)
            .await?;

        Ok(())
    }

    /// Converts a project scoped image into a silo scoped image
    pub(crate) async fn image_promote(
        self: &Arc<Self>,
        opctx: &OpContext,
        image_lookup: &ImageLookup<'_>,
    ) -> UpdateResult<db::model::Image> {
        match image_lookup {
            ImageLookup::ProjectImage(lookup) => {
                let (authz_silo, _, authz_project_image, project_image) =
                    lookup.fetch_for(authz::Action::Modify).await?;
                opctx
                    .authorize(authz::Action::CreateChild, &authz_silo)
                    .await?;
                self.db_datastore
                    .project_image_promote(
                        opctx,
                        &authz_silo,
                        &authz_project_image,
                        &project_image,
                    )
                    .await
            }
            ImageLookup::SiloImage(_) => {
                Err(Error::invalid_request("Cannot promote a silo image"))
            }
        }
    }

    /// Converts a silo scoped image into a project scoped image
    pub(crate) async fn image_demote(
        self: &Arc<Self>,
        opctx: &OpContext,
        image_lookup: &ImageLookup<'_>,
        project_lookup: &lookup::Project<'_>,
    ) -> UpdateResult<db::model::Image> {
        match image_lookup {
            ImageLookup::SiloImage(lookup) => {
                let (_, authz_silo_image, silo_image) =
                    lookup.fetch_for(authz::Action::Modify).await?;
                let (_, authz_project) =
                    project_lookup.lookup_for(authz::Action::Modify).await?;
                self.db_datastore
                    .silo_image_demote(
                        opctx,
                        &authz_silo_image,
                        &authz_project,
                        &silo_image,
                    )
                    .await
            }
            ImageLookup::ProjectImage(_) => {
                Err(Error::invalid_request("Cannot demote a project image"))
            }
        }
    }
}
