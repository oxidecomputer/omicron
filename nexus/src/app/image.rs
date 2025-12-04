// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Images (both project and silo scoped)

use crate::external_api::params;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_lookup::lookup::ImageLookup;
use nexus_db_lookup::lookup::ImageParentLookup;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use std::sync::Arc;

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
                    .image_id(id)
                    .fetch()
                    .await?;
                let lookup = match db_image.project_id {
                    Some(_) => ImageLookup::ProjectImage(
                        LookupPath::new(opctx, &self.db_datastore)
                            .project_image_id(id),
                    ),
                    None => ImageLookup::SiloImage(
                        LookupPath::new(opctx, &self.db_datastore)
                            .silo_image_id(id),
                    ),
                };
                Ok(lookup)
            }
            params::ImageSelector {
                image: NameOrId::Name(name),
                project: Some(project),
            } => {
                let image = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .project_image_name_owned(name.into());
                Ok(ImageLookup::ProjectImage(image))
            }
            params::ImageSelector {
                image: NameOrId::Name(name),
                project: None,
            } => {
                let image = self
                    .current_silo_lookup(opctx)?
                    .silo_image_name_owned(name.into());
                Ok(ImageLookup::SiloImage(image))
            }
            params::ImageSelector { image: NameOrId::Id(_), .. } => {
                Err(Error::invalid_request(
                    "when providing image as an ID, project should not be specified",
                ))
            }
        }
    }

    /// Creates an image
    pub(crate) async fn image_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        lookup_parent: &ImageParentLookup<'_>,
        params: &params::ImageCreate,
    ) -> CreateResult<db::model::Image> {
        let image_type = match lookup_parent {
            ImageParentLookup::Project(project) => {
                let (authz_silo, authz_project) =
                    project.lookup_for(authz::Action::CreateChild).await?;

                sagas::image_create::ImageType::Project {
                    authz_silo,
                    authz_project,
                }
            }

            ImageParentLookup::Silo(silo) => {
                let (.., authz_silo) =
                    silo.lookup_for(authz::Action::CreateChild).await?;

                sagas::image_create::ImageType::Silo { authz_silo }
            }
        };

        let saga_params = sagas::image_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            image_type,
            create_params: params.clone(),
        };

        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::image_create::SagaImageCreate>(saga_params)
            .await?;

        let created_image = saga_outputs
            .lookup_node_output::<db::model::Image>("created_image")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from image create saga")?;

        Ok(created_image)
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
                // Check CreateChild on the project since we're creating a ProjectImage.
                // This allows limited-collaborators to demote images.
                let (_, authz_project) = project_lookup
                    .lookup_for(authz::Action::CreateChild)
                    .await?;
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
