// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Images (both project and globally scoped)

use super::Unimpl;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Asset;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    // Project-Scoped Images

    pub async fn project_create_image(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _params: &params::ImageCreate,
    ) -> CreateResult<db::model::Image> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // TODO(https://github.com/oxidecomputer/omicron/issues/1482): When
        // we implement this, remember to insert the image within a Project
        // using "insert_resource".
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_list_images(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Image> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_image_fetch(
        &self,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        image_name: &Name,
    ) -> LookupResult<db::model::Image> {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_image_fetch_by_id(
        &self,
        opctx: &OpContext,
        image_id: &Uuid,
    ) -> LookupResult<db::model::Image> {
        let lookup_type = LookupType::ById(*image_id);
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_delete_image(
        self: &Arc<Self>,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        image_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    // Globally-Scoped Images

    pub async fn global_image_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::GlobalImageCreate,
    ) -> CreateResult<db::model::GlobalImage> {
        let new_image = match &params.source {
            params::ImageSource::Url { url } => {
                let db_block_size = db::model::BlockSize::try_from(
                    params.block_size,
                )
                .map_err(|e| Error::InvalidValue {
                    label: String::from("block_size"),
                    message: format!("block_size is invalid: {}", e),
                })?;

                let global_image_id = Uuid::new_v4();

                let volume_construction_request =
                    sled_agent_client::types::VolumeConstructionRequest::Url {
                        id: global_image_id,
                        block_size: db_block_size.to_bytes().into(),
                        url: url.clone(),
                    };

                let volume_data =
                    serde_json::to_string(&volume_construction_request)?;

                // use reqwest to query url for size
                let dur = std::time::Duration::from_secs(5);
                let client = reqwest::ClientBuilder::new()
                    .connect_timeout(dur)
                    .timeout(dur)
                    .build()
                    .map_err(|e| {
                        Error::internal_error(&format!(
                            "failed to build reqwest client: {}",
                            e
                        ))
                    })?;

                let response = client.head(url).send().await.map_err(|e| {
                    Error::InvalidValue {
                        label: String::from("url"),
                        message: format!("error querying url: {}", e),
                    }
                })?;

                if !response.status().is_success() {
                    return Err(Error::InvalidValue {
                        label: String::from("url"),
                        message: format!(
                            "querying url returned: {}",
                            response.status()
                        ),
                    });
                }

                // grab total size from content length
                let content_length = response
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .ok_or("no content length!")
                    .map_err(|e| Error::InvalidValue {
                        label: String::from("url"),
                        message: format!("error querying url: {}", e),
                    })?;

                let total_size =
                    u64::from_str(content_length.to_str().map_err(|e| {
                        Error::InvalidValue {
                            label: String::from("url"),
                            message: format!("content length invalid: {}", e),
                        }
                    })?)
                    .map_err(|e| {
                        Error::InvalidValue {
                            label: String::from("url"),
                            message: format!("content length invalid: {}", e),
                        }
                    })?;

                let size: external::ByteCount = total_size.try_into().map_err(
                    |e: external::ByteCountRangeError| Error::InvalidValue {
                        label: String::from("size"),
                        message: format!("total size is invalid: {}", e),
                    },
                )?;

                // validate total size is divisible by block size
                let block_size: u64 = params.block_size.into();
                if (size.to_bytes() % block_size) != 0 {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size {} must be divisible by block size {}",
                            size.to_bytes(),
                            block_size
                        ),
                    });
                }

                let new_image_volume =
                    db::model::Volume::new(Uuid::new_v4(), volume_data);
                let volume =
                    self.db_datastore.volume_create(new_image_volume).await?;

                db::model::GlobalImage {
                    identity: db::model::GlobalImageIdentity::new(
                        global_image_id,
                        params.identity.clone(),
                    ),
                    volume_id: volume.id(),
                    url: Some(url.clone()),
                    distribution: params.distribution.name.to_string(),
                    version: params.distribution.version,
                    digest: None, // not computed for URL type
                    block_size: db_block_size,
                    size: size.into(),
                }
            }

            params::ImageSource::Snapshot { id } => {
                let global_image_id = Uuid::new_v4();

                // Grab the snapshot to get block size
                let (.., db_snapshot) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .snapshot_id(*id)
                        .fetch()
                        .await?;

                // Copy the Volume data for this snapshot with randomized ids -
                // this is safe because the snapshot is read-only, and even
                // though volume_checkout will bump the gen numbers multiple
                // Upstairs can connect to read-only downstairs without kicking
                // each other out.

                let image_volume = self
                    .db_datastore
                    .volume_checkout_randomize_ids(db_snapshot.volume_id)
                    .await?;

                db::model::GlobalImage {
                    identity: db::model::GlobalImageIdentity::new(
                        global_image_id,
                        params.identity.clone(),
                    ),
                    volume_id: image_volume.id(),
                    url: None,
                    distribution: params.distribution.name.to_string(),
                    version: params.distribution.version,
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
                let block_size: u64 = db_block_size.to_bytes() as u64;

                let global_image_id = Uuid::new_v4();

                let volume_construction_request =
                    sled_agent_client::types::VolumeConstructionRequest::File {
                        id: global_image_id,
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
                    size.try_into().map_err(|e| Error::InvalidValue {
                        label: String::from("size"),
                        message: format!("size is invalid: {}", e),
                    })?;

                let new_image_volume =
                    db::model::Volume::new(Uuid::new_v4(), volume_data);
                let volume =
                    self.db_datastore.volume_create(new_image_volume).await?;

                db::model::GlobalImage {
                    identity: db::model::GlobalImageIdentity::new(
                        global_image_id,
                        params.identity.clone(),
                    ),
                    volume_id: volume.id(),
                    url: None,
                    distribution: "alpine".parse().map_err(|_| {
                        Error::internal_error(
                            &"alpine is not a valid distribution?",
                        )
                    })?,
                    version: "propolis-blob".into(),
                    digest: None,
                    block_size: db_block_size,
                    size: size.into(),
                }
            }
        };

        self.db_datastore.global_image_create_image(opctx, new_image).await
    }

    pub async fn global_images_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::GlobalImage> {
        self.db_datastore.global_image_list_images(opctx, pagparams).await
    }

    pub async fn global_image_fetch(
        &self,
        opctx: &OpContext,
        image_name: &Name,
    ) -> LookupResult<db::model::GlobalImage> {
        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .global_image_name(image_name)
            .fetch()
            .await?;
        Ok(db_disk)
    }

    pub async fn global_image_fetch_by_id(
        &self,
        opctx: &OpContext,
        global_image_id: &Uuid,
    ) -> LookupResult<db::model::GlobalImage> {
        let (.., db_global_image) = LookupPath::new(opctx, &self.db_datastore)
            .global_image_id(*global_image_id)
            .fetch()
            .await?;
        Ok(db_global_image)
    }

    pub async fn global_image_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        image_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let error = lookup_type.into_not_found(ResourceType::Image);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(error))
            .await)
    }
}
