// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Unimpl;
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
    pub async fn global_images_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::GlobalImage> {
        self.db_datastore.global_image_list_images(opctx, pagparams).await
    }

    pub async fn global_image_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: &params::ImageCreate,
    ) -> CreateResult<db::model::GlobalImage> {
        let new_image = match &params.source {
            params::ImageSource::Url(url) => {
                let db_block_size = db::model::BlockSize::try_from(
                    params.block_size,
                )
                .map_err(|e| Error::InvalidValue {
                    label: String::from("block_size"),
                    message: format!("block_size is invalid: {}", e),
                })?;

                let volume_construction_request = sled_agent_client::types::VolumeConstructionRequest::Volume {
                    block_size: db_block_size.to_bytes().into(),
                    sub_volumes: vec![
                        sled_agent_client::types::VolumeConstructionRequest::Url {
                            block_size: db_block_size.to_bytes().into(),
                            url: url.clone(),
                        }
                    ],
                    read_only_parent: None,
                };

                let volume_data =
                    serde_json::to_string(&volume_construction_request)?;

                // use reqwest to query url for size
                let response =
                    reqwest::Client::new().head(url).send().await.map_err(
                        |e| Error::InvalidValue {
                            label: String::from("url"),
                            message: format!("error querying url: {}", e),
                        },
                    )?;

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

                // for images backed by a url, store the ETag as the version
                let etag = response
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|x| x.to_str().ok())
                    .map(|x| x.to_string());

                let new_image_volume =
                    db::model::Volume::new(Uuid::new_v4(), volume_data);
                let volume =
                    self.db_datastore.volume_create(new_image_volume).await?;

                db::model::GlobalImage {
                    identity: db::model::GlobalImageIdentity::new(
                        Uuid::new_v4(),
                        params.identity.clone(),
                    ),
                    volume_id: volume.id(),
                    url: Some(url.clone()),
                    version: etag,
                    digest: None, // not computed for URL type
                    block_size: db_block_size,
                    size: size.into(),
                }
            }

            params::ImageSource::Snapshot(_id) => {
                return Err(Error::unavail(
                    &"creating images from snapshots not supported",
                ));
            }
        };

        self.db_datastore.global_image_create_image(opctx, new_image).await
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
