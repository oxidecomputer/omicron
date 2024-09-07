// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Image, ProjectImage, and SiloImage are almost identical. Image is the model
//! for the underlying table and ProjectImage and SiloImage are backed by views
//! of that table. They have all the same fields except ProjectImage has both a
//! silo_id and a project_id, while SiloImage only has a silo_id. Image has a
//! silo_id and an optional project_id to cover both possibilities.

use super::{BlockSize, ByteCount, Digest};
use crate::schema::{image, project_image, silo_image};
use db_macros::Resource;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Shared image definition
#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = image)]
pub struct Image {
    #[diesel(embed)]
    pub identity: ImageIdentity,

    pub silo_id: Uuid,
    pub project_id: Option<Uuid>,

    pub volume_id: Uuid,
    pub url: Option<String>,
    pub os: String,
    pub version: String,
    pub digest: Option<Digest>,
    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

#[derive(
    Queryable, Selectable, Clone, Debug, Resource, Serialize, Deserialize,
)]
#[diesel(table_name = project_image)]
pub struct ProjectImage {
    #[diesel(embed)]
    pub identity: ProjectImageIdentity,

    pub silo_id: Uuid,
    pub project_id: Uuid,
    pub volume_id: Uuid,
    pub url: Option<String>,
    pub os: String,
    pub version: String,
    pub digest: Option<Digest>,
    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

#[derive(
    Queryable, Selectable, Clone, Debug, Resource, Serialize, Deserialize,
)]
#[diesel(table_name = silo_image)]
pub struct SiloImage {
    #[diesel(embed)]
    pub identity: SiloImageIdentity,

    pub silo_id: Uuid,
    pub volume_id: Uuid,
    pub url: Option<String>,
    pub os: String,
    pub version: String,
    pub digest: Option<Digest>,
    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl TryFrom<Image> for ProjectImage {
    type Error = Error;

    fn try_from(image: Image) -> Result<Self, Self::Error> {
        match image.project_id {
            Some(project_id) => Ok(Self {
                identity: ProjectImageIdentity {
                    id: image.id(),
                    name: image.name().clone().into(),
                    description: image.description().to_string(),
                    time_created: image.time_created(),
                    time_modified: image.time_modified(),
                    time_deleted: image.time_deleted(),
                },
                silo_id: image.silo_id,
                project_id,
                volume_id: image.volume_id,
                url: image.url,
                os: image.os,
                version: image.version,
                digest: image.digest,
                block_size: image.block_size,
                size: image.size,
            }),
            None => Err(Error::internal_error(
                "tried to convert non-project image to project image",
            )),
        }
    }
}

impl TryFrom<Image> for SiloImage {
    type Error = Error;

    fn try_from(image: Image) -> Result<Self, Self::Error> {
        match image.project_id {
            Some(_) => Err(Error::internal_error(
                "tried to convert non-silo image to silo image",
            )),
            None => Ok(Self {
                identity: SiloImageIdentity {
                    id: image.id(),
                    name: image.name().clone().into(),
                    description: image.description().to_string(),
                    time_created: image.time_created(),
                    time_modified: image.time_modified(),
                    time_deleted: image.time_deleted(),
                },
                silo_id: image.silo_id,
                volume_id: image.volume_id,
                url: image.url,
                os: image.os,
                version: image.version,
                digest: image.digest,
                block_size: image.block_size,
                size: image.size,
            }),
        }
    }
}

impl From<ProjectImage> for Image {
    fn from(image: ProjectImage) -> Self {
        Self {
            identity: ImageIdentity {
                id: image.id(),
                name: image.name().clone().into(),
                description: image.description().to_string(),
                time_created: image.time_created(),
                time_modified: image.time_modified(),
                time_deleted: image.time_deleted(),
            },
            silo_id: image.silo_id,
            project_id: Some(image.project_id),
            volume_id: image.volume_id,
            url: image.url,
            os: image.os,
            version: image.version,
            digest: image.digest,
            block_size: image.block_size,
            size: image.size,
        }
    }
}

impl From<SiloImage> for Image {
    fn from(image: SiloImage) -> Self {
        Self {
            identity: ImageIdentity {
                id: image.id(),
                name: image.name().clone().into(),
                description: image.description().to_string(),
                time_created: image.time_created(),
                time_modified: image.time_modified(),
                time_deleted: image.time_deleted(),
            },
            silo_id: image.silo_id,
            project_id: None,
            volume_id: image.volume_id,
            url: image.url,
            os: image.os,
            version: image.version,
            digest: image.digest,
            block_size: image.block_size,
            size: image.size,
        }
    }
}

impl From<Image> for views::Image {
    fn from(image: Image) -> Self {
        Self {
            identity: image.identity(),
            project_id: image.project_id,
            os: image.os,
            version: image.version,
            digest: image.digest.map(|x| x.into()),
            block_size: image.block_size.into(),
            size: image.size.into(),
        }
    }
}
