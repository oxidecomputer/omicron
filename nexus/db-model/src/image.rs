// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{BlockSize, ByteCount, Digest};
use crate::impl_enum_type;
use crate::schema::{image, project_image, silo_image};
use db_macros::Resource;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type! {
    #[derive(SqlType, QueryId, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "image_kind"))]
    pub struct ImageKindEnum;

    #[derive(Clone, Copy, Serialize, Deserialize, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = ImageKindEnum)]
    pub enum ImageKind;

    Silo => b"silo"
    Project => b"project"
}

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

    pub kind: ImageKind,
    pub parent_id: Uuid,
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
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = project_image)]
pub struct ProjectImage {
    #[diesel(embed)]
    pub identity: ProjectImageIdentity,

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
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
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
        match image.kind {
            ImageKind::Project => Ok(Self {
                identity: ProjectImageIdentity {
                    id: image.id(),
                    name: image.name().clone().into(),
                    description: image.description().to_string(),
                    time_created: image.time_created(),
                    time_modified: image.time_modified(),
                    time_deleted: image.time_deleted(),
                },
                project_id: image.parent_id,
                volume_id: image.volume_id,
                url: image.url,
                os: image.os,
                version: image.version,
                digest: image.digest,
                block_size: image.block_size,
                size: image.size,
            }),
            _ => Err(Error::internal_error(
                "tried to convert non-project image to project image",
            )),
        }
    }
}

impl TryFrom<Image> for SiloImage {
    type Error = Error;

    fn try_from(image: Image) -> Result<Self, Self::Error> {
        match image.kind {
            ImageKind::Silo => Ok(Self {
                identity: SiloImageIdentity {
                    id: image.id(),
                    name: image.name().clone().into(),
                    description: image.description().to_string(),
                    time_created: image.time_created(),
                    time_modified: image.time_modified(),
                    time_deleted: image.time_deleted(),
                },
                silo_id: image.parent_id,
                volume_id: image.volume_id,
                url: image.url,
                os: image.os,
                version: image.version,
                digest: image.digest,
                block_size: image.block_size,
                size: image.size,
            }),
            _ => Err(Error::internal_error(
                "tried to convert non-silo image to silo image",
            )),
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
            kind: ImageKind::Project,
            parent_id: image.project_id,
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
            kind: ImageKind::Silo,
            parent_id: image.silo_id,
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
            parent_id: match image.kind {
                ImageKind::Project => {
                    views::SiloOrProjectId::ProjectId(image.parent_id)
                }
                ImageKind::Silo => {
                    views::SiloOrProjectId::SiloId(image.parent_id)
                }
            },
            url: image.url,
            os: image.os,
            version: image.version,
            digest: image.digest.map(|x| x.into()),
            block_size: image.block_size.into(),
            size: image.size.into(),
        }
    }
}
