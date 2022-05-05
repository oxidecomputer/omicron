// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{BlockSize, ByteCount, Digest};
use crate::db::identity::Resource;
use crate::db::schema::image;
use crate::external_api::views;
use db_macros::Resource;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Project images
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

    pub project_id: Uuid,
    pub volume_id: Uuid,
    pub url: Option<String>,
    pub version: Option<String>,
    pub digest: Option<Digest>,

    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl From<Image> for views::Image {
    fn from(image: Image) -> Self {
        Self {
            identity: image.identity(),
            project_id: image.project_id,
            url: image.url,
            version: image.version,
            digest: image.digest.map(|x| x.into()),
            block_size: image.block_size.into(),
            size: image.size.into(),
        }
    }
}
