// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Image types for version IMAGE_BLOCK_SIZE_TYPE.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    BlockSize, ByteCount, Digest, IdentityMetadata, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of an image
///
/// If `project_id` is present then the image is only visible inside that
/// project. If it's not present then the image is visible to all projects in
/// the silo.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Image {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// ID of the parent project if the image is a project image
    pub project_id: Option<Uuid>,

    /// The family of the operating system like Debian, Ubuntu, etc.
    pub os: String,

    /// Version of the operating system
    pub version: String,

    /// Hash of the image contents, if applicable
    pub digest: Option<Digest>,

    /// Size of blocks in bytes
    pub block_size: BlockSize,

    /// Total size in bytes
    pub size: ByteCount,
}

// -- View type conversions --

impl From<Image> for crate::v2025_11_20_00::image::Image {
    fn from(new: Image) -> Self {
        let Image {
            identity,
            project_id,
            os,
            version,
            digest,
            block_size,
            size,
        } = new;
        Self {
            identity,
            project_id,
            os,
            version,
            digest,
            block_size: block_size.into(),
            size,
        }
    }
}
