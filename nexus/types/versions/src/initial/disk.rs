// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for version INITIAL.

use omicron_common::api::external::{
    ByteCount, DiskState, IdentityMetadata, IdentityMetadataCreateParams,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "u32")]
pub struct BlockSize(pub u32);

impl schemars::JsonSchema for BlockSize {
    fn schema_name() -> String {
        "BlockSize".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                id: None,
                title: Some("Disk block size in bytes".to_string()),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            enum_values: Some(vec![
                serde_json::json!(512),
                serde_json::json!(2048),
                serde_json::json!(4096),
            ]),
            ..Default::default()
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiskType {
    Crucible,
}

/// View of a Disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Disk {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    /// ID of snapshot from which disk was created, if any
    pub snapshot_id: Option<Uuid>,
    /// ID of image from which disk was created, if any
    pub image_id: Option<Uuid>,
    pub size: ByteCount,
    pub block_size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
    pub disk_type: DiskType,
}

/// Different sources for a disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskSource {
    /// Create a blank disk
    Blank {
        /// Size of blocks for this disk. Valid values are: 512, 2048, or 4096.
        block_size: BlockSize,
    },

    /// Create a disk from a disk snapshot
    Snapshot { snapshot_id: Uuid },

    /// Create a disk from an image
    Image { image_id: Uuid },

    /// Create a blank disk that will accept bulk writes or pull blocks from an
    /// external source.
    ImportingBlocks { block_size: BlockSize },
}

/// Create-time parameters for a `Disk`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskCreate {
    /// The common identifying metadata for the disk
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The initial source for this disk
    pub disk_source: DiskSource,

    /// The total size of the Disk (in bytes)
    pub size: ByteCount,
}

use omicron_common::api::external::{Name, NameOrId};
use parse_display::Display;

#[derive(Deserialize, JsonSchema)]
pub struct DiskSelector {
    /// Name or ID of the project, only required if `disk` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the disk
    pub disk: NameOrId,
}

#[derive(Display, Serialize, Deserialize, JsonSchema)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum DiskMetricName {
    Activated,
    Flush,
    Read,
    ReadBytes,
    Write,
    WriteBytes,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DiskMetricsPath {
    pub disk: NameOrId,
    pub metric: DiskMetricName,
}

// equivalent to crucible_pantry_client::types::ExpectedDigest
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedDigest {
    Sha256(String),
}

/// Parameters for importing blocks with a bulk write
// equivalent to crucible_pantry_client::types::BulkWriteRequest
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImportBlocksBulkWrite {
    pub offset: u64,
    pub base64_encoded_data: String,
}

/// Parameters for finalizing a disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FinalizeDisk {
    /// If specified a snapshot of the disk will be created with the given name
    /// during finalization. If not specified, a snapshot for the disk will
    /// _not_ be created. A snapshot can be manually created once the disk
    /// transitions into the `Detached` state.
    pub snapshot_name: Option<Name>,
}
