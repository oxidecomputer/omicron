// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SSH key types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams, NameOrId, ObjectIdentity,
};
use omicron_uuid_kinds::SiloUserUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of an SSH Key
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct SshKey {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The user to whom this key belongs
    #[schemars(with = "Uuid")]
    pub silo_user_id: SiloUserUuid,

    /// SSH public key, e.g., `"ssh-ed25519 AAAAC3NzaC..."`
    pub public_key: String,
}

// Params

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SshKeySelector {
    /// ID of the silo user
    #[schemars(with = "Uuid")]
    pub silo_user_id: SiloUserUuid,
    /// Name or ID of the SSH key
    pub ssh_key: NameOrId,
}

/// Create-time parameters for an `SshKey`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SshKeyCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// SSH public key, e.g., `"ssh-ed25519 AAAAC3NzaC..."`
    pub public_key: String,
}
