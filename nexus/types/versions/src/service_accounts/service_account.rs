// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Service account types for version SERVICE_ACCOUNTS.

use api_identity::ObjectIdentity;
use chrono::{DateTime, Utc};
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams, ObjectIdentity,
};
use omicron_uuid_kinds::ProjectUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use uuid::Uuid;

/// View of a service account
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct ServiceAccount {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The project that contains this service account
    #[schemars(with = "Uuid")]
    pub project_id: ProjectUuid,
}

/// Create-time parameters for a `ServiceAccount`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// View of a service account key
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct ServiceAccountKey {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The service account this key authenticates as
    pub service_account_id: Uuid,

    /// Ed25519 public key in PEM (SubjectPublicKeyInfo) format
    pub public_key: String,

    /// When the key expires, if ever
    pub time_expires: Option<DateTime<Utc>>,
}

/// Create-time parameters for a `ServiceAccountKey`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountKeyCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// Ed25519 public key in PEM (SubjectPublicKeyInfo) format
    pub public_key: String,

    /// Lifetime of the key in seconds. If omitted, the key does not expire.
    pub ttl_seconds: Option<NonZeroU32>,
}
