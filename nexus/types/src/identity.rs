// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Identity-related traits which may be derived for DB structures.

// Copyright 2021 Oxide Computer Company

use chrono::{DateTime, Utc};
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::Name;
use omicron_uuid_kinds::GenericUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identity-related accessors for resources.
///
/// These are end-user-visible objects with names, descriptions,
/// and which may be soft-deleted.
///
/// For durable objects which do not require soft-deletion or descriptions,
/// consider the [`Asset`] trait instead.
///
/// May be derived from [`macro@db-macros::Resource`].
pub trait Resource {
    type IdType: GenericUuid;

    fn id(&self) -> Self::IdType;
    fn name(&self) -> &Name;
    fn description(&self) -> &str;
    fn time_created(&self) -> DateTime<Utc>;
    fn time_modified(&self) -> DateTime<Utc>;
    fn time_deleted(&self) -> Option<DateTime<Utc>>;

    fn identity(&self) -> IdentityMetadata {
        IdentityMetadata {
            id: self.id().into_untyped_uuid(),
            name: self.name().clone(),
            description: self.description().to_string(),
            time_created: self.time_created(),
            time_modified: self.time_modified(),
        }
    }
}

/// Identity-related metadata that's included in "asset" public API objects
/// (which generally have no name or description)
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct AssetIdentityMetadata {
    /// unique, immutable, system-controlled identifier for each resource
    pub id: Uuid,
    /// timestamp when this resource was created
    pub time_created: DateTime<Utc>,
    /// timestamp when this resource was last modified
    pub time_modified: DateTime<Utc>,
}

/// Identity-related accessors for assets.
///
/// These are objects similar to [`Resource`], but without
/// names, descriptions, or soft deletions.
///
/// May be derived from [`macro@db-macros::Asset`].
pub trait Asset {
    type IdType: GenericUuid;

    fn id(&self) -> Self::IdType;
    fn time_created(&self) -> DateTime<Utc>;
    fn time_modified(&self) -> DateTime<Utc>;

    fn identity(&self) -> AssetIdentityMetadata {
        AssetIdentityMetadata {
            id: self.id().into_untyped_uuid(),
            time_created: self.time_created(),
            time_modified: self.time_modified(),
        }
    }
}
