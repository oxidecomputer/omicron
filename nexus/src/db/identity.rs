// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Identity-related traits which may be derived for DB structures.

// Copyright 2021 Oxide Computer Company

use super::model::Name;
use chrono::{DateTime, Utc};
use omicron_common::api::external;
use uuid::Uuid;

/// Identity-related accessors for resources.
///
/// These are end-user-visible objects with names, descriptions,
/// and which may be soft-deleted.
///
/// For durable objects which do not require soft-deletion or descriptions,
/// consider the [`Asset`] trait instead.
///
/// May be derived from [`macro@db_macros::Resource`].
pub trait Resource {
    fn id(&self) -> Uuid;
    fn name(&self) -> &Name;
    fn description(&self) -> &str;
    fn time_created(&self) -> DateTime<Utc>;
    fn time_modified(&self) -> DateTime<Utc>;
    fn time_deleted(&self) -> Option<DateTime<Utc>>;

    fn identity(&self) -> external::IdentityMetadata {
        external::IdentityMetadata {
            id: self.id(),
            name: self.name().clone().into(),
            description: self.description().to_string(),
            time_created: self.time_created(),
            time_modified: self.time_modified(),
        }
    }
}

/// Identity-related accessors for assets.
///
/// These are objects similar to [`Resource`], but without
/// names, descriptions, or soft deletions.
///
/// May be derived from [`macro@db_macros::Asset`].
pub trait Asset {
    fn id(&self) -> Uuid;
    fn time_created(&self) -> DateTime<Utc>;
    fn time_modified(&self) -> DateTime<Utc>;
}
