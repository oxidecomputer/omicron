//! Identity-related traits which may be derived for DB structures.

// Copyright 2021 Oxide Computer Company

use chrono::{DateTime, Utc};
use omicron_common::api::external::Name;
use uuid::Uuid;

/// Identity-related accessors for resources.
///
/// May be derived from [`macro@db_macros::Resource`].
pub trait Resource {
    fn id(&self) -> Uuid;
    fn name(&self) -> &Name;
    fn description(&self) -> &str;
    fn time_created(&self) -> DateTime<Utc>;
    fn time_modified(&self) -> DateTime<Utc>;
    fn time_deleted(&self) -> Option<DateTime<Utc>>;
}
