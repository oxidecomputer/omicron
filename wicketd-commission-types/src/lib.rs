// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common types for the Wicketd Commission API.
//!
//! This crate re-exports the latest versions of all types from the
//! `wicketd-commission-types-versions` crate. These are floating identifiers
//! that should be used by business logic that doesn't need to care about API
//! versioning.
//!
//! The API crate (`wicketd-commission-api`) uses fixed identifiers from the
//! versions crate directly.

pub mod artifacts;
pub mod bootstrap_sleds;
pub mod inventory;
pub mod location;
pub mod rss_config;
pub mod update;
