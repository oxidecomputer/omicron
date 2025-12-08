// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type migrations for the Sled Agent API.
//!
//! This crate contains all published types for the Sled Agent API, organized by
//! the first API version in which they were introduced. Types that change
//! between versions are defined in the version module where they changed.
//!
//! ## Usage
//!
//! The API crate (`sled-agent-api`) uses fixed identifiers from this crate
//! directly, e.g., `sled_agent_types_migrations::v1::inventory::Inventory`.
//!
//! The types crate (`sled-agent-types`) re-exports the latest versions as
//! floating identifiers for use by business logic.

pub mod bootstrap_v1;
pub mod latest;
pub mod v1;
pub mod v10;
pub mod v3;
pub mod v4;
pub mod v6;
pub mod v7;
pub mod v9;
