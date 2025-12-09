// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common types for MGS.
//!
//! This crate re-exports the latest versions of all types from the
//! `gateway-types-versions` crate. These are floating identifiers that should
//! be used by business logic that doesn't need to care about API versioning.
//!
//! The API crate (`gateway-api`) uses fixed identifiers from the versions
//! crate directly.

pub mod caboose;
pub mod component;
pub mod component_details;
pub mod host;
pub mod ignition;
pub mod rot;
pub mod sensor;
pub mod task_dump;
pub mod update;
