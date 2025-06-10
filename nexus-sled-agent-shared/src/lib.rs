// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal types shared between Nexus and sled-agent, with extra dependencies
//! not in omicron-common.
//!
//! Only types that are shared between `nexus-types` and `sled-agent-types`
//! should go here.
//!
//! - If a type is used by `sled-agent-api` and Nexus, but is not required by
//!   `nexus-types`, it should go in `sled-agent-types` instead.
//! - If a type is used by `nexus-internal-api` and Nexus, but is not required
//!   by `sled-agent-types`, it should go in `nexus-types` instead.
//!
//! For more information, see the crate [README](../README.md).

pub mod inventory;
pub mod recovery_silo;
pub mod zone_images;
