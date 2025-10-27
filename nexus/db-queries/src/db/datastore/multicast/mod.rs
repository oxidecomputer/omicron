// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group management and IP allocation.
//!
//! This module provides database operations for multicast groups following
//! the bifurcated design from [RFD 488](https://rfd.shared.oxide.computer/rfd/488):
//!
//! - External groups: External-facing, allocated from IP pools
//! - Underlay groups: System-generated admin-scoped IPv6 multicast groups
//!
//! ## Typed UUID Usage
//!
//! Public datastore functions in this module use typed UUIDs for type safety:
//!
//! - **Public functions** use `MulticastGroupUuid` and `InstanceUuid` for:
//!   - Type safety at API boundaries
//!   - Clear documentation of expected ID types
//!   - Preventing UUID type confusion

pub mod groups;
pub mod members;
pub mod ops;
