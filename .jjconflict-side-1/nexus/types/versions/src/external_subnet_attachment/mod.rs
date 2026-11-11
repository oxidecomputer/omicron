// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `EXTERNAL_SUBNET_ATTACHMENT` of the Nexus external API.
//!
//! This version introduced subnet pool and external subnet creation endpoints.
//! The types here are wire-format shims that accept extra fields which were
//! later removed:
//!
//! - `SubnetPoolCreate` accepted a `pool_type` field (validated to be Unicast).
//! - `SubnetPoolMemberAdd` accepted an `identity` field (silently discarded).
//! - `ExternalSubnetAllocator::Explicit` accepted a `pool` field (validated to
//!    be None).

pub mod external_subnet;
pub mod subnet_pool;
