// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `FLOATING_IP_ALLOCATOR_UPDATE` of the Nexus external API.
//!
//! Renames `AddressSelector` to `AddressAllocator` and removes `pool` from
//! `Explicit` variant. Introduces subnet pool and external subnet types.

pub mod external_subnet;
pub mod floating_ip;
pub mod subnet_pool;
