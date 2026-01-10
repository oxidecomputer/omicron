// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `POOL_SELECTION_ENUMS` of the Nexus external API.
//!
//! This version (2026010500) refactors IP pool selection to use tagged enums
//! (`PoolSelector` and `AddressSelector`) that make invalid states
//! unrepresentable.

pub mod floating_ip;
pub mod instance;
pub mod ip_pool;
pub mod probe;
