// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_SYSTEM_IP_POOL_APIS` of the Nexus external API.
//!
//! This version (2026_06_02_00) adds an `assignment` field to `IpPool`,
//! making it an operator-facing view indicating whether the pool serves
//! customer silos or Oxide system services. Operator endpoints now return
//! this new `IpPool`. The dedicated service-pool endpoints are deprecated in
//! this version.

pub mod ip_pool;
