// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `SILO_PROJECT_IP_VERSION_AND_POOL_TYPE` of the Nexus external API.
//!
//! This version (2026_01_01_00) adds `ip_version` and `pool_type` fields to
//! SiloIpPool responses, and uses old-style single-IP network interfaces.

pub mod floating_ip;
pub mod instance;
pub mod ip_pool;
