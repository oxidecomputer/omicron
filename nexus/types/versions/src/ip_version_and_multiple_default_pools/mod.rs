// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `IP_VERSION_AND_MULTIPLE_DEFAULT_POOLS` of the Nexus external API.
//!
//! This version (2025122300) supports multiple default pools but SiloIpPool
//! views don't yet include ip_version or pool_type fields.

pub mod ip_pool;
