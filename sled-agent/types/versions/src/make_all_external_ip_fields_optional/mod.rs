// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MAKE_ALL_EXTERNAL_IP_FIELDS_OPTIONAL` of the Sled Agent API.
//!
//! Simplifies `ExternalIpConfig` from an enum to a struct with optional IPv4
//! and IPv6 fields, and changes `InstanceSledLocalConfig.external_ips` from
//! `Option<ExternalIpConfig>` to `ExternalIpConfig`.

pub mod instance;
