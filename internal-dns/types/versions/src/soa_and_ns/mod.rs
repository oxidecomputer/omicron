// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DNS configuration types for API version 2.0.0 (SOA_AND_NS).
//!
//! This version adds:
//!
//! - A `serial` field to [`config::DnsConfigParams`] and
//!   [`config::DnsConfig`] for SOA records.
//! - The [`config::DnsRecord::Ns`] variant for nameserver records.

pub mod config;
