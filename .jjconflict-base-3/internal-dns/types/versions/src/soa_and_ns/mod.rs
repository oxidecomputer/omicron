// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `SOA_AND_NS` of the DNS server API.
//!
//! This version adds:
//!
//! - A `serial` field to [`config::DnsConfigParams`] and
//!   [`config::DnsConfig`] for SOA records.
//! - The [`config::DnsRecord::Ns`] variant for nameserver records.

pub mod config;
