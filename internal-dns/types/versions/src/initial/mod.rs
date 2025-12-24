// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `INITIAL` of the DNS server API.
//!
//! This initial version introduces the core DNS configuration types:
//! [`config::DnsConfigParams`], [`config::DnsConfig`], [`config::DnsConfigZone`],
//! [`config::DnsRecord`], and [`config::Srv`].

pub mod config;
