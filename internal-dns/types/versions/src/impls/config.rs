// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::config::{DnsConfigParams, DnsConfigZone, DnsRecord, Srv};
use anyhow::ensure;
use std::net::{Ipv4Addr, Ipv6Addr};

/// Error code used when a record type cannot be represented in an older API
/// version (e.g., NS records in v1).
pub const ERROR_CODE_INCOMPATIBLE_RECORD: &str = "IncompatibleRecord";

/// Error code used when an update is already in progress.
pub const ERROR_CODE_UPDATE_IN_PROGRESS: &str = "UpdateInProgress";

/// Error code used when the provided generation number is stale.
pub const ERROR_CODE_BAD_UPDATE_GENERATION: &str = "BadUpdateGeneration";

impl DnsConfigParams {
    /// Given a high-level DNS configuration, return a reference to its sole
    /// DNS zone.
    ///
    /// # Errors
    ///
    /// Returns an error if there are 0 or more than one zones in this
    /// configuration.
    pub fn sole_zone(&self) -> Result<&DnsConfigZone, anyhow::Error> {
        ensure!(
            self.zones.len() == 1,
            "expected exactly one DNS zone, but found {}",
            self.zones.len()
        );
        Ok(&self.zones[0])
    }
}

// The `From<Ipv4Addr>` and `From<Ipv6Addr>` implementations are very slightly
// dubious, because a v4 or v6 address could also theoretically map to a DNS
// PTR record
// (https://www.cloudflare.com/learning/dns/dns-records/dns-ptr-record/).
// However, we don't support PTR records at the moment, so this is fine. Would
// certainly be worth revisiting if we do in the future, though.

impl From<Ipv4Addr> for DnsRecord {
    fn from(ip: Ipv4Addr) -> Self {
        DnsRecord::A(ip)
    }
}

impl From<Ipv6Addr> for DnsRecord {
    fn from(ip: Ipv6Addr) -> Self {
        DnsRecord::Aaaa(ip)
    }
}

impl From<Srv> for DnsRecord {
    fn from(srv: Srv) -> Self {
        DnsRecord::Srv(srv)
    }
}
