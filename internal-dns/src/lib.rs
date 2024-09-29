// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with Omicron-internal DNS (see RFD 248)

pub mod names;
pub mod resolver;

// We export these names out to the root for compatibility.
pub use names::ServiceName;
pub use names::DNS_ZONE;

// The DNS zone configuration is only require inside omicron, not for external
// consumers querying said DNS service.

#[cfg(feature = "omicron-internal")]
pub mod config;
#[cfg(feature = "omicron-internal")]
pub use config::DnsConfigBuilder;
