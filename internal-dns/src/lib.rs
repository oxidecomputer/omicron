// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with Omicron-internal DNS (see RFD 248)

pub mod config;
pub mod names;
pub mod resolver;

// We export these names out to the root for compatibility.
pub use config::DnsConfigBuilder;
pub use config::SRV;
pub use names::BackendName;
pub use names::ServiceName;
pub use names::DNS_ZONE;
