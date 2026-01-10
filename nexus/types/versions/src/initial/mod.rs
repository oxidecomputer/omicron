// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `INITIAL` of the Nexus external API.
//!
//! This was the first server-side versioned release of the Nexus external API
//! (version 2025112000).

pub mod affinity;
pub mod alert;
pub mod asset;
pub mod audit;
pub mod bfd;
pub mod certificate;
pub mod console;
pub mod device;
pub mod device_params;
pub mod disk;
pub mod external_ip;
pub mod floating_ip;
pub mod hardware;
pub mod headers;
pub mod identity_provider;
pub mod image;
pub mod instance;
pub mod internet_gateway;
pub mod ip_pool;
pub mod metrics;
pub mod multicast;
pub mod networking;
pub mod oxql;
pub mod path_params;
pub mod physical_disk;
pub mod policy;
pub mod probe;
pub mod project;
pub mod rack;
pub mod saml;
pub mod scim;
pub mod silo;
pub mod sled;
pub mod snapshot;
pub mod ssh_key;
pub mod support_bundle;
pub mod switch;
pub mod system;
pub mod timeseries;
pub mod update;
pub mod user;
pub mod vpc;
