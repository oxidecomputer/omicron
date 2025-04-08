// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service

mod plan;
/// The main implementation of the RSS service.
pub mod service;

pub use plan::service::SledConfig;
pub use plan::service::{
    from_ipaddr_to_external_floating_ip,
    from_sockaddr_to_external_floating_addr,
    from_source_nat_config_to_external_snat_ip,
};
