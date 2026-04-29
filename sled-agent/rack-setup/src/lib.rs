// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service

#[macro_use]
extern crate slog;

mod early_networking;
mod plan;
mod service;

pub use early_networking::EarlyNetworkSetup;
pub use early_networking::EarlyNetworkSetupError;
pub use plan::service::PlannedSledDescription;
pub use plan::service::ServicePlan;
pub use plan::service::SledConfig;
pub use plan::service::from_ipaddr_to_external_floating_ip;
pub use plan::service::from_sockaddr_to_external_floating_addr;
pub use service::LocalBootstrapAgent;
pub use service::RackInitializeRequestParams;
pub use service::RackInitializeRequestParseError;
pub use service::RackSetupService;
pub use service::SetupServiceError;
pub use service::rack_initialize_request_from_file;
