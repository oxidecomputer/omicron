// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack init types.

pub mod v1;
pub mod v2;

// Re-export latest version types for convenience.
// Note: New versions of these types will be added to the top-level module.
pub use v2::BfdPeerConfig;
pub use v2::BgpConfig;
pub use v2::BgpPeerConfig;
pub use v2::ExternalPortDiscovery;
pub use v2::HostPortConfig;
pub use v2::LldpAdminStatus;
pub use v2::LldpPortConfig;
pub use v2::ParseLldpAdminStatusError;
pub use v2::ParseSwitchLocationError;
pub use v2::PortConfig;
pub use v2::PortFec;
pub use v2::PortSpeed;
pub use v2::RackNetworkConfig;
pub use v2::RouteConfig;
pub use v2::SwitchLocation;
pub use v2::SwitchPorts;
pub use v2::TxEqConfig;
pub use v2::UplinkAddressConfig;
pub use v2::UplinkAddressConfigError;
