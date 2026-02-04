// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack init types.

pub mod v1;
pub mod v2;

mod impls;

// Re-export latest version types for convenience.
// Note: New versions of these types will be added to the top-level module.
pub use v1::BfdPeerConfig;
pub use v1::ExternalPortDiscovery;
pub use v1::LldpAdminStatus;
pub use v1::LldpPortConfig;
pub use v1::ParseLldpAdminStatusError;
pub use v1::ParseSwitchLocationError;
pub use v1::PortFec;
pub use v1::PortSpeed;
pub use v1::RouteConfig;
pub use v1::SwitchLocation;
pub use v1::TxEqConfig;

pub use v2::BgpConfig;
pub use v2::BgpPeerConfig;
pub use v2::HostPortConfig;
pub use v2::PortConfig;
pub use v2::RackNetworkConfig;
pub use v2::SwitchPorts;
pub use v2::UplinkAddressConfig;
pub use v2::UplinkAddressConfigError;
