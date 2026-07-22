// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod rack_setup {
    pub use crate::v1::rack_setup::AllowedSourceIps;
    pub use crate::v1::rack_setup::BgpAuthKeyId;
    pub use crate::v1::rack_setup::BgpConfig;
    pub use crate::v1::rack_setup::CertificateUploadResponse;
    pub use crate::v1::rack_setup::IpAllowList;
    pub use crate::v1::rack_setup::IpRange;
    pub use crate::v1::rack_setup::Ipv4Range;
    pub use crate::v1::rack_setup::Ipv6Range;
    pub use crate::v1::rack_setup::LinkFec;
    pub use crate::v1::rack_setup::LinkSpeed;
    pub use crate::v1::rack_setup::LldpAdminStatus;
    pub use crate::v1::rack_setup::LldpPortConfig;
    pub use crate::v1::rack_setup::ManualPortConfig;
    pub use crate::v1::rack_setup::MaxPathConfig;
    pub use crate::v1::rack_setup::PutRssUserConfigInsensitive;
    pub use crate::v1::rack_setup::RouteConfig;
    pub use crate::v1::rack_setup::RouterLifetimeConfig;
    pub use crate::v1::rack_setup::RouterPeerIpAddr;
    pub use crate::v1::rack_setup::TxEqConfig;
    pub use crate::v1::rack_setup::UplinkAddress;
    pub use crate::v1::rack_setup::UplinkIpNet;
    pub use crate::v1::rack_setup::UserSpecifiedBgpPeerConfig;
    pub use crate::v1::rack_setup::UserSpecifiedImportExportPolicy;
    pub use crate::v1::rack_setup::UserSpecifiedPortConfig;
    pub use crate::v1::rack_setup::UserSpecifiedRackNetworkConfig;
    pub use crate::v1::rack_setup::UserSpecifiedRouterPeerAddr;
    pub use crate::v1::rack_setup::UserSpecifiedUplinkAddressConfig;
}

pub mod update {
    pub use crate::v1::update::EmptyUpdateTargets;
    pub use crate::v1::update::UpdateTargets;
}
