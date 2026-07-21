// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod inventory {
    pub use crate::v1::inventory::BootstrapSled;
    pub use crate::v1::inventory::Caboose;
    pub use crate::v1::inventory::CmisDatapath;
    pub use crate::v1::inventory::CmisDatapathState;
    pub use crate::v1::inventory::CmisLaneStatus;
    pub use crate::v1::inventory::FaultFlag;
    pub use crate::v1::inventory::IgnitionFaults;
    pub use crate::v1::inventory::LocationInfo;
    pub use crate::v1::inventory::PowerState;
    pub use crate::v1::inventory::RotInfo;
    pub use crate::v1::inventory::RotSlot;
    pub use crate::v1::inventory::Sff8636LaneFaults;
    pub use crate::v1::inventory::SlotCaboose;
    pub use crate::v1::inventory::SpIdentifier;
    pub use crate::v1::inventory::SpIgnitionInfo;
    pub use crate::v1::inventory::SpInfo;
    pub use crate::v1::inventory::SpInventory;
    pub use crate::v1::inventory::SpInventoryParams;
    pub use crate::v1::inventory::SpStateInfo;
    pub use crate::v1::inventory::SpType;
    pub use crate::v1::inventory::Stage0Caboose;
    pub use crate::v1::inventory::SwitchSlot;
    pub use crate::v1::inventory::SwitchTransceivers;
    pub use crate::v1::inventory::Transceiver;
    pub use crate::v1::inventory::TransceiverDatapath;
    pub use crate::v1::inventory::TransceiverInventory;
    pub use crate::v1::inventory::TransceiverMonitors;
    pub use crate::v1::inventory::TransceiverStatus;
    pub use crate::v1::inventory::TransceiverVendor;
}

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
    pub use crate::v1::rack_setup::NewPasswordHash;
    pub use crate::v1::rack_setup::PutRecoveryUserPasswordHash;
    pub use crate::v1::rack_setup::PutRssUserConfigInsensitive;
    pub use crate::v1::rack_setup::RackOperationStatus;
    pub use crate::v1::rack_setup::RouteConfig;
    pub use crate::v1::rack_setup::RouterLifetimeConfig;
    pub use crate::v1::rack_setup::RouterPeerIpAddr;
    pub use crate::v1::rack_setup::RssStepInfo;
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
    pub use crate::v1::update::ClearUpdateStateParams;
    pub use crate::v1::update::ClearUpdateStateResponse;
    pub use crate::v1::update::EmptyUpdateTargets;
    pub use crate::v1::update::RepositoryDescription;
    pub use crate::v1::update::RunningProgress;
    pub use crate::v1::update::SpUpdateProgress;
    pub use crate::v1::update::StartUpdateOptions;
    pub use crate::v1::update::StartUpdateParams;
    pub use crate::v1::update::StepOutcome;
    pub use crate::v1::update::StepProgress;
    pub use crate::v1::update::UpdateProgress;
    pub use crate::v1::update::UpdateState;
    pub use crate::v1::update::UpdateStep;
    pub use crate::v1::update::UpdateStepStatus;
    pub use crate::v1::update::UpdateTargets;
}
