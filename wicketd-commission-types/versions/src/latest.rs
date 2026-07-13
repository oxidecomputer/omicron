// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod rack_setup {
    pub use crate::v1::rack_setup::BgpAuthKeyId;
    pub use crate::v1::rack_setup::CertificateUploadResponse;
    pub use crate::v1::rack_setup::ManualPortConfig;
    pub use crate::v1::rack_setup::PutRssUserConfigInsensitive;
    pub use crate::v1::rack_setup::UserSpecifiedBgpPeerConfig;
    pub use crate::v1::rack_setup::UserSpecifiedImportExportPolicy;
    pub use crate::v1::rack_setup::UserSpecifiedPortConfig;
    pub use crate::v1::rack_setup::UserSpecifiedRackNetworkConfig;
    pub use crate::v1::rack_setup::UserSpecifiedRouterPeerAddr;
    pub use crate::v1::rack_setup::UserSpecifiedUplinkAddressConfig;
}
