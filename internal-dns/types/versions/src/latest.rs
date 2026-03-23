// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of each type.

pub mod config {
    pub use crate::v2::config::DnsConfig;
    pub use crate::v2::config::DnsConfigParams;
    pub use crate::v2::config::DnsConfigZone;
    pub use crate::v2::config::DnsRecord;
    pub use crate::v2::config::Srv;

    pub use crate::impls::config::ERROR_CODE_BAD_UPDATE_GENERATION;
    pub use crate::impls::config::ERROR_CODE_INCOMPATIBLE_RECORD;
    pub use crate::impls::config::ERROR_CODE_UPDATE_IN_PROGRESS;
}
