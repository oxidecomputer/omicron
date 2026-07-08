// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod artifacts {
    pub use crate::v1::artifacts::*;
}

pub mod bootstrap_sleds {
    pub use crate::v1::bootstrap_sleds::*;
}

pub mod inventory {
    pub use crate::v1::inventory::*;
}

pub mod location {
    pub use crate::v1::location::*;
}

pub mod rss_config {
    pub use crate::v1::rss_config::*;
}

pub mod update {
    pub use crate::v1::update::*;
}
