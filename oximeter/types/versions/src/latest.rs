// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.

pub mod collector {
    pub use crate::v1::collector::CollectorInfo;
}

pub mod producer {
    pub use crate::v1::producer::FailedCollection;
    pub use crate::v1::producer::ProducerDetails;
    pub use crate::v1::producer::ProducerIdPathParams;
    pub use crate::v1::producer::ProducerPage;
    pub use crate::v1::producer::SuccessfulCollection;
}
