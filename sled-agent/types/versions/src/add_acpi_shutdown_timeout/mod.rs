// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod instance {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use crate::v1::instance::VmmStateRequested;

    /// The body of a request to move a previously-ensured instance into a specific
    /// runtime state.
    #[derive(Serialize, Deserialize, JsonSchema)]
    pub struct VmmPutStateBody {
        /// The state into which the instance should be driven.
        pub state: VmmStateRequested,
        // TODO doc
        pub acpi_timeout_secs: Option<u64>,
    }
}
