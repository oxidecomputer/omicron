// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multirack join completion

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The result of completing a multirack join
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct MultirackJoinCompleteResponse {
    /// The latest bootstore generation when join completes.
    pub bootstore_generation: u64,
}
