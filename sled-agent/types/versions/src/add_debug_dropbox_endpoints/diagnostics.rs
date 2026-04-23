// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics types added in `ADD_DEBUG_DROPBOX_ENDPOINTS`.

use schemars::JsonSchema;
use serde::Deserialize;

/// Path parameters for sled-diagnostics debug dropbox download requests used
/// by support bundles.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsDebugDropboxDownloadPathParam {
    /// The zone whose debug dropbox data should be collected
    pub zone: String,
}
