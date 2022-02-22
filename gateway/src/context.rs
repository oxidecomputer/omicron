// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

/// Shared state used by API request handlers
pub struct ServerContext;

impl ServerContext {
    pub fn new() -> Arc<Self> {
        Arc::new(ServerContext)
    }
}
