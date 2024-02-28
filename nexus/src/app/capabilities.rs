// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Nexus;
use nexus_db_queries::db;
use slog::Logger;

impl nexus_capabilities::Base for Nexus {
    fn log(&self) -> &Logger {
        &self.log
    }

    fn datastore(&self) -> &db::DataStore {
        &self.db_datastore
    }
}

// `Nexus` proper has all capabilities. Other contexts (background tasks, sagas)
// may choose to implement objects with a subset.
impl nexus_capabilities::SledAgent for Nexus {
    fn opctx_sled_client(&self) -> &nexus_db_queries::context::OpContext {
        // This is a bit of an historical artifact. We use the "instance
        // allocation" op context to create sled-agent clients.
        &self.opctx_alloc
    }
}

impl nexus_capabilities::FirewallRules for Nexus {}
