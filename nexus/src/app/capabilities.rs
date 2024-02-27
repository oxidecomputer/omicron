// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Nexus;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use slog::Logger;

impl nexus_capabilities::NexusBaseCapabilities for Nexus {
    fn log(&self) -> &Logger {
        &self.log
    }

    fn datastore(&self) -> &db::DataStore {
        &self.db_datastore
    }
}

impl nexus_capabilities::NexusSledAgentBaseCapabilities for Nexus {}

impl nexus_capabilities::NexusSledAgentCapabilities for Nexus {
    fn opctx(&self) -> &OpContext {
        &self.opctx_alloc
    }
}
