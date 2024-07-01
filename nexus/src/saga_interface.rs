// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces available to saga actions and undo actions

use crate::Nexus;
use nexus_db_queries::{authz, db};
use slog::Logger;
use std::fmt;
use std::sync::Arc;

// TODO-design Should this be the same thing as ServerContext?  It's
// very analogous, but maybe there's utility in having separate views for the
// HTTP server and sagas.
pub(crate) struct SagaContext {
    nexus: Arc<Nexus>,
    log: Logger,
}

impl fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaContext { (nexus) ... }")
    }
}

impl SagaContext {
    pub(crate) fn new(nexus: Arc<Nexus>, log: Logger) -> SagaContext {
        SagaContext { nexus, log }
    }

    pub(crate) fn log(&self) -> &Logger {
        &self.log
    }

    pub(crate) fn authz(&self) -> &Arc<authz::Authz> {
        &self.nexus.authz()
    }

    pub(crate) fn nexus(&self) -> &Arc<Nexus> {
        &self.nexus
    }

    pub(crate) fn datastore(&self) -> &db::DataStore {
        self.nexus.datastore()
    }
}
