// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces available to saga actions and undo actions

use crate::Nexus;
use crate::app::background::BackgroundTasks;
use nexus_auth::{authz, context::OpContext};
use nexus_db_queries::db;
use nexus_saga_interface::{DataStoreContext, NexusInterface};
use omicron_common::api::external::Error;
use slog::Logger;
use std::fmt;
use std::sync::Arc;

// TODO-design Should this be the same thing as ServerContext?  It's
// very analogous, but maybe there's utility in having separate views for the
// HTTP server and sagas.
pub struct SagaContext {
    nexus: Arc<Nexus>,
    log: Logger,
    context2: Arc<nexus_saga_interface::SagaContext>,
}

impl fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaContext { (nexus) ... }")
    }
}

impl SagaContext {
    pub(crate) fn new(nexus: Arc<Nexus>, log: Logger) -> SagaContext {
        let nexus_context = nexus_saga_interface::NexusContext::new(Arc::new(
            NexusWrapper::new(nexus.clone()),
        ));
        let context2 = Arc::new(nexus_saga_interface::SagaContext::new(
            log.clone(),
            nexus_context,
        ));
        SagaContext { nexus, log, context2 }
    }

    pub(crate) fn context2(&self) -> &Arc<nexus_saga_interface::SagaContext> {
        &self.context2
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

struct NexusWrapper {
    nexus: Arc<Nexus>,
    datastore_context: DataStoreContext,
}

impl NexusWrapper {
    fn new(nexus: Arc<Nexus>) -> Self {
        let datastore = nexus.datastore().clone();
        let datastore_context = DataStoreContext::new(datastore);
        Self { nexus, datastore_context }
    }
}

#[async_trait::async_trait]
impl NexusInterface for NexusWrapper {
    fn authz(&self) -> &Arc<authz::Authz> {
        self.nexus.authz()
    }

    fn datastore(&self) -> &DataStoreContext {
        &self.datastore_context
    }

    fn background_tasks(&self) -> &BackgroundTasks {
        self.nexus.background_tasks()
    }

    async fn instance_delete_dpd_config(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> Result<(), Error> {
        self.nexus.instance_delete_dpd_config(opctx, authz_instance).await
    }
}
