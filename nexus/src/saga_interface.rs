// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces available to saga actions and undo actions

use crate::Nexus;
use crate::{authz, db};
use omicron_common::api::external::Error;
use sled_agent_client::Client as SledAgentClient;
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

// TODO-design Should this be the same thing as ServerContext?  It's
// very analogous, but maybe there's utility in having separate views for the
// HTTP server and sagas.
pub struct SagaContext {
    nexus: Arc<Nexus>,
    log: Logger,
    authz: Arc<authz::Authz>,
}

impl fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaContext { (nexus) ... }")
    }
}

impl SagaContext {
    pub fn new(
        nexus: Arc<Nexus>,
        log: Logger,
        authz: Arc<authz::Authz>,
    ) -> SagaContext {
        SagaContext { authz, nexus, log }
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn authz(&self) -> &Arc<authz::Authz> {
        &self.authz
    }

    pub fn nexus(&self) -> &Arc<Nexus> {
        &self.nexus
    }

    pub fn datastore(&self) -> &db::DataStore {
        self.nexus.datastore()
    }

    pub async fn sled_client(
        &self,
        sled_id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        self.nexus.sled_client(sled_id).await
    }

    pub async fn random_sled_client(
        &self,
    ) -> Result<Option<Arc<SledAgentClient>>, Error> {
        Ok(match self.nexus.random_sled_id().await? {
            Some(sled_id) => Some(self.nexus.sled_client(&sled_id).await?),
            None => None,
        })
    }
}
