// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Read trust quorum related tables from the database and drive configuration
//! by talking to sled-agents.

use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use iddqd::IdHashMap;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::trust_quorum::TrustQuorumConfig;
use serde_json::json;
use std::sync::Arc;

pub struct TrustQuorumManager {
    datastore: Arc<DataStore>,

    // Trust quorum configurations for the latest `epoch` per `rack_id`
    latest_configs: IdHashMap<TrustQuorumConfig>,
}

impl TrustQuorumManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore, latest_configs: IdHashMap::new() }
    }
}

impl BackgroundTask for TrustQuorumManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a nexus_auth::context::OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {

        // First we need to see if any trust quorums have changed in the database
        // If so, we load them.
        //

        // Then we loop through the latest trust quorum configs and see if any
        // work needs to be done
    }
}
