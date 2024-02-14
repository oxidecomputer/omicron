// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! propagate routing changes in Nexus to downstream routing daemons (mgd)

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::{context::OpContext, db::DataStore};
use serde_json::json;
use std::sync::Arc;

pub struct SwitchRoutingManager {
    datastore: Arc<DataStore>,
}

impl SwitchRoutingManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for SwitchRoutingManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            json!({})
        }
        .boxed()
    }
}
