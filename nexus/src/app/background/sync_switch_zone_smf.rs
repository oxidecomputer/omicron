// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Update SMF properties in switch zones
//! These SMF properties track which interfaces are uplinks. Networking
//! services need this information to properly enable external communication
//! during rack startup

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::{context::OpContext, db::DataStore};
use serde_json::json;
use std::sync::Arc;

pub struct SwitchZoneSmfManager {
    datastore: Arc<DataStore>,
}

impl SwitchZoneSmfManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for SwitchZoneSmfManager {
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
