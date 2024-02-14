// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! propagate relevant network information needed for rack startup
//! to downstream bootstore (sled-agent)

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::{context::OpContext, db::DataStore};
use serde_json::json;
use std::sync::Arc;

pub struct BootstoreNetworkSettingsManager {
    datastore: Arc<DataStore>,
}

impl BootstoreNetworkSettingsManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for BootstoreNetworkSettingsManager {
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
