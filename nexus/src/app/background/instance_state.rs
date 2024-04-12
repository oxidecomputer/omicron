// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use futures::{future::BoxFuture, FutureExt};
use nexus_db_queries::{context::OpContext, db::DataStore};
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;

/// Background task that periodically checks instance states.
pub struct InstanceWatcher {
    datastore: Arc<DataStore>,
}

impl BackgroundTask for InstanceWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let sleds_and_vmms =
                self.datastore.vmm_list_by_sled_agent(opctx).await;

            todo!()
        }
        .boxed()
    }
}
