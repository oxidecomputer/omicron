// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting regions that need replacing and beginning that
//! process
//!
//! TODO this is currently a placeholder for a future PR

use super::common::BackgroundTask;
use crate::app::sagas::SagaRequest;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct RegionReplacementDetector {
    _datastore: Arc<DataStore>,
    _saga_request: Sender<SagaRequest>,
}

impl RegionReplacementDetector {
    pub fn new(
        datastore: Arc<DataStore>,
        saga_request: Sender<SagaRequest>,
    ) -> Self {
        RegionReplacementDetector {
            _datastore: datastore,
            _saga_request: saga_request,
        }
    }
}

impl BackgroundTask for RegionReplacementDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            warn!(&log, "region replacement task started");

            // TODO

            warn!(&log, "region replacement task done");

            json!({
                "region_replacement_started_ok": 0,
                "region_replacement_started_err": 0,
            })
        }
        .boxed()
    }
}
