// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that dispatches queued webhook events to receivers.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::{
    WebhookDispatched, WebhookDispatcherStatus,
};
use omicron_common::api::external::Error;
use std::sync::Arc;

pub struct WebhookDispatcher {
    datastore: Arc<DataStore>,
    deliverator: Activator,
}

impl BackgroundTask for WebhookDispatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let mut dispatched = Vec::new();
            let error =
                match self.actually_activate(&opctx, &mut dispatched).await {
                    Ok(_) => {
                        const MSG: &str =
                            "webhook dispatching completed successfully";
                        if !dispatched.is_empty() {
                            slog::info!(
                                &opctx.log,
                                "{MSG}";
                                "events_dispatched" => dispatched.len(),
                            );
                        } else {
                            // no sense cluttering up the logs if we didn't do
                            // anyuthing interesting today`s`
                            slog::trace!(
                                &opctx.log,
                                "{MSG}";
                                "events_dispatched" => dispatched.len(),
                            );
                        };

                        None
                    }
                    Err(error) => {
                        slog::error!(
                            &opctx.log,
                            "webhook dispatching failed";
                            "events_dispatched" => dispatched.len(),
                            "error" => &error,
                        );
                        Some(error.to_string())
                    }
                };

            // If any new deliveries were dispatched, call the deliverator!
            if !dispatched.is_empty() {
                self.deliverator.activate();
            }

            serde_json::json!(WebhookDispatcherStatus { dispatched, error })
        })
    }
}

impl WebhookDispatcher {
    pub fn new(datastore: Arc<DataStore>, deliverator: Activator) -> Self {
        Self { datastore, deliverator }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        dispatched: &mut Vec<WebhookDispatched>,
    ) -> Result<(), Error> {
        while let Some(event) =
            self.datastore.webhook_event_dispatch_next(opctx).await?
        {
            dispatched.push(event);
        }
        Ok(())
    }
}
