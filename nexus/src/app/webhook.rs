// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::WebhookEvent;
use nexus_db_queries::db::model::WebhookEventClass;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::WebhookEventUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;

impl super::Nexus {
    pub async fn webhook_receiver_config_fetch(
        &self,
        opctx: &OpContext,
        id: WebhookReceiverUuid,
    ) -> LookupResult<WebhookReceiverConfig> {
        let (authz_rx, rx) = LookupPath::new(opctx, &self.datastore())
            .webhook_receiver_id(id)
            .fetch()
            .await?;
        let (events, secrets) =
            self.datastore().webhook_rx_config_fetch(opctx, &authz_rx).await?;
        Ok(WebhookReceiverConfig { rx, secrets, events })
    }

    pub async fn webhook_receiver_create(
        &self,
        opctx: &OpContext,
        params: params::WebhookCreate,
    ) -> CreateResult<WebhookReceiverConfig> {
        // TODO(eliza): validate endpoint URI; reject underlay network IPs for
        // SSRF prevention...
        self.datastore().webhook_rx_create(&opctx, params).await
    }

    pub async fn webhook_event_publish(
        &self,
        opctx: &OpContext,
        id: WebhookEventUuid,
        event_class: WebhookEventClass,
        event: serde_json::Value,
    ) -> Result<WebhookEvent, Error> {
        let event = self
            .datastore()
            .webhook_event_create(opctx, id, event_class, event)
            .await?;
        slog::debug!(
            &opctx.log,
            "enqueued webhook event";
            "event_id" => ?id,
            "event_class" => %event.event_class,
            "time_created" => ?event.time_created,
        );

        // Once the event has been isnerted, activate the dispatcher task to
        // ensure its propagated to receivers.
        self.background_tasks.task_webhook_dispatcher.activate();

        Ok(event)
    }
}
