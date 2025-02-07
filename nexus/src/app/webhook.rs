// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::WebhookEvent;
use nexus_db_queries::db::model::WebhookEventClass;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_db_queries::db::model::WebhookSecret;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookEventUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;

impl super::Nexus {
    pub fn webhook_receiver_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        webhook_selector: params::WebhookPath,
    ) -> LookupResult<lookup::WebhookReceiver<'a>> {
        Ok(LookupPath::new(opctx, self.datastore()).webhook_receiver_id(
            WebhookReceiverUuid::from_untyped_uuid(webhook_selector.webhook_id),
        ))
    }

    pub async fn webhook_receiver_config_fetch(
        &self,
        opctx: &OpContext,
        rx: lookup::WebhookReceiver<'_>,
    ) -> LookupResult<WebhookReceiverConfig> {
        let (authz_rx, rx) = rx.fetch().await?;
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

    pub async fn webhook_receiver_secret_add(
        &self,
        opctx: &OpContext,
        rx: lookup::WebhookReceiver<'_>,
        secret: String,
    ) -> Result<views::WebhookSecretId, Error> {
        let (authz_rx,) = rx.lookup_for(authz::Action::CreateChild).await?;
        let secret = WebhookSecret::new(authz_rx.id(), secret);
        let WebhookSecret { identity, .. } = self
            .datastore()
            .webhook_rx_secret_create(opctx, &authz_rx, secret)
            .await?;
        let secret_id = identity.id();
        slog::info!(
            &opctx.log,
            "added secret to webhook receiver";
            "rx_id" => ?authz_rx.id(),
            "secret_id" => ?secret_id,
        );
        Ok(views::WebhookSecretId { id: secret_id.into_untyped_uuid() })
    }
}
