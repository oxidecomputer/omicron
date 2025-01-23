// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Webhooks

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::WebhookReceiverUuid;

pub const EVENT_CLASSES: &[&str] = &["test"];

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
        self.datastore().webhook_rx_create(&opctx, params, event_classes).await
    }
}
