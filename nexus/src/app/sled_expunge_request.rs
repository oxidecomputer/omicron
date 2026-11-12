// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SledExpungeRequestCreateOutcome;
use nexus_db_queries::db::model::SledExpungeRequest;
use nexus_types::external_api::sled as views;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SledExpungeRequestCreated {
    pub(crate) request: SledExpungeRequest,
    pub(crate) disposition: views::SledExpungeRequestDisposition,
}

impl super::Nexus {
    // TODO-RAINCLAUDE: RFD 747 request creation: a new removal is sent to its coordinator at most once, here and never on retry, since the send is not idempotent.
    pub(crate) async fn sled_expunge_request_create(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        acknowledged_serial: &str,
    ) -> Result<SledExpungeRequestCreated, Error> {
        let (authz_sled, sled) = self
            .sled_lookup(opctx, &sled_id)?
            .fetch_for(authz::Action::Modify)
            .await?;
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(sled.rack_id());
        // TODO-RAINCLAUDE: the collection is read from the database rather than the inventory loader's watch channel, so the scrimlet check sees the newest one; it is dropped before the send.
        let outcome = {
            let latest_collection = self
                .db_datastore
                .inventory_get_latest_collection(opctx)
                .await?;
            self.db_datastore
                .sled_expunge_request_create(
                    opctx,
                    &authz_sled,
                    &authz_tq,
                    acknowledged_serial,
                    latest_collection.as_ref(),
                )
                .await?
        };

        let (request, removal_config) = match outcome {
            SledExpungeRequestCreateOutcome::AlreadyOutstanding { request } => {
                self.background_tasks.task_sled_expunge.activate();
                return Ok(SledExpungeRequestCreated {
                    request,
                    disposition:
                        views::SledExpungeRequestDisposition::AlreadyOutstanding,
                });
            }
            SledExpungeRequestCreateOutcome::Created {
                request,
                removal_config,
            } => (request, removal_config),
        };

        let log = opctx.log.new(o!(
            "sled_id" => sled_id.to_string(),
            "request_id" => request.id().to_string(),
            "epoch" => removal_config.epoch.to_string(),
        ));
        match self.tq_send_reconfigure(opctx, &removal_config).await {
            Ok(()) => {
                info!(
                    log,
                    "sent sled expunge removal config to its coordinator"
                );
            }
            // TODO-RAINCLAUDE: the request and its removal config are already committed, so a failed send is logged rather than failing the call; the removal then stays preparing until it is aborted, and the sled_expunge task reports it as stuck.
            Err(error) => {
                warn!(
                    log,
                    "failed to send sled expunge removal config to its \
                     coordinator";
                    "error" => InlineErrorChain::new(&error),
                );
            }
        }
        self.background_tasks.task_trust_quorum_manager.activate();
        self.background_tasks.task_sled_expunge.activate();

        Ok(SledExpungeRequestCreated {
            request,
            disposition: views::SledExpungeRequestDisposition::Created,
        })
    }

    pub(crate) async fn sled_expunge_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<SledExpungeRequest> {
        self.db_datastore.sled_expunge_request_list(opctx, pagparams).await
    }

    pub(crate) async fn sled_expunge_request_view(
        &self,
        opctx: &OpContext,
        id: SledExpungeRequestUuid,
    ) -> LookupResult<SledExpungeRequest> {
        self.db_datastore.sled_expunge_request_get(opctx, id).await
    }
}
