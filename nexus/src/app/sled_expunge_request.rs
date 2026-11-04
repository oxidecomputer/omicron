// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::trust_quorum::ReconfigureSendOutcome;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SledExpungeRequestCreateOutcome;
use nexus_db_queries::db::datastore::authz_sled_expunge_request_from_id;
use nexus_db_queries::db::model::SledExpungeRequest;
use nexus_networking::InServiceSledClientError;
use nexus_types::external_api::sled as views;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SledExpungeRequestCreateDisposition {
    Created,
    AlreadyOutstanding,
}

impl From<SledExpungeRequestCreateDisposition>
    for views::SledExpungeRequestDisposition
{
    fn from(disposition: SledExpungeRequestCreateDisposition) -> Self {
        match disposition {
            SledExpungeRequestCreateDisposition::Created => Self::Created,
            SledExpungeRequestCreateDisposition::AlreadyOutstanding => {
                Self::AlreadyOutstanding
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SledExpungeRequestCreated {
    pub(crate) request: SledExpungeRequest,
    pub(crate) disposition: SledExpungeRequestCreateDisposition,
}

// TODO-RAINCLAUDE: whether the removal's send settled: Settled covers the coordinator accepting it and its coordinator not being an in-service sled (nothing was sent; the trust_quorum_manager task will abort the removal, and the sled_expunge task will then fail the request), both of which settle the send, which is when send_uncertain may be cleared; Uncertain does not.
#[derive(Clone, Copy, Debug)]
enum RemovalSendResolution {
    Settled,
    Uncertain,
}

// TODO-RAINCLAUDE: the send's failures are logged here because tq_send_reconfigure returns them as outcomes; their text (internal errors, sled agent responses) never reaches the request row or the API response.
fn resolve_removal_send(
    log: &Logger,
    outcome: &ReconfigureSendOutcome,
) -> RemovalSendResolution {
    match outcome {
        ReconfigureSendOutcome::Sent => {
            info!(log, "sent sled expunge removal config to its coordinator");
            RemovalSendResolution::Settled
        }
        ReconfigureSendOutcome::CoordinatorUnavailable(
            InServiceSledClientError::NotInService { baseboard_id, .. },
        ) => {
            warn!(
                log,
                "sled expunge removal config's coordinator is not an \
                 in-service sled, so nothing was sent; the \
                 trust_quorum_manager task will abort the config, and the \
                 sled_expunge task will then fail the request";
                "coordinator" => %baseboard_id,
            );
            RemovalSendResolution::Settled
        }
        ReconfigureSendOutcome::CoordinatorUnavailable(
            InServiceSledClientError::LookupFailed { error, .. },
        ) => {
            error!(
                log,
                "failed to look up the sled expunge removal config's \
                 coordinator; nothing was sent, so the removal stays \
                 preparing until it is aborted";
                "error" => InlineErrorChain::new(error),
            );
            RemovalSendResolution::Uncertain
        }
        ReconfigureSendOutcome::SendFailed(error) => {
            error!(
                log,
                "failed to send the sled expunge removal config to its \
                 coordinator; it may or may not have received it, so the \
                 removal may stay preparing until it is aborted";
                "error" => InlineErrorChain::new(error),
            );
            RemovalSendResolution::Uncertain
        }
    }
}

impl super::Nexus {
    // TODO-RAINCLAUDE: RFD 747 request creation: Modify on the sled and on its rack's trust quorum config is authorized before the datastore transaction that runs the checks and writes; a new removal is then sent to its coordinator at most once, here and never on retry, since the send is not idempotent.
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
                        SledExpungeRequestCreateDisposition::AlreadyOutstanding,
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
        let send = self.tq_send_reconfigure(opctx, &removal_config).await;
        let resolution = resolve_removal_send(&log, &send);
        self.background_tasks.task_trust_quorum_manager.activate();
        self.background_tasks.task_sled_expunge.activate();

        // TODO-RAINCLAUDE: if clearing send_uncertain fails, the response carries the row as created, which keeps it set, as the stored row does.
        let request = match resolution {
            RemovalSendResolution::Uncertain => request,
            RemovalSendResolution::Settled => match self
                .db_datastore
                .sled_expunge_request_clear_send_uncertain(
                    opctx,
                    &authz_sled_expunge_request_from_id(request.id()),
                )
                .await
            {
                Ok(cleared) => cleared,
                Err(error) => {
                    warn!(
                        log,
                        "failed to clear send_uncertain on a new sled \
                         expunge request after a settled send; the row \
                         keeps it set";
                        "error" => InlineErrorChain::new(&error),
                    );
                    request
                }
            },
        };
        Ok(SledExpungeRequestCreated {
            request,
            disposition: SledExpungeRequestCreateDisposition::Created,
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
