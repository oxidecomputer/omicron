// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use super::ValidateTransition;
use super::sled::TransitionError;
use super::trust_quorum::i64_to_epoch;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateAndQueryResult;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_lookup::LookupPath;
use nexus_db_model::Sled;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestAdvance;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_model::SledExpungeRequestTransition;
use nexus_db_model::SledExpungeRequestUnfinishedState;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use nexus_types::trust_quorum::TrustQuorumConfigState;
use nexus_types::trust_quorum::check_sled_expunge_task_gate;
use nexus_types::trust_quorum::sled_expunge_task_gate_base_epoch;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use trust_quorum_types::types::Epoch;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRequestUpdateOutcome {
    Updated,
    NotInExpectedState { current: SledExpungeRequest },
}

impl From<UpdateAndQueryResult<SledExpungeRequest>>
    for SledExpungeRequestUpdateOutcome
{
    fn from(result: UpdateAndQueryResult<SledExpungeRequest>) -> Self {
        match result.status {
            UpdateStatus::Updated => Self::Updated,
            UpdateStatus::NotUpdatedButExists => {
                Self::NotInExpectedState { current: result.found }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRequestCompleteOutcome {
    Completed { previous_sled_policy: SledPolicy },
    NotExpunging { current: SledExpungeRequest },
    ReAddPreparing { epoch: Epoch },
    ReAdded { epoch: Epoch, state: TrustQuorumConfigState },
}

// TODO-RAINCLAUDE: the transitions sled_expunge_request_transition may write; Done is left out so that only sled_expunge_request_complete enters it, in the same transaction that expunges the sled.
enum UngatedTransition {
    Advance(SledExpungeRequestAdvance),
    Failed { from: SledExpungeRequestUnfinishedState, failure_reason: String },
}

impl From<UngatedTransition> for SledExpungeRequestTransition {
    fn from(transition: UngatedTransition) -> Self {
        match transition {
            UngatedTransition::Advance(advance) => Self::Advance(advance),
            UngatedTransition::Failed { from, failure_reason } => {
                Self::Failed { from, failure_reason }
            }
        }
    }
}

impl DataStore {
    pub async fn sled_expunge_request_get(
        &self,
        opctx: &OpContext,
        id: SledExpungeRequestUuid,
    ) -> LookupResult<SledExpungeRequest> {
        let (.., request) = LookupPath::new(opctx, self)
            .sled_expunge_request(id)
            .fetch()
            .await?;
        Ok(request)
    }

    // TODO-RAINCLAUDE: pages by time_created and then id, like support_bundle_list, using the lookup_sled_expunge_request_by_creation index.
    pub async fn sled_expunge_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (chrono::DateTime<chrono::Utc>, Uuid)>,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::SLED_EXPUNGE_REQUEST_LIST,
            )
            .await?;
        use crate::db::pagination::paginated_multicolumn;
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated_multicolumn(
            dsl::sled_expunge_request,
            (dsl::time_created, dsl::id),
            pagparams,
        )
        .select(SledExpungeRequest::as_select())
        .load_async(&*conn)
        .await
        .map_err(|error| public_error_from_diesel(error, ErrorHandler::Server))
    }

    // TODO-RAINCLAUDE: every Nexus's sled_expunge task reads this each activation; batched like other background-task list-all helpers, across all racks; the batches are not one snapshot, so callers must compare-and-swap and tolerate one activation of lag.
    pub async fn sled_expunge_request_list_unfinished(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::SLED_EXPUNGE_REQUEST_LIST,
            )
            .await?;
        opctx.check_complex_operations_allowed()?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let mut requests = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = paginated(
                dsl::sled_expunge_request,
                dsl::id,
                &p.current_pagparams(),
            )
            .filter(dsl::state.eq_any(SledExpungeRequestState::UNFINISHED))
            .select(SledExpungeRequest::as_select())
            .load_async(&*conn)
            .await
            .map_err(|error| {
                public_error_from_diesel(error, ErrorHandler::Server)
            })?;
            paginator = p
                .found_batch(&batch, &|request: &SledExpungeRequest| {
                    request.id.into_untyped_uuid()
                });
            requests.extend(batch);
        }
        Ok(requests)
    }

    pub async fn sled_expunge_request_advance(
        &self,
        opctx: &OpContext,
        authz_request: &authz::SledExpungeRequest,
        advance: SledExpungeRequestAdvance,
    ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
        self.sled_expunge_request_transition(
            opctx,
            authz_request,
            UngatedTransition::Advance(advance),
        )
        .await
    }

    // TODO-RAINCLAUDE: failure_reason is shown to operators as is, so it must be operator-safe text; the power-off outcome already on the row is kept.
    pub async fn sled_expunge_request_fail(
        &self,
        opctx: &OpContext,
        authz_request: &authz::SledExpungeRequest,
        from: SledExpungeRequestUnfinishedState,
        failure_reason: String,
    ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
        self.sled_expunge_request_transition(
            opctx,
            authz_request,
            UngatedTransition::Failed { from, failure_reason },
        )
        .await
    }

    async fn sled_expunge_request_transition(
        &self,
        opctx: &OpContext,
        authz_request: &authz::SledExpungeRequest,
        transition: UngatedTransition,
    ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
        opctx.authorize(authz::Action::Modify, authz_request).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = to_db_typed_uuid(authz_request.id());
        let transition = SledExpungeRequestTransition::from(transition);
        let from = transition.from_state().state();
        diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(id))
            .filter(dsl::state.eq(from))
            .set(transition.columns(now_db_precision()))
            .check_if_exists::<SledExpungeRequest>(id)
            .execute_and_check(&conn)
            .await
            .map(SledExpungeRequestUpdateOutcome::from)
            .map_err(|error| {
                public_error_from_diesel(
                    error,
                    ErrorHandler::NotFoundByResource(authz_request),
                )
            })
    }

    // TODO-RAINCLAUDE: last_error is shown to operators as is, so callers must pass operator-safe text (no raw MGS or SQL errors); a finished request is never touched, because expected_state is always unfinished. Within one state this is last-writer-wins, not a compare-and-swap on the previous message.
    pub async fn sled_expunge_request_set_last_error(
        &self,
        opctx: &OpContext,
        authz_request: &authz::SledExpungeRequest,
        expected_state: SledExpungeRequestUnfinishedState,
        last_error: String,
    ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
        opctx.authorize(authz::Action::Modify, authz_request).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = to_db_typed_uuid(authz_request.id());
        diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(id))
            .filter(dsl::state.eq(expected_state.state()))
            .set((
                dsl::last_error.eq(Some(last_error)),
                dsl::time_modified.eq(now_db_precision()),
            ))
            .check_if_exists::<SledExpungeRequest>(id)
            .execute_and_check(&conn)
            .await
            .map(SledExpungeRequestUpdateOutcome::from)
            .map_err(|error| {
                public_error_from_diesel(
                    error,
                    ErrorHandler::NotFoundByResource(authz_request),
                )
            })
    }

    // TODO-RAINCLAUDE: the expunging step's one transaction (RFD 747): require expunging, run the task gate rather than the lockstep path's strict check, expunge the sled and its disks, then compare-and-swap to done; any error rolls all of it back. Repeating it is safe because the state re-read at the start of the transaction returns NotExpunging once the request is done; the final compare-and-swap is only an assertion. A sled that is already expunged is left alone so that a sled expunged out of band (for example through the lockstep path), even one decommissioned since, still completes.
    pub async fn sled_expunge_request_complete(
        &self,
        opctx: &OpContext,
        authz_request: &authz::SledExpungeRequest,
    ) -> Result<SledExpungeRequestCompleteOutcome, Error> {
        opctx.authorize(authz::Action::Modify, authz_request).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = to_db_typed_uuid(authz_request.id());

        // TODO-RAINCLAUDE: sled_id, rack_id, the baseboard and tq_epoch never change after insert, so reading them before the transaction is safe; they are needed to authorize the sled and trust quorum accesses up front.
        let request = Self::sled_expunge_request_get_conn(&conn, authz_request)
            .await
            .map_err(|error| {
                public_error_from_diesel(
                    error,
                    ErrorHandler::NotFoundByResource(authz_request),
                )
            })?;
        let sled_id = SledUuid::from(request.sled_id);
        let rack_id = RackUuid::from(request.rack_id);
        let authz_sled =
            authz::Sled::new(authz::FLEET, sled_id, LookupType::by_id(sled_id));
        opctx.authorize(authz::Action::Modify, &authz_sled).await?;
        opctx
            .authorize(
                authz::Action::Read,
                &authz::TrustQuorumConfig::for_rack_id(rack_id),
            )
            .await?;
        let tq_epoch = i64_to_epoch(request.tq_epoch)?;
        let baseboard_id = request.baseboard_id();
        let context = format!(
            "sled expunge request {id} for {baseboard_id} (rack {rack_id}, \
             removal epoch {tq_epoch})"
        );

        let err = OptionalError::new();
        self.transaction_retry_wrapper("sled_expunge_request_complete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let baseboard_id = baseboard_id.clone();
                let context = context.clone();
                async move {
                    let internal_error = |message: String| {
                        err.bail(TransitionError::External(
                            Error::internal_error(&format!(
                                "{context}: {message}"
                            )),
                        ))
                    };

                    let Some(current) =
                        Self::sled_expunge_request_get_conn(&conn, authz_request)
                            .await
                            .optional()?
                    else {
                        return Err(err.bail(TransitionError::External(
                            authz_request.not_found(),
                        )));
                    };
                    let transition = SledExpungeRequestTransition::Done;
                    let from = transition.from_state().state();
                    if current.state != from {
                        return Ok(
                            SledExpungeRequestCompleteOutcome::NotExpunging {
                                current,
                            },
                        );
                    }

                    // TODO-RAINCLAUDE: the loaders' own errors (for example a rack with no trust quorum config) are internal errors here, since the request was created against these configs; database errors pass through untouched so that restarts stay retryable.
                    let Some(removal_config) =
                        Self::tq_get_config_with_members_from_epoch_conn(
                            &conn, rack_id, tq_epoch,
                        )
                        .await
                        .map_err(|error| {
                            tq_load_error_into_diesel(
                                &err,
                                &context,
                                "the removal trust quorum configuration",
                                error,
                            )
                        })?
                    else {
                        return Err(internal_error(
                            "no trust quorum configuration exists at the \
                             removal epoch"
                                .to_string(),
                        ));
                    };
                    let (latest_non_aborted_config, _) =
                        Self::tq_load_latest_possible_committed_config_conn(
                            &conn, rack_id,
                        )
                        .await
                        .map_err(|error| {
                            tq_load_error_into_diesel(
                                &err,
                                &context,
                                "the latest non-aborted trust quorum \
                                 configuration",
                                error,
                            )
                        })?;
                    let base_epoch = sled_expunge_task_gate_base_epoch(
                        &removal_config,
                        &latest_non_aborted_config,
                    )
                    .map_err(|error| internal_error(error.to_string()))?;
                    // TODO-RAINCLAUDE: when the Preparing latest config builds directly on the removal, the removal config already loaded is its base.
                    let loaded_base_config = match base_epoch {
                        Some(base_epoch) if base_epoch != tq_epoch => {
                            let Some(base_config) =
                                Self::tq_get_config_with_members_from_epoch_conn(
                                    &conn, rack_id, base_epoch,
                                )
                                .await
                                .map_err(|error| {
                                    tq_load_error_into_diesel(
                                        &err,
                                        &context,
                                        "the base trust quorum configuration",
                                        error,
                                    )
                                })?
                            else {
                                return Err(internal_error(format!(
                                    "no trust quorum configuration exists at \
                                     epoch {base_epoch}, the base of the \
                                     latest configuration (epoch {})",
                                    latest_non_aborted_config.epoch,
                                )));
                            };
                            Some(base_config)
                        }
                        None | Some(_) => None,
                    };
                    let latest_base_config = loaded_base_config
                        .as_ref()
                        .or(base_epoch.map(|_| &removal_config));
                    let gate = check_sled_expunge_task_gate(
                        &removal_config,
                        &latest_non_aborted_config,
                        latest_base_config,
                        &baseboard_id,
                    )
                    .map_err(|error| internal_error(error.to_string()))?;
                    // TODO-RAINCLAUDE: RemovalPreparing and RemovalAborted are internal errors here: the task contract (not enforced here) is that a request only reaches expunging after the gate saw its removal past Preparing, and a config past Preparing can never go back or be aborted.
                    match gate {
                        SledExpungeTaskGate::SledRemoved => {}
                        SledExpungeTaskGate::ReAddPreparing { epoch } => {
                            return Ok(
                                SledExpungeRequestCompleteOutcome::ReAddPreparing {
                                    epoch,
                                },
                            );
                        }
                        SledExpungeTaskGate::ReAdded { epoch, state } => {
                            return Ok(
                                SledExpungeRequestCompleteOutcome::ReAdded {
                                    epoch,
                                    state,
                                },
                            );
                        }
                        SledExpungeTaskGate::RemovalPreparing
                        | SledExpungeTaskGate::RemovalAborted => {
                            return Err(internal_error(format!(
                                "the request is expunging, but the task gate \
                                 reported {gate:?}"
                            )));
                        }
                    }

                    let sled = {
                        use nexus_db_schema::schema::sled::dsl;
                        dsl::sled
                            .filter(dsl::time_deleted.is_null())
                            .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
                            .select(Sled::as_select())
                            .get_result_async(&conn)
                            .await
                            .optional()?
                    };
                    let Some(sled) = sled else {
                        return Err(internal_error(format!(
                            "its sled {sled_id} does not exist"
                        )));
                    };
                    if sled.rack_id() != rack_id {
                        return Err(internal_error(format!(
                            "its sled {sled_id} is in rack {}",
                            sled.rack_id(),
                        )));
                    }
                    let sled_baseboard_id = BaseboardId {
                        part_number: sled.part_number().to_string(),
                        serial_number: sled.serial_number().to_string(),
                    };
                    if sled_baseboard_id != baseboard_id {
                        return Err(internal_error(format!(
                            "its sled {sled_id} has baseboard \
                             {sled_baseboard_id}"
                        )));
                    }
                    let previous_sled_policy = match sled.policy() {
                        SledPolicy::Expunged => SledPolicy::Expunged,
                        SledPolicy::InService { .. } => {
                            Self::sled_set_policy_conn(
                                &conn,
                                sled_id,
                                SledPolicy::Expunged,
                                ValidateTransition::Yes,
                                &err,
                            )
                            .await?
                        }
                    };

                    let n_updated = diesel::update(dsl::sled_expunge_request)
                        .filter(dsl::id.eq(id))
                        .filter(dsl::state.eq(from))
                        .set(transition.columns(now_db_precision()))
                        .execute_async(&conn)
                        .await?;
                    if n_updated != 1 {
                        return Err(internal_error(format!(
                            "the request was read as {from} in this \
                             transaction, but moving it to done updated \
                             {n_updated} rows"
                        )));
                    }

                    Ok(SledExpungeRequestCompleteOutcome::Completed {
                        previous_sled_policy,
                    })
                }
            })
            .await
            .map_err(|error| match err.take() {
                Some(TransitionError::External(error)) => error,
                Some(error @ TransitionError::InvalidTransition { .. }) => {
                    Error::internal_error(&format!(
                        "{context}: {}",
                        InlineErrorChain::new(&error),
                    ))
                }
                None => public_error_from_diesel(error, ErrorHandler::Server)
                    .internal_context(context),
            })
    }

    async fn sled_expunge_request_get_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_request: &authz::SledExpungeRequest,
    ) -> Result<SledExpungeRequest, diesel::result::Error> {
        dsl::sled_expunge_request
            .filter(dsl::id.eq(to_db_typed_uuid(authz_request.id())))
            .select(SledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
    }
}

// TODO-RAINCLAUDE: maps a trust quorum loader's error inside sled_expunge_request_complete's transaction: database errors are returned untouched so restarts stay retryable, and the loader's own errors roll back as internal errors carrying the request context.
fn tq_load_error_into_diesel(
    err: &OptionalError<TransitionError>,
    context: &str,
    what: &str,
    error: TransactionError<Error>,
) -> DieselError {
    match error {
        TransactionError::Database(error) => error,
        TransactionError::CustomError(error) => {
            err.bail(TransitionError::External(Error::internal_error(
                &format!("{context}: loading {what}: {}", error_text(&error)),
            )))
        }
    }
}

// TODO-RAINCLAUDE: an error's message without the variant's display prefix (such as "Internal Error: "), for wrapping in another internal error.
fn error_text(error: &Error) -> String {
    match error {
        Error::InternalError { internal_message }
        | Error::Unauthenticated { internal_message }
        | Error::ServiceUnavailable { internal_message }
        | Error::TypeVersionMismatch { internal_message } => {
            internal_message.clone()
        }
        Error::InvalidRequest { message }
        | Error::InvalidValue { message, .. }
        | Error::InsufficientCapacity { message }
        | Error::Conflict { message }
        | Error::NotFound { message } => message.external_message().to_string(),
        Error::ObjectNotFound { .. }
        | Error::ObjectAlreadyExists { .. }
        | Error::Forbidden
        | Error::Gone => InlineErrorChain::new(error).to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::SQL_BATCH_SIZE;
    use super::SledExpungeRequestCompleteOutcome;
    use super::SledExpungeRequestUpdateOutcome;
    use super::TransitionError;
    use super::tq_load_error_into_diesel;
    use crate::authn;
    use crate::authz;
    use crate::context::OpContext;
    use crate::db::DataStore;
    use crate::db::datastore::trust_quorum::tests::ack_all_prepares;
    use crate::db::datastore::trust_quorum::tests::insert_hw_baseboard_ids;
    use crate::db::datastore::trust_quorum::tests::insert_sled_for_baseboard;
    use crate::db::datastore::trust_quorum::tests::make_authz_tq;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::sled_expunge_request::TEST_FAILURE_REASON;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use crate::db::pub_test_utils::sled_expunge_request::new_sled_expunge_request;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::prelude::*;
    use diesel::result::DatabaseErrorKind;
    use diesel::result::Error as DieselError;
    use itertools::iproduct;
    use nexus_db_errors::OptionalError;
    use nexus_db_errors::TransactionError;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::DbTypedUuid;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::SledExpungePowerOffOutcome;
    use nexus_db_model::SledExpungeRequest;
    use nexus_db_model::SledExpungeRequestAdvance;
    use nexus_db_model::SledExpungeRequestState;
    use nexus_db_model::SledExpungeRequestTransition;
    use nexus_db_model::SledExpungeRequestUnfinishedState;
    use nexus_db_model::to_db_typed_uuid;
    use nexus_db_schema::schema::sled_expunge_request::dsl;
    use nexus_types::external_api::sled::SledPolicy;
    use nexus_types::external_api::sled::SledProvisionPolicy;
    use nexus_types::trust_quorum::IsLrtqUpgrade;
    use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
    use nexus_types::trust_quorum::TrustQuorumConfigState;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::LookupType;
    use omicron_common::api::external::ResourceType;
    use omicron_common::now_db_precision;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledExpungeRequestKind;
    use omicron_uuid_kinds::SledExpungeRequestUuid;
    use omicron_uuid_kinds::SledUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use trust_quorum_types::types::Epoch;
    use uuid::Uuid;

    const TQ_EPOCH_IS_POSITIVE: &str = "tq_epoch_is_positive";
    const TIME_COMPLETED_IFF_FINISHED: &str = "time_completed_iff_finished";
    const FAILURE_REASON_IFF_FAILED: &str = "failure_reason_iff_failed";
    const POWER_OFF_OUTCOME_MATCHES_STATE: &str =
        "power_off_outcome_matches_state";
    const LAST_ERROR_ONLY_IF_UNFINISHED: &str = "last_error_only_if_unfinished";
    const UNFINISHED_BY_SLED_INDEX: &str =
        "lookup_unfinished_sled_expunge_request_by_sled";
    const UNFINISHED_BY_BASEBOARD_INDEX: &str =
        "lookup_unfinished_sled_expunge_request_by_baseboard";
    const BY_RACK_AND_EPOCH_INDEX: &str =
        "lookup_sled_expunge_request_by_rack_and_epoch";

    type Conn = async_bb8_diesel::Connection<nexus_db_lookup::DbConnection>;

    fn unique_baseboard() -> BaseboardId {
        BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: format!("serial-{}", Uuid::new_v4()),
        }
    }

    fn request_in_valid_state(
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
        state: SledExpungeRequestState,
    ) -> SledExpungeRequest {
        new_sled_expunge_request(
            RackUuid::new_v4(),
            sled_id,
            baseboard_id,
            2,
            state,
        )
    }

    async fn insert(
        conn: &Conn,
        request: &SledExpungeRequest,
    ) -> Result<usize, DieselError> {
        diesel::insert_into(dsl::sled_expunge_request)
            .values(request.clone())
            .execute_async(conn)
            .await
    }

    async fn read_back(
        conn: &Conn,
        id: DbTypedUuid<SledExpungeRequestKind>,
    ) -> SledExpungeRequest {
        dsl::sled_expunge_request
            .filter(dsl::id.eq(id))
            .select(SledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
            .expect("read back the inserted row")
    }

    fn expect_violation(
        result: &Result<usize, DieselError>,
        kind: DatabaseErrorKind,
        context: &str,
    ) -> String {
        let Err(DieselError::DatabaseError(actual_kind, info)) = result else {
            panic!("expected {kind:?} ({context})\nresult: {result:#?}");
        };
        assert_eq!(
            std::mem::discriminant(actual_kind),
            std::mem::discriminant(&kind),
            "expected {kind:?} ({context}), got {actual_kind:?}: {info:?}",
        );
        info.constraint_name()
            .unwrap_or_else(|| {
                panic!("{kind:?} names its constraint ({context}): {info:?}")
            })
            .to_string()
    }

    #[tokio::test]
    async fn test_sled_expunge_request_check_constraints() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_check_constraints");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();
        let now = now_db_precision();

        use SledExpungePowerOffOutcome::PoweredOff;
        use SledExpungeRequestState::*;
        let valid = [
            (WaitingForTrustQuorum, None, false, false, false),
            (WaitingForTrustQuorum, None, false, false, true),
            (PoweringOff, None, false, false, false),
            (PoweringOff, None, false, false, true),
            (Expunging, Some(PoweredOff), false, false, false),
            (Expunging, Some(PoweredOff), false, false, true),
            (Done, Some(PoweredOff), false, true, false),
            (Failed, None, true, true, false),
            (Failed, Some(PoweredOff), true, true, false),
        ];

        let outcomes: Vec<Option<SledExpungePowerOffOutcome>> =
            std::iter::once(None)
                .chain(
                    SledExpungePowerOffOutcome::ALL.iter().copied().map(Some),
                )
                .collect();

        let mut n_valid_inserted = 0;
        let mut reported = BTreeSet::new();
        for (
            &state,
            &power_off_outcome,
            has_failure_reason,
            has_time_completed,
            has_last_error,
        ) in iproduct!(
            SledExpungeRequestState::ALL,
            &outcomes,
            [false, true],
            [false, true],
            [false, true]
        ) {
            let request = SledExpungeRequest {
                state,
                power_off_outcome,
                send_uncertain: false,
                last_error: has_last_error
                    .then(|| "test last error".to_string()),
                failure_reason: has_failure_reason
                    .then(|| TEST_FAILURE_REASON.to_string()),
                time_completed: has_time_completed.then_some(now),
                time_created: now,
                time_modified: now,
                ..request_in_valid_state(
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    WaitingForTrustQuorum,
                )
            };
            let result = insert(&conn, &request).await;

            let key = (
                state,
                power_off_outcome,
                has_failure_reason,
                has_time_completed,
                has_last_error,
            );
            if valid.contains(&key) {
                if let Err(error) = &result {
                    panic!(
                        "expected a valid row to be inserted\n\
                         row: {request:#?}\nerror: {error:#?}"
                    );
                }
                assert_eq!(read_back(&conn, request.id).await, request);
                n_valid_inserted += 1;
            } else {
                reported.insert(expect_violation(
                    &result,
                    DatabaseErrorKind::CheckViolation,
                    &format!("{key:?}"),
                ));
            }
        }
        assert_eq!(n_valid_inserted, valid.len());
        assert_eq!(
            reported,
            [
                TIME_COMPLETED_IFF_FINISHED,
                FAILURE_REASON_IFF_FAILED,
                POWER_OFF_OUTCOME_MATCHES_STATE,
                LAST_ERROR_ONLY_IF_UNFINISHED,
            ]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>(),
        );

        for tq_epoch in [0, -1, i64::MIN] {
            let request = SledExpungeRequest {
                tq_epoch,
                ..request_in_valid_state(
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    WaitingForTrustQuorum,
                )
            };
            let constraint = expect_violation(
                &insert(&conn, &request).await,
                DatabaseErrorKind::CheckViolation,
                &format!("tq_epoch {tq_epoch}"),
            );
            assert_eq!(constraint, TQ_EPOCH_IS_POSITIVE);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_unique_indexes() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_unique_indexes");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();

        for (&existing_state, &new_state) in iproduct!(
            SledExpungeRequestState::ALL,
            SledExpungeRequestState::ALL
        ) {
            let context = format!("{existing_state:?} then {new_state:?}");
            let both_unfinished =
                !existing_state.is_finished() && !new_state.is_finished();

            let sled_id = SledUuid::new_v4();
            insert(
                &conn,
                &request_in_valid_state(
                    sled_id,
                    unique_baseboard(),
                    existing_state,
                ),
            )
            .await
            .expect("inserted the first request for a sled");
            let result = insert(
                &conn,
                &request_in_valid_state(sled_id, unique_baseboard(), new_state),
            )
            .await;
            if both_unfinished {
                let constraint = expect_violation(
                    &result,
                    DatabaseErrorKind::UniqueViolation,
                    &format!("same sled, {context}"),
                );
                assert_eq!(constraint, UNFINISHED_BY_SLED_INDEX);
            } else {
                result.unwrap_or_else(|error| {
                    panic!("same sled, {context}: {error:#?}")
                });
            }

            let baseboard_id = unique_baseboard();
            insert(
                &conn,
                &request_in_valid_state(
                    SledUuid::new_v4(),
                    baseboard_id.clone(),
                    existing_state,
                ),
            )
            .await
            .expect("inserted the first request for a baseboard");
            let result = insert(
                &conn,
                &request_in_valid_state(
                    SledUuid::new_v4(),
                    baseboard_id,
                    new_state,
                ),
            )
            .await;
            if both_unfinished {
                let constraint = expect_violation(
                    &result,
                    DatabaseErrorKind::UniqueViolation,
                    &format!("same baseboard, {context}"),
                );
                assert_eq!(constraint, UNFINISHED_BY_BASEBOARD_INDEX);
            } else {
                result.unwrap_or_else(|error| {
                    panic!("same baseboard, {context}: {error:#?}")
                });
            }
        }

        let first = request_in_valid_state(
            SledUuid::new_v4(),
            unique_baseboard(),
            SledExpungeRequestState::Failed,
        );
        insert(&conn, &first).await.expect("inserted the first request");
        let same_epoch = SledExpungeRequest {
            rack_id: first.rack_id,
            tq_epoch: first.tq_epoch,
            ..request_in_valid_state(
                SledUuid::new_v4(),
                unique_baseboard(),
                SledExpungeRequestState::Failed,
            )
        };
        let constraint = expect_violation(
            &insert(&conn, &same_epoch).await,
            DatabaseErrorKind::UniqueViolation,
            "same rack and epoch",
        );
        assert_eq!(constraint, BY_RACK_AND_EPOCH_INDEX);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn opctx_as(db: &TestDatabase, authn: authn::Context) -> OpContext {
        let log = db.opctx().log.clone();
        OpContext::for_background(
            log.clone(),
            Arc::new(authz::Authz::new(&log)),
            authn,
            Arc::clone(db.datastore()) as Arc<dyn nexus_auth::storage::Storage>,
        )
    }

    async fn insert_in_state(
        db: &TestDatabase,
        rack_id: RackUuid,
        tq_epoch: i64,
        state: SledExpungeRequestState,
    ) -> SledExpungeRequest {
        insert_sled_expunge_request(
            db.datastore(),
            rack_id,
            &unique_baseboard(),
            tq_epoch,
            state,
        )
        .await
    }

    #[tokio::test]
    async fn test_sled_expunge_request_list_unfinished() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_list_unfinished");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // TODO-RAINCLAUDE: cycling through every state gives more than SQL_BATCH_SIZE unfinished rows, so the listing spans two batches and skips the finished rows in both.
        let batch_size = usize::try_from(SQL_BATCH_SIZE.get()).unwrap();
        let rows: Vec<_> = SledExpungeRequestState::ALL
            .iter()
            .cycle()
            .take(2 * batch_size)
            .map(|&state| {
                new_sled_expunge_request(
                    RackUuid::new_v4(),
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    1,
                    state,
                )
            })
            .collect();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let inserted: Vec<SledExpungeRequest> =
            diesel::insert_into(dsl::sled_expunge_request)
                .values(rows)
                .returning(SledExpungeRequest::as_returning())
                .get_results_async(&*conn)
                .await
                .expect("bulk-inserted requests in every state");
        let mut expected: Vec<_> = inserted
            .into_iter()
            .filter(|request| !request.state.is_finished())
            .collect();
        expected.sort_by_key(|request| request.id.into_untyped_uuid());
        assert!(
            expected.len() > batch_size,
            "{} unfinished rows span more than one batch of {batch_size}",
            expected.len(),
        );

        let listed = datastore
            .sled_expunge_request_list_unfinished(opctx)
            .await
            .expect("listed unfinished requests");
        assert_eq!(listed, expected);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_viewer_denied() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_viewer_denied");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = opctx_as(&db, authn::Context::internal_read());

        let request = insert_in_state(
            &db,
            RackUuid::new_v4(),
            1,
            SledExpungeRequestState::WaitingForTrustQuorum,
        )
        .await;
        let not_found = Error::ObjectNotFound {
            type_name: ResourceType::SledExpungeRequest,
            lookup_type: LookupType::ById(request.id().into_untyped_uuid()),
        };

        assert_eq!(
            datastore
                .sled_expunge_request_get(&opctx, request.id())
                .await
                .expect_err("a fleet viewer is denied get"),
            not_found,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list(&opctx, &DataPageParams::max_page())
                .await
                .expect_err("a fleet viewer is denied list"),
            Error::Forbidden,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list_unfinished(&opctx)
                .await
                .expect_err("a fleet viewer is denied list_unfinished"),
            Error::Forbidden,
        );
        let authz_request = authz_for(&request);
        for op in TransitionOp::ALL {
            assert_eq!(
                op.run(datastore, &opctx, &authz_request)
                    .await
                    .expect_err(&format!("a fleet viewer is denied {op:?}")),
                not_found,
            );
        }
        assert_eq!(
            datastore
                .sled_expunge_request_complete(&opctx, &authz_request)
                .await
                .expect_err("a fleet viewer is denied complete"),
            not_found,
        );
        assert_eq!(read(datastore, &request).await, request);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn authz_for(request: &SledExpungeRequest) -> authz::SledExpungeRequest {
        authz::SledExpungeRequest::new(
            authz::FLEET,
            request.id(),
            LookupType::by_id(request.id()),
        )
    }

    async fn read(
        datastore: &DataStore,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequest {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        read_back(&conn, request.id).await
    }

    // TODO-RAINCLAUDE: moves time_modified an hour into the past, so a following write must strictly increase it whatever the clock resolution.
    async fn backdated(
        datastore: &DataStore,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequest {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(request.id))
            .set(
                dsl::time_modified
                    .eq(request.time_modified - chrono::Duration::hours(1)),
            )
            .execute_async(&*conn)
            .await
            .expect("backdated time_modified");
        read_back(&conn, request.id).await
    }

    const TRANSITION_FAILURE_REASON: &str = "transition failure";
    const TRANSITION_LAST_ERROR: &str = "transition last error";

    #[derive(Clone, Copy, Debug)]
    enum TransitionOp {
        Advance(SledExpungeRequestAdvance),
        Fail(SledExpungeRequestUnfinishedState),
        SetLastError(SledExpungeRequestUnfinishedState),
    }

    impl TransitionOp {
        const ALL: [Self; 8] = [
            Self::Advance(SledExpungeRequestAdvance::ToPoweringOff),
            Self::Advance(SledExpungeRequestAdvance::ToExpunging {
                power_off_outcome: SledExpungePowerOffOutcome::PoweredOff,
            }),
            Self::Fail(
                SledExpungeRequestUnfinishedState::WaitingForTrustQuorum,
            ),
            Self::Fail(SledExpungeRequestUnfinishedState::PoweringOff),
            Self::Fail(SledExpungeRequestUnfinishedState::Expunging),
            Self::SetLastError(
                SledExpungeRequestUnfinishedState::WaitingForTrustQuorum,
            ),
            Self::SetLastError(SledExpungeRequestUnfinishedState::PoweringOff),
            Self::SetLastError(SledExpungeRequestUnfinishedState::Expunging),
        ];

        fn from_state(self) -> SledExpungeRequestUnfinishedState {
            match self {
                Self::Advance(advance) => advance.from_state(),
                Self::Fail(from) | Self::SetLastError(from) => from,
            }
        }

        async fn run(
            self,
            datastore: &DataStore,
            opctx: &OpContext,
            authz_request: &authz::SledExpungeRequest,
        ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
            match self {
                Self::Advance(advance) => {
                    datastore
                        .sled_expunge_request_advance(
                            opctx,
                            authz_request,
                            advance,
                        )
                        .await
                }
                Self::Fail(from) => {
                    datastore
                        .sled_expunge_request_fail(
                            opctx,
                            authz_request,
                            from,
                            TRANSITION_FAILURE_REASON.to_string(),
                        )
                        .await
                }
                Self::SetLastError(from) => {
                    datastore
                        .sled_expunge_request_set_last_error(
                            opctx,
                            authz_request,
                            from,
                            TRANSITION_LAST_ERROR.to_string(),
                        )
                        .await
                }
            }
        }

        fn apply_to(
            self,
            before: SledExpungeRequest,
            time_modified: chrono::DateTime<chrono::Utc>,
        ) -> SledExpungeRequest {
            match self {
                Self::Advance(advance) => {
                    SledExpungeRequestTransition::Advance(advance)
                        .columns(time_modified)
                        .apply_to(before)
                }
                Self::Fail(from) => SledExpungeRequestTransition::Failed {
                    from,
                    failure_reason: TRANSITION_FAILURE_REASON.to_string(),
                }
                .columns(time_modified)
                .apply_to(before),
                Self::SetLastError(_) => SledExpungeRequest {
                    last_error: Some(TRANSITION_LAST_ERROR.to_string()),
                    time_modified,
                    ..before
                },
            }
        }
    }

    #[tokio::test]
    async fn test_sled_expunge_request_transitions() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_transitions");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());

        for (&start, op) in
            iproduct!(SledExpungeRequestState::ALL, TransitionOp::ALL)
        {
            let request =
                insert_in_state(&db, RackUuid::new_v4(), 1, start).await;
            let before = backdated(datastore, &request).await;
            let outcome = op
                .run(datastore, opctx, &authz_for(&before))
                .await
                .unwrap_or_else(|error| panic!("{op:?} from {start}: {error}"));
            let after = read(datastore, &before).await;
            if op.from_state().state() == start {
                assert_eq!(
                    outcome,
                    SledExpungeRequestUpdateOutcome::Updated,
                    "{op:?} from {start}",
                );
                assert!(
                    after.time_modified > before.time_modified,
                    "{op:?} from {start} moves time_modified forward",
                );
                assert_eq!(
                    after,
                    op.apply_to(before, after.time_modified),
                    "{op:?} from {start}",
                );
            } else {
                assert_eq!(
                    outcome,
                    SledExpungeRequestUpdateOutcome::NotInExpectedState {
                        current: before.clone(),
                    },
                    "{op:?} from {start}",
                );
                assert_eq!(after, before, "{op:?} from {start}");
            }
        }

        let missing = SledExpungeRequestUuid::new_v4();
        let authz_missing = authz::SledExpungeRequest::new(
            authz::FLEET,
            missing,
            LookupType::by_id(missing),
        );
        let not_found = Error::ObjectNotFound {
            type_name: ResourceType::SledExpungeRequest,
            lookup_type: LookupType::ById(missing.into_untyped_uuid()),
        };
        for op in TransitionOp::ALL {
            assert_eq!(
                op.run(datastore, opctx, &authz_missing)
                    .await
                    .expect_err(&format!("{op:?} on a missing request fails")),
                not_found,
            );
        }
        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &authz_missing)
                .await
                .expect_err("completing a missing request fails"),
            not_found,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    struct CompleteFixture {
        rack_id: RackUuid,
        hw_ids: Vec<HwBaseboardId>,
        members: BTreeSet<BaseboardId>,
        target: BaseboardId,
        sled_id: SledUuid,
        disk_id: PhysicalDiskUuid,
    }

    impl CompleteFixture {
        // TODO-RAINCLAUDE: a rack whose epoch 1 trust quorum holds ten baseboards, the last of which (never the coordinator) has a sled with one physical disk, and the first of which has a sled so that it can coordinate.
        async fn new(db: &TestDatabase) -> Self {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let hw_ids = insert_hw_baseboard_ids(db).await;
            let rack_id = RackUuid::new_v4();
            let target_hw = hw_ids.last().expect("at least one baseboard");
            let sled_id =
                insert_sled_for_baseboard(datastore, rack_id, target_hw).await;
            insert_sled_for_baseboard(datastore, rack_id, &hw_ids[0]).await;
            let disk = PhysicalDisk::from_parts(
                PhysicalDiskUuid::new_v4(),
                "vendor".to_string(),
                format!("serial-{sled_id}"),
                "model".to_string(),
                PhysicalDiskKind::U2,
                sled_id,
            );
            let disk_id = disk.id();
            datastore
                .physical_disk_insert(opctx, disk)
                .await
                .expect("inserted a physical disk");

            let members: BTreeSet<BaseboardId> =
                hw_ids.iter().cloned().map(BaseboardId::from).collect();
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            DataStore::tq_insert_rss_config_after_handoff(
                opctx,
                &conn,
                make_authz_tq(rack_id),
                members.clone(),
                members.first().expect("at least one member").clone(),
            )
            .await
            .expect("inserted the epoch 1 trust quorum config");

            Self {
                rack_id,
                target: BaseboardId::from(target_hw.clone()),
                hw_ids,
                members,
                sled_id,
                disk_id,
            }
        }

        fn members_without(
            &self,
            removed: &[&BaseboardId],
        ) -> BTreeSet<BaseboardId> {
            self.members
                .iter()
                .filter(|member| !removed.contains(member))
                .cloned()
                .collect()
        }

        async fn propose(
            &self,
            db: &TestDatabase,
            epoch: u64,
            last_committed_epoch: u64,
            members: BTreeSet<BaseboardId>,
        ) {
            db.datastore()
                .tq_insert_latest_config(
                    db.opctx(),
                    ProposedTrustQuorumConfig {
                        rack_id: self.rack_id,
                        epoch: Epoch(epoch),
                        is_lrtq_upgrade: IsLrtqUpgrade::No {
                            last_committed_epoch: Epoch(last_committed_epoch),
                        },
                        members,
                    },
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("proposed the epoch {epoch} config: {error}")
                });
        }

        async fn start_committing(&self, db: &TestDatabase, epoch: u64) {
            assert_eq!(
                ack_all_prepares(
                    db.datastore(),
                    db.opctx(),
                    self.rack_id,
                    epoch
                )
                .await,
                TrustQuorumConfigState::Committing,
                "epoch {epoch} starts committing",
            );
        }

        async fn commit(&self, db: &TestDatabase, epoch: u64) {
            self.start_committing(db, epoch).await;
            self.finish_committing(db, epoch).await;
        }

        async fn finish_committing(&self, db: &TestDatabase, epoch: u64) {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let config = datastore
                .tq_get_config(opctx, make_authz_tq(self.rack_id), Epoch(epoch))
                .await
                .expect("read the trust quorum config")
                .expect("the trust quorum config exists");
            assert_eq!(
                datastore
                    .tq_update_commit_status(
                        opctx,
                        make_authz_tq(self.rack_id),
                        Epoch(epoch),
                        config.members.keys().cloned().collect(),
                    )
                    .await
                    .expect("acked every commit"),
                TrustQuorumConfigState::Committed,
                "epoch {epoch} commits",
            );
        }

        async fn propose_removal(&self, db: &TestDatabase) {
            self.propose(db, 2, 1, self.members_without(&[&self.target])).await;
        }

        async fn insert_request(
            &self,
            db: &TestDatabase,
            tq_epoch: i64,
            state: SledExpungeRequestState,
        ) -> SledExpungeRequest {
            self.insert_request_for_sled(db, self.sled_id, tq_epoch, state)
                .await
        }

        async fn insert_request_for_sled(
            &self,
            db: &TestDatabase,
            sled_id: SledUuid,
            tq_epoch: i64,
            state: SledExpungeRequestState,
        ) -> SledExpungeRequest {
            insert_sled_expunge_request_for_sled(
                db.datastore(),
                self.rack_id,
                sled_id,
                &self.target,
                tq_epoch,
                state,
            )
            .await
        }

        fn internal_error(
            &self,
            request: &SledExpungeRequest,
            message: &str,
        ) -> Error {
            Error::internal_error(&format!(
                "sled expunge request {} for {} (rack {}, removal epoch {}): \
                 {message}",
                request.id, self.target, self.rack_id, request.tq_epoch,
            ))
        }

        async fn sled_and_disk_policy(
            &self,
            db: &TestDatabase,
        ) -> (SledPolicy, PhysicalDiskPolicy) {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let (_, sled) = LookupPath::new(opctx, datastore)
                .sled_id(self.sled_id)
                .fetch()
                .await
                .expect("looked up the sled");
            let disk_policy = {
                use nexus_db_schema::schema::physical_disk::dsl;
                let conn = datastore.pool_connection_for_tests().await.unwrap();
                dsl::physical_disk
                    .filter(dsl::id.eq(to_db_typed_uuid(self.disk_id)))
                    .select(PhysicalDisk::as_select())
                    .get_result_async(&*conn)
                    .await
                    .expect("looked up the physical disk")
                    .disk_policy
            };
            (sled.policy(), disk_policy)
        }
    }

    const IN_SERVICE: (SledPolicy, PhysicalDiskPolicy) = (
        SledPolicy::InService {
            provision_policy: SledProvisionPolicy::Provisionable,
        },
        PhysicalDiskPolicy::InService,
    );
    const EXPUNGED: (SledPolicy, PhysicalDiskPolicy) =
        (SledPolicy::Expunged, PhysicalDiskPolicy::Expunged);

    async fn set_last_error(
        db: &TestDatabase,
        opctx: &OpContext,
        request: &SledExpungeRequest,
        message: &str,
    ) -> SledExpungeRequest {
        let from = request
            .state
            .as_unfinished()
            .expect("last_error is only set on an unfinished request");
        assert_eq!(
            db.datastore()
                .sled_expunge_request_set_last_error(
                    opctx,
                    &authz_for(request),
                    from,
                    message.to_string(),
                )
                .await
                .expect("set last_error"),
            SledExpungeRequestUpdateOutcome::Updated,
        );
        read(db.datastore(), request).await
    }

    #[track_caller]
    fn assert_done(before: &SledExpungeRequest, after: &SledExpungeRequest) {
        let time_completed =
            after.time_completed.expect("a done request has time_completed");
        assert_eq!(time_completed, after.time_modified);
        assert!(time_completed >= before.time_modified);
        assert_eq!(
            after,
            &SledExpungeRequest {
                state: SledExpungeRequestState::Done,
                last_error: None,
                time_completed: Some(time_completed),
                time_modified: after.time_modified,
                ..before.clone()
            },
        );
        assert_eq!(
            after,
            &SledExpungeRequestTransition::Done
                .columns(after.time_modified)
                .apply_to(before.clone()),
            "the SQL update matches apply_to",
        );
    }

    #[tokio::test]
    async fn test_sled_expunge_request_complete() {
        let logctx = dev::test_setup_log("test_sled_expunge_request_complete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());
        let fixture = CompleteFixture::new(&db).await;
        fixture.propose_removal(&db).await;
        fixture.start_committing(&db, 2).await;

        let request = fixture
            .insert_request(&db, 2, SledExpungeRequestState::Expunging)
            .await;
        let request =
            set_last_error(&db, opctx, &request, "sled was powered back on")
                .await;
        let authz_request = authz_for(&request);

        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &authz_request)
                .await
                .expect("completed the request"),
            SledExpungeRequestCompleteOutcome::Completed {
                previous_sled_policy: IN_SERVICE.0,
            },
        );
        let done = read(datastore, &request).await;
        assert_done(&request, &done);
        assert_eq!(
            done.power_off_outcome,
            Some(SledExpungePowerOffOutcome::PoweredOff),
        );
        assert_eq!(fixture.sled_and_disk_policy(&db).await, EXPUNGED);

        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &authz_request)
                .await
                .expect("complete succeeds"),
            SledExpungeRequestCompleteOutcome::NotExpunging {
                current: done.clone(),
            },
        );
        assert_eq!(read(datastore, &request).await, done);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_complete_gate_refuses() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_complete_gate_refuses",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());
        let fixture = CompleteFixture::new(&db).await;
        fixture.propose_removal(&db).await;
        fixture.commit(&db, 2).await;
        // TODO-RAINCLAUDE: check_no_new_member_being_expunged_conn refuses re-adding a sled with an unfinished request, so the re-add is proposed before the request row exists; the gate's re-added outcomes are defensive checks.
        fixture.propose(&db, 3, 2, fixture.members.clone()).await;
        fixture.commit(&db, 3).await;

        let request = fixture
            .insert_request(&db, 2, SledExpungeRequestState::Expunging)
            .await;
        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &authz_for(&request))
                .await
                .expect("complete succeeds"),
            SledExpungeRequestCompleteOutcome::ReAdded {
                epoch: Epoch(3),
                state: TrustQuorumConfigState::Committed,
            },
        );
        assert_eq!(read(datastore, &request).await, request);
        assert_eq!(fixture.sled_and_disk_policy(&db).await, IN_SERVICE);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_complete_sled_already_expunged() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_complete_sled_already_expunged",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());

        for decommission in [false, true] {
            let fixture = CompleteFixture::new(&db).await;
            fixture.propose_removal(&db).await;
            fixture.start_committing(&db, 2).await;
            let (authz_sled, _) = LookupPath::new(db.opctx(), datastore)
                .sled_id(fixture.sled_id)
                .fetch_for(authz::Action::Modify)
                .await
                .expect("looked up the sled");
            datastore
                .sled_set_policy_to_expunged(db.opctx(), &authz_sled)
                .await
                .expect("expunged the sled");
            if decommission {
                datastore
                    .sled_set_state_to_decommissioned(db.opctx(), &authz_sled)
                    .await
                    .expect("decommissioned the sled");
            }
            assert_eq!(fixture.sled_and_disk_policy(&db).await, EXPUNGED);

            let request = fixture
                .insert_request(&db, 2, SledExpungeRequestState::Expunging)
                .await;
            assert_eq!(
                datastore
                    .sled_expunge_request_complete(opctx, &authz_for(&request))
                    .await
                    .expect("completed the request"),
                SledExpungeRequestCompleteOutcome::Completed {
                    previous_sled_policy: SledPolicy::Expunged,
                },
                "decommission: {decommission}",
            );
            assert_done(&request, &read(datastore, &request).await);
            assert_eq!(fixture.sled_and_disk_policy(&db).await, EXPUNGED);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn sled_policy(db: &TestDatabase, sled_id: SledUuid) -> SledPolicy {
        let (_, sled) = LookupPath::new(db.opctx(), db.datastore())
            .sled_id(sled_id)
            .fetch()
            .await
            .expect("looked up the sled");
        sled.policy()
    }

    #[derive(Debug)]
    enum InternalErrorCase {
        RemovalAborted,
        MissingRemovalConfig,
        MissingSled,
        SledInOtherRack,
        SledWithOtherBaseboard,
    }

    #[tokio::test]
    async fn test_sled_expunge_request_complete_internal_errors() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_complete_internal_errors",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());

        for case in [
            InternalErrorCase::RemovalAborted,
            InternalErrorCase::MissingRemovalConfig,
            InternalErrorCase::MissingSled,
            InternalErrorCase::SledInOtherRack,
            InternalErrorCase::SledWithOtherBaseboard,
        ] {
            let fixture = CompleteFixture::new(&db).await;
            fixture.propose_removal(&db).await;
            let other_hw = &fixture.hw_ids[1];
            let (request, other_sled_id, message) = match case {
                InternalErrorCase::RemovalAborted => {
                    datastore
                        .tq_abort_config(
                            db.opctx(),
                            make_authz_tq(fixture.rack_id),
                            Epoch(2),
                            "test".to_string(),
                        )
                        .await
                        .expect("aborted the removal");
                    let request = fixture
                        .insert_request(
                            &db,
                            2,
                            SledExpungeRequestState::Expunging,
                        )
                        .await;
                    (
                        request,
                        None,
                        "the request is expunging, but the task gate \
                         reported RemovalAborted"
                            .to_string(),
                    )
                }
                InternalErrorCase::MissingRemovalConfig => {
                    fixture.start_committing(&db, 2).await;
                    let request = fixture
                        .insert_request(
                            &db,
                            9,
                            SledExpungeRequestState::Expunging,
                        )
                        .await;
                    (
                        request,
                        None,
                        "no trust quorum configuration exists at the removal \
                         epoch"
                            .to_string(),
                    )
                }
                InternalErrorCase::MissingSled => {
                    fixture.start_committing(&db, 2).await;
                    let missing = SledUuid::new_v4();
                    let request = fixture
                        .insert_request_for_sled(
                            &db,
                            missing,
                            2,
                            SledExpungeRequestState::Expunging,
                        )
                        .await;
                    (
                        request,
                        None,
                        format!("its sled {missing} does not exist"),
                    )
                }
                InternalErrorCase::SledInOtherRack => {
                    fixture.start_committing(&db, 2).await;
                    let other_rack = RackUuid::new_v4();
                    let other_sled_id = insert_sled_for_baseboard(
                        datastore, other_rack, other_hw,
                    )
                    .await;
                    let request = fixture
                        .insert_request_for_sled(
                            &db,
                            other_sled_id,
                            2,
                            SledExpungeRequestState::Expunging,
                        )
                        .await;
                    (
                        request,
                        Some(other_sled_id),
                        format!(
                            "its sled {other_sled_id} is in rack {other_rack}"
                        ),
                    )
                }
                InternalErrorCase::SledWithOtherBaseboard => {
                    fixture.start_committing(&db, 2).await;
                    let other_sled_id = insert_sled_for_baseboard(
                        datastore,
                        fixture.rack_id,
                        other_hw,
                    )
                    .await;
                    let request = fixture
                        .insert_request_for_sled(
                            &db,
                            other_sled_id,
                            2,
                            SledExpungeRequestState::Expunging,
                        )
                        .await;
                    (
                        request,
                        Some(other_sled_id),
                        format!(
                            "its sled {other_sled_id} has baseboard {}",
                            BaseboardId::from(other_hw.clone()),
                        ),
                    )
                }
            };

            assert_eq!(
                datastore
                    .sled_expunge_request_complete(opctx, &authz_for(&request))
                    .await
                    .expect_err(&format!("{case:?} is an internal error")),
                fixture.internal_error(&request, &message),
                "{case:?}",
            );
            assert_eq!(read(datastore, &request).await, request, "{case:?}");
            assert_eq!(
                fixture.sled_and_disk_policy(&db).await,
                IN_SERVICE,
                "{case:?}",
            );
            if let Some(other_sled_id) = other_sled_id {
                assert_eq!(
                    sled_policy(&db, other_sled_id).await,
                    IN_SERVICE.0,
                    "{case:?}",
                );
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    const N_RACE_ROUNDS: usize = 5;

    #[tokio::test]
    async fn test_sled_expunge_request_complete_races_fail() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_complete_races_fail",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx_a = &opctx_as(&db, authn::Context::internal_api());
        let opctx_b = &opctx_as(&db, authn::Context::internal_api());

        for round in 0..N_RACE_ROUNDS {
            let fixture = CompleteFixture::new(&db).await;
            fixture.propose_removal(&db).await;
            fixture.start_committing(&db, 2).await;
            let request = fixture
                .insert_request(&db, 2, SledExpungeRequestState::Expunging)
                .await;
            let authz_request = authz_for(&request);

            let (completed, failed) = tokio::join!(
                datastore
                    .sled_expunge_request_complete(opctx_a, &authz_request),
                datastore.sled_expunge_request_fail(
                    opctx_b,
                    &authz_request,
                    SledExpungeRequestUnfinishedState::Expunging,
                    "raced failure".to_string(),
                ),
            );
            let (completed, failed) =
                (completed.expect("complete"), failed.expect("fail"));
            let after = read(datastore, &request).await;
            match (&completed, &failed) {
                (
                    SledExpungeRequestCompleteOutcome::Completed {
                        previous_sled_policy,
                    },
                    SledExpungeRequestUpdateOutcome::NotInExpectedState {
                        current,
                    },
                ) => {
                    assert_eq!(*previous_sled_policy, IN_SERVICE.0);
                    assert_eq!(current, &after, "round {round}");
                    assert_done(&request, &after);
                    assert_eq!(
                        fixture.sled_and_disk_policy(&db).await,
                        EXPUNGED
                    );
                }
                (
                    SledExpungeRequestCompleteOutcome::NotExpunging { current },
                    SledExpungeRequestUpdateOutcome::Updated,
                ) => {
                    assert_eq!(current, &after, "round {round}");
                    assert_eq!(after.state, SledExpungeRequestState::Failed);
                    assert_eq!(
                        fixture.sled_and_disk_policy(&db).await,
                        IN_SERVICE,
                        "round {round}: the sled stays in service when fail wins",
                    );
                }
                (completed, failed) => panic!(
                    "round {round}: exactly one of complete and fail wins: \
                     {completed:?} {failed:?}"
                ),
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_tq_load_error_into_diesel() {
        let restart = || {
            DieselError::DatabaseError(
                DatabaseErrorKind::SerializationFailure,
                Box::new(
                    "restart transaction: TransactionRetryWithProtoRefreshError"
                        .to_string(),
                ),
            )
        };
        let err = OptionalError::new();
        let mapped = tq_load_error_into_diesel(
            &err,
            "context",
            "a config",
            TransactionError::Database(restart()),
        );
        assert_eq!(format!("{mapped:?}"), format!("{:?}", restart()));
        assert!(
            nexus_db_errors::retryable(&mapped),
            "a restart error stays retryable",
        );
        assert!(err.clone().take().is_none(), "a database error is not bailed",);

        let mapped = tq_load_error_into_diesel(
            &err,
            "context",
            "a config",
            TransactionError::CustomError(Error::invalid_request("no configs")),
        );
        let DieselError::RollbackTransaction = mapped else {
            panic!("a loader error rolls back, got {mapped:?}");
        };
        let Some(TransitionError::External(bailed)) = err.clone().take() else {
            panic!("a loader error is bailed as an external error");
        };
        assert_eq!(
            bailed,
            Error::internal_error("context: loading a config: no configs"),
        );
    }
}
