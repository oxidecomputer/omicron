// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use super::ValidateTransition;
use super::sled::TransitionError;
use super::trust_quorum::epoch_to_i64;
use super::trust_quorum::i64_to_epoch;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
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
use nexus_types::external_api::sled::SledProvisionPolicy;
use nexus_types::inventory::Collection;
use nexus_types::sled_expunge::FinishedSledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeScrimletEvidence;
use nexus_types::sled_expunge::check_sled_expunge_not_scrimlet;
use nexus_types::sled_expunge::check_sled_expunge_policy_and_state;
use nexus_types::sled_expunge::check_sled_expunge_serial_acknowledged;
use nexus_types::sled_expunge::sled_expunge_removal_refusal;
use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use nexus_types::trust_quorum::TrustQuorumConfig;
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
use sled_agent_types::inventory::SledRole;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use trust_quorum_types::types::Epoch;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRequestUpdateOutcome {
    Updated { request: SledExpungeRequest },
    NotInExpectedState { current: SledExpungeRequest },
}

// TODO-RAINCLAUDE: records that contradict the request (its trust quorum records or its sled row) are an internal error naming the request, with nothing written.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRequestCompleteOutcome {
    Completed { previous_sled_policy: SledPolicy },
    NotExpunging { current: SledExpungeRequest },
    ReAddPreparing { epoch: Epoch },
    ReAdded { epoch: Epoch, state: TrustQuorumConfigState },
}

// TODO-RAINCLAUDE: what sled_expunge_request_check_task_gate (and, inside complete, sled_expunge_task_gate_conn) found. Trust quorum records that contradict the request or each other are an internal error naming the request, not an outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeTaskGateOutcome {
    RemovalConfigMissing,
    Gate(SledExpungeTaskGate),
}

pub fn authz_sled_expunge_request_from_id(
    id: SledExpungeRequestUuid,
) -> authz::SledExpungeRequest {
    authz::SledExpungeRequest::new(authz::FLEET, id, LookupType::by_id(id))
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRequestCreateOutcome {
    Created {
        request: SledExpungeRequest,
        removal_config: Box<TrustQuorumConfig>,
    },
    AlreadyOutstanding {
        request: SledExpungeRequest,
    },
}

const TRUST_QUORUM_CONFIGURATION_PKEY: &str = "trust_quorum_configuration_pkey";
const TRUST_QUORUM_ACTIVE_CONFIGURATIONS_INDEX: &str =
    "trust_quorum_active_configurations";
const UNFINISHED_BY_SLED_INDEX: &str =
    "lookup_unfinished_sled_expunge_request_by_sled";
const UNFINISHED_BY_BASEBOARD_INDEX: &str =
    "lookup_unfinished_sled_expunge_request_by_baseboard";

// TODO-RAINCLAUDE: why sled_expunge_request_create's transaction rolled back without a database error: Refused carries the public error as is; UniqueViolation carries what the database said about the constraint an insert hit (at 2.7 or 2.9), and the sled's baseboard, classified by CreateUniqueViolation after the rollback.
#[derive(Debug)]
enum CreateTransactionError {
    Refused(Error),
    UniqueViolation(CreateUniqueViolationInfo),
}

#[derive(Debug)]
struct CreateUniqueViolationInfo {
    constraint: Option<String>,
    message: String,
    details: Option<String>,
    baseboard_id: BaseboardId,
}

// TODO-RAINCLAUDE: Raced: a concurrent insert took the trust quorum epoch (primary key or active-configurations index) or created this sled's unfinished request, so re-read. BaseboardRequest: the unfinished-by-baseboard index, which the explicit check before 2.2 normally reports first. Unexpected: any other constraint (for example (rack_id, tq_epoch) or a trust_quorum_member row) is an internal error naming it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateUniqueViolation {
    Raced,
    BaseboardRequest,
    Unexpected,
}

impl CreateUniqueViolation {
    fn classify(constraint: Option<&str>) -> Self {
        match constraint {
            Some(
                TRUST_QUORUM_CONFIGURATION_PKEY
                | TRUST_QUORUM_ACTIVE_CONFIGURATIONS_INDEX
                | UNFINISHED_BY_SLED_INDEX,
            ) => Self::Raced,
            Some(UNFINISHED_BY_BASEBOARD_INDEX) => Self::BaseboardRequest,
            Some(_) | None => Self::Unexpected,
        }
    }
}

// TODO-RAINCLAUDE: check 2.1's replay of the sled's unfinished request, shared by the transaction and the re-read after a rolled-back unique violation: the request must be for the authorized rack and carry the acknowledged serial number.
fn sled_expunge_request_replay_outstanding(
    sled_id: SledUuid,
    rack_id: RackUuid,
    acknowledged_serial: &str,
    request: SledExpungeRequest,
) -> Result<SledExpungeRequestCreateOutcome, Error> {
    if RackUuid::from(request.rack_id) != rack_id {
        return Err(Error::internal_error(&format!(
            "creating an expunge request for sled {sled_id}: its unfinished \
             request {} is for rack {}, but the trust quorum config \
             authorized is for rack {rack_id}",
            request.id, request.rack_id,
        )));
    }
    check_sled_expunge_serial_acknowledged(
        sled_id,
        &request.serial_number,
        acknowledged_serial,
    )?;
    Ok(SledExpungeRequestCreateOutcome::AlreadyOutstanding { request })
}

fn sled_expunge_request_raced_conflict(sled_id: SledUuid) -> Error {
    Error::conflict(format!(
        "Cannot expunge sled {sled_id}: a concurrent trust quorum \
         reconfiguration or sled expunge request conflicted with this one. \
         Retry the request; if this persists, contact Oxide support."
    ))
}

fn sled_expunge_request_baseboard_conflict(
    sled_id: SledUuid,
    baseboard_id: &BaseboardId,
    blocking: &SledExpungeRequest,
) -> Error {
    Error::conflict(format!(
        "Cannot expunge sled {sled_id}: its baseboard {baseboard_id} already \
         has an unfinished expunge request, {}, for sled {} (state {}). Wait \
         for that request to finish, then retry. Use \
         sled_expunge_request_view to follow its progress; its last_error \
         says what, if anything, needs attention. If it is not making \
         progress, contact Oxide support.",
        blocking.id, blocking.sled_id, blocking.state,
    ))
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

    // TODO-RAINCLAUDE: RFD 747 request creation, one retried transaction running checks 2.1 to 2.9 in order. 2.1 comes first so that a retry after a lost response finds the sled's unfinished request, even once the sled has left the trust quorum; it still needs the same serial number. Database errors stay retryable; every refusal rolls back all of the writes. Modify on the sled and on the rack's trust quorum config is checked here, once each, before the transaction; authz_tq must be for the sled's rack.
    pub async fn sled_expunge_request_create(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        authz_tq: &authz::TrustQuorumConfig,
        acknowledged_serial: &str,
        latest_collection: Option<&Collection>,
    ) -> Result<SledExpungeRequestCreateOutcome, Error> {
        opctx.authorize(authz::Action::Modify, authz_sled).await?;
        opctx.authorize(authz::Action::Modify, authz_tq).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let sled_id = authz_sled.id();
        let rack_id = authz_tq.rack().id();

        let err = OptionalError::new();
        let result = self
            .transaction_retry_wrapper("sled_expunge_request_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::sled_expunge_request_create_conn(
                        &conn,
                        authz_sled,
                        authz_tq,
                        acknowledged_serial,
                        latest_collection,
                        &err,
                    )
                    .await
                }
            })
            .await;
        let error = match result {
            Ok(outcome) => return Ok(outcome),
            Err(error) => error,
        };
        let violation = match err.take() {
            None => {
                return Err(public_error_from_diesel(
                    error,
                    ErrorHandler::Server,
                ));
            }
            Some(CreateTransactionError::Refused(error)) => return Err(error),
            Some(CreateTransactionError::UniqueViolation(violation)) => {
                violation
            }
        };
        let classified =
            CreateUniqueViolation::classify(violation.constraint.as_deref());
        let log_message = "unique violation creating a sled expunge request";
        match classified {
            CreateUniqueViolation::Raced
            | CreateUniqueViolation::BaseboardRequest => warn!(
                opctx.log, "{log_message}";
                "sled_id" => %sled_id,
                "constraint" => ?violation.constraint,
                "classified" => ?classified,
            ),
            CreateUniqueViolation::Unexpected => error!(
                opctx.log, "{log_message}";
                "sled_id" => %sled_id,
                "constraint" => ?violation.constraint,
                "message" => &violation.message,
                "details" => ?violation.details,
            ),
        }
        let baseboard_violation = match classified {
            CreateUniqueViolation::Raced => None,
            CreateUniqueViolation::BaseboardRequest => {
                Some(&violation.baseboard_id)
            }
            CreateUniqueViolation::Unexpected => {
                let details = match &violation.details {
                    Some(details) => format!(" ({details})"),
                    None => String::new(),
                };
                return Err(Error::internal_error(&format!(
                    "creating an expunge request for sled {sled_id}: \
                     unexpected unique violation of {}: {}{details}",
                    violation
                        .constraint
                        .as_deref()
                        .unwrap_or("an unnamed constraint"),
                    violation.message,
                )));
            }
        };
        Self::sled_expunge_request_create_raced_conn(
            &conn,
            sled_id,
            rack_id,
            acknowledged_serial,
            baseboard_violation,
        )
        .await
    }

    async fn sled_expunge_request_create_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_sled: &authz::Sled,
        authz_tq: &authz::TrustQuorumConfig,
        acknowledged_serial: &str,
        latest_collection: Option<&Collection>,
        err: &OptionalError<CreateTransactionError>,
    ) -> Result<SledExpungeRequestCreateOutcome, DieselError> {
        let sled_id = authz_sled.id();
        let rack_id = authz_tq.rack().id();
        let refuse =
            |error: Error| err.bail(CreateTransactionError::Refused(error));

        if let Some(request) =
            Self::sled_expunge_request_unfinished_for_sled_conn(conn, sled_id)
                .await?
        {
            return sled_expunge_request_replay_outstanding(
                sled_id,
                rack_id,
                acknowledged_serial,
                request,
            )
            .map_err(refuse);
        }

        let sled = Self::sled_get_live_conn(conn, sled_id).await?;
        let Some(sled) = sled else {
            return Err(refuse(authz_sled.not_found()));
        };
        if sled.rack_id() != rack_id {
            return Err(refuse(Error::internal_error(&format!(
                "creating an expunge request for sled {sled_id}: the sled is \
                 in rack {}, but the trust quorum config authorized is for \
                 rack {rack_id}",
                sled.rack_id(),
            ))));
        }
        let baseboard_id = sled.baseboard_id();
        // TODO-RAINCLAUDE: the unfinished-by-baseboard unique index would otherwise fail the insert at 2.9; another sled row with this baseboard (for example one decommissioned since its request was created) still has an unfinished request.
        if let Some(blocking) =
            Self::sled_expunge_request_unfinished_for_baseboard_conn(
                conn,
                &baseboard_id,
            )
            .await?
        {
            return Err(refuse(sled_expunge_request_baseboard_conflict(
                sled_id,
                &baseboard_id,
                &blocking,
            )));
        }
        let unique_violation_or = |error: DieselError| {
            if let DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                info,
            ) = &error
            {
                err.bail(CreateTransactionError::UniqueViolation(
                    CreateUniqueViolationInfo {
                        constraint: info.constraint_name().map(str::to_string),
                        message: info.message().to_string(),
                        details: info.details().map(str::to_string),
                        baseboard_id: baseboard_id.clone(),
                    },
                ))
            } else {
                error
            }
        };

        if let Err(refusal) = check_sled_expunge_policy_and_state(
            sled_id,
            sled.policy(),
            sled.state().into(),
        ) {
            let latest_finished =
                Self::sled_expunge_request_latest_for_sled_conn(conn, sled_id)
                    .await?;
            let latest_finished = match latest_finished {
                None => None,
                Some(request) => {
                    let Some(state) = request.state.as_finished() else {
                        return Err(refuse(Error::internal_error(&format!(
                            "creating an expunge request for sled {sled_id}: \
                             request {} is {}, but this transaction found no \
                             unfinished request for the sled",
                            request.id, request.state,
                        ))));
                    };
                    Some(FinishedSledExpungeRequest { id: request.id(), state })
                }
            };
            return Err(refuse(refusal.into_error(latest_finished)));
        }

        check_sled_expunge_serial_acknowledged(
            sled_id,
            sled.serial_number(),
            acknowledged_serial,
        )
        .map_err(refuse)?;

        let sled_row_role = if sled.is_scrimlet() {
            SledRole::Scrimlet
        } else {
            SledRole::Gimlet
        };
        check_sled_expunge_not_scrimlet(
            sled_id,
            &SledExpungeScrimletEvidence::from_collection(
                latest_collection,
                sled_id,
                &baseboard_id,
            ),
            sled_row_role,
        )
        .map_err(refuse)?;

        let base = Self::tq_load_reconfiguration_base_conn(conn, rack_id)
            .await
            .map_err(|error| match error {
                TransactionError::Database(error) => error,
                TransactionError::CustomError(error) => refuse(error),
            })?;
        let (last_committed_config, latest_epoch) =
            base.require_last_committed().map_err(refuse)?;

        let proposed = ProposedTrustQuorumConfig::remove_sled(
            last_committed_config,
            latest_epoch.next(),
            &baseboard_id,
        )
        .map_err(|refusal| {
            refuse(sled_expunge_removal_refusal(
                sled_id,
                last_committed_config,
                refusal,
            ))
        })?;
        let removal_config =
            Self::tq_insert_config_on_base_conn(conn, authz_tq, base, proposed)
                .await
                .map_err(|error| match error {
                    TransactionError::Database(error) => {
                        unique_violation_or(error)
                    }
                    TransactionError::CustomError(error) => refuse(error),
                })?;

        let policy_err = OptionalError::new();
        Self::sled_set_policy_conn(
            conn,
            sled_id,
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            ValidateTransition::Yes,
            &policy_err,
        )
        .await
        .map_err(|error| match policy_err.take() {
            Some(TransitionError::External(error)) => refuse(error),
            Some(error @ TransitionError::InvalidTransition { .. }) => {
                refuse(Error::internal_error(&format!(
                    "creating an expunge request for sled {sled_id}: making \
                     the sled non-provisionable after checking that it is in \
                     service and active: {}",
                    InlineErrorChain::new(&error),
                )))
            }
            None => error,
        })?;

        let tq_epoch = epoch_to_i64(removal_config.epoch).map_err(refuse)?;
        let request = diesel::insert_into(dsl::sled_expunge_request)
            .values(SledExpungeRequest::new(
                rack_id,
                sled_id,
                baseboard_id.clone(),
                tq_epoch,
            ))
            .returning(SledExpungeRequest::as_returning())
            .get_result_async(conn)
            .await
            .map_err(unique_violation_or)?;

        Ok(SledExpungeRequestCreateOutcome::Created {
            request,
            removal_config: Box::new(removal_config),
        })
    }

    // TODO-RAINCLAUDE: after a Raced or BaseboardRequest unique violation rolled the creation back: this sled's unfinished request is replayed as at 2.1 if there is one, else another sled row's unfinished request for the baseboard is named, else it is a conflict that a retry resolves through the checks.
    async fn sled_expunge_request_create_raced_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
        rack_id: RackUuid,
        acknowledged_serial: &str,
        baseboard_violation: Option<&BaseboardId>,
    ) -> Result<SledExpungeRequestCreateOutcome, Error> {
        let to_public =
            |error| public_error_from_diesel(error, ErrorHandler::Server);
        if let Some(request) =
            Self::sled_expunge_request_unfinished_for_sled_conn(conn, sled_id)
                .await
                .map_err(to_public)?
        {
            return sled_expunge_request_replay_outstanding(
                sled_id,
                rack_id,
                acknowledged_serial,
                request,
            );
        }
        let Some(baseboard_id) = baseboard_violation else {
            return Err(sled_expunge_request_raced_conflict(sled_id));
        };
        let blocking =
            Self::sled_expunge_request_unfinished_for_baseboard_conn(
                conn,
                baseboard_id,
            )
            .await
            .map_err(to_public)?;
        Err(match blocking {
            Some(blocking) => sled_expunge_request_baseboard_conflict(
                sled_id,
                baseboard_id,
                &blocking,
            ),
            None => sled_expunge_request_raced_conflict(sled_id),
        })
    }

    async fn sled_expunge_request_unfinished_for_sled_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<Option<SledExpungeRequest>, DieselError> {
        dsl::sled_expunge_request
            .filter(dsl::sled_id.eq(to_db_typed_uuid(sled_id)))
            .filter(dsl::state.eq_any(SledExpungeRequestState::UNFINISHED))
            .select(SledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
            .optional()
    }

    async fn sled_get_live_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<Option<Sled>, DieselError> {
        use nexus_db_schema::schema::sled::dsl;
        dsl::sled
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
            .select(Sled::as_select())
            .get_result_async(conn)
            .await
            .optional()
    }

    async fn sled_expunge_request_unfinished_for_baseboard_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        baseboard_id: &BaseboardId,
    ) -> Result<Option<SledExpungeRequest>, DieselError> {
        dsl::sled_expunge_request
            .filter(dsl::part_number.eq(baseboard_id.part_number.clone()))
            .filter(dsl::serial_number.eq(baseboard_id.serial_number.clone()))
            .filter(dsl::state.eq_any(SledExpungeRequestState::UNFINISHED))
            .select(SledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
            .optional()
    }

    async fn sled_expunge_request_latest_for_sled_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<Option<SledExpungeRequest>, DieselError> {
        dsl::sled_expunge_request
            .filter(dsl::sled_id.eq(to_db_typed_uuid(sled_id)))
            .order_by((dsl::time_created.desc(), dsl::id.desc()))
            .select(SledExpungeRequest::as_select())
            .first_async(conn)
            .await
            .optional()
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

    // TODO-RAINCLAUDE: the power-off outcome already on the row is kept.
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
        let updated = diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(id))
            .filter(dsl::state.eq(from))
            .set(transition.columns(now_db_precision()))
            .returning(SledExpungeRequest::as_returning())
            .get_result_async(&*conn)
            .await
            .optional();
        Self::sled_expunge_request_update_outcome(&conn, authz_request, updated)
            .await
    }

    // TODO-RAINCLAUDE: never touches a finished request, because expected_state is always unfinished; within one state this is last-writer-wins, not a compare-and-swap on the previous message.
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
        let updated = diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(id))
            .filter(dsl::state.eq(expected_state.state()))
            .set((
                dsl::last_error.eq(Some(last_error)),
                dsl::time_modified.eq(now_db_precision()),
            ))
            .returning(SledExpungeRequest::as_returning())
            .get_result_async(&*conn)
            .await
            .optional();
        Self::sled_expunge_request_update_outcome(&conn, authz_request, updated)
            .await
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
        let context = sled_expunge_request_context(&request);
        let tq_epoch = i64_to_epoch(request.tq_epoch)
            .map_err(|error| error.internal_context(context.clone()))?;
        let baseboard_id = request.baseboard_id();

        let err = OptionalError::new();
        self.transaction_retry_wrapper("sled_expunge_request_complete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let baseboard_id = baseboard_id.clone();
                let context = context.clone();
                async move {

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

                    let gate = match Self::sled_expunge_task_gate_conn(
                        &conn,
                        &err,
                        &context,
                        rack_id,
                        tq_epoch,
                        &baseboard_id,
                    )
                    .await?
                    {
                        SledExpungeTaskGateOutcome::RemovalConfigMissing => {
                            return Err(bail_internal(
                                &err,
                                &context,
                                "no trust quorum configuration exists at the \
                                 removal epoch",
                            ));
                        }
                        SledExpungeTaskGateOutcome::Gate(gate) => gate,
                    };
                    // TODO-RAINCLAUDE: RemovalPreparing and RemovalAborted come back as an internal error here: the task contract (not enforced here) is that a request only reaches expunging after the gate saw its removal past Preparing, and a config past Preparing can never go back or be aborted.
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
                            return Err(bail_internal(&err, &context, format!(
                                "the request is expunging, but the task gate \
                                 reported {gate:?}"
                            )));
                        }
                    }

                    let sled = Self::sled_get_live_conn(&conn, sled_id).await?;
                    let Some(sled) = sled else {
                        return Err(bail_internal(&err, &context, format!(
                            "its sled {sled_id} does not exist"
                        )));
                    };
                    if sled.rack_id() != rack_id {
                        return Err(bail_internal(&err, &context, format!(
                            "its sled {sled_id} is in rack {}",
                            sled.rack_id(),
                        )));
                    }
                    let sled_baseboard_id = sled.baseboard_id();
                    if sled_baseboard_id != baseboard_id {
                        return Err(bail_internal(&err, &context, format!(
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
                        return Err(err.bail(TransitionError::External(
                            Error::internal_error(&format!(
                                "the request was read as {from} in this \
                                 transaction, but moving it to done updated \
                                 {n_updated} rows"
                            ))
                            .internal_context(context.clone()),
                        )));
                    }

                    Ok(SledExpungeRequestCompleteOutcome::Completed {
                        previous_sled_policy,
                    })
                }
            })
            .await
            .map_err(|error| {
                sled_expunge_transaction_error(err.take(), error, &context)
            })
    }

    // TODO-RAINCLAUDE: the waiting_for_trust_quorum step's gate check (RFD 747), outside sled_expunge_request_complete: every gate input is read in one transaction so the gate sees one snapshot of the rack's configs, and it uses the same loader as complete so the two cannot drift. It writes nothing; acting on SledRemoved relies on check_no_new_member_being_expunged_conn refusing re-adds and on complete re-running the gate in its own transaction. Database errors stay retryable inside the transaction; loader and gate errors roll it back as an internal error naming the request.
    pub async fn sled_expunge_request_check_task_gate(
        &self,
        opctx: &OpContext,
        request: &SledExpungeRequest,
    ) -> Result<SledExpungeTaskGateOutcome, Error> {
        // TODO-RAINCLAUDE: taking the listed row rather than re-reading it is fine: rack_id, tq_epoch and the baseboard never change after insert, and the request's state is not a gate input.
        let rack_id = RackUuid::from(request.rack_id);
        let authz_request = authz_sled_expunge_request_from_id(request.id());
        opctx.authorize(authz::Action::Read, &authz_request).await?;
        opctx
            .authorize(
                authz::Action::Read,
                &authz::TrustQuorumConfig::for_rack_id(rack_id),
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let context = sled_expunge_request_context(request);
        let tq_epoch = i64_to_epoch(request.tq_epoch)
            .map_err(|error| error.internal_context(context.clone()))?;
        let baseboard_id = request.baseboard_id();

        let err = OptionalError::new();
        self.transaction_retry_wrapper("sled_expunge_request_check_task_gate")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let baseboard_id = baseboard_id.clone();
                let context = context.clone();
                async move {
                    Self::sled_expunge_task_gate_conn(
                        &conn,
                        &err,
                        &context,
                        rack_id,
                        tq_epoch,
                        &baseboard_id,
                    )
                    .await
                }
            })
            .await
            .map_err(|error| {
                sled_expunge_transaction_error(err.take(), error, &context)
            })
    }

    // TODO-RAINCLAUDE: loads the task gate's inputs (the removal config, the latest non-aborted config, and the base sled_expunge_task_gate_base_epoch names) and runs the gate, for sled_expunge_request_complete and sled_expunge_request_check_task_gate. A missing removal config is its own outcome, so that each caller decides what it means (the task fails the request; complete reports it as an internal error); a missing base config, and every other inconsistent input, bails through err as an internal error prefixed with context.
    async fn sled_expunge_task_gate_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: &OptionalError<TransitionError>,
        context: &str,
        rack_id: RackUuid,
        tq_epoch: Epoch,
        baseboard_id: &BaseboardId,
    ) -> Result<SledExpungeTaskGateOutcome, DieselError> {
        let removal_config =
            match Self::tq_get_config_with_members_from_epoch_conn(
                conn, rack_id, tq_epoch,
            )
            .await
            {
                Ok(Some(config)) => config,
                Ok(None) => {
                    return Ok(
                        SledExpungeTaskGateOutcome::RemovalConfigMissing,
                    );
                }
                Err(error) => {
                    return Err(tq_load_error(
                        err,
                        context,
                        "the removal trust quorum configuration",
                        error,
                    ));
                }
            };
        let latest_non_aborted_config =
            match Self::tq_load_latest_possible_committed_config_conn(
                conn, rack_id,
            )
            .await
            {
                Ok((config, _)) => config,
                Err(error) => {
                    return Err(tq_load_error(
                        err,
                        context,
                        "the latest non-aborted trust quorum configuration",
                        error,
                    ));
                }
            };
        let base_epoch = match sled_expunge_task_gate_base_epoch(
            &removal_config,
            &latest_non_aborted_config,
        ) {
            Ok(base_epoch) => base_epoch,
            Err(error) => return Err(bail_internal(err, context, error)),
        };
        // TODO-RAINCLAUDE: when the Preparing latest config builds directly on the removal, the removal config already loaded is its base.
        let loaded_base_config = match base_epoch {
            Some(base_epoch) if base_epoch != tq_epoch => {
                match Self::tq_get_config_with_members_from_epoch_conn(
                    conn, rack_id, base_epoch,
                )
                .await
                {
                    Ok(Some(base_config)) => Some(base_config),
                    Ok(None) => {
                        return Err(bail_internal(
                            err,
                            context,
                            format!(
                                "no trust quorum configuration exists at epoch \
                             {base_epoch}, the base of the latest \
                             configuration (epoch {})",
                                latest_non_aborted_config.epoch,
                            ),
                        ));
                    }
                    Err(error) => {
                        return Err(tq_load_error(
                            err,
                            context,
                            "the base trust quorum configuration",
                            error,
                        ));
                    }
                }
            }
            None | Some(_) => None,
        };
        let latest_base_config =
            loaded_base_config.as_ref().or(base_epoch.map(|_| &removal_config));
        match check_sled_expunge_task_gate(
            &removal_config,
            &latest_non_aborted_config,
            latest_base_config,
            baseboard_id,
        ) {
            Ok(gate) => Ok(SledExpungeTaskGateOutcome::Gate(gate)),
            Err(error) => Err(bail_internal(err, context, error)),
        }
    }

    // TODO-RAINCLAUDE: maps a conditional update's RETURNING result: the updated row, or, when the state filter matched nothing, the row as a second statement reads it now. That row may have moved on since the update missed, but it can't be back in the expected state, since states only move forward and callers pass states they read; production never deletes these rows, and the update's `.optional()` turns a missing row into None, so a 404 only ever comes from the re-read.
    async fn sled_expunge_request_update_outcome(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_request: &authz::SledExpungeRequest,
        updated: Result<Option<SledExpungeRequest>, DieselError>,
    ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
        let to_public = |error| {
            public_error_from_diesel(
                error,
                ErrorHandler::NotFoundByResource(authz_request),
            )
        };
        match updated.map_err(to_public)? {
            Some(request) => {
                Ok(SledExpungeRequestUpdateOutcome::Updated { request })
            }
            None => {
                let current =
                    Self::sled_expunge_request_get_conn(conn, authz_request)
                        .await
                        .map_err(to_public)?;
                Ok(SledExpungeRequestUpdateOutcome::NotInExpectedState {
                    current,
                })
            }
        }
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

fn sled_expunge_request_context(request: &SledExpungeRequest) -> String {
    format!(
        "sled expunge request {} for {} (rack {}, removal epoch {})",
        request.id,
        request.baseboard_id(),
        request.rack_id,
        request.tq_epoch,
    )
}

// TODO-RAINCLAUDE: maps a sled expunge request transaction's failure to the public error: a bailed External error as is, an InvalidTransition or a database error as an internal error carrying the request context.
fn sled_expunge_transaction_error(
    taken: Option<TransitionError>,
    error: DieselError,
    context: &str,
) -> Error {
    match taken {
        Some(TransitionError::External(error)) => error,
        Some(error @ TransitionError::InvalidTransition { .. }) => {
            Error::internal_error(&InlineErrorChain::new(&error).to_string())
                .internal_context(context.to_string())
        }
        None => public_error_from_diesel(error, ErrorHandler::Server)
            .internal_context(context.to_string()),
    }
}

// TODO-RAINCLAUDE: maps a trust quorum loader's error inside sled_expunge_task_gate_conn: database errors are returned untouched so restarts stay retryable, and the loader's own errors bail through err as an internal error carrying the request context.
fn tq_load_error(
    err: &OptionalError<TransitionError>,
    context: &str,
    what: &str,
    error: TransactionError<Error>,
) -> DieselError {
    match error {
        TransactionError::Database(error) => error,
        TransactionError::CustomError(error) => bail_internal(
            err,
            context,
            format!("loading {what}: {}", InlineErrorChain::new(&error)),
        ),
    }
}

// TODO-RAINCLAUDE: rolls back a sled expunge request transaction with an internal error naming the request, for records that contradict it and for a trust quorum loader's non-database errors.
fn bail_internal<M: std::fmt::Display>(
    err: &OptionalError<TransitionError>,
    context: &str,
    message: M,
) -> DieselError {
    err.bail(TransitionError::External(
        Error::internal_error(&message.to_string())
            .internal_context(context.to_string()),
    ))
}

#[cfg(test)]
mod test {
    use super::super::ValidateTransition;
    use super::super::test_utils::Expected;
    use super::super::test_utils::sled_set_state;
    use super::SQL_BATCH_SIZE;
    use super::SledExpungeRequestCompleteOutcome;
    use super::SledExpungeRequestCreateOutcome;
    use super::SledExpungeRequestUpdateOutcome;
    use super::SledExpungeTaskGateOutcome;
    use super::TRUST_QUORUM_CONFIGURATION_PKEY;
    use super::TransitionError;
    use super::UNFINISHED_BY_BASEBOARD_INDEX;
    use super::UNFINISHED_BY_SLED_INDEX;
    use super::authz_sled_expunge_request_from_id;
    use super::sled_expunge_request_baseboard_conflict;
    use super::sled_expunge_request_raced_conflict;
    use super::tq_load_error;
    use crate::authn;
    use crate::authz;
    use crate::context::OpContext;
    use crate::db::DataStore;
    use crate::db::datastore::trust_quorum::tests::insert_hw_baseboard_ids;
    use crate::db::datastore::trust_quorum::tests::insert_sled_for_baseboard;
    use crate::db::datastore::trust_quorum::tests::make_authz_tq;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::sled_expunge_request::TEST_FAILURE_REASON;
    use crate::db::pub_test_utils::sled_expunge_request::backdate_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use crate::db::pub_test_utils::sled_expunge_request::new_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::set_sled_expunge_request_state;
    use crate::db::pub_test_utils::trust_quorum::TrustQuorumConfigDriver;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::prelude::*;
    use diesel::result::DatabaseErrorKind;
    use diesel::result::Error as DieselError;
    use gateway_types::component::SpState;
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
    use nexus_inventory::CollectionBuilder;
    use nexus_inventory::examples as inventory_examples;
    use nexus_inventory::examples::OmicronFileSourceResolverExampleKind;
    use nexus_types::external_api::sled::SledPolicy;
    use nexus_types::external_api::sled::SledProvisionPolicy;
    use nexus_types::external_api::sled::SledState;
    use nexus_types::inventory::Collection;
    use nexus_types::inventory::SpType;
    use nexus_types::sled_expunge::FinishedSledExpungeRequest;
    use nexus_types::sled_expunge::FinishedSledExpungeRequestState;
    use nexus_types::sled_expunge::SWITCH_ATTACHED_SLED_CUBBIES;
    use nexus_types::sled_expunge::SledExpungeScrimletEvidence;
    use nexus_types::sled_expunge::check_sled_expunge_not_scrimlet;
    use nexus_types::sled_expunge::check_sled_expunge_policy_and_state;
    use nexus_types::sled_expunge::check_sled_expunge_serial_acknowledged;
    use nexus_types::sled_expunge::sled_expunge_removal_refusal;
    use nexus_types::trust_quorum::RemoveSledError;
    use nexus_types::trust_quorum::SledExpungeTaskGate;
    use nexus_types::trust_quorum::TrustQuorumConfig;
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
    use proptest::prelude::*;
    use sled_agent_types::inventory::SledRole;
    use sled_agent_types::inventory::SvcsEnabledNotOnlineResult;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use trust_quorum_types::membership::TRUST_QUORUM_MIN_MEMBERS;
    use trust_quorum_types::membership::TrustQuorumMembers;
    use trust_quorum_types::membership::TrustQuorumMembersError;
    use trust_quorum_types::types::Epoch;
    use uuid::Uuid;

    const TQ_EPOCH_IS_POSITIVE: &str = "tq_epoch_is_positive";
    const TIME_COMPLETED_IFF_FINISHED: &str = "time_completed_iff_finished";
    const FAILURE_REASON_IFF_FAILED: &str = "failure_reason_iff_failed";
    const POWER_OFF_OUTCOME_MATCHES_STATE: &str =
        "power_off_outcome_matches_state";
    const LAST_ERROR_ONLY_IF_UNFINISHED: &str = "last_error_only_if_unfinished";
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
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

        // TODO-RAINCLAUDE: creation classifies a unique violation by constraint name, so the trust quorum config primary key's name is pinned here too.
        let fixture = CompleteFixture::new(&db).await;
        let configs = db
            .datastore()
            .tq_list_config(
                db.opctx(),
                make_authz_tq(fixture.rack_id),
                &DataPageParams::max_page(),
            )
            .await
            .expect("listed the trust quorum configs");
        let [epoch_1] = configs.as_slice() else {
            panic!("the fixture has one trust quorum config: {configs:?}");
        };
        let result = diesel::insert_into(
            nexus_db_schema::schema::trust_quorum_configuration::dsl::trust_quorum_configuration,
        )
        .values(epoch_1.clone())
        .execute_async(&*conn)
        .await;
        assert_eq!(
            expect_violation(
                &result,
                DatabaseErrorKind::UniqueViolation,
                "a second config at epoch 1",
            ),
            TRUST_QUORUM_CONFIGURATION_PKEY,
        );

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
        assert_eq!(
            datastore
                .sled_expunge_request_check_task_gate(&opctx, &request)
                .await
                .expect_err("a fleet viewer is denied the task gate check"),
            not_found,
        );
        assert_eq!(read(datastore, &request).await, request);
        assert_eq!(
            create(
                &db,
                &CreateInput {
                    opctx: &opctx,
                    sled_id: SledUuid::new_v4(),
                    rack_id: RackUuid::new_v4(),
                    serial: "serial".to_string(),
                    collection: None,
                },
            )
            .await
            .expect_err("a fleet viewer is denied create"),
            Error::Forbidden,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn authz_for(request: &SledExpungeRequest) -> authz::SledExpungeRequest {
        authz_sled_expunge_request_from_id(request.id())
    }

    async fn read(
        datastore: &DataStore,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequest {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        read_back(&conn, request.id).await
    }

    async fn assert_updated<D: std::fmt::Display>(
        datastore: &DataStore,
        outcome: SledExpungeRequestUpdateOutcome,
        authz_request: &authz::SledExpungeRequest,
        context: D,
    ) -> SledExpungeRequest {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let stored = read_back(&conn, authz_request.id().into()).await;
        assert_eq!(
            outcome,
            SledExpungeRequestUpdateOutcome::Updated {
                request: stored.clone()
            },
            "{context}",
        );
        stored
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
            let before =
                backdate_sled_expunge_request(datastore, &request).await;
            let outcome = op
                .run(datastore, opctx, &authz_for(&before))
                .await
                .unwrap_or_else(|error| panic!("{op:?} from {start}: {error}"));
            let after = read(datastore, &before).await;
            if op.from_state().state() == start {
                assert_eq!(
                    outcome,
                    SledExpungeRequestUpdateOutcome::Updated {
                        request: after.clone(),
                    },
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

    #[derive(Clone, Copy, Debug)]
    enum FixtureTrustQuorum {
        Rss,
        Lrtq,
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
            Self::with_members(db, 10, FixtureTrustQuorum::Rss).await
        }

        // TODO-RAINCLAUDE: as new, with only the first `members` of the ten baseboards in the fixture, and in the trust quorum unless it is Lrtq, which leaves the rack with no trust quorum config.
        async fn with_members(
            db: &TestDatabase,
            members: usize,
            trust_quorum: FixtureTrustQuorum,
        ) -> Self {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let mut hw_ids = insert_hw_baseboard_ids(db).await;
            hw_ids.truncate(members);
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
            match trust_quorum {
                FixtureTrustQuorum::Rss => {
                    let conn =
                        datastore.pool_connection_for_tests().await.unwrap();
                    DataStore::tq_insert_rss_config_after_handoff(
                        opctx,
                        &conn,
                        make_authz_tq(rack_id),
                        TrustQuorumMembers::new(members.clone()).expect(
                            "test membership is within the size bounds",
                        ),
                        members.first().expect("at least one member").clone(),
                    )
                    .await
                    .expect("inserted the epoch 1 trust quorum config");
                }
                FixtureTrustQuorum::Lrtq => {}
            }

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

        fn tq<'a>(&self, db: &'a TestDatabase) -> TrustQuorumConfigDriver<'a> {
            TrustQuorumConfigDriver {
                opctx: db.opctx(),
                datastore: db.datastore(),
                rack_id: self.rack_id,
            }
        }

        async fn propose_removal(&self, db: &TestDatabase) {
            self.tq(db)
                .propose(2, 1, &self.members_without(&[&self.target]))
                .await;
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

        fn context_message(
            &self,
            request: &SledExpungeRequest,
            message: &str,
        ) -> String {
            format!(
                "sled expunge request {} for {} (rack {}, removal epoch {}): \
                 {message}",
                request.id, self.target, self.rack_id, request.tq_epoch,
            )
        }

        // TODO-RAINCLAUDE: deletes a trust quorum config and its members; nothing in Nexus deletes trust quorum configs, so this is the only way to reach some inconsistencies.
        async fn delete_config(&self, db: &TestDatabase, epoch: i64) {
            use nexus_db_schema::schema::trust_quorum_configuration::dsl as config_dsl;
            use nexus_db_schema::schema::trust_quorum_member::dsl as member_dsl;
            let conn =
                db.datastore().pool_connection_for_tests().await.unwrap();
            let rack_id = self.rack_id.into_untyped_uuid();
            diesel::delete(member_dsl::trust_quorum_member)
                .filter(member_dsl::rack_id.eq(rack_id))
                .filter(member_dsl::epoch.eq(epoch))
                .execute_async(&*conn)
                .await
                .expect("deleted the config's members");
            diesel::delete(config_dsl::trust_quorum_configuration)
                .filter(config_dsl::rack_id.eq(rack_id))
                .filter(config_dsl::epoch.eq(epoch))
                .execute_async(&*conn)
                .await
                .expect("deleted the config");
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
        assert_updated(
            db.datastore(),
            db.datastore()
                .sled_expunge_request_set_last_error(
                    opctx,
                    &authz_for(request),
                    from,
                    message.to_string(),
                )
                .await
                .expect("set last_error"),
            &authz_for(request),
            "set last_error",
        )
        .await
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
        fixture.tq(&db).ack_all_prepares(2).await;

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
        fixture.tq(&db).commit(2).await;
        // TODO-RAINCLAUDE: check_no_new_member_being_expunged_conn refuses re-adding a sled with an unfinished request, so the re-add is proposed before the request row exists; the gate's re-added outcomes are defensive checks.
        fixture.tq(&db).propose(3, 2, &fixture.members).await;
        fixture.tq(&db).commit(3).await;

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
            fixture.tq(&db).ack_all_prepares(2).await;
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
    enum InconsistentCase {
        RemovalAborted,
        MissingRemovalConfig,
        MissingSled,
        SledInOtherRack,
        SledWithOtherBaseboard,
    }

    #[tokio::test]
    async fn test_sled_expunge_request_complete_inconsistent() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_complete_inconsistent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());

        for case in [
            InconsistentCase::RemovalAborted,
            InconsistentCase::MissingRemovalConfig,
            InconsistentCase::MissingSled,
            InconsistentCase::SledInOtherRack,
            InconsistentCase::SledWithOtherBaseboard,
        ] {
            let fixture = CompleteFixture::new(&db).await;
            fixture.propose_removal(&db).await;
            let other_hw = &fixture.hw_ids[1];
            let (request, other_sled_id, message) = match case {
                InconsistentCase::RemovalAborted => {
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
                InconsistentCase::MissingRemovalConfig => {
                    fixture.tq(&db).ack_all_prepares(2).await;
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
                InconsistentCase::MissingSled => {
                    fixture.tq(&db).ack_all_prepares(2).await;
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
                InconsistentCase::SledInOtherRack => {
                    fixture.tq(&db).ack_all_prepares(2).await;
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
                InconsistentCase::SledWithOtherBaseboard => {
                    fixture.tq(&db).ack_all_prepares(2).await;
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
                    .expect_err("inconsistent records are an internal error"),
                Error::internal_error(
                    &fixture.context_message(&request, &message)
                ),
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
            fixture.tq(&db).ack_all_prepares(2).await;
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
                    SledExpungeRequestUpdateOutcome::Updated { .. },
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

    #[derive(Debug)]
    enum TaskGateCase {
        RemovalPreparing,
        RemovalAborted,
        RemovalConfigMissing,
        UnrelatedPreparingOnRemoval,
        UnrelatedPreparingOnLaterBase,
        ReAdded,
    }

    #[tokio::test]
    async fn test_sled_expunge_request_check_task_gate() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_check_task_gate");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());

        for case in [
            TaskGateCase::RemovalPreparing,
            TaskGateCase::RemovalAborted,
            TaskGateCase::RemovalConfigMissing,
            TaskGateCase::UnrelatedPreparingOnRemoval,
            TaskGateCase::UnrelatedPreparingOnLaterBase,
            TaskGateCase::ReAdded,
        ] {
            let fixture = CompleteFixture::new(&db).await;
            fixture.propose_removal(&db).await;
            // TODO-RAINCLAUDE: not hw_ids[0], the fixture's only member with an in-service sled, which must stay in every config so that one can be proposed.
            let other = BaseboardId::from(fixture.hw_ids[1].clone());
            let without_both =
                fixture.members_without(&[&fixture.target, &other]);
            let (tq_epoch, expected) = match case {
                TaskGateCase::RemovalPreparing => (
                    2,
                    SledExpungeTaskGateOutcome::Gate(
                        SledExpungeTaskGate::RemovalPreparing,
                    ),
                ),
                TaskGateCase::RemovalAborted => {
                    datastore
                        .tq_abort_config(
                            db.opctx(),
                            make_authz_tq(fixture.rack_id),
                            Epoch(2),
                            "test".to_string(),
                        )
                        .await
                        .expect("aborted the removal");
                    (
                        2,
                        SledExpungeTaskGateOutcome::Gate(
                            SledExpungeTaskGate::RemovalAborted,
                        ),
                    )
                }
                TaskGateCase::RemovalConfigMissing => {
                    (9, SledExpungeTaskGateOutcome::RemovalConfigMissing)
                }
                TaskGateCase::UnrelatedPreparingOnRemoval => {
                    fixture.tq(&db).commit(2).await;
                    fixture.tq(&db).propose(3, 2, &without_both).await;
                    (
                        2,
                        SledExpungeTaskGateOutcome::Gate(
                            SledExpungeTaskGate::SledRemoved,
                        ),
                    )
                }
                TaskGateCase::UnrelatedPreparingOnLaterBase => {
                    fixture.tq(&db).commit(2).await;
                    fixture
                        .tq(&db)
                        .propose(
                            3,
                            2,
                            &fixture.members_without(&[&fixture.target]),
                        )
                        .await;
                    fixture.tq(&db).commit(3).await;
                    fixture.tq(&db).propose(4, 3, &without_both).await;
                    (
                        2,
                        SledExpungeTaskGateOutcome::Gate(
                            SledExpungeTaskGate::SledRemoved,
                        ),
                    )
                }
                TaskGateCase::ReAdded => {
                    fixture.tq(&db).commit(2).await;
                    fixture.tq(&db).propose(3, 2, &fixture.members).await;
                    fixture.tq(&db).commit(3).await;
                    (
                        2,
                        SledExpungeTaskGateOutcome::Gate(
                            SledExpungeTaskGate::ReAdded {
                                epoch: Epoch(3),
                                state: TrustQuorumConfigState::Committed,
                            },
                        ),
                    )
                }
            };
            let request = fixture
                .insert_request(
                    &db,
                    tq_epoch,
                    SledExpungeRequestState::WaitingForTrustQuorum,
                )
                .await;

            assert_eq!(
                datastore
                    .sled_expunge_request_check_task_gate(opctx, &request)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{case:?}: checked the gate: {error}")
                    }),
                expected,
                "{case:?}",
            );
            assert_eq!(read(datastore, &request).await, request, "{case:?}");
            assert_eq!(
                fixture.sled_and_disk_policy(&db).await,
                IN_SERVICE,
                "{case:?}",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_task_gate_missing_base_config() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_task_gate_missing_base_config",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());
        let fixture = CompleteFixture::new(&db).await;
        fixture.propose_removal(&db).await;
        fixture.tq(&db).commit(2).await;
        fixture
            .tq(&db)
            .propose(3, 2, &fixture.members_without(&[&fixture.target]))
            .await;
        fixture.tq(&db).commit(3).await;
        fixture
            .tq(&db)
            .propose(4, 3, &fixture.members_without(&[&fixture.target]))
            .await;
        // TODO-RAINCLAUDE: epoch 3 is the base of the Preparing epoch 4.
        fixture.delete_config(&db, 3).await;
        let request = fixture
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        let message = "no trust quorum configuration exists at epoch 3, the \
                       base of the latest configuration (epoch 4)";

        assert_eq!(
            datastore
                .sled_expunge_request_check_task_gate(opctx, &request)
                .await
                .expect_err("a missing base config is an internal error"),
            Error::internal_error(&fixture.context_message(&request, message)),
        );

        // TODO-RAINCLAUDE: complete loads the same inputs and reports the same internal error.
        set_sled_expunge_request_state(
            datastore,
            &request,
            SledExpungeRequestState::Expunging,
        )
        .await;
        let expunging = read(datastore, &request).await;
        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &authz_for(&expunging))
                .await
                .expect_err("a missing base config is an internal error"),
            Error::internal_error(
                &fixture.context_message(&expunging, message)
            ),
        );
        assert_eq!(read(datastore, &request).await, expunging);
        assert_eq!(fixture.sled_and_disk_policy(&db).await, IN_SERVICE);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: an invalid sled policy transition is an internal error too: a sled whose state no longer allows a policy change (decommissioned, though its policy says in service) makes sled_set_policy_conn refuse the expunge, which rolls back as an internal error naming the request, leaving the sled as it was.
    #[tokio::test]
    async fn test_sled_expunge_request_complete_invalid_transition_is_error() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_complete_invalid_transition_is_error",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());
        let fixture = CompleteFixture::new(&db).await;
        fixture.propose_removal(&db).await;
        fixture.tq(&db).ack_all_prepares(2).await;
        let request = fixture
            .insert_request(&db, 2, SledExpungeRequestState::Expunging)
            .await;
        sled_set_state(
            db.opctx(),
            datastore,
            fixture.sled_id,
            nexus_db_model::SledState::Decommissioned,
            ValidateTransition::No,
            Expected::Ok(nexus_db_model::SledState::Active),
        )
        .await
        .expect("marked the sled decommissioned");

        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &authz_for(&request))
                .await
                .expect_err("an invalid sled transition is an error"),
            Error::internal_error(&fixture.context_message(
                &request,
                &format!(
                    "sled id {} has current policy \"in service\" and state \
                     \"decommissioned\" and the transition to policy \
                     \"expunged\" is not permitted",
                    fixture.sled_id,
                ),
            )),
        );
        assert_eq!(read(datastore, &request).await, request);
        assert_eq!(fixture.sled_and_disk_policy(&db).await, IN_SERVICE);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_tq_load_error() {
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
        let mapped = tq_load_error(
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
        assert!(
            err.take().is_none(),
            "a database error does not bail with a custom error"
        );

        let err = OptionalError::new();
        let mapped = tq_load_error(
            &err,
            "context",
            "a config",
            TransactionError::CustomError(Error::invalid_request("no configs")),
        );
        assert_eq!(
            format!("{mapped:?}"),
            format!("{:?}", DieselError::RollbackTransaction),
        );
        let Some(TransitionError::External(error)) = err.take() else {
            panic!("a loader error bails with an external error");
        };
        assert_eq!(
            error,
            Error::internal_error(
                "context: loading a config: Invalid Request: no configs"
            ),
        );
    }

    fn authz_sled(sled_id: SledUuid) -> authz::Sled {
        authz::Sled::new(authz::FLEET, sled_id, LookupType::by_id(sled_id))
    }

    // TODO-RAINCLAUDE: the scrimlet refusal creation should report for this sled and collection, or None if the rule allows it.
    fn scrimlet_refusal(
        sled_id: SledUuid,
        baseboard_id: &BaseboardId,
        collection: Option<&Collection>,
        sled_row_role: SledRole,
    ) -> Option<Error> {
        check_sled_expunge_not_scrimlet(
            sled_id,
            &SledExpungeScrimletEvidence::from_collection(
                collection,
                sled_id,
                baseboard_id,
            ),
            sled_row_role,
        )
        .err()
    }

    fn example_sled_agent(
        sled_id: SledUuid,
        baseboard_id: &BaseboardId,
        role: SledRole,
    ) -> sled_agent_types::inventory::Inventory {
        inventory_examples::sled_agent(
            sled_id,
            baseboard_id.clone(),
            role,
            vec![],
            vec![],
            vec![],
            None,
            inventory_examples::file_source_resolver(
                OmicronFileSourceResolverExampleKind::Success {
                    deserialized_zone_manifest: false,
                    has_mupdate_override: false,
                },
            ),
            SvcsEnabledNotOnlineResult::DataUnavailable,
        )
    }

    // TODO-RAINCLAUDE: a collection holding exactly the scrimlet evidence given for one sled: its sled agent record, its SP record, both, or neither.
    fn collection_with_evidence(
        sled_id: SledUuid,
        baseboard_id: &BaseboardId,
        evidence: CollectionEvidence,
    ) -> Collection {
        let CollectionEvidence { sled_agent_role, sp_slot } = evidence;
        let mut builder = CollectionBuilder::new("test");
        if let Some(role) = sled_agent_role {
            builder
                .found_sled_inventory(
                    "test",
                    example_sled_agent(sled_id, baseboard_id, role),
                )
                .expect("recorded the sled agent");
        }
        if let Some(slot) = sp_slot {
            builder.found_sp_state(
                "test",
                RackUuid::new_v4(),
                SpType::Sled,
                slot,
                SpState {
                    serial_number: baseboard_id.serial_number.clone(),
                    model: baseboard_id.part_number.clone(),
                    ..inventory_examples::sp_state("test")
                },
            );
        }
        builder.build()
    }

    #[derive(Clone)]
    struct CreateInput<'a> {
        opctx: &'a OpContext,
        sled_id: SledUuid,
        rack_id: RackUuid,
        serial: String,
        collection: Option<Collection>,
    }

    async fn create(
        db: &TestDatabase,
        input: &CreateInput<'_>,
    ) -> Result<SledExpungeRequestCreateOutcome, Error> {
        db.datastore()
            .sled_expunge_request_create(
                input.opctx,
                &authz_sled(input.sled_id),
                &make_authz_tq(input.rack_id),
                &input.serial,
                input.collection.as_ref(),
            )
            .await
    }

    // TODO-RAINCLAUDE: what creation may write that the tests can see for one sled: its request rows, its policy, and every trust quorum config of its rack (epoch and state), which covers a new removal config and a base moved to CommittedPartially.
    #[derive(Debug, PartialEq, Eq)]
    struct CreateWrites {
        requests: Vec<SledExpungeRequest>,
        sled_policy: SledPolicy,
        configs: Vec<(i64, TrustQuorumConfigState)>,
    }

    impl CompleteFixture {
        fn create_input<'a>(&self, opctx: &'a OpContext) -> CreateInput<'a> {
            self.create_input_for(opctx, self.sled_id, &self.target)
        }

        fn create_input_for<'a>(
            &self,
            opctx: &'a OpContext,
            sled_id: SledUuid,
            baseboard_id: &BaseboardId,
        ) -> CreateInput<'a> {
            CreateInput {
                opctx,
                sled_id,
                rack_id: self.rack_id,
                serial: baseboard_id.serial_number.clone(),
                collection: None,
            }
        }

        async fn create_as_admin(
            &self,
            db: &TestDatabase,
        ) -> Result<SledExpungeRequestCreateOutcome, Error> {
            create(db, &self.create_input(db.opctx())).await
        }

        async fn create_writes(&self, db: &TestDatabase) -> CreateWrites {
            let sled_id = self.sled_id;
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let requests = dsl::sled_expunge_request
                .filter(dsl::sled_id.eq(to_db_typed_uuid(sled_id)))
                .order_by(dsl::id)
                .select(SledExpungeRequest::as_select())
                .load_async(&*conn)
                .await
                .expect("listed the sled's requests");
            let configs = datastore
                .tq_list_config(
                    opctx,
                    make_authz_tq(self.rack_id),
                    &DataPageParams::max_page(),
                )
                .await
                .expect("listed the trust quorum configs")
                .into_iter()
                .map(|config| {
                    (config.epoch, TrustQuorumConfigState::from(config.state))
                })
                .collect();
            CreateWrites {
                requests,
                sled_policy: sled_policy(db, sled_id).await,
                configs,
            }
        }

        // TODO-RAINCLAUDE: asserts that creating with input fails with expected and writes nothing visible for the fixture's sled and rack.
        async fn expect_create_refused(
            &self,
            db: &TestDatabase,
            input: &CreateInput<'_>,
            expected: Error,
        ) {
            let before = self.create_writes(db).await;
            assert_eq!(
                create(db, input).await.expect_err("creation is refused"),
                expected,
            );
            assert_eq!(self.create_writes(db).await, before, "{expected}");
        }

        async fn set_is_scrimlet(&self, db: &TestDatabase, is_scrimlet: bool) {
            use nexus_db_schema::schema::sled::dsl;
            let conn =
                db.datastore().pool_connection_for_tests().await.unwrap();
            let updated = diesel::update(dsl::sled)
                .filter(dsl::id.eq(to_db_typed_uuid(self.sled_id)))
                .set(dsl::is_scrimlet.eq(is_scrimlet))
                .execute_async(&*conn)
                .await
                .expect("set is_scrimlet");
            assert_eq!(updated, 1);
        }

        async fn expunge_sled(&self, db: &TestDatabase) {
            db.datastore()
                .sled_set_policy_to_expunged(
                    db.opctx(),
                    &authz_sled(self.sled_id),
                )
                .await
                .expect("expunged the sled");
        }

        // TODO-RAINCLAUDE: asserts that outcome is a new request removing the fixture's sled from the epoch 1 trust quorum, and that creation wrote exactly that request, the epoch 2 removal config, and the non-provisionable policy on top of before.
        async fn assert_created(
            &self,
            db: &TestDatabase,
            outcome: SledExpungeRequestCreateOutcome,
            before: CreateWrites,
        ) -> SledExpungeRequest {
            let (request, removal_config) = expect_created(outcome);
            assert_eq!(
                request,
                SledExpungeRequest {
                    id: request.id,
                    time_created: request.time_created,
                    time_modified: request.time_created,
                    rack_id: self.rack_id.into(),
                    sled_id: self.sled_id.into(),
                    part_number: self.target.part_number.clone(),
                    serial_number: self.target.serial_number.clone(),
                    tq_epoch: 2,
                    state: SledExpungeRequestState::WaitingForTrustQuorum,
                    power_off_outcome: None,
                    last_error: None,
                    failure_reason: None,
                    time_completed: None,
                },
            );
            assert_eq!(read(db.datastore(), &request).await, request);

            assert_eq!(removal_config.rack_id, self.rack_id);
            assert_eq!(removal_config.epoch, Epoch(2));
            assert_eq!(removal_config.last_committed_epoch, Some(Epoch(1)));
            assert_eq!(removal_config.state, TrustQuorumConfigState::Preparing);
            assert_eq!(
                removal_config.members.keys().cloned().collect::<BTreeSet<_>>(),
                self.members_without(&[&self.target]),
            );
            let mut stored = self.tq(db).config(2).await;
            // TODO-RAINCLAUDE: the returned config's time_created is Rust-side, at a finer resolution than the stored one.
            stored.time_created = removal_config.time_created;
            assert_eq!(stored, removal_config);

            let non_provisionable = SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            };
            assert_eq!(
                self.sled_and_disk_policy(db).await,
                (non_provisionable, PhysicalDiskPolicy::InService),
            );

            let CreateWrites { mut requests, sled_policy: _, mut configs } =
                before;
            requests.push(request.clone());
            requests.sort_by_key(|request| request.id);
            configs.push((2, TrustQuorumConfigState::Preparing));
            assert_eq!(
                self.create_writes(db).await,
                CreateWrites {
                    requests,
                    sled_policy: non_provisionable,
                    configs
                },
            );
            request
        }
    }

    #[track_caller]
    fn expect_created(
        outcome: SledExpungeRequestCreateOutcome,
    ) -> (SledExpungeRequest, TrustQuorumConfig) {
        match outcome {
            SledExpungeRequestCreateOutcome::Created {
                request,
                removal_config,
            } => (request, *removal_config),
            SledExpungeRequestCreateOutcome::AlreadyOutstanding { request } => {
                panic!("expected a new request, found outstanding {request:?}")
            }
        }
    }

    fn unexpected_violation(
        sled_id: SledUuid,
        constraint: &str,
        details: &str,
    ) -> Error {
        Error::internal_error(&format!(
            "creating an expunge request for sled {sled_id}: unexpected unique \
             violation of {constraint}: duplicate key value violates unique \
             constraint \"{constraint}\" ({details})"
        ))
    }

    fn serial_mismatch(sled_id: SledUuid, serial: &str, given: &str) -> Error {
        check_sled_expunge_serial_acknowledged(sled_id, serial, given)
            .expect_err("the serial numbers differ")
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create() {
        let logctx = dev::test_setup_log("test_sled_expunge_request_create");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let fixture = CompleteFixture::new(&db).await;

        let before = fixture.create_writes(&db).await;
        let request = fixture
            .assert_created(
                &db,
                fixture.create_as_admin(&db).await.expect("created a request"),
                before,
            )
            .await;

        let writes = fixture.create_writes(&db).await;
        assert_eq!(
            fixture.create_as_admin(&db).await.expect("create succeeds"),
            SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                request: request.clone(),
            },
        );
        assert_eq!(fixture.create_writes(&db).await, writes);
        let wrong_serial = CreateInput {
            serial: "not-the-serial".to_string(),
            ..fixture.create_input(db.opctx())
        };
        fixture
            .expect_create_refused(
                &db,
                &wrong_serial,
                serial_mismatch(
                    fixture.sled_id,
                    &fixture.target.serial_number,
                    "not-the-serial",
                ),
            )
            .await;

        fixture.tq(&db).commit(2).await;
        let writes = fixture.create_writes(&db).await;
        assert_eq!(
            fixture
                .create_as_admin(&db)
                .await
                .expect("create succeeds once the sled is not a member"),
            SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                request: request.clone(),
            },
        );
        assert_eq!(fixture.create_writes(&db).await, writes);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[track_caller]
    fn split_outcomes(
        outcomes: [&SledExpungeRequestCreateOutcome; 2],
    ) -> (Vec<&SledExpungeRequest>, Vec<&SledExpungeRequest>) {
        let mut created = Vec::new();
        let mut outstanding = Vec::new();
        for outcome in outcomes {
            match outcome {
                SledExpungeRequestCreateOutcome::Created {
                    request, ..
                } => created.push(request),
                SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                    request,
                } => outstanding.push(request),
            }
        }
        (created, outstanding)
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create_concurrent() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_create_concurrent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;

        // TODO-RAINCLAUDE: a fresh rack per trial, since a successful create uses up the sled; several trials so that more of the runs interleave inside the two transactions.
        for _ in 0..5 {
            let fixture = CompleteFixture::new(&db).await;
            let (first, second) = tokio::join!(
                fixture.create_as_admin(&db),
                fixture.create_as_admin(&db),
            );
            let first = first.expect("the first create succeeds");
            let second = second.expect("the second create succeeds");
            let (created, outstanding) = split_outcomes([&first, &second]);
            assert_eq!(
                (created.len(), outstanding.len()),
                (1, 1),
                "exactly one concurrent create makes a request: {first:?}, \
                 {second:?}",
            );
            assert_eq!(created[0], outstanding[0]);

            let writes = fixture.create_writes(&db).await;
            assert_eq!(writes.requests, vec![created[0].clone()]);
            assert_eq!(
                writes.configs,
                vec![
                    (1, TrustQuorumConfigState::Committed),
                    (2, TrustQuorumConfigState::Preparing),
                ],
                "exactly one removal config was proposed",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create_raced_conn() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_create_raced_conn");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let sled_id = SledUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let baseboard_id = unique_baseboard();
        let serial = baseboard_id.serial_number.as_str();
        let raced =
            async |rack_id: RackUuid,
                   serial: &str,
                   baseboard_violation: Option<&BaseboardId>| {
                DataStore::sled_expunge_request_create_raced_conn(
                    &conn,
                    sled_id,
                    rack_id,
                    serial,
                    baseboard_violation,
                )
                .await
            };
        let baseboard = &baseboard_id;
        let raced_conflict = sled_expunge_request_raced_conflict(sled_id);

        insert_sled_expunge_request_for_sled(
            datastore,
            rack_id,
            sled_id,
            &baseboard_id,
            1,
            SledExpungeRequestState::Failed,
        )
        .await;
        assert_eq!(
            raced(rack_id, serial, Some(baseboard)).await,
            Err(raced_conflict.clone()),
            "a finished request does not count",
        );

        let blocking = insert_sled_expunge_request(
            datastore,
            RackUuid::new_v4(),
            &baseboard_id,
            2,
            SledExpungeRequestState::PoweringOff,
        )
        .await;
        assert_eq!(
            raced(rack_id, serial, Some(baseboard)).await,
            Err(sled_expunge_request_baseboard_conflict(
                sled_id,
                &baseboard_id,
                &blocking,
            )),
            "another sled row's request blocks the baseboard",
        );
        assert_eq!(
            raced(rack_id, serial, None).await,
            Err(raced_conflict.clone()),
            "the baseboard is only consulted for a baseboard violation",
        );
        diesel::delete(dsl::sled_expunge_request)
            .filter(dsl::id.eq(blocking.id))
            .execute_async(&*conn)
            .await
            .expect("deleted the blocking request");

        // TODO-RAINCLAUDE: a concurrent create for this same sled also trips the baseboard index; it is replayed as this sled's request, never named as the blocker.
        let unfinished = insert_sled_expunge_request_for_sled(
            datastore,
            rack_id,
            sled_id,
            &baseboard_id,
            3,
            SledExpungeRequestState::WaitingForTrustQuorum,
        )
        .await;
        assert_eq!(
            raced(rack_id, serial, Some(baseboard)).await,
            Ok(SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                request: unfinished,
            }),
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn still_preparing_conflict(epoch: u64) -> Error {
        Error::conflict(format!(
            "Trust quorum reconfiguration in progress: still preparing in \
             epoch {epoch}. Wait for it to finish and retry, or abort it to \
             reconfigure now."
        ))
    }

    fn below_commit_threshold_conflict(
        committing: &TrustQuorumConfig,
    ) -> Error {
        let min_acks = usize::from(committing.threshold.0)
            + usize::from(committing.commit_crash_tolerance);
        Error::conflict(format!(
            "Trust quorum reconfiguration in progress: not enough nodes have \
             acked commit in epoch {}. Expected at least {min_acks} acks, Got \
             {} acks. If this persists please contact Oxide support.",
            committing.epoch,
            committing.acked_commits(),
        ))
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create_commits_base_partially() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_create_commits_base_partially",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let fixture = CompleteFixture::new(&db).await;
        let other = BaseboardId::from(fixture.hw_ids[1].clone());
        fixture
            .tq(&db)
            .propose(2, 1, &fixture.members_without(&[&other]))
            .await;
        fixture.tq(&db).ack_all_prepares(2).await;
        let committing = fixture.tq(&db).config(2).await;
        let min_acks = usize::from(committing.threshold.0)
            + usize::from(committing.commit_crash_tolerance);
        assert!(min_acks < committing.members.len());
        assert_eq!(
            datastore
                .tq_update_commit_status(
                    opctx,
                    make_authz_tq(fixture.rack_id),
                    Epoch(2),
                    committing.members.keys().take(min_acks).cloned().collect(),
                )
                .await
                .expect("acked K+Z commits"),
            TrustQuorumConfigState::Committing,
        );

        // TODO-RAINCLAUDE: a 2.9 failure rolls back tq_insert_config_on_base_conn's move of epoch 2 to CommittedPartially, not only the new config.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let at_epoch = insert_sled_expunge_request(
            datastore,
            fixture.rack_id,
            &unique_baseboard(),
            3,
            SledExpungeRequestState::Done,
        )
        .await;
        fixture
            .expect_create_refused(
                &db,
                &fixture.create_input(opctx),
                unexpected_violation(
                    fixture.sled_id,
                    BY_RACK_AND_EPOCH_INDEX,
                    &format!(
                        "Key (rack_id,tq_epoch)=('{}',3) already exists.",
                        fixture.rack_id,
                    ),
                ),
            )
            .await;
        assert_eq!(
            fixture.create_writes(&db).await.configs,
            vec![
                (1, TrustQuorumConfigState::Committed),
                (2, TrustQuorumConfigState::Committing),
            ],
        );
        diesel::delete(dsl::sled_expunge_request)
            .filter(dsl::id.eq(at_epoch.id))
            .execute_async(&*conn)
            .await
            .expect("deleted the request at the removal epoch");

        let (request, removal_config) = expect_created(
            fixture.create_as_admin(&db).await.expect("created a request"),
        );
        assert_eq!(request.tq_epoch, 3);
        assert_eq!(removal_config.last_committed_epoch, Some(Epoch(2)));
        assert_eq!(
            fixture.create_writes(&db).await.configs,
            vec![
                (1, TrustQuorumConfigState::Committed),
                (2, TrustQuorumConfigState::CommittedPartially),
                (3, TrustQuorumConfigState::Preparing),
            ],
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create_after_failed_request() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_create_after_failed_request",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let fixture = CompleteFixture::new(&db).await;
        fixture.propose_removal(&db).await;
        datastore
            .tq_abort_config(
                opctx,
                make_authz_tq(fixture.rack_id),
                Epoch(2),
                "test abort".to_string(),
            )
            .await
            .expect("aborted epoch 2");
        let failed = fixture
            .insert_request(&db, 2, SledExpungeRequestState::Failed)
            .await;
        datastore
            .sled_set_provision_policy(
                opctx,
                &authz_sled(fixture.sled_id),
                SledProvisionPolicy::NonProvisionable,
            )
            .await
            .expect("made the sled non-provisionable");

        let (request, removal_config) = expect_created(
            fixture
                .create_as_admin(&db)
                .await
                .expect("created after a failed request and aborted removal"),
        );
        assert_ne!(request.id, failed.id);
        assert_eq!(request.tq_epoch, 3);
        assert_eq!(removal_config.epoch, Epoch(3));
        assert_eq!(removal_config.last_committed_epoch, Some(Epoch(1)));
        let writes = fixture.create_writes(&db).await;
        assert_eq!(
            writes.requests.len(),
            2,
            "the failed request and the new one",
        );
        assert_eq!(
            writes.configs,
            vec![
                (1, TrustQuorumConfigState::Committed),
                (2, TrustQuorumConfigState::Aborted),
                (3, TrustQuorumConfigState::Preparing),
            ],
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    const WRONG_SERIAL: &str = "not-the-serial";

    #[derive(Clone, Copy, Debug)]
    enum InputSled {
        Fixture,
        Missing,
    }

    #[derive(Clone, Copy, Debug)]
    enum InputRack {
        Fixture,
        Other,
    }

    #[derive(Clone, Copy, Debug)]
    enum AcknowledgedSerial {
        Correct,
        Wrong,
    }

    // TODO-RAINCLAUDE: at most one unfinished request per baseboard, so this sled's own unfinished request and another sled row's for the same baseboard are mutually exclusive.
    #[derive(Clone, Copy, Debug)]
    enum UnfinishedRequest {
        None,
        ThisSled(SledExpungeRequestState),
        OtherSledRow(SledExpungeRequestState),
    }

    #[derive(Clone, Copy, Debug)]
    struct CollectionEvidence {
        sled_agent_role: Option<SledRole>,
        sp_slot: Option<u16>,
    }

    #[derive(Clone, Copy, Debug)]
    enum TargetMembership {
        Member,
        NotMember,
    }

    #[derive(Clone, Copy, Debug)]
    enum InFlight {
        None,
        Preparing,
        CommittingBelowThreshold,
    }

    // TODO-RAINCLAUDE: NotMember is set up by committing an epoch 2 that removes the target, so it needs at least one member more than the minimum; in-flight configs sit on top of the last committed config with its membership.
    #[derive(Clone, Copy, Debug)]
    enum CaseTrustQuorum {
        Configured {
            members: usize,
            target: TargetMembership,
            in_flight: InFlight,
        },
        Lrtq,
    }

    #[derive(Clone, Debug)]
    struct CreateCase {
        input_sled: InputSled,
        input_rack: InputRack,
        serial: AcknowledgedSerial,
        unfinished: UnfinishedRequest,
        finished_oldest_first: Vec<FinishedSledExpungeRequestState>,
        policy: SledPolicy,
        state: SledState,
        sled_row_role: SledRole,
        collection: Option<CollectionEvidence>,
        trust_quorum: CaseTrustQuorum,
    }

    // TODO-RAINCLAUDE: weighted so that about one case in twelve has no fault and is created, and every check is regularly the first to fail (measured over 128 cases).
    fn create_case_strategy() -> impl Strategy<Value = CreateCase> {
        let unfinished_state =
            || proptest::sample::select(SledExpungeRequestState::UNFINISHED);
        let unfinished = prop_oneof![
            6 => Just(UnfinishedRequest::None),
            1 => unfinished_state().prop_map(UnfinishedRequest::ThisSled),
            1 => unfinished_state().prop_map(UnfinishedRequest::OtherSledRow),
        ];
        let finished = proptest::collection::vec(
            prop_oneof![
                Just(FinishedSledExpungeRequestState::Done),
                Just(FinishedSledExpungeRequestState::Failed),
            ],
            0..=2,
        );
        let policy = prop_oneof![
            4 => Just(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            }),
            2 => Just(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            }),
            1 => Just(SledPolicy::Expunged),
        ];
        let state = prop_oneof![
            6 => Just(SledState::Active),
            1 => Just(SledState::Decommissioned),
        ];
        let sled_row_role = prop_oneof![
            5 => Just(SledRole::Gimlet),
            1 => Just(SledRole::Scrimlet),
        ];
        let sled_agent_role = prop_oneof![
            2 => Just(None),
            2 => Just(Some(SledRole::Gimlet)),
            1 => Just(Some(SledRole::Scrimlet)),
        ];
        let sp_slot = prop_oneof![
            2 => Just(None),
            2 => proptest::sample::select(vec![0u16, 1, 15]).prop_map(Some),
            1 => proptest::sample::select(SWITCH_ATTACHED_SLED_CUBBIES.to_vec())
                .prop_map(Some),
        ];
        let collection =
            proptest::option::of((sled_agent_role, sp_slot).prop_map(
                |(sled_agent_role, sp_slot)| CollectionEvidence {
                    sled_agent_role,
                    sp_slot,
                },
            ));
        let in_flight = || {
            prop_oneof![
                4 => Just(InFlight::None),
                1 => Just(InFlight::Preparing),
                1 => Just(InFlight::CommittingBelowThreshold),
            ]
        };
        let above_minimum = TRUST_QUORUM_MIN_MEMBERS + 1..=10;
        let trust_quorum = prop_oneof![
            4 => (above_minimum.clone(), in_flight()).prop_map(
                |(members, in_flight)| CaseTrustQuorum::Configured {
                    members,
                    target: TargetMembership::Member,
                    in_flight,
                },
            ),
            1 => in_flight().prop_map(|in_flight| CaseTrustQuorum::Configured {
                members: TRUST_QUORUM_MIN_MEMBERS,
                target: TargetMembership::Member,
                in_flight,
            }),
            1 => (above_minimum, in_flight()).prop_map(
                |(members, in_flight)| CaseTrustQuorum::Configured {
                    members,
                    target: TargetMembership::NotMember,
                    in_flight,
                },
            ),
            1 => Just(CaseTrustQuorum::Lrtq),
        ];
        let input_sled = prop_oneof![
            12 => Just(InputSled::Fixture),
            1 => Just(InputSled::Missing),
        ];
        let input_rack = prop_oneof![
            12 => Just(InputRack::Fixture),
            1 => Just(InputRack::Other),
        ];
        let serial = prop_oneof![
            6 => Just(AcknowledgedSerial::Correct),
            1 => Just(AcknowledgedSerial::Wrong),
        ];
        (
            input_sled,
            input_rack,
            serial,
            unfinished,
            finished,
            policy,
            state,
            sled_row_role,
            collection,
            trust_quorum,
        )
            .prop_map(
                |(
                    input_sled,
                    input_rack,
                    serial,
                    unfinished,
                    finished_oldest_first,
                    policy,
                    state,
                    sled_row_role,
                    collection,
                    trust_quorum,
                )| CreateCase {
                    input_sled,
                    input_rack,
                    serial,
                    unfinished,
                    finished_oldest_first,
                    policy,
                    state,
                    sled_row_role,
                    collection,
                    trust_quorum,
                },
            )
    }

    struct CreateSetup {
        fixture: CompleteFixture,
        missing_sled_id: SledUuid,
        other_rack_id: RackUuid,
        unfinished: Option<SledExpungeRequest>,
        blocking: Option<SledExpungeRequest>,
        latest_finished: Option<FinishedSledExpungeRequest>,
        last_committed: Option<TrustQuorumConfig>,
        in_flight: Option<TrustQuorumConfig>,
        collection: Option<Collection>,
    }

    // TODO-RAINCLAUDE: the checks of sled_expunge_request_create in the order it documents (2.1 to 2.6, with the sled lookup and its rack check between the 2.1 replay and the baseboard check); the expected outcome is that of the first check that applies.
    #[derive(Clone, Copy, Debug)]
    enum CreateCheck {
        ReplayOutstanding,
        SledExists,
        SledInAuthorizedRack,
        BaseboardUnclaimed,
        PolicyAndState,
        SerialAcknowledged,
        NotScrimlet,
        TrustQuorumConfigured,
        NoReconfigurationInFlight,
        MemberOfTrustQuorum,
        KeepsMinimumMembers,
    }

    const CREATE_CHECK_ORDER: [CreateCheck; 11] = [
        CreateCheck::ReplayOutstanding,
        CreateCheck::SledExists,
        CreateCheck::SledInAuthorizedRack,
        CreateCheck::BaseboardUnclaimed,
        CreateCheck::PolicyAndState,
        CreateCheck::SerialAcknowledged,
        CreateCheck::NotScrimlet,
        CreateCheck::TrustQuorumConfigured,
        CreateCheck::NoReconfigurationInFlight,
        CreateCheck::MemberOfTrustQuorum,
        CreateCheck::KeepsMinimumMembers,
    ];

    #[derive(Debug)]
    enum ExpectedCreate {
        Refused(Error),
        AlreadyOutstanding(SledExpungeRequest),
        Created,
    }

    impl CreateCase {
        async fn set_up(&self, db: &TestDatabase) -> CreateSetup {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let fixture = match self.trust_quorum {
                CaseTrustQuorum::Configured { members, .. } => {
                    CompleteFixture::with_members(
                        db,
                        members,
                        FixtureTrustQuorum::Rss,
                    )
                    .await
                }
                CaseTrustQuorum::Lrtq => {
                    CompleteFixture::with_members(
                        db,
                        10,
                        FixtureTrustQuorum::Lrtq,
                    )
                    .await
                }
            };

            let (last_committed, in_flight) = match self.trust_quorum {
                CaseTrustQuorum::Lrtq => (None, None),
                CaseTrustQuorum::Configured {
                    members: _,
                    target,
                    in_flight,
                } => {
                    let (base_epoch, base_members) = match target {
                        TargetMembership::Member => {
                            (1, fixture.members.clone())
                        }
                        TargetMembership::NotMember => {
                            let without =
                                fixture.members_without(&[&fixture.target]);
                            fixture.tq(db).propose(2, 1, &without).await;
                            fixture.tq(db).commit(2).await;
                            (2, without)
                        }
                    };
                    let in_flight_epoch = base_epoch + 1;
                    let in_flight = match in_flight {
                        InFlight::None => None,
                        InFlight::Preparing => {
                            fixture
                                .tq(db)
                                .propose(
                                    in_flight_epoch,
                                    base_epoch,
                                    &base_members,
                                )
                                .await;
                            Some(fixture.tq(db).config(in_flight_epoch).await)
                        }
                        InFlight::CommittingBelowThreshold => {
                            fixture
                                .tq(db)
                                .propose(
                                    in_flight_epoch,
                                    base_epoch,
                                    &base_members,
                                )
                                .await;
                            fixture
                                .tq(db)
                                .ack_all_prepares(in_flight_epoch)
                                .await;
                            let committing =
                                fixture.tq(db).config(in_flight_epoch).await;
                            assert_eq!(committing.acked_commits(), 0);
                            Some(committing)
                        }
                    };
                    (Some(fixture.tq(db).config(base_epoch).await), in_flight)
                }
            };

            match self.policy {
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                } => {}
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::NonProvisionable,
                } => {
                    datastore
                        .sled_set_provision_policy(
                            opctx,
                            &authz_sled(fixture.sled_id),
                            SledProvisionPolicy::NonProvisionable,
                        )
                        .await
                        .expect("made the sled non-provisionable");
                }
                SledPolicy::Expunged => fixture.expunge_sled(db).await,
            }
            match self.state {
                SledState::Active => {}
                SledState::Decommissioned => {
                    sled_set_state(
                        opctx,
                        datastore,
                        fixture.sled_id,
                        nexus_db_model::SledState::Decommissioned,
                        ValidateTransition::No,
                        Expected::Ok(nexus_db_model::SledState::Active),
                    )
                    .await
                    .expect("marked the sled decommissioned");
                }
            }
            match self.sled_row_role {
                SledRole::Gimlet => {}
                SledRole::Scrimlet => fixture.set_is_scrimlet(db, true).await,
            }

            // TODO-RAINCLAUDE: explicit, distinct time_created values, since two inserts can share one at the database's resolution.
            let first_created = now_db_precision() - chrono::Duration::days(1);
            let mut latest_finished = None;
            for (hours, &state) in (0..).zip(&self.finished_oldest_first) {
                let request_state = match state {
                    FinishedSledExpungeRequestState::Done => {
                        SledExpungeRequestState::Done
                    }
                    FinishedSledExpungeRequestState::Failed => {
                        SledExpungeRequestState::Failed
                    }
                };
                let request = fixture
                    .insert_request(db, 100 + hours, request_state)
                    .await;
                set_time_created(
                    db,
                    &request,
                    first_created + chrono::Duration::hours(hours),
                )
                .await;
                latest_finished = Some(FinishedSledExpungeRequest {
                    id: request.id(),
                    state,
                });
            }

            let (unfinished, blocking) = match self.unfinished {
                UnfinishedRequest::None => (None, None),
                UnfinishedRequest::ThisSled(state) => {
                    (Some(fixture.insert_request(db, 99, state).await), None)
                }
                UnfinishedRequest::OtherSledRow(state) => {
                    let blocking = insert_sled_expunge_request(
                        datastore,
                        RackUuid::new_v4(),
                        &fixture.target,
                        99,
                        state,
                    )
                    .await;
                    (None, Some(blocking))
                }
            };

            let collection = self.collection.map(|evidence| {
                collection_with_evidence(
                    fixture.sled_id,
                    &fixture.target,
                    evidence,
                )
            });

            CreateSetup {
                fixture,
                missing_sled_id: SledUuid::new_v4(),
                other_rack_id: RackUuid::new_v4(),
                unfinished,
                blocking,
                latest_finished,
                last_committed,
                in_flight,
                collection,
            }
        }

        fn input<'a>(
            &self,
            setup: &CreateSetup,
            opctx: &'a OpContext,
        ) -> CreateInput<'a> {
            let fixture = &setup.fixture;
            CreateInput {
                opctx,
                sled_id: match self.input_sled {
                    InputSled::Fixture => fixture.sled_id,
                    InputSled::Missing => setup.missing_sled_id,
                },
                rack_id: match self.input_rack {
                    InputRack::Fixture => fixture.rack_id,
                    InputRack::Other => setup.other_rack_id,
                },
                serial: match self.serial {
                    AcknowledgedSerial::Correct => {
                        fixture.target.serial_number.clone()
                    }
                    AcknowledgedSerial::Wrong => WRONG_SERIAL.to_string(),
                },
                collection: setup.collection.clone(),
            }
        }

        // TODO-RAINCLAUDE: the scrimlet rule restated independently of check_sled_expunge_not_scrimlet, which only supplies the exact wording.
        fn is_scrimlet(&self) -> bool {
            let by_sled_row = match self.sled_row_role {
                SledRole::Scrimlet => true,
                SledRole::Gimlet => false,
            };
            let Some(CollectionEvidence { sled_agent_role, sp_slot }) =
                self.collection
            else {
                return by_sled_row;
            };
            let by_role = match sled_agent_role {
                Some(SledRole::Scrimlet) => true,
                Some(SledRole::Gimlet) | None => false,
            };
            let by_slot = sp_slot.is_some_and(|slot| {
                SWITCH_ATTACHED_SLED_CUBBIES.contains(&slot)
            });
            let no_records = sled_agent_role.is_none() && sp_slot.is_none();
            by_role || by_slot || (no_records && by_sled_row)
        }

        fn check_outcome(
            &self,
            setup: &CreateSetup,
            check: CreateCheck,
        ) -> Option<ExpectedCreate> {
            let fixture = &setup.fixture;
            let sled_id = fixture.sled_id;
            let refused = |error: Error| Some(ExpectedCreate::Refused(error));
            match check {
                CreateCheck::ReplayOutstanding => {
                    let request = match self.input_sled {
                        InputSled::Fixture => setup.unfinished.as_ref()?,
                        InputSled::Missing => return None,
                    };
                    match (self.input_rack, self.serial) {
                        (InputRack::Other, _) => {
                            refused(Error::internal_error(&format!(
                                "creating an expunge request for sled \
                                 {sled_id}: its unfinished request {} is for \
                                 rack {}, but the trust quorum config \
                                 authorized is for rack {}",
                                request.id,
                                fixture.rack_id,
                                setup.other_rack_id,
                            )))
                        }
                        (InputRack::Fixture, AcknowledgedSerial::Wrong) => {
                            refused(serial_mismatch(
                                sled_id,
                                &request.serial_number,
                                WRONG_SERIAL,
                            ))
                        }
                        (InputRack::Fixture, AcknowledgedSerial::Correct) => {
                            Some(ExpectedCreate::AlreadyOutstanding(
                                request.clone(),
                            ))
                        }
                    }
                }
                CreateCheck::SledExists => match self.input_sled {
                    InputSled::Fixture => None,
                    InputSled::Missing => refused(Error::ObjectNotFound {
                        type_name: ResourceType::Sled,
                        lookup_type: LookupType::ById(
                            setup.missing_sled_id.into_untyped_uuid(),
                        ),
                    }),
                },
                CreateCheck::SledInAuthorizedRack => match self.input_rack {
                    InputRack::Fixture => None,
                    InputRack::Other => {
                        refused(Error::internal_error(&format!(
                            "creating an expunge request for sled {sled_id}: the \
                         sled is in rack {}, but the trust quorum config \
                         authorized is for rack {}",
                            fixture.rack_id, setup.other_rack_id,
                        )))
                    }
                },
                CreateCheck::BaseboardUnclaimed => {
                    let blocking = setup.blocking.as_ref()?;
                    refused(sled_expunge_request_baseboard_conflict(
                        sled_id,
                        &fixture.target,
                        blocking,
                    ))
                }
                CreateCheck::PolicyAndState => {
                    let expungeable = match (self.policy, self.state) {
                        (SledPolicy::InService { .. }, SledState::Active) => {
                            true
                        }
                        (
                            SledPolicy::InService { .. },
                            SledState::Decommissioned,
                        )
                        | (
                            SledPolicy::Expunged,
                            SledState::Active | SledState::Decommissioned,
                        ) => false,
                    };
                    if expungeable {
                        return None;
                    }
                    refused(
                        check_sled_expunge_policy_and_state(
                            sled_id,
                            self.policy,
                            self.state,
                        )
                        .expect_err("the policy and state are refused")
                        .into_error(setup.latest_finished),
                    )
                }
                CreateCheck::SerialAcknowledged => match self.serial {
                    AcknowledgedSerial::Correct => None,
                    AcknowledgedSerial::Wrong => refused(serial_mismatch(
                        sled_id,
                        &fixture.target.serial_number,
                        WRONG_SERIAL,
                    )),
                },
                CreateCheck::NotScrimlet => {
                    let refusal = scrimlet_refusal(
                        sled_id,
                        &fixture.target,
                        setup.collection.as_ref(),
                        self.sled_row_role,
                    );
                    assert_eq!(
                        refusal.is_some(),
                        self.is_scrimlet(),
                        "the scrimlet rule agrees with the test's: {refusal:?}",
                    );
                    refusal.map(ExpectedCreate::Refused)
                }
                CreateCheck::TrustQuorumConfigured => match self.trust_quorum {
                    CaseTrustQuorum::Configured { .. } => None,
                    CaseTrustQuorum::Lrtq => refused(Error::conflict(format!(
                        "Missing trust quorum configurations for rack {}. \
                             Upgrade to trust quorum required.",
                        fixture.rack_id,
                    ))),
                },
                CreateCheck::NoReconfigurationInFlight => {
                    let in_flight = setup.in_flight.as_ref()?;
                    match self.trust_quorum {
                        CaseTrustQuorum::Configured {
                            in_flight: InFlight::Preparing,
                            ..
                        } => {
                            refused(still_preparing_conflict(in_flight.epoch.0))
                        }
                        CaseTrustQuorum::Configured {
                            in_flight: InFlight::CommittingBelowThreshold,
                            ..
                        } => {
                            refused(below_commit_threshold_conflict(in_flight))
                        }
                        CaseTrustQuorum::Configured {
                            in_flight: InFlight::None,
                            ..
                        }
                        | CaseTrustQuorum::Lrtq => {
                            panic!(
                                "an in-flight config was set up for {self:?}"
                            )
                        }
                    }
                }
                CreateCheck::MemberOfTrustQuorum => match self.trust_quorum {
                    CaseTrustQuorum::Configured {
                        target: TargetMembership::NotMember,
                        ..
                    } => refused(sled_expunge_removal_refusal(
                        sled_id,
                        setup
                            .last_committed
                            .as_ref()
                            .expect("a configured rack has a committed config"),
                        RemoveSledError::NotMember {
                            sled: fixture.target.clone(),
                        },
                    )),
                    CaseTrustQuorum::Configured {
                        target: TargetMembership::Member,
                        ..
                    }
                    | CaseTrustQuorum::Lrtq => None,
                },
                CreateCheck::KeepsMinimumMembers => match self.trust_quorum {
                    CaseTrustQuorum::Configured {
                        members,
                        target: TargetMembership::Member,
                        ..
                    } if members <= TRUST_QUORUM_MIN_MEMBERS => {
                        refused(sled_expunge_removal_refusal(
                            sled_id,
                            setup.last_committed.as_ref().expect(
                                "a configured rack has a committed config",
                            ),
                            RemoveSledError::InvalidMembership {
                                rack_id: fixture.rack_id,
                                sled: fixture.target.clone(),
                                error: TrustQuorumMembersError::TooFew {
                                    num_members: members - 1,
                                },
                            },
                        ))
                    }
                    CaseTrustQuorum::Configured { .. }
                    | CaseTrustQuorum::Lrtq => None,
                },
            }
        }

        fn expected(&self, setup: &CreateSetup) -> ExpectedCreate {
            CREATE_CHECK_ORDER
                .iter()
                .find_map(|&check| self.check_outcome(setup, check))
                .unwrap_or(ExpectedCreate::Created)
        }

        async fn run(&self, db: &TestDatabase) {
            let setup = self.set_up(db).await;
            let input = self.input(&setup, db.opctx());
            let fixture = &setup.fixture;
            match self.expected(&setup) {
                ExpectedCreate::Refused(error) => {
                    fixture.expect_create_refused(db, &input, error).await;
                }
                ExpectedCreate::AlreadyOutstanding(request) => {
                    let before = fixture.create_writes(db).await;
                    assert_eq!(
                        create(db, &input).await,
                        Ok(SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                            request,
                        }),
                    );
                    assert_eq!(fixture.create_writes(db).await, before);
                }
                ExpectedCreate::Created => {
                    let before = fixture.create_writes(db).await;
                    fixture
                        .assert_created(
                            db,
                            create(db, &input)
                                .await
                                .expect("created a request"),
                            before,
                        )
                        .await;
                }
            }
        }
    }

    async fn set_time_created(
        db: &TestDatabase,
        request: &SledExpungeRequest,
        time_created: chrono::DateTime<chrono::Utc>,
    ) {
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();
        let updated = diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(request.id))
            .set(dsl::time_created.eq(time_created))
            .execute_async(&*conn)
            .await
            .expect("set the request's time_created");
        assert_eq!(updated, 1);
    }

    // TODO-RAINCLAUDE: a fresh rack per case, all in one database, so that cases are independent and shrink cleanly.
    #[test]
    fn test_sled_expunge_request_create_checks_in_order() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("built a tokio runtime");
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_create_checks_in_order",
        );
        let db =
            runtime.block_on(TestDatabase::new_with_datastore(&logctx.log));

        proptest!(ProptestConfig::with_cases(128), |(case in create_case_strategy())| {
            runtime.block_on(case.run(&db));
        });

        runtime.block_on(db.terminate());
        logctx.cleanup_successful();
    }
}
