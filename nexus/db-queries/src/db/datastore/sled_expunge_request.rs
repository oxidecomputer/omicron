// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use super::ValidateTransition;
use super::sled::TransitionError;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
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
use nexus_db_model::DbSledExpungeRequestState;
use nexus_db_model::DbSledExpungeSpSlotSource;
use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
use nexus_db_model::SledExpungeRequestTransitionColumns;
use nexus_db_model::SpMgsSlot;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::external_api::sled::SledProvisionPolicy;
use nexus_types::inventory::Collection;
use nexus_types::sled_expunge::FinishedSledExpungeRequest;
use nexus_types::sled_expunge::SWITCH_ATTACHED_SLED_CUBBIES;
use nexus_types::sled_expunge::SledExpungeCreationPresence;
use nexus_types::sled_expunge::SledExpungeCreationProbe;
use nexus_types::sled_expunge::SledExpungeCreationSpSlotSource;
use nexus_types::sled_expunge::SledExpungeCubbyPrecondition;
use nexus_types::sled_expunge::SledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeRequestAdvance;
use nexus_types::sled_expunge::SledExpungeRequestStateKind;
use nexus_types::sled_expunge::SledExpungeRequestTransition;
use nexus_types::sled_expunge::SledExpungeRequestUnfinishedState;
use nexus_types::sled_expunge::SledExpungeScrimletEvidence;
use nexus_types::sled_expunge::SledExpungeSpSlotChange;
use nexus_types::sled_expunge::check_sled_expunge_baseboard_recorded;
use nexus_types::sled_expunge::check_sled_expunge_not_scrimlet;
use nexus_types::sled_expunge::check_sled_expunge_policy_and_state;
use nexus_types::sled_expunge::check_sled_expunge_serial_acknowledged;
use nexus_types::sled_expunge::sled_expunge_removal_refusal;
use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use nexus_types::trust_quorum::SledExpungeTaskGatePlan;
use nexus_types::trust_quorum::TrustQuorumConfig;
use nexus_types::trust_quorum::TrustQuorumConfigPastPreparing;
use nexus_types::trust_quorum::sled_expunge_task_gate_plan;
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
    Updated { request: SledExpungeRequest },
    NotInExpectedState { current: SledExpungeRequest },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRecordSpSlotOutcome {
    Updated { request: SledExpungeRequest },
    NotInExpectedState { current: SledExpungeRequest },
    SwitchAttachedCubby { sp_slot: u16 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeRequestCompleteOutcome {
    Completed { previous_sled_policy: SledPolicy },
    NotExpunging { current: SledExpungeRequest },
    ReAddPreparing { epoch: Epoch },
    ReAdded { epoch: Epoch, state: TrustQuorumConfigPastPreparing },
}

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

// TODO-RAINCLAUDE: A sled's unfinished request that has passed the two replay
// checks, which are that the request is for the authorized rack and that the
// caller acknowledged its serial number. Only
// `sled_expunge_request_replay_outstanding` builds one, so holding one means
// both checks ran.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub struct SledExpungeRequestOutstanding(SledExpungeRequest);

impl SledExpungeRequestOutstanding {
    pub fn into_outcome(self) -> SledExpungeRequestCreateOutcome {
        SledExpungeRequestCreateOutcome::AlreadyOutstanding { request: self.0 }
    }
}

const TRUST_QUORUM_CONFIGURATION_PKEY: &str = "trust_quorum_configuration_pkey";
const TRUST_QUORUM_ACTIVE_CONFIGURATIONS_INDEX: &str =
    "trust_quorum_active_configurations";
const UNFINISHED_BY_SLED_INDEX: &str =
    "lookup_unfinished_sled_expunge_request_by_sled";
const UNFINISHED_BY_BASEBOARD_INDEX: &str =
    "lookup_unfinished_sled_expunge_request_by_baseboard";

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

// TODO-RAINCLAUDE: What a unique violation during creation means, judged by the
// constraint that was hit.
//
// * `Raced` means a concurrent trust quorum reconfiguration, or a concurrent
//   creation for this sled, got there first. The caller re-reads to find out
//   which.
// * `BaseboardRequest` means another unfinished request has this baseboard. The
//   explicit check earlier in the transaction reports this first, unless that
//   request was created after the check ran.
// * `Unexpected` is any other constraint, and is an internal error.
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

fn sled_expunge_request_replay_outstanding(
    sled_id: SledUuid,
    rack_id: RackUuid,
    acknowledged_serial: &str,
    request: SledExpungeRequest,
) -> Result<SledExpungeRequestOutstanding, Error> {
    if request.rack_id != rack_id {
        return Err(Error::internal_error(&format!(
            "creating an expunge request for sled {sled_id}: its unfinished \
             request {} is for rack {}, but the trust quorum config \
             authorized is for rack {rack_id}",
            request.id, request.rack_id,
        )));
    }
    check_sled_expunge_serial_acknowledged(
        sled_id,
        &request.baseboard_id.serial_number,
        acknowledged_serial,
    )?;
    Ok(SledExpungeRequestOutstanding(request))
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
        blocking.id,
        blocking.sled_id,
        blocking.state.kind(),
    ))
}

// TODO-RAINCLAUDE: The transitions that `sled_expunge_request_transition` may
// write. `Done` is left out so that the only way to enter it is
// `sled_expunge_request_complete`, which checks the task gate and expunges the
// sled in the same transaction.
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
        decode_sled_expunge_request(request)
    }

    pub async fn sled_expunge_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (chrono::DateTime<chrono::Utc>, Uuid)>,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let rows = paginated_multicolumn(
            dsl::sled_expunge_request,
            (dsl::time_created, dsl::id),
            pagparams,
        )
        .select(DbSledExpungeRequest::as_select())
        .load_async(&*conn)
        .await
        .map_err(|error| {
            public_error_from_diesel(error, ErrorHandler::Server)
        })?;
        rows.into_iter().map(decode_sled_expunge_request).collect()
    }

    // TODO-RAINCLAUDE: Lists every unfinished sled expunge request, across all
    // racks, for the `sled_expunge` background task.
    //
    // The list is read in batches paged by `sled_id`, which is unique among
    // unfinished requests, so no request is listed twice. The batches aren't
    // one snapshot, so callers must compare-and-swap when they act on a
    // request. A request created partway through the scan can be missed, and
    // the next activation picks it up.
    pub async fn sled_expunge_request_list_unfinished(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
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
                dsl::sled_id,
                &p.current_pagparams(),
            )
            .filter(dsl::state.eq_any(DbSledExpungeRequestState::UNFINISHED))
            .select(DbSledExpungeRequest::as_select())
            .load_async(&*conn)
            .await
            .map_err(|error| {
                public_error_from_diesel(error, ErrorHandler::Server)
            })?;
            paginator =
                p.found_batch(&batch, &|request: &DbSledExpungeRequest| {
                    request.sled_id.into_untyped_uuid()
                });
            for row in batch {
                requests.push(decode_sled_expunge_request(row)?);
            }
        }
        Ok(requests)
    }

    // TODO-RAINCLAUDE: Creates a sled expunge request, running the creation
    // checks from RFD 747 in order inside one transaction.
    //
    // The first check looks for an unfinished request for this sled, and
    // returns it if there is one. It comes first so that a retry after a lost
    // response finds the request the first attempt created, even though the
    // sled may have left the trust quorum by then.
    pub async fn sled_expunge_request_create(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        authz_tq: &authz::TrustQuorumConfig,
        acknowledged_serial: &str,
        latest_collection: Option<&Collection>,
        probe: SledExpungeCreationProbe,
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
                        probe,
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
            | CreateUniqueViolation::BaseboardRequest => {
                warn!(
                    opctx.log, "{log_message}";
                    "sled_id" => %sled_id,
                    "constraint" => ?violation.constraint,
                    "classified" => ?classified,
                );
            }
            CreateUniqueViolation::Unexpected => {}
        }
        let baseboard_violation = match classified {
            CreateUniqueViolation::Raced => None,
            CreateUniqueViolation::BaseboardRequest => {
                Some(&violation.baseboard_id)
            }
            CreateUniqueViolation::Unexpected => {
                error!(
                    opctx.log, "{log_message}";
                    "sled_id" => %sled_id,
                    "constraint" => ?violation.constraint,
                    "message" => &violation.message,
                    "details" => ?violation.details,
                );
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
        probe: SledExpungeCreationProbe,
        err: &OptionalError<CreateTransactionError>,
    ) -> Result<SledExpungeRequestCreateOutcome, DieselError> {
        let sled_id = authz_sled.id();
        let rack_id = authz_tq.rack().id();
        let refuse =
            |error: Error| err.bail(CreateTransactionError::Refused(error));

        if let Some(row) =
            Self::sled_expunge_request_unfinished_for_sled_conn(conn, sled_id)
                .await?
        {
            return decode_sled_expunge_request(row)
                .and_then(|request| {
                    sled_expunge_request_replay_outstanding(
                        sled_id,
                        rack_id,
                        acknowledged_serial,
                        request,
                    )
                })
                .map(SledExpungeRequestOutstanding::into_outcome)
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
        // TODO-RAINCLAUDE: Without this check, the unique index on unfinished
        // requests by baseboard would only refuse the request at the insert,
        // after the later checks and the trust quorum writes have run. Checking
        // here refuses it early, with a 409 that names the blocking request.
        // That request can belong to a different sled row with the same
        // baseboard.
        if let Some(blocking) =
            Self::sled_expunge_request_unfinished_for_baseboard_conn(
                conn,
                &baseboard_id,
            )
            .await?
        {
            let blocking =
                decode_sled_expunge_request(blocking).map_err(refuse)?;
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
                    let kind = SledExpungeRequestStateKind::from(request.state);
                    let Some(state) = kind.as_finished() else {
                        return Err(refuse(Error::internal_error(&format!(
                            "creating an expunge request for sled {sled_id}: \
                             request {} is {kind}, but this transaction found \
                             no unfinished request for the sled",
                            request.id,
                        ))));
                    };
                    Some(FinishedSledExpungeRequest { id: request.id(), state })
                }
            };
            return Err(refuse(refusal.into_error(latest_finished)));
        }
        let presence = match probe {
            SledExpungeCreationProbe::Probed(presence) => presence,
            SledExpungeCreationProbe::NotProbed => {
                return Err(refuse(Error::internal_error(&format!(
                    "creating an expunge request for sled {sled_id}: the \
                     caller skipped the presence probe because its policy \
                     and state check refused, but this transaction's policy \
                     and state check passed"
                ))));
            }
        };

        check_sled_expunge_baseboard_recorded(sled_id, &baseboard_id)
            .map_err(refuse)?;
        check_sled_expunge_serial_acknowledged(
            sled_id,
            sled.serial_number(),
            acknowledged_serial,
        )
        .map_err(refuse)?;

        let sled_row_role = sled.role();
        let inventory_evidence = SledExpungeScrimletEvidence::from_collection(
            latest_collection,
            sled_id,
            &baseboard_id,
        );
        let scrimlet_evidence = match presence {
            SledExpungeCreationPresence::Present {
                sp_slot,
                source: SledExpungeCreationSpSlotSource::LiveScan,
            } => inventory_evidence.with_mgs_sp_slot(sp_slot),
            SledExpungeCreationPresence::Present {
                sp_slot: _,
                source: SledExpungeCreationSpSlotSource::Inventory,
            }
            | SledExpungeCreationPresence::Gone => inventory_evidence,
        };
        check_sled_expunge_not_scrimlet(
            sled_id,
            &scrimlet_evidence,
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

        let new_request = DbSledExpungeRequest::new(
            rack_id,
            sled_id,
            baseboard_id.clone(),
            presence.recorded_cubby(),
            removal_config.epoch,
        )
        .map_err(|error| {
            refuse(Error::internal_error(&format!(
                "creating an expunge request for sled {sled_id}: {error:#}"
            )))
        })?;
        let request = diesel::insert_into(dsl::sled_expunge_request)
            .values(new_request)
            .returning(DbSledExpungeRequest::as_returning())
            .get_result_async(conn)
            .await
            .map_err(unique_violation_or)?;
        let request = decode_sled_expunge_request(request).map_err(refuse)?;

        Ok(SledExpungeRequestCreateOutcome::Created {
            request,
            removal_config: Box::new(removal_config),
        })
    }

    // TODO-RAINCLAUDE: Decides what to return after a unique violation rolled
    // the creation back.
    //
    // * If this sled now has an unfinished request, a concurrent creation won,
    //   and that request is returned as a replay.
    // * Otherwise, if the violation was on the baseboard index and another sled
    //   row's request still holds the baseboard, the 409 names that request.
    // * Anything else is a conflict that the caller can retry.
    async fn sled_expunge_request_create_raced_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
        rack_id: RackUuid,
        acknowledged_serial: &str,
        baseboard_violation: Option<&BaseboardId>,
    ) -> Result<SledExpungeRequestCreateOutcome, Error> {
        let to_public =
            |error| public_error_from_diesel(error, ErrorHandler::Server);
        if let Some(row) =
            Self::sled_expunge_request_unfinished_for_sled_conn(conn, sled_id)
                .await
                .map_err(to_public)?
        {
            return sled_expunge_request_replay_outstanding(
                sled_id,
                rack_id,
                acknowledged_serial,
                decode_sled_expunge_request(row)?,
            )
            .map(SledExpungeRequestOutstanding::into_outcome);
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
                &decode_sled_expunge_request(blocking)?,
            ),
            None => sled_expunge_request_raced_conflict(sled_id),
        })
    }

    async fn sled_expunge_request_unfinished_for_sled_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<Option<DbSledExpungeRequest>, DieselError> {
        dsl::sled_expunge_request
            .filter(dsl::sled_id.eq(to_db_typed_uuid(sled_id)))
            .filter(dsl::state.eq_any(DbSledExpungeRequestState::UNFINISHED))
            .select(DbSledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
            .optional()
    }

    async fn sled_expunge_request_unfinished_for_baseboard_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        baseboard_id: &BaseboardId,
    ) -> Result<Option<DbSledExpungeRequest>, DieselError> {
        dsl::sled_expunge_request
            .filter(dsl::part_number.eq(baseboard_id.part_number.clone()))
            .filter(dsl::serial_number.eq(baseboard_id.serial_number.clone()))
            .filter(dsl::state.eq_any(DbSledExpungeRequestState::UNFINISHED))
            .select(DbSledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
            .optional()
    }

    async fn sled_expunge_request_latest_for_sled_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<Option<DbSledExpungeRequest>, DieselError> {
        dsl::sled_expunge_request
            .filter(dsl::sled_id.eq(to_db_typed_uuid(sled_id)))
            .order_by((dsl::time_created.desc(), dsl::id.desc()))
            .select(DbSledExpungeRequest::as_select())
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
        let updated = Self::sled_expunge_request_transition_conn(
            &conn,
            authz_request.id(),
            SledExpungeRequestTransition::from(transition),
            now_db_precision(),
        )
        .await;
        Self::sled_expunge_request_update_outcome(&conn, authz_request, updated)
            .await
    }

    // TODO-RAINCLAUDE: Runs one transition as a compare-and-swap on its `from`
    // state and its recorded cubby precondition, and returns `None` if the row
    // didn't match. Production transitions and `pub_test_utils` both go through
    // this function, so the test helper can't drift from production.
    pub(crate) async fn sled_expunge_request_transition_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: SledExpungeRequestUuid,
        transition: SledExpungeRequestTransition,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<DbSledExpungeRequest>, DieselError> {
        let from =
            DbSledExpungeRequestState::from(transition.from_state().kind());
        let update = diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .filter(dsl::state.eq(from));
        let cubby_precondition = transition.cubby_precondition();
        let columns = SledExpungeRequestTransitionColumns::new(transition, now);
        match cubby_precondition {
            SledExpungeCubbyPrecondition::Any => {
                update
                    .set(columns)
                    .returning(DbSledExpungeRequest::as_returning())
                    .get_result_async(conn)
                    .await
            }
            SledExpungeCubbyPrecondition::Equals(expected_cubby) => {
                let expected_sp_slot =
                    expected_cubby.map(|cubby| SpMgsSlot::from(cubby.slot));
                let expected_source = expected_cubby
                    .map(|cubby| DbSledExpungeSpSlotSource::from(cubby.source));
                update
                    .filter(dsl::sp_slot.is_not_distinct_from(expected_sp_slot))
                    .filter(
                        dsl::sp_slot_source
                            .is_not_distinct_from(expected_source),
                    )
                    .set(columns)
                    .returning(DbSledExpungeRequest::as_returning())
                    .get_result_async(conn)
                    .await
            }
        }
        .optional()
    }

    // TODO-RAINCLAUDE: Records the cubby that the powering-off step found the
    // sled in. The update is a compare-and-swap on `change`'s previous
    // `sp_slot`, the value the caller last read, so a concurrent change comes
    // back as `NotInExpectedState`.
    //
    // A switch-attached cubby is refused before anything is read or written,
    // because a sled there is a scrimlet, and this path never powers off a
    // scrimlet.
    pub async fn sled_expunge_request_record_sp_slot(
        &self,
        opctx: &OpContext,
        authz_request: &authz::SledExpungeRequest,
        change: SledExpungeSpSlotChange,
    ) -> Result<SledExpungeRecordSpSlotOutcome, Error> {
        opctx.authorize(authz::Action::Modify, authz_request).await?;
        let sp_slot = change.sp_slot();
        if SWITCH_ATTACHED_SLED_CUBBIES.contains(&sp_slot) {
            return Ok(SledExpungeRecordSpSlotOutcome::SwitchAttachedCubby {
                sp_slot,
            });
        }
        let expected_sp_slot = change.previous_sp_slot().map(SpMgsSlot::from);
        let cubby = change.recorded_cubby();
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = to_db_typed_uuid(authz_request.id());
        let updated = diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(id))
            .filter(dsl::state.eq(DbSledExpungeRequestState::PoweringOff))
            .filter(dsl::sp_slot.is_not_distinct_from(expected_sp_slot))
            .set((
                dsl::sp_slot.eq(Some(SpMgsSlot::from(cubby.slot))),
                dsl::sp_slot_source
                    .eq(Some(DbSledExpungeSpSlotSource::from(cubby.source))),
                dsl::time_modified.eq(now_db_precision()),
            ))
            .returning(DbSledExpungeRequest::as_returning())
            .get_result_async(&*conn)
            .await
            .optional();
        let outcome = Self::sled_expunge_request_update_outcome(
            &conn,
            authz_request,
            updated,
        )
        .await?;
        Ok(match outcome {
            SledExpungeRequestUpdateOutcome::Updated { request } => {
                SledExpungeRecordSpSlotOutcome::Updated { request }
            }
            SledExpungeRequestUpdateOutcome::NotInExpectedState { current } => {
                SledExpungeRecordSpSlotOutcome::NotInExpectedState { current }
            }
        })
    }

    // TODO-RAINCLAUDE: Sets `last_error` on a request that is still in
    // `expected_state`. That state is always unfinished, so a finished request
    // is never touched. Within one state the last writer wins, since the update
    // doesn't compare against the previous message.
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
            .filter(
                dsl::state
                    .eq(DbSledExpungeRequestState::from(expected_state.kind())),
            )
            .set((
                dsl::last_error.eq(Some(last_error)),
                dsl::time_modified.eq(now_db_precision()),
            ))
            .returning(DbSledExpungeRequest::as_returning())
            .get_result_async(&*conn)
            .await
            .optional();
        Self::sled_expunge_request_update_outcome(&conn, authz_request, updated)
            .await
    }

    // TODO-RAINCLAUDE: Expunges the sled and its disks and moves the request to
    // `done`, in one transaction.
    //
    // Calling this again is safe, because a request that is already done comes
    // back as `NotExpunging`. A sled that is already expunged is left as it is,
    // so that a request still completes when its sled was expunged some other
    // way.
    pub async fn sled_expunge_request_complete(
        &self,
        opctx: &OpContext,
        request: &SledExpungeRequest,
    ) -> Result<SledExpungeRequestCompleteOutcome, Error> {
        // TODO-RAINCLAUDE: The caller's copy of the request may be stale, and
        // that is fine, because the transaction doesn't trust it. The copy is
        // used in these ways:
        //
        // * Its ID says which row to complete.
        // * Its sled and rack are what we authorize against here, before the
        //   transaction starts.
        // * The transaction compares it with the row, as described below.
        //
        // The transaction re-reads the row by ID. It checks that the row's
        // rack, sled, baseboard and `tq_epoch` match the caller's copy, and
        // returns an internal error if they don't. Those columns never change
        // after insert, so a mismatch means a bug, not a race. This check is
        // also why it's enough to authorize against the caller's sled and rack:
        // they are the same ones the transaction goes on to use.
        //
        // The state is taken from the re-read row, not from the caller's copy.
        // If the row is no longer `expunging`, the transaction writes nothing
        // and returns `NotExpunging`.
        let authz_request = authz_sled_expunge_request_from_id(request.id);
        opctx.authorize(authz::Action::Modify, &authz_request).await?;
        let sled_id = request.sled_id;
        let rack_id = request.rack_id;
        let authz_sled =
            authz::Sled::new(authz::FLEET, sled_id, LookupType::by_id(sled_id));
        opctx.authorize(authz::Action::Modify, &authz_sled).await?;
        opctx
            .authorize(
                authz::Action::Read,
                &authz::TrustQuorumConfig::for_rack_id(rack_id),
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let context = sled_expunge_request_context(request);

        let err = OptionalError::new();
        self.transaction_retry_wrapper("sled_expunge_request_complete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let context = context.clone();
                async move {
                    Self::sled_expunge_request_complete_conn(
                        &conn, &err, &context, request,
                    )
                    .await
                }
            })
            .await
            .map_err(|error| {
                sled_expunge_transaction_error(err.take(), error, &context)
            })
    }

    async fn sled_expunge_request_complete_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: &OptionalError<TransitionError>,
        context: &str,
        request: &SledExpungeRequest,
    ) -> Result<SledExpungeRequestCompleteOutcome, DieselError> {
        let authz_request = &authz_sled_expunge_request_from_id(request.id);
        let Some(current) =
            Self::sled_expunge_request_get_conn(conn, authz_request)
                .await
                .optional()?
        else {
            return Err(
                err.bail(TransitionError::External(authz_request.not_found()))
            );
        };
        let current = match decode_sled_expunge_request(current) {
            Ok(current) => current,
            Err(error) => {
                return Err(err.bail(TransitionError::External(
                    error.internal_context(context.to_string()),
                )));
            }
        };
        fn identity(
            request: &SledExpungeRequest,
        ) -> (RackUuid, SledUuid, &BaseboardId, Epoch) {
            (
                request.rack_id,
                request.sled_id,
                &request.baseboard_id,
                request.tq_epoch,
            )
        }
        if identity(&current) != identity(request) {
            return Err(bail_internal(
                err,
                context,
                format!(
                    "the caller's row (rack {}, sled {}, baseboard {}, \
                     removal epoch {}) does not match the stored row (rack \
                     {}, sled {}, baseboard {}, removal epoch {})",
                    request.rack_id,
                    request.sled_id,
                    request.baseboard_id,
                    request.tq_epoch,
                    current.rack_id,
                    current.sled_id,
                    current.baseboard_id,
                    current.tq_epoch,
                ),
            ));
        }
        let transition = SledExpungeRequestTransition::Done;
        let from = transition.from_state();
        if current.state.kind() != from.kind() {
            return Ok(SledExpungeRequestCompleteOutcome::NotExpunging {
                current,
            });
        }
        let sled_id = current.sled_id;
        let rack_id = current.rack_id;
        let tq_epoch = request.tq_epoch;
        let baseboard_id = &current.baseboard_id;

        let gate = match Self::sled_expunge_task_gate_conn(
            conn,
            err,
            context,
            rack_id,
            tq_epoch,
            baseboard_id,
        )
        .await?
        {
            SledExpungeTaskGateOutcome::RemovalConfigMissing => {
                return Err(bail_internal(
                    err,
                    context,
                    "no trust quorum configuration exists at the removal \
                     epoch",
                ));
            }
            SledExpungeTaskGateOutcome::Gate(gate) => gate,
        };
        // TODO-RAINCLAUDE: A request only reaches `expunging` after the gate
        // has seen its removal config past `Preparing`, and a config past
        // `Preparing` can't go back or be aborted. So `RemovalPreparing` or
        // `RemovalAborted` here means there is a bug.
        match gate {
            SledExpungeTaskGate::SledRemoved => {}
            SledExpungeTaskGate::ReAddPreparing { epoch } => {
                return Ok(SledExpungeRequestCompleteOutcome::ReAddPreparing {
                    epoch,
                });
            }
            SledExpungeTaskGate::ReAdded { epoch, state } => {
                return Ok(SledExpungeRequestCompleteOutcome::ReAdded {
                    epoch,
                    state,
                });
            }
            SledExpungeTaskGate::RemovalPreparing
            | SledExpungeTaskGate::RemovalAborted => {
                return Err(bail_internal(
                    err,
                    context,
                    format!(
                        "the request is expunging, but the task gate reported \
                         {gate:?}"
                    ),
                ));
            }
        }

        let sled = Self::sled_get_live_conn(conn, sled_id).await?;
        let Some(sled) = sled else {
            return Err(bail_internal(
                err,
                context,
                format!("its sled {sled_id} does not exist"),
            ));
        };
        if sled.rack_id() != rack_id {
            return Err(bail_internal(
                err,
                context,
                format!("its sled {sled_id} is in rack {}", sled.rack_id()),
            ));
        }
        let sled_baseboard_id = sled.baseboard_id();
        if sled_baseboard_id != *baseboard_id {
            return Err(bail_internal(
                err,
                context,
                format!("its sled {sled_id} has baseboard {sled_baseboard_id}"),
            ));
        }
        let previous_sled_policy = match sled.policy() {
            SledPolicy::Expunged => SledPolicy::Expunged,
            SledPolicy::InService { .. } => {
                Self::sled_set_policy_conn(
                    conn,
                    sled_id,
                    SledPolicy::Expunged,
                    ValidateTransition::Yes,
                    err,
                )
                .await?
            }
        };

        let updated = Self::sled_expunge_request_transition_conn(
            conn,
            authz_request.id(),
            transition,
            now_db_precision(),
        )
        .await?;
        match updated {
            Some(_) => {}
            None => {
                return Err(bail_internal(
                    err,
                    context,
                    format!(
                        "the request was read as {from} in this transaction, \
                         but moving it to done updated no row"
                    ),
                ));
            }
        }

        Ok(SledExpungeRequestCompleteOutcome::Completed {
            previous_sled_policy,
        })
    }

    // TODO-RAINCLAUDE: Checks the task gate for a request without writing
    // anything. Everything the gate looks at is read in one transaction, so the
    // gate sees a single snapshot of the rack's trust quorum configs.
    pub async fn sled_expunge_request_check_task_gate(
        &self,
        opctx: &OpContext,
        request: &SledExpungeRequest,
    ) -> Result<SledExpungeTaskGateOutcome, Error> {
        // TODO-RAINCLAUDE: It's safe to use the caller's copy of the request
        // here without re-reading the row, even if the copy is stale. The gate
        // only takes the request's `rack_id`, `tq_epoch` and baseboard, and
        // those columns never change after insert. Everything else the gate
        // looks at is the rack's trust quorum configs, which the transaction
        // reads itself.
        //
        // The request's state isn't one of the gate's inputs. If the state has
        // changed since the caller read it, the caller finds out when it acts
        // on the gate's answer. Each of those writes is a compare-and-swap on
        // the state, and it comes back as `NotInExpectedState`.
        let rack_id = request.rack_id;
        let authz_request = authz_sled_expunge_request_from_id(request.id);
        opctx.authorize(authz::Action::Read, &authz_request).await?;
        opctx
            .authorize(
                authz::Action::Read,
                &authz::TrustQuorumConfig::for_rack_id(rack_id),
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let context = sled_expunge_request_context(request);
        let tq_epoch = request.tq_epoch;
        let baseboard_id = request.baseboard_id.clone();

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

    // TODO-RAINCLAUDE: Loads the task gate's inputs and runs the gate. A
    // missing removal config is returned as its own outcome rather than as an
    // error, because the two callers treat it differently: the background task
    // fails the request, and `sled_expunge_request_complete` reports an
    // internal error.
    async fn sled_expunge_task_gate_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: &OptionalError<TransitionError>,
        context: &str,
        rack_id: RackUuid,
        tq_epoch: Epoch,
        baseboard_id: &BaseboardId,
    ) -> Result<SledExpungeTaskGateOutcome, DieselError> {
        // TODO-RAINCLAUDE: The latest config is loaded first because it is also
        // the removal config when it is at the removal epoch, and reusing it
        // saves a second load. If loading it fails with anything other than a
        // database error, we hold on to that error until the removal config has
        // been looked up, so that a missing removal config is still reported as
        // `RemovalConfigMissing`.
        let latest_loaded =
            match Self::tq_load_latest_possible_committed_config_conn(
                conn, rack_id,
            )
            .await
            {
                Ok((config, _highest_epoch)) => Ok(config),
                Err(TransactionError::Database(error)) => return Err(error),
                Err(error @ TransactionError::CustomError(_)) => Err(error),
            };
        let removal_fetched = match &latest_loaded {
            Ok(latest) if latest.epoch == tq_epoch => None,
            Ok(_) | Err(_) => {
                match Self::tq_get_config_with_members_from_epoch_conn(
                    conn, rack_id, tq_epoch,
                )
                .await
                {
                    Ok(Some(config)) => Some(config),
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
                }
            }
        };
        let latest_non_aborted_config = match latest_loaded {
            Ok(config) => config,
            Err(error) => {
                return Err(tq_load_error(
                    err,
                    context,
                    "the latest non-aborted trust quorum configuration",
                    error,
                ));
            }
        };
        let removal_config =
            removal_fetched.as_ref().unwrap_or(&latest_non_aborted_config);
        let plan = match sled_expunge_task_gate_plan(
            removal_config,
            &latest_non_aborted_config,
        ) {
            Ok(plan) => plan,
            Err(error) => return Err(bail_internal(err, context, error)),
        };
        let gate = match plan {
            SledExpungeTaskGatePlan::Ready(ready) => ready.check(baseboard_id),
            SledExpungeTaskGatePlan::Load(load) => {
                let base_epoch = load.epoch();
                let base_config =
                    match Self::tq_get_config_with_members_from_epoch_conn(
                        conn, rack_id, base_epoch,
                    )
                    .await
                    {
                        Ok(Some(base_config)) => base_config,
                        Ok(None) => {
                            return Err(bail_internal(
                                err,
                                context,
                                format!(
                                    "no trust quorum configuration exists at \
                                     epoch {base_epoch}, the base of the \
                                     latest configuration (epoch {})",
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
                    };
                load.check(&base_config, baseboard_id)
            }
        };
        match gate {
            Ok(gate) => Ok(SledExpungeTaskGateOutcome::Gate(gate)),
            Err(error) => Err(bail_internal(err, context, error)),
        }
    }

    // TODO-RAINCLAUDE: Turns the result of a conditional update into an
    // outcome. If the update matched no row, the row is re-read and returned as
    // `NotInExpectedState`.
    //
    // The re-read row can't be back in the expected state, because states only
    // move forward. Its `sp_slot` can be back at the expected value, but only
    // if the sled was relocated to another cubby and back while two Nexuses
    // raced on this request, and the task doesn't guard against that.
    async fn sled_expunge_request_update_outcome(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_request: &authz::SledExpungeRequest,
        updated: Result<Option<DbSledExpungeRequest>, DieselError>,
    ) -> Result<SledExpungeRequestUpdateOutcome, Error> {
        let to_public = |error| {
            public_error_from_diesel(
                error,
                ErrorHandler::NotFoundByResource(authz_request),
            )
        };
        match updated.map_err(to_public)? {
            Some(request) => Ok(SledExpungeRequestUpdateOutcome::Updated {
                request: decode_sled_expunge_request(request)?,
            }),
            None => {
                let current =
                    Self::sled_expunge_request_get_conn(conn, authz_request)
                        .await
                        .map_err(to_public)?;
                Ok(SledExpungeRequestUpdateOutcome::NotInExpectedState {
                    current: decode_sled_expunge_request(current)?,
                })
            }
        }
    }

    async fn sled_expunge_request_get_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_request: &authz::SledExpungeRequest,
    ) -> Result<DbSledExpungeRequest, diesel::result::Error> {
        dsl::sled_expunge_request
            .filter(dsl::id.eq(to_db_typed_uuid(authz_request.id())))
            .select(DbSledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
    }
}

fn sled_expunge_request_context(request: &SledExpungeRequest) -> String {
    format!(
        "sled expunge request {} for {} (rack {}, removal epoch {})",
        request.id, request.baseboard_id, request.rack_id, request.tq_epoch,
    )
}

fn decode_sled_expunge_request(
    row: DbSledExpungeRequest,
) -> Result<SledExpungeRequest, Error> {
    SledExpungeRequest::try_from(row)
        .map_err(|error| Error::internal_error(format!("{error:#}")))
}

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

// TODO-RAINCLAUDE: Database errors are returned as they are, so that the
// transaction can still be retried. The loader's own errors become an internal
// error.
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
    use super::SledExpungeRecordSpSlotOutcome;
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
    use crate::db::pub_test_utils::sled_expunge_request::TEST_SP_SLOT;
    use crate::db::pub_test_utils::sled_expunge_request::backdate_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::decode;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_with_sp_slot;
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
    use nexus_db_model::DbSledExpungePowerOffOutcome;
    use nexus_db_model::DbSledExpungeRequestState;
    use nexus_db_model::DbSledExpungeSpSlotSource;
    use nexus_db_model::DbTypedUuid;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
    use nexus_db_model::SledExpungeRequestTransitionColumns;
    use nexus_db_model::SpMgsSlot;
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
    use nexus_types::sled_expunge::SWITCH_ATTACHED_SLED_CUBBIES;
    use nexus_types::sled_expunge::SledExpungeCreationPresence;
    use nexus_types::sled_expunge::SledExpungeCreationProbe;
    use nexus_types::sled_expunge::SledExpungeCreationSpSlotSource;
    use nexus_types::sled_expunge::SledExpungeCubbyPowerOffOutcome;
    use nexus_types::sled_expunge::SledExpungePowerOff;
    use nexus_types::sled_expunge::SledExpungeRecordedCubby;
    use nexus_types::sled_expunge::SledExpungeRequest;
    use nexus_types::sled_expunge::SledExpungeRequestAdvance;
    use nexus_types::sled_expunge::SledExpungeRequestFinishedState;
    use nexus_types::sled_expunge::SledExpungeRequestState;
    use nexus_types::sled_expunge::SledExpungeRequestStateKind;
    use nexus_types::sled_expunge::SledExpungeRequestTransition;
    use nexus_types::sled_expunge::SledExpungeRequestUnfinishedState;
    use nexus_types::sled_expunge::SledExpungeScrimletEvidence;
    use nexus_types::sled_expunge::SledExpungeSpSlotChange;
    use nexus_types::sled_expunge::SledExpungeSpSlotSource;
    use nexus_types::sled_expunge::check_sled_expunge_baseboard_recorded;
    use nexus_types::sled_expunge::check_sled_expunge_not_scrimlet;
    use nexus_types::sled_expunge::check_sled_expunge_policy_and_state;
    use nexus_types::trust_quorum::SledExpungeTaskGate;
    use nexus_types::trust_quorum::TrustQuorumConfig;
    use nexus_types::trust_quorum::TrustQuorumConfigPastPreparing;
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
    use omicron_uuid_kinds::SledUuid;
    use proptest::prelude::*;
    use sled_agent_types::inventory::SledRole;
    use sled_agent_types::inventory::SvcsEnabledNotOnlineResult;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use trust_quorum_types::membership::TRUST_QUORUM_MIN_MEMBERS;
    use trust_quorum_types::membership::TrustQuorumMembers;
    use trust_quorum_types::types::Epoch;
    use uuid::Uuid;

    const SP_SLOT_SOURCE_IFF_SP_SLOT: &str = "sp_slot_source_iff_sp_slot";
    const SP_SLOT_IS_SLED_CUBBY: &str = "sp_slot_is_sled_cubby";
    const POWER_OFF_OUTCOME_MATCHES_SP_SLOT: &str =
        "power_off_outcome_matches_sp_slot";
    const TIME_COMPLETED_IFF_FINISHED: &str = "time_completed_iff_finished";
    const FAILURE_REASON_IFF_FAILED: &str = "failure_reason_iff_failed";
    const POWER_OFF_OUTCOME_MATCHES_STATE: &str =
        "power_off_outcome_matches_state";
    const LAST_ERROR_ONLY_IF_UNFINISHED: &str = "last_error_only_if_unfinished";
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
        state: SledExpungeRequestStateKind,
    ) -> DbSledExpungeRequest {
        new_sled_expunge_request(
            RackUuid::new_v4(),
            sled_id,
            baseboard_id,
            Epoch(2),
            state,
        )
    }

    async fn insert(
        conn: &Conn,
        request: &DbSledExpungeRequest,
    ) -> Result<usize, DieselError> {
        diesel::insert_into(dsl::sled_expunge_request)
            .values(request.clone())
            .execute_async(conn)
            .await
    }

    async fn read_back(
        conn: &Conn,
        id: DbTypedUuid<SledExpungeRequestKind>,
    ) -> DbSledExpungeRequest {
        dsl::sled_expunge_request
            .filter(dsl::id.eq(id))
            .select(DbSledExpungeRequest::as_select())
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
            *actual_kind, kind,
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
        let valid = |state| {
            request_in_valid_state(
                SledUuid::new_v4(),
                unique_baseboard(),
                state,
            )
        };

        // TODO-RAINCLAUDE: Each row breaks exactly one CHECK, so that the
        // constraint the database reports can only be that one.
        let cases = [
            (
                TIME_COMPLETED_IFF_FINISHED,
                DbSledExpungeRequest {
                    time_completed: Some(now_db_precision()),
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                FAILURE_REASON_IFF_FAILED,
                DbSledExpungeRequest {
                    failure_reason: Some(TEST_FAILURE_REASON.to_string()),
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                POWER_OFF_OUTCOME_MATCHES_STATE,
                DbSledExpungeRequest {
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::PoweredOff,
                    ),
                    ..valid(SledExpungeRequestStateKind::PoweringOff)
                },
            ),
            (
                LAST_ERROR_ONLY_IF_UNFINISHED,
                DbSledExpungeRequest {
                    last_error: Some("test last error".to_string()),
                    ..valid(SledExpungeRequestStateKind::Done)
                },
            ),
            (
                SP_SLOT_SOURCE_IFF_SP_SLOT,
                DbSledExpungeRequest {
                    sp_slot_source: None,
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                SP_SLOT_SOURCE_IFF_SP_SLOT,
                DbSledExpungeRequest {
                    sp_slot: None,
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                SP_SLOT_IS_SLED_CUBBY,
                DbSledExpungeRequest {
                    sp_slot: Some(SpMgsSlot::from(32)),
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                POWER_OFF_OUTCOME_MATCHES_SP_SLOT,
                DbSledExpungeRequest {
                    sp_slot: None,
                    sp_slot_source: None,
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::PoweredOff,
                    ),
                    ..valid(SledExpungeRequestStateKind::Expunging)
                },
            ),
            (
                POWER_OFF_OUTCOME_MATCHES_SP_SLOT,
                DbSledExpungeRequest {
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::AbsentAtCreation,
                    ),
                    ..valid(SledExpungeRequestStateKind::Done)
                },
            ),
        ];
        for (expected_constraint, request) in cases {
            let constraint = expect_violation(
                &insert(&conn, &request).await,
                DatabaseErrorKind::CheckViolation,
                expected_constraint,
            );
            assert_eq!(constraint, expected_constraint);
        }

        insert(
            &conn,
            &DbSledExpungeRequest {
                sp_slot: None,
                sp_slot_source: None,
                ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
            },
        )
        .await
        .expect("inserted a request with neither sp_slot nor sp_slot_source");

        // TODO-RAINCLAUDE: The cases above show what the CHECKs reject. These
        // rows show that the same CHECKs don't reject too much. Each row sits
        // right next to something that is rejected:
        //
        // * Slot 31 is the highest sled cubby, so `sp_slot_is_sled_cubby`
        //   allows it. Slot 32 was rejected above.
        // * `absent_at_creation` with no cubby is the one pairing that
        //   `power_off_outcome_matches_sp_slot` allows without a cubby. The
        //   same outcome with a cubby was rejected above.
        // * A `failed` request can have an outcome, because a request can fail
        //   after its power-off step finished.
        //   `power_off_outcome_matches_state` allows `failed` with or without
        //   one. This row's outcome is `other_board_present`, which needs a
        //   cubby, and the row has one.
        let allowed = [
            DbSledExpungeRequest {
                sp_slot: Some(SpMgsSlot::from(31)),
                ..valid(SledExpungeRequestStateKind::Expunging)
            },
            DbSledExpungeRequest {
                sp_slot: None,
                sp_slot_source: None,
                power_off_outcome: Some(
                    DbSledExpungePowerOffOutcome::AbsentAtCreation,
                ),
                ..valid(SledExpungeRequestStateKind::Done)
            },
            DbSledExpungeRequest {
                power_off_outcome: Some(
                    DbSledExpungePowerOffOutcome::OtherBoardPresent,
                ),
                ..valid(SledExpungeRequestStateKind::Failed)
            },
        ];
        for request in &allowed {
            insert(&conn, request).await.unwrap_or_else(|error| {
                panic!("inserted {request:?}: {error:#?}")
            });
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
            SledExpungeRequestStateKind::ALL,
            SledExpungeRequestStateKind::ALL
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

        // TODO-RAINCLAUDE: Creation tells unique violations apart by constraint
        // name, so this test also pins the name of the trust quorum config
        // table's primary key.
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

    #[tokio::test]
    async fn test_sled_expunge_request_list_unfinished() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_list_unfinished");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // TODO-RAINCLAUDE: Cycling through every state for `2 * SQL_BATCH_SIZE`
        // rows gives more than `SQL_BATCH_SIZE` unfinished rows, so the listing
        // needs a second batch and has finished rows to skip along the way.
        let batch_size = usize::try_from(SQL_BATCH_SIZE.get()).unwrap();
        let rows: Vec<_> = SledExpungeRequestStateKind::ALL
            .iter()
            .cycle()
            .take(2 * batch_size)
            .map(|&state| {
                new_sled_expunge_request(
                    RackUuid::new_v4(),
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    Epoch(1),
                    state,
                )
            })
            .collect();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let inserted: Vec<DbSledExpungeRequest> =
            diesel::insert_into(dsl::sled_expunge_request)
                .values(rows)
                .returning(DbSledExpungeRequest::as_returning())
                .get_results_async(&*conn)
                .await
                .expect("bulk-inserted requests in every state");
        let mut expected: Vec<_> = inserted
            .into_iter()
            .filter(|request| {
                !SledExpungeRequestStateKind::from(request.state).is_finished()
            })
            .map(|request| {
                SledExpungeRequest::try_from(request)
                    .expect("decoded an inserted request")
            })
            .collect();
        expected.sort_by_key(|request| request.sled_id.into_untyped_uuid());
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
    async fn test_sled_expunge_request_viewer_can_read() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_viewer_can_read");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = opctx_as(&db, authn::Context::internal_read());

        let request = insert_sled_expunge_request(
            datastore,
            RackUuid::new_v4(),
            unique_baseboard(),
            Epoch(1),
            SledExpungeRequestStateKind::WaitingForTrustQuorum,
        )
        .await;

        assert_eq!(
            datastore
                .sled_expunge_request_get(&opctx, request.id)
                .await
                .expect("a fleet viewer can get a request"),
            request,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list(&opctx, &DataPageParams::max_page())
                .await
                .expect("a fleet viewer can list requests"),
            vec![request.clone()],
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list_unfinished(&opctx)
                .await
                .expect("a fleet viewer can list unfinished requests"),
            vec![request.clone()],
        );
        let authz_request = authz_for(&request);
        for op in TransitionOp::all() {
            assert_eq!(
                op.run(datastore, &opctx, &authz_request)
                    .await
                    .expect_err(&format!("a fleet viewer is denied {op:?}")),
                Error::Forbidden,
            );
        }
        assert_eq!(
            datastore
                .sled_expunge_request_record_sp_slot(
                    &opctx,
                    &authz_request,
                    SledExpungeSpSlotChange::found_after_absent(OTHER_SP_SLOT,),
                )
                .await
                .expect_err("a fleet viewer is denied record_sp_slot"),
            Error::Forbidden,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_complete(&opctx, &request)
                .await
                .expect_err("a fleet viewer is denied complete"),
            Error::Forbidden,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_check_task_gate(&opctx, &request)
                .await
                .expect("a fleet viewer can run the task gate check"),
            SledExpungeTaskGateOutcome::RemovalConfigMissing,
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
                    probe: SledExpungeCreationProbe::Probed(test_presence(
                        SledExpungeCreationSpSlotSource::Inventory
                    )),
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
        authz_sled_expunge_request_from_id(request.id)
    }

    async fn read_row(
        datastore: &DataStore,
        request: &SledExpungeRequest,
    ) -> DbSledExpungeRequest {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        read_back(&conn, to_db_typed_uuid(request.id)).await
    }

    async fn read(
        datastore: &DataStore,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequest {
        decode(read_row(datastore, request).await)
    }

    async fn assert_updated<D: std::fmt::Display>(
        datastore: &DataStore,
        outcome: SledExpungeRequestUpdateOutcome,
        authz_request: &authz::SledExpungeRequest,
        context: D,
    ) -> SledExpungeRequest {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let stored = decode(read_back(&conn, authz_request.id().into()).await);
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
        fn all() -> [Self; 8] {
            [
                Self::Advance(SledExpungeRequestAdvance::ToPoweringOff),
                Self::Advance(SledExpungeRequestAdvance::ToExpunging {
                    power_off: SledExpungePowerOff::Cubby {
                        cubby: SledExpungeRecordedCubby {
                            slot: TEST_SP_SLOT,
                            source: SledExpungeSpSlotSource::Inventory,
                        },
                        outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                    },
                }),
                Self::Fail(
                    SledExpungeRequestUnfinishedState::WaitingForTrustQuorum,
                ),
                Self::Fail(SledExpungeRequestUnfinishedState::PoweringOff),
                Self::Fail(SledExpungeRequestUnfinishedState::Expunging),
                Self::SetLastError(
                    SledExpungeRequestUnfinishedState::WaitingForTrustQuorum,
                ),
                Self::SetLastError(
                    SledExpungeRequestUnfinishedState::PoweringOff,
                ),
                Self::SetLastError(
                    SledExpungeRequestUnfinishedState::Expunging,
                ),
            ]
        }

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
            before: DbSledExpungeRequest,
            time_modified: chrono::DateTime<chrono::Utc>,
        ) -> DbSledExpungeRequest {
            match self {
                Self::Advance(advance) => {
                    SledExpungeRequestTransitionColumns::new(
                        SledExpungeRequestTransition::Advance(advance),
                        time_modified,
                    )
                    .apply_to(before)
                }
                Self::Fail(from) => SledExpungeRequestTransitionColumns::new(
                    SledExpungeRequestTransition::Failed {
                        from,
                        failure_reason: TRANSITION_FAILURE_REASON.to_string(),
                    },
                    time_modified,
                )
                .apply_to(before),
                Self::SetLastError(_) => DbSledExpungeRequest {
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
            iproduct!(SledExpungeRequestStateKind::ALL, TransitionOp::all())
        {
            let request = insert_sled_expunge_request(
                datastore,
                RackUuid::new_v4(),
                unique_baseboard(),
                Epoch(1),
                start,
            )
            .await;
            let before =
                backdate_sled_expunge_request(datastore, &request).await;
            let before_row = read_row(datastore, &before).await;
            let outcome = op
                .run(datastore, opctx, &authz_for(&before))
                .await
                .unwrap_or_else(|error| panic!("{op:?} from {start}: {error}"));
            let after_row = read_row(datastore, &before).await;
            let after = decode(after_row.clone());
            if op.from_state().kind() == start {
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
                    after_row,
                    op.apply_to(before_row, after_row.time_modified),
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

        let missing_request = decode(new_sled_expunge_request(
            RackUuid::new_v4(),
            SledUuid::new_v4(),
            unique_baseboard(),
            Epoch(2),
            SledExpungeRequestStateKind::Expunging,
        ));
        let missing = missing_request.id;
        let authz_missing = authz_for(&missing_request);
        let not_found = Error::ObjectNotFound {
            type_name: ResourceType::SledExpungeRequest,
            lookup_type: LookupType::ById(missing.into_untyped_uuid()),
        };
        for op in TransitionOp::all() {
            assert_eq!(
                op.run(datastore, opctx, &authz_missing)
                    .await
                    .expect_err(&format!("{op:?} on a missing request fails")),
                not_found,
            );
        }
        assert_eq!(
            datastore
                .sled_expunge_request_record_sp_slot(
                    opctx,
                    &authz_missing,
                    SledExpungeSpSlotChange::found_after_absent(OTHER_SP_SLOT,),
                )
                .await
                .expect_err("recording sp_slot on a missing request fails"),
            not_found,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &missing_request)
                .await
                .expect_err("completing a missing request fails"),
            not_found,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    const OTHER_SP_SLOT: u16 = TEST_SP_SLOT + 1;

    #[tokio::test]
    async fn test_sled_expunge_request_record_sp_slot() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_record_sp_slot");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());
        let recorded = OTHER_SP_SLOT;

        for (&state, sp_slot) in iproduct!(
            SledExpungeRequestStateKind::ALL,
            [None, Some(TEST_SP_SLOT)]
        ) {
            let request = insert_sled_expunge_request_with_sp_slot(
                datastore,
                RackUuid::new_v4(),
                &unique_baseboard(),
                sp_slot,
                Epoch(1),
                state,
            )
            .await;
            if let Some(unfinished) = state.as_unfinished() {
                let outcome = datastore
                    .sled_expunge_request_set_last_error(
                        opctx,
                        &authz_for(&request),
                        unfinished,
                        TRANSITION_LAST_ERROR.to_string(),
                    )
                    .await
                    .expect("set last_error");
                assert_updated(
                    datastore,
                    outcome,
                    &authz_for(&request),
                    "set last_error",
                )
                .await;
            }
            let before =
                backdate_sled_expunge_request(datastore, &request).await;
            let before_row = read_row(datastore, &before).await;
            let authz_request = authz_for(&before);
            let context = format!("{state} with sp_slot {sp_slot:?}");
            let relocated_from = |previous| {
                SledExpungeSpSlotChange::relocated(previous, recorded)
                    .expect("test relocates the sled to a different cubby")
            };
            let (change, stale, expected_source) = match sp_slot {
                None => (
                    SledExpungeSpSlotChange::found_after_absent(recorded),
                    relocated_from(TEST_SP_SLOT),
                    SledExpungeSpSlotSource::FoundAfterAbsent,
                ),
                Some(previous) => (
                    relocated_from(previous),
                    SledExpungeSpSlotChange::found_after_absent(recorded),
                    SledExpungeSpSlotSource::Relocated,
                ),
            };
            assert_eq!(
                datastore
                    .sled_expunge_request_record_sp_slot(
                        opctx,
                        &authz_request,
                        stale,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("{context}: {error}")),
                SledExpungeRecordSpSlotOutcome::NotInExpectedState {
                    current: before.clone(),
                },
                "{context}: recording {stale:?}",
            );
            assert_eq!(read(datastore, &before).await, before, "{context}");
            let outcome = datastore
                .sled_expunge_request_record_sp_slot(
                    opctx,
                    &authz_request,
                    change,
                )
                .await
                .unwrap_or_else(|error| panic!("{context}: {error}"));
            let records = match state {
                SledExpungeRequestStateKind::PoweringOff => true,
                SledExpungeRequestStateKind::WaitingForTrustQuorum
                | SledExpungeRequestStateKind::Expunging
                | SledExpungeRequestStateKind::Done
                | SledExpungeRequestStateKind::Failed => false,
            };
            if !records {
                assert_eq!(
                    outcome,
                    SledExpungeRecordSpSlotOutcome::NotInExpectedState {
                        current: before.clone(),
                    },
                    "{context}",
                );
                assert_eq!(read(datastore, &before).await, before, "{context}");
                continue;
            }
            let after_row = read_row(datastore, &before).await;
            let after = decode(after_row.clone());
            assert_eq!(
                outcome,
                SledExpungeRecordSpSlotOutcome::Updated {
                    request: after.clone()
                },
                "{context}",
            );
            assert!(
                after.time_modified > before.time_modified,
                "{context}: recording sp_slot moves time_modified forward",
            );
            assert_eq!(
                after_row,
                DbSledExpungeRequest {
                    sp_slot: Some(SpMgsSlot::from(recorded)),
                    sp_slot_source: Some(DbSledExpungeSpSlotSource::from(
                        expected_source
                    )),
                    time_modified: after_row.time_modified,
                    ..before_row
                },
                "{context}",
            );

            // TODO-RAINCLAUDE: A retry of the same call loses its
            // compare-and-swap, because the slot it expected was overwritten.
            assert_eq!(
                datastore
                    .sled_expunge_request_record_sp_slot(
                        opctx,
                        &authz_request,
                        change,
                    )
                    .await
                    .expect("a stale compare-and-swap returns Ok"),
                SledExpungeRecordSpSlotOutcome::NotInExpectedState {
                    current: after.clone(),
                },
                "{context}: retrying the same call",
            );
            assert_eq!(read(datastore, &after).await, after, "{context}");
        }

        let request = insert_sled_expunge_request_with_sp_slot(
            datastore,
            RackUuid::new_v4(),
            &unique_baseboard(),
            None,
            Epoch(1),
            SledExpungeRequestStateKind::PoweringOff,
        )
        .await;
        let switch_attached = SWITCH_ATTACHED_SLED_CUBBIES[0];
        assert_eq!(
            datastore
                .sled_expunge_request_record_sp_slot(
                    opctx,
                    &authz_for(&request),
                    SledExpungeSpSlotChange::found_after_absent(
                        switch_attached,
                    ),
                )
                .await
                .expect("recording a switch-attached cubby is not an error"),
            SledExpungeRecordSpSlotOutcome::SwitchAttachedCubby {
                sp_slot: switch_attached,
            },
        );
        assert_eq!(read(datastore, &request).await, request);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_advance_to_expunging() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_advance_to_expunging",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = &opctx_as(&db, authn::Context::internal_api());

        let power_off_at = |slot: u16, source: SledExpungeSpSlotSource| {
            SledExpungePowerOff::Cubby {
                cubby: SledExpungeRecordedCubby { slot, source },
                outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
            }
        };
        // TODO-RAINCLAUDE: Rows here record their slot as found in inventory.
        let power_offs = [
            power_off_at(TEST_SP_SLOT, SledExpungeSpSlotSource::Inventory),
            power_off_at(TEST_SP_SLOT, SledExpungeSpSlotSource::Relocated),
            power_off_at(OTHER_SP_SLOT, SledExpungeSpSlotSource::Inventory),
            SledExpungePowerOff::AbsentAtCreation,
        ];
        for (row_sp_slot, power_off) in iproduct!(
            [None, Some(TEST_SP_SLOT), Some(OTHER_SP_SLOT)],
            power_offs
        ) {
            let request = insert_sled_expunge_request_with_sp_slot(
                datastore,
                RackUuid::new_v4(),
                &unique_baseboard(),
                row_sp_slot,
                Epoch(1),
                SledExpungeRequestStateKind::PoweringOff,
            )
            .await;
            let before =
                backdate_sled_expunge_request(datastore, &request).await;
            let before_row = read_row(datastore, &before).await;
            let authz_request = authz_for(&before);
            let context =
                format!("{power_off:?} on a row with sp_slot {row_sp_slot:?}");
            let outcome = datastore
                .sled_expunge_request_advance(
                    opctx,
                    &authz_request,
                    SledExpungeRequestAdvance::ToExpunging { power_off },
                )
                .await
                .unwrap_or_else(|error| panic!("{context}: {error}"));
            if power_off.cubby() != before.state.recorded_cubby() {
                assert_eq!(
                    outcome,
                    SledExpungeRequestUpdateOutcome::NotInExpectedState {
                        current: before.clone(),
                    },
                    "{context}",
                );
                assert_eq!(read(datastore, &before).await, before, "{context}");
                continue;
            }
            let after =
                assert_updated(datastore, outcome, &authz_request, &context)
                    .await;
            assert_eq!(
                read_row(datastore, &after).await,
                DbSledExpungeRequest {
                    state: DbSledExpungeRequestState::Expunging,
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::from(power_off)
                    ),
                    time_modified: after.time_modified,
                    ..before_row
                },
                "{context}",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create_mgs_switch_attached_cubby() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_create_mgs_switch_attached_cubby",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let fixture = CompleteFixture::new(&db).await;
        let slot = SWITCH_ATTACHED_SLED_CUBBIES[0];
        let input = CreateInput {
            probe: SledExpungeCreationProbe::Probed(
                SledExpungeCreationPresence::Present {
                    sp_slot: slot,
                    source: SledExpungeCreationSpSlotSource::LiveScan,
                },
            ),
            ..fixture.create_input(db.opctx())
        };
        fixture
            .expect_create_refused(
                &db,
                &input,
                Error::conflict(format!(
                    "Cannot expunge sled {}: it is a scrimlet (it manages a \
                     switch), since MGS found its service processor in \
                     switch-attached cubby {slot}. Expunging a scrimlet \
                     through this API is not supported. Contact Oxide \
                     support.",
                    fixture.sled_id,
                )),
            )
            .await;

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
        // TODO-RAINCLAUDE: Builds a rack whose epoch 1 trust quorum has ten
        // members. The last baseboard is the expunge target, with a sled and
        // one physical disk, and it is never the coordinator. The first
        // baseboard also has a sled, so that it can coordinate.
        async fn new(db: &TestDatabase) -> Self {
            Self::with_members(db, 10, FixtureTrustQuorum::Rss).await
        }

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
            tq_epoch: u64,
            state: SledExpungeRequestStateKind,
        ) -> SledExpungeRequest {
            self.insert_request_for_sled(db, self.sled_id, tq_epoch, state)
                .await
        }

        async fn insert_request_for_sled(
            &self,
            db: &TestDatabase,
            sled_id: SledUuid,
            tq_epoch: u64,
            state: SledExpungeRequestStateKind,
        ) -> SledExpungeRequest {
            insert_sled_expunge_request_for_sled(
                db.datastore(),
                self.rack_id,
                sled_id,
                &self.target,
                Epoch(tq_epoch),
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

        // TODO-RAINCLAUDE: Deletes a trust quorum config and its members. Nexus
        // never deletes trust quorum configs, so some of the inconsistencies
        // these tests cover can only be set up this way.
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
            .kind()
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
    fn assert_done(
        before: &DbSledExpungeRequest,
        after: &DbSledExpungeRequest,
    ) {
        let time_completed =
            after.time_completed.expect("a done request has time_completed");
        assert_eq!(time_completed, after.time_modified);
        assert!(time_completed >= before.time_modified);
        assert_eq!(
            after,
            &DbSledExpungeRequest {
                state: DbSledExpungeRequestState::Done,
                last_error: None,
                time_completed: Some(time_completed),
                time_modified: after.time_modified,
                ..before.clone()
            },
        );
        assert_eq!(
            after,
            &SledExpungeRequestTransitionColumns::new(
                SledExpungeRequestTransition::Done,
                after.time_modified,
            )
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
            .insert_request(&db, 2, SledExpungeRequestStateKind::Expunging)
            .await;
        let request =
            set_last_error(&db, opctx, &request, "sled was powered back on")
                .await;
        let before_row = read_row(datastore, &request).await;

        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &request)
                .await
                .expect("completed the request"),
            SledExpungeRequestCompleteOutcome::Completed {
                previous_sled_policy: IN_SERVICE.0,
            },
        );
        let done_row = read_row(datastore, &request).await;
        let done = decode(done_row.clone());
        assert_done(&before_row, &done_row);
        assert_eq!(
            done_row.power_off_outcome,
            Some(DbSledExpungePowerOffOutcome::PoweredOff),
        );
        assert_eq!(fixture.sled_and_disk_policy(&db).await, EXPUNGED);

        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &request)
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
        fixture.tq(&db).propose(3, 2, &fixture.members).await;
        fixture.tq(&db).commit(3).await;

        let request = fixture
            .insert_request(&db, 2, SledExpungeRequestStateKind::Expunging)
            .await;
        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &request)
                .await
                .expect("complete succeeds"),
            SledExpungeRequestCompleteOutcome::ReAdded {
                epoch: Epoch(3),
                state: TrustQuorumConfigPastPreparing::Committed,
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
                .insert_request(&db, 2, SledExpungeRequestStateKind::Expunging)
                .await;
            let before_row = read_row(datastore, &request).await;
            assert_eq!(
                datastore
                    .sled_expunge_request_complete(opctx, &request)
                    .await
                    .expect("completed the request"),
                SledExpungeRequestCompleteOutcome::Completed {
                    previous_sled_policy: SledPolicy::Expunged,
                },
                "decommission: {decommission}",
            );
            assert_done(&before_row, &read_row(datastore, &request).await);
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
        CallerRowMismatch,
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
            InconsistentCase::CallerRowMismatch,
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
                            SledExpungeRequestStateKind::Expunging,
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
                            SledExpungeRequestStateKind::Expunging,
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
                            SledExpungeRequestStateKind::Expunging,
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
                            SledExpungeRequestStateKind::Expunging,
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
                            SledExpungeRequestStateKind::Expunging,
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
                InconsistentCase::CallerRowMismatch => {
                    fixture.tq(&db).ack_all_prepares(2).await;
                    let request = fixture
                        .insert_request(
                            &db,
                            2,
                            SledExpungeRequestStateKind::Expunging,
                        )
                        .await;
                    let message = format!(
                        "the caller's row (rack {rack}, sled {sled}, baseboard \
                         {board}, removal epoch 3) does not match the stored \
                         row (rack {rack}, sled {sled}, baseboard {board}, \
                         removal epoch 2)",
                        rack = request.rack_id,
                        sled = request.sled_id,
                        board = request.baseboard_id,
                    );
                    (request, None, message)
                }
            };
            let caller_row = match case {
                InconsistentCase::CallerRowMismatch => SledExpungeRequest {
                    tq_epoch: Epoch(request.tq_epoch.0 + 1),
                    ..request.clone()
                },
                InconsistentCase::RemovalAborted
                | InconsistentCase::MissingRemovalConfig
                | InconsistentCase::MissingSled
                | InconsistentCase::SledInOtherRack
                | InconsistentCase::SledWithOtherBaseboard => request.clone(),
            };

            assert_eq!(
                datastore
                    .sled_expunge_request_complete(opctx, &caller_row)
                    .await
                    .expect_err("inconsistent records are an internal error"),
                Error::internal_error(
                    &fixture.context_message(&caller_row, &message)
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
                .insert_request(&db, 2, SledExpungeRequestStateKind::Expunging)
                .await;
            let authz_request = authz_for(&request);
            let before_row = read_row(datastore, &request).await;

            let (completed, failed) = tokio::join!(
                datastore.sled_expunge_request_complete(opctx_a, &request),
                datastore.sled_expunge_request_fail(
                    opctx_b,
                    &authz_request,
                    SledExpungeRequestUnfinishedState::Expunging,
                    "raced failure".to_string(),
                ),
            );
            let (completed, failed) = (
                completed.expect("complete returns an outcome"),
                failed.expect("fail returns an outcome"),
            );
            let after_row = read_row(datastore, &request).await;
            let after = decode(after_row.clone());
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
                    assert_done(&before_row, &after_row);
                    assert_eq!(
                        fixture.sled_and_disk_policy(&db).await,
                        EXPUNGED
                    );
                }
                (
                    SledExpungeRequestCompleteOutcome::NotExpunging { current },
                    SledExpungeRequestUpdateOutcome::Updated {
                        request: failed,
                    },
                ) => {
                    assert_eq!(current, &after, "round {round}");
                    assert_eq!(failed, &after, "round {round}");
                    assert_eq!(
                        after_row,
                        DbSledExpungeRequest {
                            state: DbSledExpungeRequestState::Failed,
                            failure_reason: Some("raced failure".to_string()),
                            last_error: None,
                            time_completed: after_row.time_completed,
                            time_modified: after_row.time_modified,
                            ..before_row.clone()
                        },
                        "round {round}",
                    );
                    assert_eq!(
                        after_row.time_completed,
                        Some(after_row.time_modified),
                        "round {round}: a failed request is completed when \
                         it fails",
                    );
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
            // TODO-RAINCLAUDE: This can't be `hw_ids[0]`. That is the only
            // member other than the target with an in-service sled, so it has
            // to stay in every config for there to be a coordinator.
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
                                state:
                                    TrustQuorumConfigPastPreparing::Committed,
                            },
                        ),
                    )
                }
            };
            let request = fixture
                .insert_request(
                    &db,
                    tq_epoch,
                    SledExpungeRequestStateKind::WaitingForTrustQuorum,
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
        fixture.delete_config(&db, 3).await;
        let request = fixture
            .insert_request(
                &db,
                2,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
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

        set_sled_expunge_request_state(
            datastore,
            &request,
            SledExpungeRequestStateKind::Expunging,
        )
        .await;
        let expunging = read(datastore, &request).await;
        assert_eq!(
            datastore
                .sled_expunge_request_complete(opctx, &expunging)
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

    // TODO-RAINCLAUDE: This shows that `sled_expunge_request_complete` rolls
    // back with an internal error when the sled's state doesn't allow the
    // policy change. Here the sled is decommissioned while its policy still
    // says in service, and it is left as it was.
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
            .insert_request(&db, 2, SledExpungeRequestStateKind::Expunging)
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
                .sled_expunge_request_complete(opctx, &request)
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
        probe: SledExpungeCreationProbe,
    }

    fn test_presence(
        source: SledExpungeCreationSpSlotSource,
    ) -> SledExpungeCreationPresence {
        SledExpungeCreationPresence::Present { sp_slot: TEST_SP_SLOT, source }
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
                input.probe,
            )
            .await
    }

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
                probe: SledExpungeCreationProbe::Probed(test_presence(
                    SledExpungeCreationSpSlotSource::Inventory,
                )),
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
                .select(DbSledExpungeRequest::as_select())
                .load_async(&*conn)
                .await
                .expect("listed the sled's requests")
                .into_iter()
                .map(decode)
                .collect();
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

        async fn assert_created(
            &self,
            db: &TestDatabase,
            outcome: SledExpungeRequestCreateOutcome,
            presence: SledExpungeCreationPresence,
            before: CreateWrites,
        ) -> SledExpungeRequest {
            let (request, removal_config) = expect_created(outcome);
            let cubby = match presence {
                SledExpungeCreationPresence::Present {
                    sp_slot,
                    source: SledExpungeCreationSpSlotSource::Inventory,
                } => Some(SledExpungeRecordedCubby {
                    slot: sp_slot,
                    source: SledExpungeSpSlotSource::Inventory,
                }),
                SledExpungeCreationPresence::Present {
                    sp_slot,
                    source: SledExpungeCreationSpSlotSource::LiveScan,
                } => Some(SledExpungeRecordedCubby {
                    slot: sp_slot,
                    source: SledExpungeSpSlotSource::LiveScan,
                }),
                SledExpungeCreationPresence::Gone => None,
            };
            assert_eq!(
                request,
                SledExpungeRequest {
                    id: request.id,
                    time_created: request.time_created,
                    time_modified: request.time_created,
                    rack_id: self.rack_id,
                    sled_id: self.sled_id,
                    baseboard_id: self.target.clone(),
                    tq_epoch: Epoch(2),
                    state: SledExpungeRequestState::WaitingForTrustQuorum {
                        cubby,
                        last_error: None,
                    },
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
            // TODO-RAINCLAUDE: The returned config's `time_created` was set in
            // Rust, at a finer resolution than the database stores, so it is
            // left out of the comparison.
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

    // TODO-RAINCLAUDE: Checks that `error` maps to the given HTTP status. The
    // expected refusals built with this helper below are written out by hand
    // with their status, rather than built by the functions under test.
    fn with_status(error: Error, status: u16) -> Error {
        assert_eq!(
            dropshot::HttpError::from(error.clone()).status_code.as_u16(),
            status,
            "{error:?} is a {status}",
        );
        error
    }

    fn serial_mismatch(sled_id: SledUuid, given: &str) -> Error {
        with_status(
            Error::invalid_request(format!(
                "Cannot expunge sled {sled_id}: the serial number given to \
                 confirm the request ({given:?}) does not match the sled's \
                 serial number. Check that this is the sled you mean to \
                 expunge (its serial number is in the sled's baseboard \
                 details, e.g. oxide system hardware sled view), then retry \
                 with that serial number."
            )),
            400,
        )
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create() {
        let logctx = dev::test_setup_log("test_sled_expunge_request_create");
        // TODO-RAINCLAUDE: Each presence is recorded as the new request's
        // `sp_slot` and `sp_slot_source`. A replay returns the request as it
        // was created, whatever the replay's probe says, including a probe
        // that found the same cubby through a different source.
        let inventory_presence =
            test_presence(SledExpungeCreationSpSlotSource::Inventory);
        let live_scan_presence =
            test_presence(SledExpungeCreationSpSlotSource::LiveScan);
        let presences = [
            (inventory_presence, SledExpungeCreationPresence::Gone),
            (live_scan_presence, inventory_presence),
            (SledExpungeCreationPresence::Gone, live_scan_presence),
        ];
        for (presence, replay_presence) in presences {
            let db = TestDatabase::new_with_datastore(&logctx.log).await;
            let fixture = CompleteFixture::new(&db).await;
            let input = CreateInput {
                probe: SledExpungeCreationProbe::Probed(presence),
                ..fixture.create_input(db.opctx())
            };
            let replay_input = CreateInput {
                probe: SledExpungeCreationProbe::Probed(replay_presence),
                ..fixture.create_input(db.opctx())
            };
            let not_probed_input = CreateInput {
                probe: SledExpungeCreationProbe::NotProbed,
                ..fixture.create_input(db.opctx())
            };
            fixture
                .expect_create_refused(
                    &db,
                    &not_probed_input,
                    Error::internal_error(&format!(
                        "creating an expunge request for sled {}: the caller \
                         skipped the presence probe because its policy and \
                         state check refused, but this transaction's policy \
                         and state check passed",
                        fixture.sled_id,
                    )),
                )
                .await;

            let before = fixture.create_writes(&db).await;
            let request = fixture
                .assert_created(
                    &db,
                    create(&db, &input).await.expect("created a request"),
                    presence,
                    before,
                )
                .await;

            let writes = fixture.create_writes(&db).await;
            for replay in [&input, &replay_input, &not_probed_input] {
                assert_eq!(
                    create(&db, replay).await.expect("create succeeds"),
                    SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                        request: request.clone(),
                    },
                    "replay with {:?} of a request created with {presence:?}",
                    replay.probe,
                );
            }
            assert_eq!(fixture.create_writes(&db).await, writes);
            let wrong_serial = CreateInput {
                serial: "not-the-serial".to_string(),
                ..input.clone()
            };
            fixture
                .expect_create_refused(
                    &db,
                    &wrong_serial,
                    serial_mismatch(fixture.sled_id, "not-the-serial"),
                )
                .await;

            fixture.tq(&db).commit(2).await;
            let writes = fixture.create_writes(&db).await;
            assert_eq!(
                create(&db, &input)
                    .await
                    .expect("create succeeds once the sled is not a member"),
                SledExpungeRequestCreateOutcome::AlreadyOutstanding {
                    request: request.clone(),
                },
            );
            assert_eq!(fixture.create_writes(&db).await, writes);

            db.terminate().await;
        }
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_create_unrecorded_baseboard() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_create_unrecorded_baseboard",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        // TODO-RAINCLAUDE: The acknowledged serial is the one on the sled row,
        // so the unrecorded part or serial number is the only thing that can
        // refuse the request.
        for (clear_part, clear_serial) in [(true, false), (false, true)] {
            let fixture = CompleteFixture::new(&db).await;
            let mut recorded = fixture.target.clone();
            if clear_part {
                recorded.part_number = String::new();
            }
            if clear_serial {
                recorded.serial_number = String::new();
            }
            {
                use nexus_db_schema::schema::sled::dsl;
                let conn =
                    db.datastore().pool_connection_for_tests().await.unwrap();
                let updated = diesel::update(dsl::sled)
                    .filter(dsl::id.eq(to_db_typed_uuid(fixture.sled_id)))
                    .set((
                        dsl::part_number.eq(recorded.part_number.clone()),
                        dsl::serial_number.eq(recorded.serial_number.clone()),
                    ))
                    .execute_async(&*conn)
                    .await
                    .expect("cleared the sled row's baseboard field");
                assert_eq!(updated, 1);
            }
            let input = fixture.create_input_for(
                db.opctx(),
                fixture.sled_id,
                &recorded,
            );
            fixture
                .expect_create_refused(
                    &db,
                    &input,
                    check_sled_expunge_baseboard_recorded(
                        fixture.sled_id,
                        &recorded,
                    )
                    .expect_err("an unrecorded baseboard is refused"),
                )
                .await;
        }
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

        // TODO-RAINCLAUDE: We run several trials so that more of them
        // interleave the two transactions. Each trial needs a fresh rack,
        // because a successful create uses up the sled.
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
            Epoch(1),
            SledExpungeRequestStateKind::Failed,
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
            baseboard_id.clone(),
            Epoch(2),
            SledExpungeRequestStateKind::PoweringOff,
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
            .filter(dsl::id.eq(to_db_typed_uuid(blocking.id)))
            .execute_async(&*conn)
            .await
            .expect("deleted the blocking request");

        // TODO-RAINCLAUDE: A concurrent create for this same sled also trips
        // the baseboard index. It must be replayed as this sled's request
        // rather than named as the blocker.
        let unfinished = insert_sled_expunge_request_for_sled(
            datastore,
            rack_id,
            sled_id,
            &baseboard_id,
            Epoch(3),
            SledExpungeRequestStateKind::WaitingForTrustQuorum,
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

    // TODO-RAINCLAUDE: Once creation's own checks have passed, only a
    // concurrent request can make the request-row insert fail. So this test
    // adds a unique index of its own to force that failure.
    const TEST_REJECT_INDEX: &str = "test_reject_sled_expunge_request_epoch";

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

        // TODO-RAINCLAUDE: This shows that a failure at the request-row insert
        // also rolls back the move of epoch 2 to `CommittedPartially`, and not
        // only the new config.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::sql_query(format!(
            "CREATE UNIQUE INDEX {TEST_REJECT_INDEX} \
             ON omicron.public.sled_expunge_request (rack_id, tq_epoch)"
        ))
        .execute_async(&*conn)
        .await
        .expect("created the test-only unique index");
        let at_epoch = insert_sled_expunge_request(
            datastore,
            fixture.rack_id,
            unique_baseboard(),
            Epoch(3),
            SledExpungeRequestStateKind::Done,
        )
        .await;
        fixture
            .expect_create_refused(
                &db,
                &fixture.create_input(opctx),
                unexpected_violation(
                    fixture.sled_id,
                    TEST_REJECT_INDEX,
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
            .filter(dsl::id.eq(to_db_typed_uuid(at_epoch.id)))
            .execute_async(&*conn)
            .await
            .expect("deleted the request at the removal epoch");

        let (request, removal_config) = expect_created(
            fixture.create_as_admin(&db).await.expect("created a request"),
        );
        assert_eq!(request.tq_epoch, Epoch(3));
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
            .insert_request(&db, 2, SledExpungeRequestStateKind::Failed)
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
        assert_eq!(request.tq_epoch, Epoch(3));
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

    // TODO-RAINCLAUDE: The unfinished request that exists before the create, if
    // any. The unique index allows one unfinished request per baseboard, so
    // this sled's request and another sled row's request for the same baseboard
    // can't both exist.
    #[derive(Clone, Copy, Debug)]
    enum UnfinishedRequest {
        None,
        ThisSled(SledExpungeRequestStateKind),
        OtherSledRow(SledExpungeRequestStateKind),
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

    // TODO-RAINCLAUDE: The trust quorum a case starts from. `NotMember` is set
    // up by committing an epoch 2 that removes the target, so it needs at least
    // one member more than the minimum.
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
        finished_oldest_first: Vec<SledExpungeRequestFinishedState>,
        policy: SledPolicy,
        state: SledState,
        sled_row_role: SledRole,
        collection: Option<CollectionEvidence>,
        trust_quorum: CaseTrustQuorum,
    }

    // TODO-RAINCLAUDE: The weights are chosen so that about one case in twelve
    // passes every check and creates a request, and so that every check is
    // regularly the first one to fail.
    fn create_case_strategy() -> impl Strategy<Value = CreateCase> {
        let unfinished_state = || {
            proptest::sample::select(SledExpungeRequestStateKind::UNFINISHED)
        };
        let unfinished = prop_oneof![
            6 => Just(UnfinishedRequest::None),
            1 => unfinished_state().prop_map(UnfinishedRequest::ThisSled),
            1 => unfinished_state().prop_map(UnfinishedRequest::OtherSledRow),
        ];
        let finished = proptest::collection::vec(
            prop_oneof![
                Just(SledExpungeRequestFinishedState::Done),
                Just(SledExpungeRequestFinishedState::Failed),
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

    // TODO-RAINCLAUDE: The checks `sled_expunge_request_create` makes, in the
    // order it makes them. The expected outcome of a case is the outcome of the
    // first check that applies.
    //
    // The test decides for itself whether each check applies, rather than
    // asking the code under test. Where it does call a production function, as
    // in the policy and state check and the scrimlet check, that function only
    // supplies the exact error text.
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

            // TODO-RAINCLAUDE: Each request gets an explicit, distinct
            // `time_created`, because two inserts can otherwise get the same
            // timestamp at the database's resolution, and the test needs to
            // know which request is the latest.
            let first_created = now_db_precision() - chrono::Duration::days(1);
            let mut latest_finished = None;
            for (hours, &state) in (0u32..).zip(&self.finished_oldest_first) {
                let request = fixture
                    .insert_request(db, 100 + u64::from(hours), state.kind())
                    .await;
                set_time_created(
                    db,
                    &request,
                    first_created + chrono::Duration::hours(i64::from(hours)),
                )
                .await;
                latest_finished =
                    Some(FinishedSledExpungeRequest { id: request.id, state });
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
                        fixture.target.clone(),
                        Epoch(99),
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
                probe: if self.policy_and_state_expungeable() {
                    SledExpungeCreationProbe::Probed(self.presence())
                } else {
                    SledExpungeCreationProbe::NotProbed
                },
            }
        }

        fn policy_and_state_expungeable(&self) -> bool {
            match (self.policy, self.state) {
                (SledPolicy::InService { .. }, SledState::Active) => true,
                (SledPolicy::InService { .. }, SledState::Decommissioned)
                | (
                    SledPolicy::Expunged,
                    SledState::Active | SledState::Decommissioned,
                ) => false,
            }
        }

        fn presence(&self) -> SledExpungeCreationPresence {
            match self.collection {
                Some(CollectionEvidence {
                    sled_agent_role: _,
                    sp_slot: Some(sp_slot),
                }) => SledExpungeCreationPresence::Present {
                    sp_slot,
                    source: SledExpungeCreationSpSlotSource::Inventory,
                },
                Some(CollectionEvidence {
                    sled_agent_role: _,
                    sp_slot: None,
                })
                | None => SledExpungeCreationPresence::Gone,
            }
        }

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
                            refused(serial_mismatch(sled_id, WRONG_SERIAL))
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
                    if self.policy_and_state_expungeable() {
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
                    AcknowledgedSerial::Wrong => {
                        refused(serial_mismatch(sled_id, WRONG_SERIAL))
                    }
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
                    } => {
                        let config = setup
                            .last_committed
                            .as_ref()
                            .expect("a configured rack has a committed config");
                        refused(with_status(
                            Error::invalid_request(format!(
                                "Cannot expunge sled {sled_id}: its baseboard \
                                 {} is not a member of the last committed \
                                 trust quorum configuration for rack {} \
                                 (epoch {}, committed). This API only \
                                 expunges sleds that are trust quorum \
                                 members. If this sled has already left the \
                                 trust quorum, contact Oxide support to \
                                 expunge it.",
                                fixture.target, fixture.rack_id, config.epoch,
                            )),
                            400,
                        ))
                    }
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
                        refused(with_status(
                            Error::conflict(format!(
                                "Cannot expunge sled {sled_id}: the trust \
                                 quorum for rack {} has {members} sleds and \
                                 must keep at least \
                                 {TRUST_QUORUM_MIN_MEMBERS}, so this sled \
                                 can't be removed from it. Contact Oxide \
                                 support.",
                                fixture.rack_id,
                            )),
                            409,
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
                            self.presence(),
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
            .filter(dsl::id.eq(to_db_typed_uuid(request.id)))
            .set(dsl::time_created.eq(time_created))
            .execute_async(&*conn)
            .await
            .expect("set the request's time_created");
        assert_eq!(updated, 1);
    }

    // TODO-RAINCLAUDE: Every case gets a fresh rack in one shared database, so
    // that cases don't affect each other and a failing case shrinks cleanly.
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
