// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: background task that drives operator-requested sled expunges (RFD 747); part 1 drives only waiting_for_trust_quorum, and powering_off and expunging are listed but left alone.

use crate::app::background::BackgroundTask;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use iddqd::IdOrdMap;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestAdvance;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_model::SledExpungeRequestUnfinishedState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledExpungeRequestUpdateOutcome;
use nexus_db_queries::db::datastore::SledExpungeTaskGateOutcome;
use nexus_db_queries::db::datastore::authz_sled_expunge_request_from_id;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::external_api::sled::SledProvisionPolicy;
use nexus_types::internal_api::background::SledExpungeRequestOutcome;
use nexus_types::internal_api::background::SledExpungeRequestStatus;
use nexus_types::internal_api::background::SledExpungeStatus;
use nexus_types::internal_api::background::SledExpungeTaskRequestState;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use omicron_common::api::external::Error;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::SledUuid;
use serde_json::json;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use trust_quorum_types::types::Epoch;

// TODO-RAINCLAUDE: value. How long a removal may stay Preparing, measured from the request's time_created, before last_error tells the operator the removal has stayed preparing.
const REMOVAL_PREPARING_ATTENTION_THRESHOLD: TimeDelta = TimeDelta::minutes(10);

// TODO-RAINCLAUDE: value. Requests are driven concurrently, at most this many at once, so one slow request does not hold up the others' turn; the activation still awaits every request, and there is no per-request timeout yet (part 2 adds one with the MGS calls).
const MAX_CONCURRENT_REQUESTS: usize = 16;

pub struct SledExpunge {
    datastore: Arc<DataStore>,
}

impl SledExpunge {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    // TODO-RAINCLAUDE: now is passed in so that tests can move the clock past the stuck-Preparing threshold without sleeping.
    pub(crate) async fn actually_activate(
        &self,
        opctx: &OpContext,
        now: DateTime<Utc>,
    ) -> SledExpungeStatus {
        let requests = match self
            .datastore
            .sled_expunge_request_list_unfinished(opctx)
            .await
        {
            Ok(requests) => requests,
            Err(error) => {
                error!(
                    &opctx.log,
                    "failed to list unfinished sled expunge requests";
                    &error,
                );
                return SledExpungeStatus::ListError {
                    error: format!(
                        "listing unfinished requests: {}",
                        InlineErrorChain::new(&error),
                    ),
                };
            }
        };

        let statuses: Vec<SledExpungeRequestStatus> = stream::iter(requests)
            .map(|request| self.drive_request(opctx, request, now))
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        let requests = IdOrdMap::from_iter_unique(statuses).expect(
            "list_unfinished pages by id, so each request is listed once",
        );
        SledExpungeStatus::Listed { requests }
    }

    async fn drive_request(
        &self,
        opctx: &OpContext,
        request: SledExpungeRequest,
        now: DateTime<Utc>,
    ) -> SledExpungeRequestStatus {
        let log = opctx.log.new(o!(
            "sled_expunge_request_id" => request.id().to_string(),
            "sled_id" => request.sled_id.to_string(),
            "tq_epoch" => request.tq_epoch,
            "state" => request.state.to_string(),
        ));
        let (outcome, inconsistent_records) = match request
            .state
            .as_unfinished()
        {
            Some(
                from @ SledExpungeRequestUnfinishedState::WaitingForTrustQuorum,
            ) => {
                self.drive_waiting_for_trust_quorum(
                    opctx, &log, &request, from, now,
                )
                .await
            }
            Some(
                SledExpungeRequestUnfinishedState::PoweringOff
                | SledExpungeRequestUnfinishedState::Expunging,
            ) => {
                debug!(
                    &log,
                    "not driving sled expunge request in this state yet"
                );
                (SledExpungeRequestOutcome::NotHandledYet, None)
            }
            // TODO-RAINCLAUDE: list_unfinished filters on the unfinished states, so this is unreachable short of a bug there.
            None => (
                error_outcome(
                    &log,
                    "unfinished request list returned a finished request",
                    &Error::internal_error(&format!(
                        "listed as unfinished, but in state {}",
                        request.state,
                    )),
                ),
                None,
            ),
        };
        SledExpungeRequestStatus {
            request_id: request.id(),
            sled_id: SledUuid::from(request.sled_id),
            state: task_request_state(request.state),
            outcome,
            inconsistent_records,
        }
    }

    // TODO-RAINCLAUDE: returns the outcome and, when the gate found the trust quorum records inconsistent, the internal detail for the status.
    async fn drive_waiting_for_trust_quorum(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        from: SledExpungeRequestUnfinishedState,
        now: DateTime<Utc>,
    ) -> (SledExpungeRequestOutcome, Option<String>) {
        let gate = match self
            .datastore
            .sled_expunge_request_check_task_gate(opctx, request)
            .await
        {
            Ok(SledExpungeTaskGateOutcome::Gate(gate)) => gate,
            Ok(SledExpungeTaskGateOutcome::RemovalConfigMissing) => {
                error!(
                    log,
                    "no trust quorum configuration exists at the request's \
                     removal epoch; failing the request"
                );
                let outcome = self
                    .fail(
                        opctx,
                        log,
                        request,
                        from,
                        FailureReason::RemovalConfigMissing,
                    )
                    .await;
                return (outcome, None);
            }
            Ok(SledExpungeTaskGateOutcome::Inconsistent { message }) => {
                error!(
                    log,
                    "sled expunge request's trust quorum records are \
                     inconsistent";
                    "error" => &message,
                );
                let outcome = self
                    .set_last_error(
                        opctx,
                        log,
                        request,
                        from,
                        NeedsAttention::InconsistentRecords,
                    )
                    .await;
                return (outcome, Some(message));
            }
            Err(error) => {
                return (
                    error_outcome(
                        log,
                        "checking the trust quorum gate",
                        &error,
                    ),
                    None,
                );
            }
        };
        let outcome = match gate {
            SledExpungeTaskGate::SledRemoved => {
                self.advance_to_powering_off(opctx, log, request).await
            }
            SledExpungeTaskGate::RemovalPreparing => {
                if now - request.time_created
                    < REMOVAL_PREPARING_ATTENTION_THRESHOLD
                {
                    debug!(log, "waiting for the removal to finish preparing");
                    SledExpungeRequestOutcome::RemovalPreparing
                } else {
                    self.set_last_error(
                        opctx,
                        log,
                        request,
                        from,
                        NeedsAttention::RemovalStuckPreparing,
                    )
                    .await
                }
            }
            SledExpungeTaskGate::RemovalAborted => {
                self.fail(
                    opctx,
                    log,
                    request,
                    from,
                    FailureReason::RemovalAborted,
                )
                .await
            }
            SledExpungeTaskGate::ReAddPreparing { epoch } => {
                self.set_last_error(
                    opctx,
                    log,
                    request,
                    from,
                    NeedsAttention::ReAddPreparing { re_add_epoch: epoch },
                )
                .await
            }
            SledExpungeTaskGate::ReAdded { epoch, state } => {
                warn!(
                    log,
                    "the sled was added back to the trust quorum";
                    "re_add_epoch" => %epoch,
                    "re_add_state" => ?state,
                );
                self.fail(
                    opctx,
                    log,
                    request,
                    from,
                    FailureReason::ReAdded { re_add_epoch: epoch },
                )
                .await
            }
        };
        (outcome, None)
    }

    async fn advance_to_powering_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequestOutcome {
        let result = self
            .datastore
            .sled_expunge_request_advance(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                SledExpungeRequestAdvance::ToPoweringOff,
            )
            .await;
        update_outcome(log, "advancing to powering_off", result, || {
            info!(log, "trust quorum removed the sled; powering it off next");
            SledExpungeRequestOutcome::AdvancedToPoweringOff
        })
    }

    async fn set_last_error(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        expected_state: SledExpungeRequestUnfinishedState,
        attention: NeedsAttention,
    ) -> SledExpungeRequestOutcome {
        let last_error = attention.render(request);
        // TODO-RAINCLAUDE: skipping an unchanged message keeps time_modified meaningful; the comparison is against the row as listed, so a concurrent writer can at worst cause one redundant write.
        if request.last_error.as_deref() == Some(last_error.as_str()) {
            info!(
                log,
                "sled expunge request still needs attention";
                "last_error" => &last_error,
            );
            return SledExpungeRequestOutcome::LastErrorUnchanged {
                last_error,
            };
        }
        let result = self
            .datastore
            .sled_expunge_request_set_last_error(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                expected_state,
                last_error.clone(),
            )
            .await;
        update_outcome(log, "setting last_error", result, || {
            warn!(
                log,
                "sled expunge request needs attention";
                "last_error" => &last_error,
            );
            SledExpungeRequestOutcome::LastErrorSet { last_error }
        })
    }

    async fn fail(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        from: SledExpungeRequestUnfinishedState,
        reason: FailureReason,
    ) -> SledExpungeRequestOutcome {
        let sled_id = SledUuid::from(request.sled_id);
        // TODO-RAINCLAUDE: the failure reason states the sled's policy as read now. A missing sled row still fails the request, saying the policy could not be read; any other read error leaves the request for the next activation.
        let sled_policy = match LookupPath::new(opctx, &self.datastore)
            .sled_id(sled_id)
            .fetch()
            .await
        {
            Ok((_, sled)) => Some(sled.policy()),
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "the request's sled no longer exists");
                None
            }
            Err(
                error @ (Error::InternalError { .. }
                | Error::Unauthenticated { .. }
                | Error::ServiceUnavailable { .. }
                | Error::TypeVersionMismatch { .. }
                | Error::InvalidRequest { .. }
                | Error::InvalidValue { .. }
                | Error::InsufficientCapacity { .. }
                | Error::Conflict { .. }
                | Error::NotFound { .. }
                | Error::ObjectAlreadyExists { .. }
                | Error::Forbidden
                | Error::Gone),
            ) => {
                return error_outcome(
                    log,
                    "reading the sled's policy to fail the request",
                    &error,
                );
            }
        };
        let failure_reason = reason.render(request, sled_policy);
        let result = self
            .datastore
            .sled_expunge_request_fail(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                from,
                failure_reason.clone(),
            )
            .await;
        update_outcome(log, "failing the request", result, || {
            warn!(
                log,
                "failed sled expunge request";
                "failure_reason" => &failure_reason,
            );
            SledExpungeRequestOutcome::Failed { failure_reason }
        })
    }
}

impl BackgroundTask for SledExpunge {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status =
                self.actually_activate(opctx, now_db_precision()).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    json!({ "error": format!("failed to serialize status: {err}") })
                }
            }
        })
    }
}

fn task_request_state(
    state: SledExpungeRequestState,
) -> SledExpungeTaskRequestState {
    match state {
        SledExpungeRequestState::WaitingForTrustQuorum => {
            SledExpungeTaskRequestState::WaitingForTrustQuorum
        }
        SledExpungeRequestState::PoweringOff => {
            SledExpungeTaskRequestState::PoweringOff
        }
        SledExpungeRequestState::Expunging => {
            SledExpungeTaskRequestState::Expunging
        }
        SledExpungeRequestState::Done => SledExpungeTaskRequestState::Done,
        SledExpungeRequestState::Failed => SledExpungeTaskRequestState::Failed,
    }
}

// TODO-RAINCLAUDE: maps one compare-and-swap's result: updated runs on success, a lost compare-and-swap is reported (another Nexus moved the request; the next activation sees its new state), and an error is logged and reported.
fn update_outcome<F>(
    log: &Logger,
    step: &str,
    result: Result<SledExpungeRequestUpdateOutcome, Error>,
    updated: F,
) -> SledExpungeRequestOutcome
where
    F: FnOnce() -> SledExpungeRequestOutcome,
{
    match result {
        Ok(SledExpungeRequestUpdateOutcome::Updated) => updated(),
        Ok(SledExpungeRequestUpdateOutcome::NotInExpectedState { current }) => {
            info!(
                log,
                "sled expunge request changed concurrently; leaving it to the \
                 next activation";
                "step" => step,
                "current_state" => current.state.to_string(),
            );
            SledExpungeRequestOutcome::NotInExpectedState {
                current_state: task_request_state(current.state),
            }
        }
        Err(error) => error_outcome(log, step, &error),
    }
}

fn error_outcome(
    log: &Logger,
    step: &str,
    error: &Error,
) -> SledExpungeRequestOutcome {
    error!(log, "sled expunge request step failed"; "step" => step, error);
    SledExpungeRequestOutcome::Error {
        error: format!("{step}: {}", InlineErrorChain::new(error)),
    }
}

// TODO-RAINCLAUDE: last_error and failure_reason are shown to operators as is, so each is rendered here, in one place, from a typed reason carrying only epochs and ids; no database, MGS, or internal error text can reach them. This module is their only writer, so a module-local enum is enough; an OperatorMessage newtype in nexus-types would only pay off once another crate writes these columns.
#[derive(Clone, Copy, Debug)]
enum NeedsAttention {
    RemovalStuckPreparing,
    ReAddPreparing { re_add_epoch: Epoch },
    InconsistentRecords,
}

impl NeedsAttention {
    fn render(self, request: &SledExpungeRequest) -> String {
        let rack_id = request.rack_id;
        let tq_epoch = request.tq_epoch;
        match self {
            NeedsAttention::RemovalStuckPreparing => format!(
                "The rack membership change that removes this sled (version \
                 {tq_epoch}) has stayed preparing for more than {} minutes. \
                 Possible causes include an unreachable coordinator sled or \
                 too many unreachable member sleds; check them before \
                 resubmitting. If rack_membership_status (rack {rack_id}) \
                 shows version {tq_epoch} as in_progress, abort it with \
                 rack_membership_abort, passing version {tq_epoch}. If the \
                 abort is refused because the change is already committing, \
                 no action is needed. Once this request shows failed, submit \
                 a new expunge request for the sled.",
                REMOVAL_PREPARING_ATTENTION_THRESHOLD.num_minutes(),
            ),
            NeedsAttention::ReAddPreparing { re_add_epoch } => format!(
                "A rack membership change that adds this sled back to the \
                 rack (version {re_add_epoch}) is in progress, so this \
                 request cannot continue. If that change was not intended, \
                 abort it with rack_membership_abort (rack {rack_id}), \
                 passing version {re_add_epoch}, and this request continues; \
                 if the change starts committing, this request fails. If \
                 this persists, contact Oxide support.",
            ),
            NeedsAttention::InconsistentRecords => {
                "This request's rack membership records are inconsistent, so \
                 it cannot continue. Contact Oxide support."
                    .to_string()
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FailureReason {
    RemovalAborted,
    RemovalConfigMissing,
    ReAdded { re_add_epoch: Epoch },
}

impl FailureReason {
    // TODO-RAINCLAUDE: sled_policy is None when the sled's row could not be found.
    fn render(
        self,
        request: &SledExpungeRequest,
        sled_policy: Option<SledPolicy>,
    ) -> String {
        let tq_epoch = request.tq_epoch;
        let what_happened = match self {
            FailureReason::RemovalAborted => format!(
                "The rack membership change that removes this sled (version \
                 {tq_epoch}) was aborted, so the sled is still a member of \
                 the rack and was neither powered off nor expunged. To try \
                 again, submit a new expunge request for the sled."
            ),
            FailureReason::RemovalConfigMissing => format!(
                "This request's rack membership change (version {tq_epoch}) \
                 could not be found, so the sled was neither powered off nor \
                 expunged. Contact Oxide support."
            ),
            FailureReason::ReAdded { re_add_epoch } => format!(
                "The sled was added back to the rack (rack membership version \
                 {re_add_epoch}) after its removal at version {tq_epoch}, so \
                 it was not expunged. Contact Oxide support."
            ),
        };
        let sled_id = request.sled_id;
        let policy = match sled_policy {
            Some(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            }) => format!(
                "At the time this request failed, the sled's provision policy \
                 was non-provisionable (creating an expunge request makes the \
                 sled non-provisionable, and failing it does not undo that). \
                 If you want the sled to host new resources again, set its \
                 provision policy to provisionable with \
                 sled_set_provision_policy (sled {sled_id})."
            ),
            Some(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            }) => "At the time this request failed, the sled's provision \
                   policy was provisionable."
                .to_string(),
            Some(SledPolicy::Expunged) => {
                "At the time this request failed, the sled was expunged."
                    .to_string()
            }
            None => "The sled's current policy could not be read.".to_string(),
        };
        format!("{what_happened} {policy}")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::prelude::*;
    use nexus_auth::authz;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_queries::db::datastore::TqPrepareStatusOutcome;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::helpers::SledUpdateBuilder;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_expect_written;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::set_sled_expunge_request_state;
    use nexus_types::identity::Asset;
    use nexus_types::trust_quorum::IsLrtqUpgrade;
    use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
    use nexus_types::trust_quorum::TrustQuorumConfig;
    use nexus_types::trust_quorum::TrustQuorumConfigState;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::RackUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use trust_quorum_types::configuration::Configuration;
    use trust_quorum_types::crypto::Sha3_256Digest;
    use uuid::Uuid;

    // TODO-RAINCLAUDE: a rack whose committed epoch 1 trust quorum has five members; target has a sled row, set non-provisionable as request creation does, and the epoch 1 coordinator has an in-service sled row so that later configs can be proposed.
    struct TestRack {
        rack_id: RackUuid,
        members: BTreeSet<BaseboardId>,
        target: BaseboardId,
        sled_id: SledUuid,
    }

    impl TestRack {
        async fn new(db: &TestDatabase) -> Self {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let rack_id = RackUuid::new_v4();
            let sled = sled_upsert_expect_written(
                datastore,
                SledUpdateBuilder::new().rack_id(rack_id).build(),
            )
            .await;
            let sled_id = sled.id();
            let target = BaseboardId {
                part_number: sled.part_number().to_string(),
                serial_number: sled.serial_number().to_string(),
            };
            let mut members = BTreeSet::from([target.clone()]);
            while members.len() < 5 {
                members.insert(BaseboardId {
                    part_number: "test-part".to_string(),
                    serial_number: Uuid::new_v4().to_string(),
                });
            }

            let conn = datastore
                .pool_connection_for_tests()
                .await
                .expect("got a database connection");
            {
                use nexus_db_schema::schema::hw_baseboard_id::dsl;
                let rows: Vec<HwBaseboardId> = members
                    .iter()
                    .map(|member| HwBaseboardId {
                        id: Uuid::new_v4(),
                        part_number: member.part_number.clone(),
                        serial_number: member.serial_number.clone(),
                    })
                    .collect();
                diesel::insert_into(dsl::hw_baseboard_id)
                    .values(rows)
                    .execute_async(&*conn)
                    .await
                    .expect("inserted hw_baseboard_id rows");
            }
            let coordinator = members
                .iter()
                .find(|member| **member != target)
                .expect("another member")
                .clone();
            sled_upsert_for_baseboard(datastore, rack_id, &coordinator).await;
            DataStore::tq_insert_rss_config_after_handoff(
                opctx,
                &conn,
                authz::TrustQuorumConfig::for_rack_id(rack_id),
                members.clone(),
                coordinator,
            )
            .await
            .expect("inserted the epoch 1 trust quorum config");

            let rack = Self { rack_id, members, target, sled_id };
            rack.set_provision_policy(
                db,
                SledProvisionPolicy::NonProvisionable,
            )
            .await;
            rack
        }

        async fn set_provision_policy(
            &self,
            db: &TestDatabase,
            policy: SledProvisionPolicy,
        ) {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let (authz_sled,) = LookupPath::new(opctx, datastore)
                .sled_id(self.sled_id)
                .lookup_for(authz::Action::Modify)
                .await
                .expect("looked up the sled");
            datastore
                .sled_set_provision_policy(opctx, &authz_sled, policy)
                .await
                .expect("set the sled's provision policy");
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

        fn authz_tq(&self) -> authz::TrustQuorumConfig {
            authz::TrustQuorumConfig::for_rack_id(self.rack_id)
        }

        async fn config(
            &self,
            db: &TestDatabase,
            epoch: u64,
        ) -> TrustQuorumConfig {
            db.datastore()
                .tq_get_config(db.opctx(), self.authz_tq(), Epoch(epoch))
                .await
                .expect("loaded the trust quorum config")
                .unwrap_or_else(|| {
                    panic!("the trust quorum config at epoch {epoch} exists")
                })
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

        async fn propose_removal(&self, db: &TestDatabase) {
            self.propose(db, 2, 1, self.members_without(&[&self.target])).await;
        }

        async fn start_committing(&self, db: &TestDatabase, epoch: u64) {
            let config = self.config(db, epoch).await;
            let members: BTreeSet<_> = config.members.keys().cloned().collect();
            let coordinator_config = Configuration {
                rack_id: config.rack_id,
                epoch: config.epoch,
                coordinator: config.coordinator.clone(),
                members: members
                    .iter()
                    .cloned()
                    .map(|id| (id, Sha3_256Digest([0u8; 32])))
                    .collect(),
                threshold: config.threshold,
                encrypted_rack_secrets: None,
            };
            let outcome = db
                .datastore()
                .tq_update_prepare_status(
                    db.opctx(),
                    coordinator_config,
                    members,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("acked prepares at epoch {epoch}: {error}")
                });
            let TqPrepareStatusOutcome::Committing { config: _ } = outcome
            else {
                panic!("acked every prepare at epoch {epoch}: {outcome:?}");
            };
        }

        async fn commit(&self, db: &TestDatabase, epoch: u64) {
            self.start_committing(db, epoch).await;
            let config = self.config(db, epoch).await;
            let state = db
                .datastore()
                .tq_update_commit_status(
                    db.opctx(),
                    self.authz_tq(),
                    Epoch(epoch),
                    config.members.keys().cloned().collect(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("acked commits at epoch {epoch}: {error}")
                });
            assert_eq!(state, TrustQuorumConfigState::Committed);
        }

        async fn abort(&self, db: &TestDatabase, epoch: u64) {
            db.datastore()
                .tq_abort_config(
                    db.opctx(),
                    self.authz_tq(),
                    Epoch(epoch),
                    "test".to_string(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("aborted the epoch {epoch} config: {error}")
                });
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

        fn status(
            &self,
            request: &SledExpungeRequest,
            outcome: SledExpungeRequestOutcome,
        ) -> SledExpungeRequestStatus {
            status_for(request, outcome)
        }

        fn non_provisionable_text(&self) -> String {
            non_provisionable_text(self.sled_id)
        }

        fn removal_aborted_text(&self) -> String {
            format!("{REMOVAL_ABORTED_TEXT} {}", self.non_provisionable_text())
        }

        fn stuck_preparing_text(&self) -> String {
            let rack_id = self.rack_id;
            format!(
                "The rack membership change that removes this sled (version \
                 2) has stayed preparing for more than 10 minutes. Possible \
                 causes include an unreachable coordinator sled or too many \
                 unreachable member sleds; check them before resubmitting. \
                 If rack_membership_status (rack {rack_id}) shows version 2 \
                 as in_progress, abort it with rack_membership_abort, \
                 passing version 2. If the abort is refused because the \
                 change is already committing, no action is needed. Once \
                 this request shows failed, submit a new expunge request for \
                 the sled."
            )
        }

        fn re_add_preparing_text(&self) -> String {
            format!(
                "A rack membership change that adds this sled back to the \
                 rack (version 3) is in progress, so this request cannot \
                 continue. If that change was not intended, abort it with \
                 rack_membership_abort (rack {}), passing version 3, and this \
                 request continues; if the change starts committing, this \
                 request fails. If this persists, contact Oxide support.",
                self.rack_id,
            )
        }
    }

    const REMOVAL_ABORTED_TEXT: &str = "The rack membership change that \
        removes this sled (version 2) was aborted, so the sled is still a \
        member of the rack and was neither powered off nor expunged. To try \
        again, submit a new expunge request for the sled.";
    const INCONSISTENT_RECORDS_TEXT: &str = "This request's rack membership \
        records are inconsistent, so it cannot continue. Contact Oxide \
        support.";
    const POLICY_UNREADABLE_TEXT: &str =
        "The sled's current policy could not be read.";

    fn non_provisionable_text(sled_id: SledUuid) -> String {
        format!(
            "At the time this request failed, the sled's provision policy was \
             non-provisionable (creating an expunge request makes the sled \
             non-provisionable, and failing it does not undo that). If you \
             want the sled to host new resources again, set its provision \
             policy to provisionable with sled_set_provision_policy (sled \
             {sled_id})."
        )
    }

    fn status_for(
        request: &SledExpungeRequest,
        outcome: SledExpungeRequestOutcome,
    ) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            request_id: request.id(),
            sled_id: SledUuid::from(request.sled_id),
            state: task_request_state(request.state),
            outcome,
            inconsistent_records: None,
        }
    }

    async fn read(
        db: &TestDatabase,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequest {
        db.datastore()
            .sled_expunge_request_get(db.opctx(), request.id())
            .await
            .expect("read the sled expunge request")
    }

    // TODO-RAINCLAUDE: moves time_modified an hour into the past, so a following write must strictly increase it whatever the clock resolution.
    async fn backdated(
        db: &TestDatabase,
        request: &SledExpungeRequest,
    ) -> SledExpungeRequest {
        use nexus_db_schema::schema::sled_expunge_request::dsl;
        let conn = db
            .datastore()
            .pool_connection_for_tests()
            .await
            .expect("got a database connection");
        diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(request.id))
            .set(
                dsl::time_modified
                    .eq(request.time_modified - TimeDelta::hours(1)),
            )
            .execute_async(&*conn)
            .await
            .expect("backdated time_modified");
        read(db, request).await
    }

    fn listed(entries: Vec<SledExpungeRequestStatus>) -> SledExpungeStatus {
        SledExpungeStatus::Listed {
            requests: IdOrdMap::from_iter_unique(entries)
                .expect("expected entries are unique"),
        }
    }

    #[tokio::test]
    async fn test_sled_expunge_stuck_preparing_sets_last_error() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_stuck_preparing_sets_last_error",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let task = SledExpunge::new(db.datastore().clone());
        let rack = TestRack::new(&db).await;
        rack.propose_removal(&db).await;
        let inserted = rack
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        let request = backdated(&db, &inserted).await;
        let threshold_at =
            request.time_created + REMOVAL_PREPARING_ATTENTION_THRESHOLD;

        assert_eq!(
            task.actually_activate(
                db.opctx(),
                threshold_at - TimeDelta::microseconds(1),
            )
            .await,
            listed(vec![rack.status(
                &request,
                SledExpungeRequestOutcome::RemovalPreparing,
            )]),
        );
        assert_eq!(read(&db, &request).await, request);

        let stuck = rack.stuck_preparing_text();
        assert_eq!(
            task.actually_activate(db.opctx(), threshold_at).await,
            listed(vec![rack.status(
                &request,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: stuck.clone(),
                },
            )]),
        );
        let flagged = read(&db, &request).await;
        assert_eq!(
            flagged,
            SledExpungeRequest {
                last_error: Some(stuck.clone()),
                time_modified: flagged.time_modified,
                ..request.clone()
            },
        );
        assert!(flagged.time_modified > request.time_modified);

        // TODO-RAINCLAUDE: the same message is not rewritten, so time_modified stays put; this runs at threshold_at itself, so the threshold is measured from time_created and not from time_modified.
        assert_eq!(
            task.actually_activate(db.opctx(), threshold_at).await,
            listed(vec![rack.status(
                &flagged,
                SledExpungeRequestOutcome::LastErrorUnchanged {
                    last_error: stuck,
                },
            )]),
        );
        assert_eq!(read(&db, &request).await, flagged);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: one request per way the task can act on a request, all driven by one activation; each status shows the write landed, since a lost compare-and-swap reports NotInExpectedState.
    #[tokio::test]
    async fn test_sled_expunge_drives_requests_independently() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_drives_requests_independently",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let task = SledExpunge::new(db.datastore().clone());

        let removed = TestRack::new(&db).await;
        removed.propose_removal(&db).await;
        removed.start_committing(&db, 2).await;
        let removed_request = removed
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;

        let aborted = TestRack::new(&db).await;
        aborted.propose_removal(&db).await;
        aborted.abort(&db, 2).await;
        let aborted_request = aborted
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;

        // TODO-RAINCLAUDE: no config exists at epoch 9.
        let missing_config = TestRack::new(&db).await;
        let missing_config_request = missing_config
            .insert_request(
                &db,
                9,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;

        let preparing = TestRack::new(&db).await;
        preparing.propose_removal(&db).await;
        let preparing_request = preparing
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;

        // TODO-RAINCLAUDE: check_no_new_member_being_expunged_conn refuses re-adding a sled with an unfinished request, so each re-add is proposed before the request row exists; the task's re-add handling is defensive.
        let re_add_preparing = TestRack::new(&db).await;
        let re_added = TestRack::new(&db).await;
        let mut re_add_requests = Vec::new();
        for rack in [&re_add_preparing, &re_added] {
            rack.propose_removal(&db).await;
            rack.commit(&db, 2).await;
            rack.propose(&db, 3, 2, rack.members.clone()).await;
            re_add_requests.push(
                rack.insert_request(
                    &db,
                    2,
                    SledExpungeRequestState::WaitingForTrustQuorum,
                )
                .await,
            );
        }
        re_added.start_committing(&db, 3).await;
        let [re_add_preparing_request, re_added_request] = re_add_requests
            .try_into()
            .expect("inserted one request for each rack");

        let powering_off = TestRack::new(&db).await;
        powering_off.propose_removal(&db).await;
        powering_off.commit(&db, 2).await;
        let powering_off_request = powering_off
            .insert_request(&db, 2, SledExpungeRequestState::PoweringOff)
            .await;

        let expunging = TestRack::new(&db).await;
        expunging.propose_removal(&db).await;
        expunging.commit(&db, 2).await;
        let expunging_request = expunging
            .insert_request(&db, 2, SledExpungeRequestState::Expunging)
            .await;

        // TODO-RAINCLAUDE: the request points at epoch 1, the committed RSS config that still includes the sled, which the gate reports as inconsistent records.
        let inconsistent = TestRack::new(&db).await;
        let inconsistent_request = inconsistent
            .insert_request(
                &db,
                1,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;

        // TODO-RAINCLAUDE: the request's sled row does not exist, so it fails without the sled's policy.
        let missing_sled = TestRack::new(&db).await;
        missing_sled.propose_removal(&db).await;
        missing_sled.abort(&db, 2).await;
        let missing_sled_request = missing_sled
            .insert_request_for_sled(
                &db,
                SledUuid::new_v4(),
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;

        let inconsistent_detail = format!(
            "sled expunge request {} for {} (rack {}, removal epoch 1): the \
             removal configuration (epoch 1, state Committed) still includes \
             the sled",
            inconsistent_request.id, inconsistent.target, inconsistent.rack_id,
        );
        assert_eq!(
            task.actually_activate(db.opctx(), now_db_precision()).await,
            listed(vec![
                removed.status(
                    &removed_request,
                    SledExpungeRequestOutcome::AdvancedToPoweringOff,
                ),
                aborted.status(
                    &aborted_request,
                    SledExpungeRequestOutcome::Failed {
                        failure_reason: aborted.removal_aborted_text(),
                    },
                ),
                missing_config.status(
                    &missing_config_request,
                    SledExpungeRequestOutcome::Failed {
                        failure_reason: format!(
                            "This request's rack membership change (version \
                             9) could not be found, so the sled was neither \
                             powered off nor expunged. Contact Oxide \
                             support. {}",
                            missing_config.non_provisionable_text(),
                        ),
                    },
                ),
                preparing.status(
                    &preparing_request,
                    SledExpungeRequestOutcome::RemovalPreparing,
                ),
                re_add_preparing.status(
                    &re_add_preparing_request,
                    SledExpungeRequestOutcome::LastErrorSet {
                        last_error: re_add_preparing.re_add_preparing_text(),
                    },
                ),
                re_added.status(
                    &re_added_request,
                    SledExpungeRequestOutcome::Failed {
                        failure_reason: format!(
                            "The sled was added back to the rack (rack \
                             membership version 3) after its removal at \
                             version 2, so it was not expunged. Contact Oxide \
                             support. {}",
                            re_added.non_provisionable_text(),
                        ),
                    },
                ),
                powering_off.status(
                    &powering_off_request,
                    SledExpungeRequestOutcome::NotHandledYet,
                ),
                expunging.status(
                    &expunging_request,
                    SledExpungeRequestOutcome::NotHandledYet,
                ),
                SledExpungeRequestStatus {
                    inconsistent_records: Some(inconsistent_detail),
                    ..inconsistent.status(
                        &inconsistent_request,
                        SledExpungeRequestOutcome::LastErrorSet {
                            last_error: INCONSISTENT_RECORDS_TEXT.to_string(),
                        },
                    )
                },
                missing_sled.status(
                    &missing_sled_request,
                    SledExpungeRequestOutcome::Failed {
                        failure_reason: format!(
                            "{REMOVAL_ABORTED_TEXT} {POLICY_UNREADABLE_TEXT}"
                        ),
                    },
                ),
            ]),
        );
        assert_eq!(read(&db, &preparing_request).await, preparing_request);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: drive_request is handed the row as another Nexus listed it, after this one already moved it, so its compare-and-swap loses.
    #[tokio::test]
    async fn test_sled_expunge_stale_row_is_not_in_expected_state() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_stale_row_is_not_in_expected_state",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let task = SledExpunge::new(db.datastore().clone());

        let rack = TestRack::new(&db).await;
        rack.propose_removal(&db).await;
        rack.start_committing(&db, 2).await;
        let stale = rack
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        set_sled_expunge_request_state(
            db.datastore(),
            &stale,
            SledExpungeRequestState::PoweringOff,
        )
        .await;
        let current = read(&db, &stale).await;

        assert_eq!(
            task.drive_request(db.opctx(), stale.clone(), now_db_precision())
                .await,
            rack.status(
                &stale,
                SledExpungeRequestOutcome::NotInExpectedState {
                    current_state: SledExpungeTaskRequestState::PoweringOff,
                },
            ),
        );
        assert_eq!(read(&db, &stale).await, current);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_sled_expunge_failure_reason_policies() {
        let sled_id = SledUuid::new_v4();
        let request = SledExpungeRequest::new(
            RackUuid::new_v4(),
            sled_id,
            BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: "test-serial".to_string(),
            },
            2,
        );
        let render =
            |policy| FailureReason::RemovalAborted.render(&request, policy);
        assert_eq!(
            render(Some(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            })),
            format!(
                "{REMOVAL_ABORTED_TEXT} {}",
                non_provisionable_text(sled_id)
            ),
        );
        assert_eq!(
            render(Some(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            })),
            format!(
                "{REMOVAL_ABORTED_TEXT} At the time this request failed, the \
                 sled's provision policy was provisionable."
            ),
        );
        assert_eq!(
            render(Some(SledPolicy::Expunged)),
            format!(
                "{REMOVAL_ABORTED_TEXT} At the time this request failed, the \
                 sled was expunged."
            ),
        );
        assert_eq!(
            render(None),
            format!("{REMOVAL_ABORTED_TEXT} {POLICY_UNREADABLE_TEXT}"),
        );
    }
}
