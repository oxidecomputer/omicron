// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: background task that drives operator-requested sled expunges (RFD 747) through waiting_for_trust_quorum, powering_off and expunging; a request whose removal is past Preparing goes on to powering_off, and one whose sled is powered off goes on to expunge, in the same activation.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use gateway_types::component::SpType;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use itertools::Itertools;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledExpungePowerOffOutcome;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestAdvance;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_model::SledExpungeRequestUnfinishedState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledExpungeRequestCompleteOutcome;
use nexus_db_queries::db::datastore::SledExpungeRequestUpdateOutcome;
use nexus_db_queries::db::datastore::SledExpungeTaskGateOutcome;
use nexus_db_queries::db::datastore::authz_sled_expunge_request_from_id;
use nexus_networking::GatewayClient;
use nexus_networking::GatewaysByRack;
use nexus_networking::PowerOffAtSlotOutcome;
use nexus_networking::power_off_sled_at_slot;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::external_api::sled::SledProvisionPolicy;
use nexus_types::internal_api::background::SledExpungeMgsDiscovery;
use nexus_types::internal_api::background::SledExpungeMgsPowerOffFailure;
use nexus_types::internal_api::background::SledExpungeMgsRackIdUnknown;
use nexus_types::internal_api::background::SledExpungePowerCheck;
use nexus_types::internal_api::background::SledExpungePowerOffWait;
use nexus_types::internal_api::background::SledExpungeRequestOutcome;
use nexus_types::internal_api::background::SledExpungeRequestStatus;
use nexus_types::internal_api::background::SledExpungeStatus;
use nexus_types::internal_api::background::SledExpungeTaskRequestState;
use nexus_types::internal_api::background::SledExpungeTaskStep;
use nexus_types::inventory::Collection;
use nexus_types::sled_expunge::MgsPowerOffFailure;
use nexus_types::sled_expunge::SWITCH_ATTACHED_SLED_CUBBIES;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use omicron_common::api::external::Error;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use serde_json::json;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::watch;
use trust_quorum_types::types::Epoch;

// TODO-RAINCLAUDE: value. How long a removal may stay Preparing, measured from the request's time_created, before last_error tells the operator the removal has stayed preparing.
const REMOVAL_PREPARING_ATTENTION_THRESHOLD: TimeDelta = TimeDelta::minutes(10);

// TODO-RAINCLAUDE: value. How many consecutive activations must end a request in an error that should not resolve by retrying before last_error tells the operator; a single such error may be a transient fault that the next activation gets past, so it goes in the status only.
const INTERNAL_ERROR_ATTENTION_THRESHOLD: u32 = 3;

// TODO-RAINCLAUDE: value. Requests are driven concurrently, at most this many at once, so one slow request does not hold up the others' turn; each request is bounded by PER_REQUEST_TIMEOUT, so a stalled one holds a turn for at most that long, and a request queued behind MAX_CONCURRENT_REQUESTS stalled ones waits at most one timeout for its turn.
const MAX_CONCURRENT_REQUESTS: usize = 16;

// TODO-RAINCLAUDE: value. A backstop over all of one request's driving in one activation, well above the worst case with every MGS call at its 15s client timeout: the activation's MGS discovery, if this request is the one that starts it (DNS, then each MGS's rack ID, about 20s), re-reading the request row, then a power-off of about 30s per MGS (a re-check and a command), through each MGS of the rack (normally two, one per switch), plus the database writes; so a stalled MGS normally ends in a failed power-off and a last_error rather than in this timeout. A timed-out request is abandoned mid-step and nothing more is written for it; every step is a compare-and-swap or idempotent, so the next activation picks it up from whatever state it reached.
const PER_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub struct SledExpungeActivators {
    pub instance_watcher: Activator,
    pub blueprint_planner: Activator,
}

impl SledExpungeActivators {
    fn activate(&self) {
        self.instance_watcher.activate();
        self.blueprint_planner.activate();
    }
}

// TODO-RAINCLAUDE: inventory_rx is the inventory loader's latest collection, read once per activation; inventory_collection is activated whenever a request could not be powered off for a reason a newer collection may resolve (no collection, sled not found, slot holds another board, every MGS failed).
pub struct SledExpunge {
    datastore: Arc<DataStore>,
    resolver: internal_dns_resolver::Resolver,
    inventory_rx: watch::Receiver<Option<Arc<Collection>>>,
    inventory_collection: Activator,
    activators: SledExpungeActivators,
    internal_errors: IdOrdMap<InternalErrorStreak>,
}

// TODO-RAINCLAUDE: how many consecutive activations ended this request in an error that should not resolve by retrying. Kept in memory only, so each Nexus counts separately and a Nexus restart starts its counts over, which at worst delays the last_error. An activation the request gets through without an error drops the entry, as does the request leaving the unfinished listing; a retryable error leaves the count as it was, and a timeout keeps whatever count it reached.
#[derive(Clone, Debug, PartialEq, Eq)]
struct InternalErrorStreak {
    request_id: SledExpungeRequestUuid,
    consecutive: u32,
}

impl IdOrdItem for InternalErrorStreak {
    type Key<'a> = SledExpungeRequestUuid;
    fn key(&self) -> Self::Key<'_> {
        self.request_id
    }
    id_upcast!();
}

// TODO-RAINCLAUDE: what an activation that ended in an error does to the request's streak: a retryable error leaves it as it was, and any other error extends it, surfacing the generic last_error once it reaches INTERNAL_ERROR_ATTENTION_THRESHOLD.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InternalErrorStreakStep {
    Unchanged,
    BelowThreshold { consecutive: u32 },
    Surface { consecutive: u32 },
}

fn extend_internal_error_streak(
    persistent_error: bool,
    consecutive: u32,
) -> InternalErrorStreakStep {
    if !persistent_error {
        return InternalErrorStreakStep::Unchanged;
    }
    let consecutive = consecutive.saturating_add(1);
    if consecutive < INTERNAL_ERROR_ATTENTION_THRESHOLD {
        InternalErrorStreakStep::BelowThreshold { consecutive }
    } else {
        InternalErrorStreakStep::Surface { consecutive }
    }
}

// TODO-RAINCLAUDE: the result of one MGS discovery. An MGS whose rack could not be learned is left out of its rack, so power-off is tried only through the rack's other MGSes.
enum MgsDiscovery {
    Failed(String),
    Discovered(GatewaysByRack),
}

impl MgsDiscovery {
    fn clients_for(&self, rack_id: RackUuid) -> &[GatewayClient] {
        match self {
            MgsDiscovery::Failed(_) => &[],
            MgsDiscovery::Discovered(gateways) => {
                gateways.for_rack(&rack_id).unwrap_or(&[])
            }
        }
    }

    fn status(&self) -> SledExpungeMgsDiscovery {
        match self {
            MgsDiscovery::Failed(error) => {
                SledExpungeMgsDiscovery::Failed { error: error.clone() }
            }
            MgsDiscovery::Discovered(gateways) => {
                // TODO-RAINCLAUDE: DNS can name one MGS more than once, so the status keeps one entry per address, with the last error seen for it.
                let mut rack_id_unknown = IdOrdMap::new();
                for (gateway, error) in gateways.unknown() {
                    rack_id_unknown.insert_overwrite(
                        SledExpungeMgsRackIdUnknown {
                            mgs: gateway.addr,
                            error: InlineErrorChain::new(error).to_string(),
                        },
                    );
                }
                SledExpungeMgsDiscovery::Discovered { rack_id_unknown }
            }
        }
    }
}

// TODO-RAINCLAUDE: what every request in one activation shares: MGS discovery, run lazily by the first request in powering_off (including one that advanced to it in this activation) whose sled inventory places in a sled cubby, at most once per activation, and not at all if no request needs it. The OnceCell is cancel-safe, so a request timing out mid-discovery leaves it to the next request that needs it; started records that it began, so the status can tell "never needed" from "cancelled before it finished".
struct ActivationMgs<'r> {
    log: &'r Logger,
    resolver: &'r internal_dns_resolver::Resolver,
    started: AtomicBool,
    discovery: tokio::sync::OnceCell<MgsDiscovery>,
}

impl<'r> ActivationMgs<'r> {
    fn new(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
    ) -> Self {
        Self {
            log,
            resolver,
            started: AtomicBool::new(false),
            discovery: tokio::sync::OnceCell::new(),
        }
    }

    async fn get(&self) -> &MgsDiscovery {
        self.discovery
            .get_or_init(|| {
                self.started.store(true, Ordering::Relaxed);
                discover_gateways(self.log, self.resolver)
            })
            .await
    }

    fn status(&self) -> SledExpungeMgsDiscovery {
        match self.discovery.get() {
            Some(discovery) => discovery.status(),
            None if self.started.load(Ordering::Relaxed) => {
                SledExpungeMgsDiscovery::Unfinished
            }
            None => SledExpungeMgsDiscovery::NotNeeded,
        }
    }
}

// TODO-RAINCLAUDE: what driving one request found besides its outcome, kept outside the per-request timeout so that a timed-out request still reports the step it was in and whatever it had found. latest_row is the row as this activation's last successful write returned it, so a persistent error's last_error is written against the request's current state and last_error. internal_errors starts at the request's streak from earlier activations and counts this one if it ends in a persistent error.
#[derive(Default)]
struct RequestProgress {
    step: SledExpungeTaskStep,
    latest_row: Option<SledExpungeRequest>,
    persistent_error: bool,
    internal_errors: u32,
    inconsistent_records: Option<String>,
    power_check: Option<SledExpungePowerCheck>,
}

// TODO-RAINCLAUDE: drives one request, recording its progress as it goes; a trait (rather than a closure) so that its future may borrow the request and progress, and so that tests can drive requests that stall.
trait RequestDriver {
    async fn drive(
        &self,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome;
}

// TODO-RAINCLAUDE: the slot decide() found the sled in, and the rack's MGSes to power it off there through.
#[derive(Clone, Copy)]
struct PowerOffAt<'a> {
    clients: &'a [GatewayClient],
    slot: u16,
}

// TODO-RAINCLAUDE: what the powering_off step takes from its activation: the lazily discovered MGSes and the inventory collection read at the start of the activation.
#[derive(Clone, Copy)]
struct PoweringOffInputs<'a> {
    mgs: &'a ActivationMgs<'a>,
    collection: Option<&'a Collection>,
}

struct ActivationDriver<'x> {
    task: &'x SledExpunge,
    opctx: &'x OpContext,
    inputs: PoweringOffInputs<'x>,
    now: DateTime<Utc>,
}

impl RequestDriver for ActivationDriver<'_> {
    async fn drive(
        &self,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        self.task
            .drive_request(self.opctx, self.inputs, request, self.now, progress)
            .await
    }
}

impl SledExpunge {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns_resolver::Resolver,
        inventory_rx: watch::Receiver<Option<Arc<Collection>>>,
        inventory_collection: Activator,
        activators: SledExpungeActivators,
    ) -> Self {
        Self {
            datastore,
            resolver,
            inventory_rx,
            inventory_collection,
            activators,
            internal_errors: IdOrdMap::new(),
        }
    }

    // TODO-RAINCLAUDE: now is passed in so that tests can move the clock past the stuck-Preparing threshold without sleeping.
    pub(crate) async fn actually_activate(
        &mut self,
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

        let collection = self.inventory_rx.borrow_and_update().clone();
        let mgs = ActivationMgs::new(&opctx.log, &self.resolver);
        let driver = ActivationDriver {
            task: self,
            opctx,
            inputs: PoweringOffInputs {
                mgs: &mgs,
                collection: collection.as_deref(),
            },
            now,
        };
        let (statuses, internal_errors) = drive_requests_with_timeout(
            &opctx.log,
            requests,
            &driver,
            &self.activators,
            &self.internal_errors,
        )
        .await;
        self.internal_errors = internal_errors;
        let requests = IdOrdMap::from_iter_unique(statuses).expect(
            "list_unfinished pages by id, so each request is listed once",
        );
        SledExpungeStatus::Listed { requests, mgs_discovery: mgs.status() }
    }

    async fn drive_request(
        &self,
        opctx: &OpContext,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let log = request_log(&opctx.log, request);
        let outcome =
            self.drive_steps(opctx, &log, inputs, request, now, progress).await;
        self.surface_internal_error(opctx, &log, request, outcome, progress)
            .await
    }

    // TODO-RAINCLAUDE: an error that should not resolve by retrying (anything but ServiceUnavailable) extends the request's streak, and once the streak reaches INTERNAL_ERROR_ATTENTION_THRESHOLD the request gets a generic last_error, written against the row as this activation last left it.
    async fn surface_internal_error(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        outcome: SledExpungeRequestOutcome,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let SledExpungeRequestOutcome::Error { error, last_error: None } =
            outcome
        else {
            return outcome;
        };
        let consecutive = match extend_internal_error_streak(
            progress.persistent_error,
            progress.internal_errors,
        ) {
            InternalErrorStreakStep::Unchanged => None,
            InternalErrorStreakStep::BelowThreshold { consecutive } => {
                progress.internal_errors = consecutive;
                info!(
                    log,
                    "not yet surfacing the error to the operator";
                    "consecutive_internal_errors" => consecutive,
                    "threshold" => INTERNAL_ERROR_ATTENTION_THRESHOLD,
                );
                None
            }
            InternalErrorStreakStep::Surface { consecutive } => {
                Some(consecutive)
            }
        };
        let Some(consecutive) = consecutive else {
            return SledExpungeRequestOutcome::Error {
                error,
                last_error: None,
            };
        };
        progress.internal_errors = consecutive;
        let latest =
            progress.latest_row.clone().unwrap_or_else(|| request.clone());
        let Some(state) = latest.state.as_unfinished() else {
            return SledExpungeRequestOutcome::Error {
                error,
                last_error: None,
            };
        };
        // TODO-RAINCLAUDE: when the last_error write itself does not land, the status error says so, so that it can't be mistaken for an error still below the threshold.
        match self
            .set_last_error(
                opctx,
                log,
                &latest,
                state,
                NeedsAttention::InternalError,
                progress,
            )
            .await
        {
            SledExpungeRequestOutcome::LastErrorSet { last_error }
            | SledExpungeRequestOutcome::LastErrorUnchanged { last_error } => {
                SledExpungeRequestOutcome::Error {
                    error,
                    last_error: Some(last_error),
                }
            }
            SledExpungeRequestOutcome::NotInExpectedState { current_state } => {
                SledExpungeRequestOutcome::Error {
                    error: format!(
                        "{error}; then not setting last_error: the request \
                         is now {current_state}"
                    ),
                    last_error: None,
                }
            }
            SledExpungeRequestOutcome::Error { error: write_error, .. } => {
                SledExpungeRequestOutcome::Error {
                    error: format!("{error}; then {write_error}"),
                    last_error: None,
                }
            }
            unexpected @ (SledExpungeRequestOutcome::RemovalPreparing
            | SledExpungeRequestOutcome::Completed { .. }
            | SledExpungeRequestOutcome::Failed { .. }
            | SledExpungeRequestOutcome::TimedOut { .. }) => {
                SledExpungeRequestOutcome::Error {
                    error: format!(
                        "{error}; then setting last_error returned \
                         {unexpected:?}"
                    ),
                    last_error: None,
                }
            }
        }
    }

    async fn drive_steps(
        &self,
        opctx: &OpContext,
        log: &Logger,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        match request.state.as_unfinished() {
            Some(SledExpungeRequestUnfinishedState::WaitingForTrustQuorum) => {
                self.drive_waiting_for_trust_quorum(
                    opctx, log, inputs, request, now, progress,
                )
                .await
            }
            Some(SledExpungeRequestUnfinishedState::PoweringOff) => {
                self.drive_powering_off(opctx, log, inputs, request, progress)
                    .await
            }
            Some(SledExpungeRequestUnfinishedState::Expunging) => {
                self.complete(opctx, log, request, progress).await
            }
            // TODO-RAINCLAUDE: list_unfinished filters on the unfinished states, so this is unreachable short of a bug there.
            None => error_outcome(
                log,
                progress,
                "unfinished request list returned a finished request",
                &Error::internal_error(&format!(
                    "listed as unfinished, but in state {}",
                    request.state,
                )),
            ),
        }
    }

    async fn drive_waiting_for_trust_quorum(
        &self,
        opctx: &OpContext,
        log: &Logger,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let from = SledExpungeRequestUnfinishedState::WaitingForTrustQuorum;
        progress.step = SledExpungeTaskStep::CheckingTrustQuorum;
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
                return self
                    .fail(
                        opctx,
                        log,
                        request,
                        from,
                        FailureReason::RemovalConfigMissing,
                        progress,
                    )
                    .await;
            }
            Ok(SledExpungeTaskGateOutcome::Inconsistent { message }) => {
                return self
                    .inconsistent(opctx, log, request, from, message, progress)
                    .await;
            }
            Err(error) => {
                return error_outcome(
                    log,
                    progress,
                    "checking the trust quorum gate",
                    &error,
                );
            }
        };
        match gate {
            SledExpungeTaskGate::SledRemoved => {
                progress.step = SledExpungeTaskStep::AdvancingToPoweringOff;
                let result = self
                    .datastore
                    .sled_expunge_request_advance(
                        opctx,
                        &authz_sled_expunge_request_from_id(request.id()),
                        SledExpungeRequestAdvance::ToPoweringOff,
                    )
                    .await;
                match require_updated(
                    log,
                    progress,
                    "advancing to powering_off",
                    result,
                ) {
                    // TODO-RAINCLAUDE: goes straight on to powering_off with the row the advance returned, as powering_off goes on to expunging, so a removal that is already past Preparing needs no second activation.
                    Ok(powering_off) => {
                        info!(
                            log,
                            "trust quorum removed the sled; powering it off"
                        );
                        self.drive_powering_off(
                            opctx,
                            log,
                            inputs,
                            &powering_off,
                            progress,
                        )
                        .await
                    }
                    Err(outcome) => outcome,
                }
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
                        NeedsAttention::RemovalStuckPreparing {
                            send: if request.send_uncertain {
                                RemovalSend::Uncertain
                            } else {
                                RemovalSend::Confirmed
                            },
                        },
                        progress,
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
                    progress,
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
                    progress,
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
                    progress,
                )
                .await
            }
        }
    }

    // TODO-RAINCLAUDE: RFD 747's powering_off step: decide() finds the sled's slot in the latest loaded inventory collection before any MGS is needed, and only a sled found in a sled slot is powered off there through the rack's MGSes; every other outcome waits with a last_error.
    async fn drive_powering_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::ResolvingPower;
        let decision = decide(request, inputs.collection);
        debug!(log, "decided where to power off the sled"; "decision" => ?decision);
        let slot = match decision {
            Ok(slot) => slot,
            Err(wait) => {
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        request,
                        inputs.collection,
                        wait,
                        progress,
                    )
                    .await;
            }
        };
        let clients =
            inputs.mgs.get().await.clients_for(RackUuid::from(request.rack_id));
        if clients.is_empty() {
            return self
                .wait_to_power_off(
                    opctx,
                    log,
                    request,
                    inputs.collection,
                    SledExpungePowerOffWait::NoMgs,
                    progress,
                )
                .await;
        }
        self.power_off(
            opctx,
            log,
            request,
            inputs.collection,
            PowerOffAt { clients, slot },
            progress,
        )
        .await
    }

    // TODO-RAINCLAUDE: MGS accepting the A2 command is the only proof of power-off, and then the request advances to expunging and is expunged in this same activation; if the slot holds another board or no MGS could power it off, it waits with a last_error.
    async fn power_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        collection: Option<&Collection>,
        at: PowerOffAt<'_>,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let PowerOffAt { clients, slot } = at;
        // TODO-RAINCLAUDE: the request is re-read once before the MGS loop so that a Nexus whose listing is stale does not power off a sled whose request another Nexus has already moved on (for example, one that failed and whose sled was then added back and powered on); the window between this read and the commands is accepted.
        let step = "re-reading the request before powering off";
        let current = match self
            .datastore
            .sled_expunge_request_get(opctx, request.id())
            .await
        {
            Ok(current) => current,
            Err(error) => return error_outcome(log, progress, step, &error),
        };
        match current.state {
            SledExpungeRequestState::PoweringOff => {}
            SledExpungeRequestState::WaitingForTrustQuorum
            | SledExpungeRequestState::Expunging
            | SledExpungeRequestState::Done
            | SledExpungeRequestState::Failed => {
                return not_in_expected_state_outcome(log, step, &current);
            }
        }
        progress.latest_row = Some(current.clone());

        progress.step = SledExpungeTaskStep::SendingPowerOff;
        let outcome =
            power_off_sled_at_slot(log, clients, &current.baseboard_id(), slot)
                .await;
        let wait = match outcome {
            PowerOffAtSlotOutcome::PoweredOff { mgs } => {
                progress.power_check =
                    Some(SledExpungePowerCheck::PoweredOff { slot, mgs });
                return self
                    .advance_to_expunging(opctx, log, &current, progress)
                    .await;
            }
            PowerOffAtSlotOutcome::SlotHoldsOtherBoard { mgs, found } => {
                SledExpungePowerOffWait::SlotHoldsOtherBoard {
                    slot,
                    mgs,
                    found,
                }
            }
            PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts } => {
                SledExpungePowerOffWait::NotPoweredOff {
                    slot,
                    failed_attempts: failed_attempts
                        .into_iter()
                        .map(|attempt| SledExpungeMgsPowerOffFailure {
                            mgs: attempt.mgs,
                            stage: attempt.failure,
                            error: attempt.error,
                        })
                        .collect(),
                }
            }
        };
        self.wait_to_power_off(opctx, log, &current, collection, wait, progress)
            .await
    }

    // TODO-RAINCLAUDE: records why the sled was not powered off, wakes inventory collection if a newer collection may resolve it, and sets the matching last_error.
    async fn wait_to_power_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        collection: Option<&Collection>,
        wait: SledExpungePowerOffWait,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        if wakes_inventory_collection(&wait, collection, request) {
            self.inventory_collection.activate();
        }
        progress.power_check =
            Some(SledExpungePowerCheck::Waiting { wait: wait.clone() });
        self.set_last_error(
            opctx,
            log,
            request,
            SledExpungeRequestUnfinishedState::PoweringOff,
            NeedsAttention::PowerOff(wait),
            progress,
        )
        .await
    }

    async fn advance_to_expunging(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::AdvancingToExpunging;
        let result = self
            .datastore
            .sled_expunge_request_advance(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                SledExpungeRequestAdvance::ToExpunging {
                    power_off_outcome: SledExpungePowerOffOutcome::PoweredOff,
                },
            )
            .await;
        let expunging = match require_updated(
            log,
            progress,
            "advancing to expunging",
            result,
        ) {
            Ok(expunging) => expunging,
            Err(outcome) => return outcome,
        };
        info!(log, "sled is powered off; expunging it now");
        self.complete(opctx, log, &expunging, progress).await
    }

    // TODO-RAINCLAUDE: RFD 747's expunging step: complete re-runs the task gate and expunges the sled in one transaction. It does not resolve the sled's identity again: power-on is out of scope for v1, and the request was only ever advanced after MGS accepted A2. ReAddPreparing and inconsistent records wait with a last_error, and a re-add that has started committing fails the request.
    async fn complete(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let from = SledExpungeRequestUnfinishedState::Expunging;
        progress.step = SledExpungeTaskStep::Completing;
        match self
            .datastore
            .sled_expunge_request_complete(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
            )
            .await
        {
            Ok(SledExpungeRequestCompleteOutcome::Completed {
                previous_sled_policy,
            }) => {
                info!(
                    log,
                    "expunged the sled; sled expunge request is done";
                    "previous_sled_policy" => ?previous_sled_policy,
                );
                // TODO-RAINCLAUDE: as the lockstep sled_expunge does: instance_watcher marks VMMs on the expunged sled Failed sooner, and the planner acts on the expungement.
                self.activators.activate();
                SledExpungeRequestOutcome::Completed { previous_sled_policy }
            }
            Ok(SledExpungeRequestCompleteOutcome::NotExpunging { current }) => {
                not_in_expected_state_outcome(
                    log,
                    "completing the request",
                    &current,
                )
            }
            Ok(SledExpungeRequestCompleteOutcome::ReAddPreparing { epoch }) => {
                self.set_last_error(
                    opctx,
                    log,
                    request,
                    from,
                    NeedsAttention::ReAddPreparing { re_add_epoch: epoch },
                    progress,
                )
                .await
            }
            Ok(SledExpungeRequestCompleteOutcome::ReAdded { epoch, state }) => {
                warn!(
                    log,
                    "the sled was added back to the trust quorum after its \
                     request entered expunging";
                    "re_add_epoch" => %epoch,
                    "re_add_state" => ?state,
                );
                self.fail(
                    opctx,
                    log,
                    request,
                    from,
                    FailureReason::ReAdded { re_add_epoch: epoch },
                    progress,
                )
                .await
            }
            Ok(SledExpungeRequestCompleteOutcome::Inconsistent { message }) => {
                self.inconsistent(opctx, log, request, from, message, progress)
                    .await
            }
            Err(error) => {
                error_outcome(log, progress, "completing the request", &error)
            }
        }
    }

    // TODO-RAINCLAUDE: the request's records contradict each other, which won't resolve by retrying: the internal detail goes in the status, and the operator gets a generic last_error.
    async fn inconsistent(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        expected_state: SledExpungeRequestUnfinishedState,
        message: String,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        error!(
            log,
            "sled expunge request's records are inconsistent";
            "error" => &message,
        );
        progress.inconsistent_records = Some(message);
        self.set_last_error(
            opctx,
            log,
            request,
            expected_state,
            NeedsAttention::InconsistentRecords,
            progress,
        )
        .await
    }

    async fn set_last_error(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        expected_state: SledExpungeRequestUnfinishedState,
        attention: NeedsAttention,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let last_error = attention.render(request);
        // TODO-RAINCLAUDE: skipping an unchanged message keeps time_modified meaningful; callers pass the freshest row this activation has (as listed, or as its last write returned it), so a concurrent writer can at worst cause one redundant write.
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
        progress.step = SledExpungeTaskStep::SettingLastError;
        let result = self
            .datastore
            .sled_expunge_request_set_last_error(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                expected_state,
                last_error.clone(),
            )
            .await;
        match require_updated(log, progress, "setting last_error", result) {
            Ok(_) => {
                warn!(
                    log,
                    "sled expunge request needs attention";
                    "last_error" => &last_error,
                );
                SledExpungeRequestOutcome::LastErrorSet { last_error }
            }
            Err(outcome) => outcome,
        }
    }

    async fn fail(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        from: SledExpungeRequestUnfinishedState,
        reason: FailureReason,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::Failing;
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
                    progress,
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
        match require_updated(log, progress, "failing the request", result) {
            Ok(_) => {
                warn!(
                    log,
                    "failed sled expunge request";
                    "failure_reason" => &failure_reason,
                );
                SledExpungeRequestOutcome::Failed { failure_reason }
            }
            Err(outcome) => outcome,
        }
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

fn request_log(log: &Logger, request: &SledExpungeRequest) -> Logger {
    log.new(o!(
        "sled_expunge_request_id" => request.id().to_string(),
        "sled_id" => request.sled_id.to_string(),
        "tq_epoch" => request.tq_epoch,
        "state" => request.state.to_string(),
    ))
}

// TODO-RAINCLAUDE: drives every request concurrently, at most MAX_CONCURRENT_REQUESTS at once, each bounded by PER_REQUEST_TIMEOUT, so a request stalled on one MGS (or anything else) delays the others only by holding one of the concurrency slots for at most that long; nothing bounds the activation as a whole beyond that. A timed-out request reports TimedOut in the step it had reached, with whatever progress it recorded, and its driving future is dropped; if it may have just completed (it timed out while advancing to expunging or completing), instance_watcher and blueprint_planner are activated anyway, since an extra activation is harmless and a missed one waits for their periodic activations.
async fn drive_requests_with_timeout<D: RequestDriver>(
    log: &Logger,
    requests: Vec<SledExpungeRequest>,
    driver: &D,
    activators: &SledExpungeActivators,
    internal_errors: &IdOrdMap<InternalErrorStreak>,
) -> (Vec<SledExpungeRequestStatus>, IdOrdMap<InternalErrorStreak>) {
    let driven: Vec<(SledExpungeRequestStatus, Option<InternalErrorStreak>)> =
        stream::iter(requests)
            .map(|request| async move {
                let mut progress = RequestProgress {
                    internal_errors: internal_errors
                        .get(&request.id())
                        .map_or(0, |streak| streak.consecutive),
                    ..RequestProgress::default()
                };
                let outcome = match tokio::time::timeout(
                    PER_REQUEST_TIMEOUT,
                    driver.drive(&request, &mut progress),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        error!(
                            request_log(log, &request),
                            "driving sled expunge request timed out; the next \
                             activation retries it";
                            "timeout" => ?PER_REQUEST_TIMEOUT,
                            "step" => %progress.step,
                        );
                        match progress.step {
                            SledExpungeTaskStep::AdvancingToExpunging
                            | SledExpungeTaskStep::Completing => {
                                activators.activate();
                            }
                            SledExpungeTaskStep::Starting
                            | SledExpungeTaskStep::CheckingTrustQuorum
                            | SledExpungeTaskStep::ResolvingPower
                            | SledExpungeTaskStep::SendingPowerOff
                            | SledExpungeTaskStep::AdvancingToPoweringOff
                            | SledExpungeTaskStep::SettingLastError
                            | SledExpungeTaskStep::Failing => {}
                        }
                        SledExpungeRequestOutcome::TimedOut {
                            step: progress.step,
                        }
                    }
                };
                let status = request_status(&request, outcome, progress);
                let streak =
                    (status.consecutive_internal_errors > 0).then(|| {
                        InternalErrorStreak {
                            request_id: request.id(),
                            consecutive: status.consecutive_internal_errors,
                        }
                    });
                (status, streak)
            })
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
    let mut statuses = Vec::with_capacity(driven.len());
    let mut streaks = IdOrdMap::new();
    for (status, streak) in driven {
        statuses.push(status);
        if let Some(streak) = streak {
            streaks.insert_overwrite(streak);
        }
    }
    (statuses, streaks)
}

fn request_status(
    request: &SledExpungeRequest,
    outcome: SledExpungeRequestOutcome,
    progress: RequestProgress,
) -> SledExpungeRequestStatus {
    let RequestProgress {
        step: _,
        latest_row,
        persistent_error: _,
        internal_errors,
        inconsistent_records,
        power_check,
    } = progress;
    let consecutive_internal_errors = match &outcome {
        SledExpungeRequestOutcome::Error { .. }
        | SledExpungeRequestOutcome::TimedOut { .. } => internal_errors,
        SledExpungeRequestOutcome::RemovalPreparing
        | SledExpungeRequestOutcome::LastErrorSet { .. }
        | SledExpungeRequestOutcome::LastErrorUnchanged { .. }
        | SledExpungeRequestOutcome::Completed { .. }
        | SledExpungeRequestOutcome::Failed { .. }
        | SledExpungeRequestOutcome::NotInExpectedState { .. } => 0,
    };
    let state = task_request_state(request.state);
    // TODO-RAINCLAUDE: Completed and Failed say where the request ended (complete writes done inside its own transaction, not through require_updated); otherwise the last row this activation wrote says, if it wrote one.
    let state_after = match &outcome {
        SledExpungeRequestOutcome::Completed { .. } => {
            Some(SledExpungeTaskRequestState::Done)
        }
        SledExpungeRequestOutcome::Failed { .. } => {
            Some(SledExpungeTaskRequestState::Failed)
        }
        SledExpungeRequestOutcome::RemovalPreparing
        | SledExpungeRequestOutcome::LastErrorSet { .. }
        | SledExpungeRequestOutcome::LastErrorUnchanged { .. }
        | SledExpungeRequestOutcome::NotInExpectedState { .. }
        | SledExpungeRequestOutcome::TimedOut { .. }
        | SledExpungeRequestOutcome::Error { .. } => {
            latest_row.map(|row| task_request_state(row.state))
        }
    }
    .filter(|after| *after != state);
    SledExpungeRequestStatus {
        request_id: request.id(),
        sled_id: SledUuid::from(request.sled_id),
        state,
        state_after,
        outcome,
        consecutive_internal_errors,
        inconsistent_records,
        power_check,
    }
}

// TODO-RAINCLAUDE: discovery failing outright leaves every request with no MGS for its rack, so each waits with the no-MGS last_error; the error itself goes in the status and log only, never in last_error.
async fn discover_gateways(
    log: &Logger,
    resolver: &internal_dns_resolver::Resolver,
) -> MgsDiscovery {
    match GatewaysByRack::resolve_all_gateways(log, resolver).await {
        Ok(gateways) => {
            for (gateway, error) in gateways.unknown() {
                warn!(
                    log,
                    "could not discover an MGS's rack; requests do not power \
                     off through it";
                    "mgs_addr" => %gateway.addr,
                    InlineErrorChain::new(error),
                );
            }
            MgsDiscovery::Discovered(gateways)
        }
        Err(error) => {
            let error = format!("{error:#}");
            error!(
                log,
                "failed to discover any MGS; requests see no MGS for their \
                 rack";
                "error" => &error,
            );
            MgsDiscovery::Failed(error)
        }
    }
}

// TODO-RAINCLAUDE: the slot the latest loaded inventory collection puts the request's sled in, or why it can't be powered off there. The collection can be a loader period or more stale, which power_off_sled_at_slot's identity re-check makes safe. An SP that is not a sled's, or one in another rack, should not happen for a request's sled, so those wait for support.
fn decide(
    request: &SledExpungeRequest,
    collection: Option<&Collection>,
) -> Result<u16, SledExpungePowerOffWait> {
    let Some(collection) = collection else {
        return Err(SledExpungePowerOffWait::NoInventory);
    };
    let Some(sp) = collection.sps.get(&request.baseboard_id()) else {
        return Err(SledExpungePowerOffWait::NotInInventory);
    };
    match sp.sp_type {
        SpType::Sled => {}
        SpType::Switch | SpType::Power => {
            return Err(SledExpungePowerOffWait::NotASled {
                sp_type: sp.sp_type,
            });
        }
    }
    if sp.rack_id != RackUuid::from(request.rack_id) {
        return Err(SledExpungePowerOffWait::InAnotherRack {
            rack_id: sp.rack_id,
        });
    }
    if SWITCH_ATTACHED_SLED_CUBBIES.contains(&sp.sp_slot) {
        return Err(SledExpungePowerOffWait::SwitchAttachedCubby {
            slot: sp.sp_slot,
        });
    }
    Ok(sp.sp_slot)
}

// TODO-RAINCLAUDE: whether a wait should wake inventory collection: only for the waits a newer collection may resolve (no collection, sled not found, its slot holding another board, or an MGS that could not read the slot's identity), and only if the loaded collection started before the request's time_modified, which marks when its current wait began (set_last_error leaves an unchanged last_error alone), so each wait wakes collection at most until a collection taken during it is loaded.
fn wakes_inventory_collection(
    wait: &SledExpungePowerOffWait,
    collection: Option<&Collection>,
    request: &SledExpungeRequest,
) -> bool {
    let newer_collection_may_help = match wait {
        SledExpungePowerOffWait::NoInventory
        | SledExpungePowerOffWait::NotInInventory
        | SledExpungePowerOffWait::SlotHoldsOtherBoard { .. } => true,
        SledExpungePowerOffWait::NotPoweredOff { failed_attempts, .. } => {
            failed_attempts.iter().any(|attempt| match attempt.stage {
                MgsPowerOffFailure::RecheckFailed => true,
                MgsPowerOffFailure::PowerOffFailed => false,
            })
        }
        SledExpungePowerOffWait::NotASled { .. }
        | SledExpungePowerOffWait::InAnotherRack { .. }
        | SledExpungePowerOffWait::SwitchAttachedCubby { .. }
        | SledExpungePowerOffWait::NoMgs => false,
    };
    newer_collection_may_help
        && collection.is_none_or(|collection| {
            collection.time_started < request.time_modified
        })
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

// TODO-RAINCLAUDE: maps one compare-and-swap's result: the updated row on success, else the outcome to report: a lost compare-and-swap (another Nexus moved the request; the next activation sees its new state), or an error, logged.
fn require_updated(
    log: &Logger,
    progress: &mut RequestProgress,
    step: &str,
    result: Result<SledExpungeRequestUpdateOutcome, Error>,
) -> Result<SledExpungeRequest, SledExpungeRequestOutcome> {
    match result {
        Ok(SledExpungeRequestUpdateOutcome::Updated { request }) => {
            progress.latest_row = Some(request.clone());
            Ok(request)
        }
        Ok(SledExpungeRequestUpdateOutcome::NotInExpectedState { current }) => {
            Err(not_in_expected_state_outcome(log, step, &current))
        }
        Err(error) => Err(error_outcome(log, progress, step, &error)),
    }
}

fn not_in_expected_state_outcome(
    log: &Logger,
    step: &str,
    current: &SledExpungeRequest,
) -> SledExpungeRequestOutcome {
    info!(
        log,
        "sled expunge request changed concurrently; leaving it to the next \
         activation";
        "step" => step,
        "current_state" => current.state.to_string(),
    );
    SledExpungeRequestOutcome::NotInExpectedState {
        current_state: task_request_state(current.state),
    }
}

// TODO-RAINCLAUDE: a retryable error (Error::retryable: only ServiceUnavailable, the database or another service briefly unreachable) stays in the status only; every other error kind should not resolve by retrying, so surface_internal_error counts it toward a generic last_error.
fn error_outcome(
    log: &Logger,
    progress: &mut RequestProgress,
    step: &str,
    error: &Error,
) -> SledExpungeRequestOutcome {
    if error.retryable() {
        warn!(log, "sled expunge request step failed"; "step" => step, error);
    } else {
        error!(log, "sled expunge request step failed"; "step" => step, error);
    }
    progress.persistent_error = !error.retryable();
    SledExpungeRequestOutcome::Error {
        error: format!("{step}: {}", InlineErrorChain::new(error)),
        last_error: None,
    }
}

// TODO-RAINCLAUDE: whether creation settled the removal's send (send_uncertain clear): the coordinator accepted it, or its coordinator was not in service so nothing was sent.
#[derive(Clone, Copy, Debug)]
enum RemovalSend {
    Confirmed,
    Uncertain,
}

// TODO-RAINCLAUDE: why a request needs attention, carrying its typed reason; this module is the only writer of last_error, so a module-local enum is enough, and an OperatorMessage newtype in nexus-types would only pay off once another crate writes these columns.
#[derive(Clone, Debug)]
enum NeedsAttention {
    RemovalStuckPreparing { send: RemovalSend },
    ReAddPreparing { re_add_epoch: Epoch },
    InconsistentRecords,
    InternalError,
    PowerOff(SledExpungePowerOffWait),
}

const KEEPS_RETRYING: &str =
    "Nexus will keep retrying; if this persists, contact Oxide support.";

impl NeedsAttention {
    // TODO-RAINCLAUDE: last_error is shown to operators as is, so it is rendered here, in one place, and the rendering takes only epochs, ids, cubby (slot) numbers and switch numbers from the reason; no database, MGS, or internal error text, and no MGS address, reaches it.
    fn render(&self, request: &SledExpungeRequest) -> String {
        let rack_id = request.rack_id;
        let tq_epoch = request.tq_epoch;
        match self {
            // TODO-RAINCLAUDE: send_uncertain means creation could not confirm the change reached its coordinator, which only support can tell apart from a coordinator that is stuck, so that text gives no abort steps.
            NeedsAttention::RemovalStuckPreparing {
                send: RemovalSend::Uncertain,
            } => {
                format!(
                    "The rack membership change that removes this sled \
                     (version {tq_epoch}) has stayed preparing for more than \
                     {} minutes, and when this request was created Nexus \
                     could not confirm that the change reached the sled \
                     coordinating it, so the change may never have started. \
                     Contact Oxide support.",
                    REMOVAL_PREPARING_ATTENTION_THRESHOLD.num_minutes(),
                )
            }
            NeedsAttention::RemovalStuckPreparing {
                send: RemovalSend::Confirmed,
            } => format!(
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
                "This request's records are inconsistent, so it cannot \
                 continue. Contact Oxide support."
                    .to_string()
            }
            NeedsAttention::InternalError => format!(
                "Nexus hit an internal error while driving this request. \
                 {KEEPS_RETRYING}"
            ),
            NeedsAttention::PowerOff(wait) => power_off_wait_text(wait),
        }
    }
}

fn power_off_wait_text(wait: &SledExpungePowerOffWait) -> String {
    match wait {
        SledExpungePowerOffWait::SwitchAttachedCubby { slot } => format!(
            "This sled was found in switch-attached cubby {slot}, so it is a \
             scrimlet (it manages a switch), and Nexus will not power it off. \
             Expunging a scrimlet through this API is not supported. Contact \
             Oxide support."
        ),
        SledExpungePowerOffWait::NoMgs => format!(
            "Nexus could not find the management gateway of either switch in \
             this sled's rack, so it cannot find the sled. {KEEPS_RETRYING}"
        ),
        SledExpungePowerOffWait::NoInventory => format!(
            "Nexus has not loaded an inventory collection yet, so it cannot \
             tell which cubby this sled is in. {KEEPS_RETRYING}"
        ),
        SledExpungePowerOffWait::NotASled { .. } => "The latest inventory \
            collection reports this sled's baseboard on a service processor \
            that is not a sled's, so Nexus will not power it off. Contact \
            Oxide support."
            .to_string(),
        SledExpungePowerOffWait::InAnotherRack { rack_id } => format!(
            "The latest inventory collection found this sled in rack \
             {rack_id}, not in this request's rack, so Nexus will not power \
             it off. Contact Oxide support."
        ),
        SledExpungePowerOffWait::NotInInventory => format!(
            "This sled was not found in the latest inventory collection, so \
             Nexus cannot tell which cubby it is in. Requests for sleds that \
             have been physically removed are not handled yet. \
             {KEEPS_RETRYING}"
        ),
        SledExpungePowerOffWait::SlotHoldsOtherBoard { slot, .. } => format!(
            "Cubby {slot}, where the latest inventory collection found this \
             sled, now holds another board, so Nexus did not send a power-off \
             to it. {KEEPS_RETRYING}"
        ),
        SledExpungePowerOffWait::NotPoweredOff { slot, failed_attempts } => {
            format!(
                "Nexus could not power off this sled in cubby {slot} ({}). \
                 {KEEPS_RETRYING}",
                failed_attempts
                    .iter()
                    .map(|attempt| power_off_failure_text(&attempt.stage))
                    .collect::<BTreeSet<_>>()
                    .iter()
                    .join("; "),
            )
        }
    }
}

fn power_off_failure_text(failure: &MgsPowerOffFailure) -> &'static str {
    match failure {
        MgsPowerOffFailure::RecheckFailed => {
            "a management gateway could not check the sled's service \
             processor's identity"
        }
        MgsPowerOffFailure::PowerOffFailed => {
            "a management gateway did not confirm the power-off command"
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
    // TODO-RAINCLAUDE: failure_reason is shown to operators as is, so like last_error it is rendered only here, from epochs, ids and policies. sled_policy is None when the sled's row could not be found. A re-add can fail a request in expunging, after the sled was powered off, so that text says so, from the row's power_off_outcome.
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
            FailureReason::ReAdded { re_add_epoch } => {
                let power_off = match request.power_off_outcome {
                    None => "",
                    Some(SledExpungePowerOffOutcome::PoweredOff) => {
                        " Nexus had powered the sled off."
                    }
                };
                format!(
                    "The sled was added back to the rack (rack membership \
                     version {re_add_epoch}) after its removal at version \
                     {tq_epoch}, so this request did not expunge \
                     it.{power_off} Contact Oxide support."
                )
            }
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
    use gateway_types::component::PowerState;
    use gateway_types::component::SpType;
    use nexus_auth::authz;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_queries::db::datastore::TqPrepareStatusOutcome;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::helpers::SledUpdateBuilder;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_expect_written;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::set_sled_expunge_request_state;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::identity::Asset;
    use nexus_types::inventory::ServiceProcessor;
    use nexus_types::trust_quorum::IsLrtqUpgrade;
    use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
    use nexus_types::trust_quorum::TrustQuorumConfig;
    use nexus_types::trust_quorum::TrustQuorumConfigState;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledExpungeRequestUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
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
    const INCONSISTENT_RECORDS_TEXT: &str = "This request's records are \
        inconsistent, so it cannot continue. Contact Oxide support.";
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
        request_status(request, outcome, RequestProgress::default())
    }

    const NO_INVENTORY_TEXT: &str = "Nexus has not loaded an inventory \
        collection yet, so it cannot tell which cubby this sled is in. Nexus \
        will keep retrying; if this persists, contact Oxide support.";

    // TODO-RAINCLAUDE: a waiting request whose removal has committed, in a database-only test: it advances and goes on to powering_off, where it waits for an inventory collection.
    fn chained_no_inventory_status(
        request: &SledExpungeRequest,
    ) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            state_after: Some(SledExpungeTaskRequestState::PoweringOff),
            ..no_inventory_status(request)
        }
    }

    // TODO-RAINCLAUDE: a powering_off request in a database-only test, where no inventory collection is loaded, so the request waits with a last_error before any MGS is needed.
    fn no_inventory_status(
        request: &SledExpungeRequest,
    ) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            power_check: Some(SledExpungePowerCheck::Waiting {
                wait: SledExpungePowerOffWait::NoInventory,
            }),
            ..status_for(
                request,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: NO_INVENTORY_TEXT.to_string(),
                },
            )
        }
    }

    fn no_dns_servers_resolver(
        db: &TestDatabase,
    ) -> internal_dns_resolver::Resolver {
        internal_dns_resolver::Resolver::new_from_addrs(
            db.opctx().log.clone(),
            &[],
        )
        .expect("created a DNS resolver with no servers")
    }

    // TODO-RAINCLAUDE: a task with no DNS servers, so MGS discovery fails, and no inventory collection loaded, so a powering_off request waits for one; its activators are wired up, and handles to them are returned so tests can check them.
    fn test_task(db: &TestDatabase) -> (SledExpunge, SledExpungeActivators) {
        let activators = test_activators();
        let task = SledExpunge::new(
            db.datastore().clone(),
            no_dns_servers_resolver(db),
            watch::channel(None).1,
            wired_up_activator(),
            activators.clone(),
        );
        (task, activators)
    }

    fn wired_up_activator() -> Activator {
        let activator = Activator::new();
        activator.mark_wired_up().expect("a new activator is not wired up");
        activator
    }

    fn test_activators() -> SledExpungeActivators {
        SledExpungeActivators {
            instance_watcher: wired_up_activator(),
            blueprint_planner: wired_up_activator(),
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

    fn statuses<I>(entries: I) -> IdOrdMap<SledExpungeRequestStatus>
    where
        I: IntoIterator<Item = SledExpungeRequestStatus>,
    {
        IdOrdMap::from_iter_unique(entries)
            .expect("expected entries are unique")
    }

    fn listed(entries: Vec<SledExpungeRequestStatus>) -> SledExpungeStatus {
        SledExpungeStatus::Listed {
            requests: statuses(entries),
            mgs_discovery: SledExpungeMgsDiscovery::NotNeeded,
        }
    }

    // TODO-RAINCLAUDE: drives one request directly, as an activation would, with the task's own lazy MGS discovery.
    async fn drive_one(
        task: &SledExpunge,
        opctx: &OpContext,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
    ) -> SledExpungeRequestStatus {
        let mgs = ActivationMgs::new(&opctx.log, &task.resolver);
        let mut progress = RequestProgress::default();
        let outcome = task
            .drive_request(
                opctx,
                PoweringOffInputs { mgs: &mgs, collection: None },
                request,
                now,
                &mut progress,
            )
            .await;
        request_status(request, outcome, progress)
    }

    #[tokio::test]
    async fn test_sled_expunge_stuck_preparing_sets_last_error() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_stuck_preparing_sets_last_error",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (mut task, _) = test_task(&db);
        let rack = TestRack::new(&db).await;
        rack.propose_removal(&db).await;
        let inserted = rack
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        // TODO-RAINCLAUDE: creation confirmed the removal's send, so the message gives the abort-and-resubmit steps; the send_uncertain message is checked in test_sled_expunge_power_off_texts.
        db.datastore()
            .sled_expunge_request_clear_send_uncertain(
                db.opctx(),
                &authz_sled_expunge_request_from_id(inserted.id()),
            )
            .await
            .expect("cleared send_uncertain");
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
        let (mut task, activators) = test_task(&db);
        let mut expected = Vec::new();

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
        expected.push(chained_no_inventory_status(&removed_request));

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
        expected.push(aborted.status(
            &aborted_request,
            SledExpungeRequestOutcome::Failed {
                failure_reason: aborted.removal_aborted_text(),
            },
        ));

        // TODO-RAINCLAUDE: no config exists at epoch 9.
        let missing_config = TestRack::new(&db).await;
        let missing_config_request = missing_config
            .insert_request(
                &db,
                9,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        expected.push(missing_config.status(
            &missing_config_request,
            SledExpungeRequestOutcome::Failed {
                failure_reason: format!(
                    "This request's rack membership change (version 9) could \
                     not be found, so the sled was neither powered off nor \
                     expunged. Contact Oxide support. {}",
                    missing_config.non_provisionable_text(),
                ),
            },
        ));

        let preparing = TestRack::new(&db).await;
        preparing.propose_removal(&db).await;
        let preparing_request = preparing
            .insert_request(
                &db,
                2,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        expected.push(preparing.status(
            &preparing_request,
            SledExpungeRequestOutcome::RemovalPreparing,
        ));

        // TODO-RAINCLAUDE: check_no_new_member_being_expunged_conn refuses re-adding a sled with an unfinished request, so each re-add is proposed before the request row exists; the task's re-add handling is defensive. A re-add still preparing waits with a last_error, and one that has started committing fails the request, whose failure reason says whether Nexus had powered the sled off (it had, for a request in expunging).
        for (state, committing_power_off_sentence) in [
            (SledExpungeRequestState::WaitingForTrustQuorum, None),
            (SledExpungeRequestState::WaitingForTrustQuorum, Some("")),
            (SledExpungeRequestState::Expunging, None),
            (
                SledExpungeRequestState::Expunging,
                Some(" Nexus had powered the sled off."),
            ),
        ] {
            let rack = TestRack::new(&db).await;
            rack.propose_removal(&db).await;
            rack.commit(&db, 2).await;
            rack.propose(&db, 3, 2, rack.members.clone()).await;
            let request = rack.insert_request(&db, 2, state).await;
            let outcome = match committing_power_off_sentence {
                None => SledExpungeRequestOutcome::LastErrorSet {
                    last_error: rack.re_add_preparing_text(),
                },
                Some(sentence) => {
                    rack.start_committing(&db, 3).await;
                    SledExpungeRequestOutcome::Failed {
                        failure_reason: re_added_text(
                            sentence,
                            &non_provisionable_policy_sentence(rack.sled_id),
                        ),
                    }
                }
            };
            expected.push(rack.status(&request, outcome));
        }

        let powering_off = TestRack::new(&db).await;
        powering_off.propose_removal(&db).await;
        powering_off.commit(&db, 2).await;
        let powering_off_request = powering_off
            .insert_request(&db, 2, SledExpungeRequestState::PoweringOff)
            .await;
        expected.push(no_inventory_status(&powering_off_request));

        let expunging = TestRack::new(&db).await;
        expunging.propose_removal(&db).await;
        expunging.commit(&db, 2).await;
        let expunging_request = expunging
            .insert_request(&db, 2, SledExpungeRequestState::Expunging)
            .await;
        expected.push(expunging.status(
            &expunging_request,
            SledExpungeRequestOutcome::Completed {
                previous_sled_policy: SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::NonProvisionable,
                },
            },
        ));

        // TODO-RAINCLAUDE: the request points at epoch 1, the committed RSS config that still includes the sled, which the gate reports as inconsistent records.
        let inconsistent = TestRack::new(&db).await;
        let inconsistent_request = inconsistent
            .insert_request(
                &db,
                1,
                SledExpungeRequestState::WaitingForTrustQuorum,
            )
            .await;
        expected.push(SledExpungeRequestStatus {
            inconsistent_records: Some(format!(
                "sled expunge request {} for {} (rack {}, removal epoch 1): \
                 the removal configuration (epoch 1, state Committed) still \
                 includes the sled",
                inconsistent_request.id,
                inconsistent.target,
                inconsistent.rack_id,
            )),
            ..inconsistent.status(
                &inconsistent_request,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: INCONSISTENT_RECORDS_TEXT.to_string(),
                },
            )
        });

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
        expected.push(missing_sled.status(
            &missing_sled_request,
            SledExpungeRequestOutcome::Failed {
                failure_reason: format!(
                    "{REMOVAL_ABORTED_TEXT} {POLICY_UNREADABLE_TEXT}"
                ),
            },
        ));

        assert_eq!(
            task.actually_activate(db.opctx(), now_db_precision()).await,
            listed(expected),
        );
        activators.instance_watcher.assert_activated(
            "completing the expunging request activates instance_watcher",
        );
        activators.blueprint_planner.assert_activated(
            "completing the expunging request activates blueprint_planner",
        );
        assert_eq!(read(&db, &preparing_request).await, preparing_request);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: the request is handed over as another Nexus listed it, after this one already moved it on, so the advance's compare-and-swap loses. Then power_off re-reads the row before any MGS call, so for a request that has since failed it reaches no MGS: its status has no power check (the unreachable MGS here would have recorded one before the last_error write found the request failed).
    #[tokio::test]
    async fn test_sled_expunge_stale_row_is_not_in_expected_state() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_stale_row_is_not_in_expected_state",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &db.opctx().log;

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
        assert_eq!(
            drive_one(&task, db.opctx(), &stale, now_db_precision()).await,
            rack.status(
                &stale,
                SledExpungeRequestOutcome::NotInExpectedState {
                    current_state: SledExpungeTaskRequestState::PoweringOff,
                },
            ),
        );

        let powering_off = read(&db, &stale).await;
        set_sled_expunge_request_state(
            db.datastore(),
            &powering_off,
            SledExpungeRequestState::Failed,
        )
        .await;
        let unreachable = GatewayClient::from_addr(
            log,
            "[::1]:1".parse().expect("parsed an MGS address"),
        );
        let mut progress = RequestProgress::default();
        let outcome = task
            .power_off(
                db.opctx(),
                log,
                &powering_off,
                None,
                PowerOffAt {
                    clients: std::slice::from_ref(&unreachable),
                    slot: 3,
                },
                &mut progress,
            )
            .await;
        assert_eq!(
            request_status(&powering_off, outcome, progress),
            rack.status(
                &powering_off,
                SledExpungeRequestOutcome::NotInExpectedState {
                    current_state: SledExpungeTaskRequestState::Failed,
                },
            ),
        );

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

    #[test]
    fn test_sled_expunge_power_off_texts() {
        let request = test_request();
        assert_eq!(
            NeedsAttention::PowerOff(
                SledExpungePowerOffWait::SwitchAttachedCubby { slot: 14 }
            )
            .render(&request),
            switch_attached_cubby_text(14),
        );
        assert_eq!(
            NeedsAttention::InternalError.render(&request),
            INTERNAL_ERROR_TEXT,
        );
        assert_eq!(
            NeedsAttention::RemovalStuckPreparing {
                send: RemovalSend::Uncertain
            }
            .render(&request),
            "The rack membership change that removes this sled (version 2) \
             has stayed preparing for more than 10 minutes, and when this \
             request was created Nexus could not confirm that the change \
             reached the sled coordinating it, so the change may never have \
             started. Contact Oxide support.",
        );
        assert_eq!(
            NeedsAttention::PowerOff(SledExpungePowerOffWait::NotPoweredOff {
                slot: 3,
                failed_attempts: [
                    ("[::1]:1", MgsPowerOffFailure::PowerOffFailed),
                    ("[::1]:2", MgsPowerOffFailure::RecheckFailed),
                    ("[::1]:3", MgsPowerOffFailure::PowerOffFailed),
                ]
                .into_iter()
                .map(|(mgs, stage)| SledExpungeMgsPowerOffFailure {
                    mgs: mgs.parse().expect("parsed an MGS address"),
                    stage,
                    error: "error".to_string(),
                })
                .collect(),
            })
            .render(&request),
            format!(
                "Nexus could not power off this sled in cubby 3 (a \
                 management gateway could not check the sled's service \
                 processor's identity; a management gateway did not confirm \
                 the power-off command). {KEEPS_RETRYING}"
            ),
        );
        // TODO-RAINCLAUDE: a re-add fails a request in expunging after power-off, which the failure reason states; in waiting_for_trust_quorum there is no power-off outcome, so there is no such sentence.
        let policy = Some(SledPolicy::Expunged);
        let re_added = FailureReason::ReAdded { re_add_epoch: Epoch(3) };
        let cases = [
            (None, ""),
            (
                Some(SledExpungePowerOffOutcome::PoweredOff),
                " Nexus had powered the sled off.",
            ),
        ];
        for (power_off_outcome, sentence) in cases {
            let request =
                SledExpungeRequest { power_off_outcome, ..request.clone() };
            assert_eq!(
                re_added.render(&request, policy),
                re_added_text(sentence, "the sled was expunged."),
            );
        }
    }

    // TODO-RAINCLAUDE: decide's outcome for each way the latest collection can place, or fail to place, the request's sled.
    #[test]
    fn test_sled_expunge_decide() {
        let request = test_request();
        let rack_id = RackUuid::from(request.rack_id);
        let other_rack = RackUuid::new_v4();
        let sp = |sp_type, sp_slot, rack_id| ServiceProcessor {
            time_collected: now_db_precision(),
            source: "test".to_string(),
            sp_type,
            sp_slot,
            baseboard_revision: 0,
            hubris_archive: "test".to_string(),
            power_state: PowerState::A0,
            rack_id,
        };
        assert_eq!(
            decide(&request, None),
            Err(SledExpungePowerOffWait::NoInventory)
        );
        let cases = [
            (None, Err(SledExpungePowerOffWait::NotInInventory)),
            (
                Some(sp(SpType::Switch, 0, rack_id)),
                Err(SledExpungePowerOffWait::NotASled {
                    sp_type: SpType::Switch,
                }),
            ),
            (
                Some(sp(SpType::Sled, 3, other_rack)),
                Err(SledExpungePowerOffWait::InAnotherRack {
                    rack_id: other_rack,
                }),
            ),
            (
                Some(sp(SpType::Sled, 14, rack_id)),
                Err(SledExpungePowerOffWait::SwitchAttachedCubby { slot: 14 }),
            ),
            (Some(sp(SpType::Sled, 3, rack_id)), Ok(3)),
        ];
        for (sp, expected) in cases {
            let mut collection = CollectionBuilder::new("test").build();
            if let Some(sp) = sp.clone() {
                collection.sps.insert(Arc::new(request.baseboard_id()), sp);
            }
            assert_eq!(decide(&request, Some(&collection)), expected, "{sp:?}");
        }
    }

    const INTERNAL_ERROR_TEXT: &str = "Nexus hit an internal error while \
        driving this request. Nexus will keep retrying; if this persists, \
        contact Oxide support.";

    fn re_added_text(power_off_sentence: &str, policy: &str) -> String {
        format!(
            "The sled was added back to the rack (rack membership version 3) \
             after its removal at version 2, so this request did not expunge \
             it.{power_off_sentence} Contact Oxide support. At the time this \
             request failed, {policy}"
        )
    }

    // TODO-RAINCLAUDE: never answers for the stalled requests, which first record some progress, as if they had powered the sled off and stalled in stall_step; the others answer at once, recording when.
    struct StallingDriver {
        stalled: BTreeSet<SledExpungeRequestUuid>,
        stall_step: SledExpungeTaskStep,
        finished: std::sync::Mutex<
            Vec<(SledExpungeRequestUuid, tokio::time::Instant)>,
        >,
    }

    impl StallingDriver {
        fn new<'r, I>(stalled: I, stall_step: SledExpungeTaskStep) -> Self
        where
            I: IntoIterator<Item = &'r SledExpungeRequest>,
        {
            Self {
                stalled: stalled
                    .into_iter()
                    .map(|request| request.id())
                    .collect(),
                stall_step,
                finished: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    fn stalled_power_check() -> SledExpungePowerCheck {
        SledExpungePowerCheck::PoweredOff {
            slot: 3,
            mgs: "[::1]:12225".parse().expect("parsed an MGS address"),
        }
    }

    impl RequestDriver for StallingDriver {
        async fn drive(
            &self,
            request: &SledExpungeRequest,
            progress: &mut RequestProgress,
        ) -> SledExpungeRequestOutcome {
            if self.stalled.contains(&request.id()) {
                progress.step = self.stall_step;
                progress.power_check = Some(stalled_power_check());
                std::future::pending::<()>().await;
            }
            self.finished
                .lock()
                .expect("the finished list is not poisoned")
                .push((request.id(), tokio::time::Instant::now()));
            SledExpungeRequestOutcome::RemovalPreparing
        }
    }

    // TODO-RAINCLAUDE: paused time makes the per-request timeout deterministic: tokio advances the clock only when every future is idle, so the stalled request times out at exactly PER_REQUEST_TIMEOUT while the healthy one finishes at once. A timed-out request keeps the step and progress it had reached.
    #[tokio::test(start_paused = true)]
    async fn test_sled_expunge_stalled_request_does_not_block_others() {
        let log = slog::Logger::root(slog::Discard, o!());
        let stalled = test_request();
        let healthy = test_request();
        let driver =
            StallingDriver::new([&stalled], SledExpungeTaskStep::Completing);
        let started = tokio::time::Instant::now();

        let (statuses_out, internal_errors) = drive_requests_with_timeout(
            &log,
            vec![stalled.clone(), healthy.clone()],
            &driver,
            &test_activators(),
            &IdOrdMap::new(),
        )
        .await;
        assert_eq!(internal_errors, IdOrdMap::new());
        assert_eq!(
            driver
                .finished
                .into_inner()
                .expect("the finished list is not poisoned"),
            vec![(healthy.id(), started)],
        );
        assert_eq!(tokio::time::Instant::now() - started, PER_REQUEST_TIMEOUT);
        assert_eq!(
            statuses(statuses_out),
            statuses([
                SledExpungeRequestStatus {
                    power_check: Some(stalled_power_check()),
                    ..status_for(
                        &stalled,
                        SledExpungeRequestOutcome::TimedOut {
                            step: SledExpungeTaskStep::Completing,
                        },
                    )
                },
                status_for(
                    &healthy,
                    SledExpungeRequestOutcome::RemovalPreparing,
                ),
            ]),
        );
    }

    // TODO-RAINCLAUDE: a request that times out after it may have entered expunging activates instance_watcher and blueprint_planner, and one that times out sending the power-off command does not, since it cannot have completed; either way its internal-error streak is kept as it was.
    #[tokio::test(start_paused = true)]
    async fn test_sled_expunge_timeout_activates_by_step() {
        let log = slog::Logger::root(slog::Discard, o!());
        for (step, activates) in [
            (SledExpungeTaskStep::AdvancingToExpunging, true),
            (SledExpungeTaskStep::SendingPowerOff, false),
        ] {
            let stalled = test_request();
            let driver = StallingDriver::new([&stalled], step);
            let activators = test_activators();
            let streak = InternalErrorStreak {
                request_id: stalled.id(),
                consecutive: 2,
            };
            let earlier = IdOrdMap::from_iter_unique([streak.clone()])
                .expect("one streak");

            let (statuses_out, internal_errors) = drive_requests_with_timeout(
                &log,
                vec![stalled.clone()],
                &driver,
                &activators,
                &earlier,
            )
            .await;

            assert_eq!(
                statuses(statuses_out),
                statuses([SledExpungeRequestStatus {
                    consecutive_internal_errors: 2,
                    power_check: Some(stalled_power_check()),
                    ..status_for(
                        &stalled,
                        SledExpungeRequestOutcome::TimedOut { step },
                    )
                }]),
                "timed out in {step}",
            );
            assert_eq!(internal_errors, earlier, "timed out in {step}");
            if activates {
                activators.instance_watcher.assert_activated(&format!(
                    "timing out in {step} activates instance_watcher"
                ));
                activators.blueprint_planner.assert_activated(&format!(
                    "timing out in {step} activates blueprint_planner"
                ));
            } else {
                activators.instance_watcher.assert_not_activated(&format!(
                    "timing out in {step} does not activate instance_watcher"
                ));
                activators.blueprint_planner.assert_not_activated(&format!(
                    "timing out in {step} does not activate \
                     blueprint_planner"
                ));
            }
        }
    }

    fn test_request() -> SledExpungeRequest {
        SledExpungeRequest::new(
            RackUuid::new_v4(),
            SledUuid::new_v4(),
            BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            },
            2,
        )
    }

    fn switch_attached_cubby_text(slot: u16) -> String {
        format!(
            "This sled was found in switch-attached cubby {slot}, so it is a \
             scrimlet (it manages a switch), and Nexus will not power it off. \
             Expunging a scrimlet through this API is not supported. Contact \
             Oxide support."
        )
    }

    fn non_provisionable_policy_sentence(sled_id: SledUuid) -> String {
        format!(
            "the sled's provision policy was non-provisionable (creating an \
             expunge request makes the sled non-provisionable, and failing it \
             does not undo that). If you want the sled to host new resources \
             again, set its provision policy to provisionable with \
             sled_set_provision_policy (sled {sled_id})."
        )
    }

    // TODO-RAINCLAUDE: sets the sled row's state directly, bypassing the transition checks, to make the expunge in complete fail with an error that should not resolve by retrying.
    async fn set_sled_state(
        db: &TestDatabase,
        sled_id: SledUuid,
        state: nexus_db_model::SledState,
    ) {
        use nexus_db_schema::schema::sled::dsl;
        let conn = db
            .datastore()
            .pool_connection_for_tests()
            .await
            .expect("got a database connection");
        diesel::update(dsl::sled)
            .filter(dsl::id.eq(sled_id.into_untyped_uuid()))
            .set(dsl::sled_state.eq(state))
            .execute_async(&*conn)
            .await
            .expect("set the sled's state");
    }

    async fn complete_error(
        db: &TestDatabase,
        request: &SledExpungeRequest,
    ) -> String {
        let error = db
            .datastore()
            .sled_expunge_request_complete(
                db.opctx(),
                &authz_sled_expunge_request_from_id(request.id()),
            )
            .await
            .expect_err("a decommissioned sled refuses the expunge");
        format!("completing the request: {}", InlineErrorChain::new(&error))
    }

    #[test]
    fn test_sled_expunge_extend_internal_error_streak() {
        let threshold = INTERNAL_ERROR_ATTENTION_THRESHOLD;
        for (persistent_error, consecutive, expected) in [
            (false, threshold - 1, InternalErrorStreakStep::Unchanged),
            (
                true,
                threshold - 2,
                InternalErrorStreakStep::BelowThreshold {
                    consecutive: threshold - 1,
                },
            ),
            (
                true,
                threshold - 1,
                InternalErrorStreakStep::Surface { consecutive: threshold },
            ),
            (
                true,
                u32::MAX,
                InternalErrorStreakStep::Surface { consecutive: u32::MAX },
            ),
        ] {
            assert_eq!(
                extend_internal_error_streak(persistent_error, consecutive),
                expected,
                "persistent_error: {persistent_error}, consecutive: \
                 {consecutive}",
            );
        }
    }

    // TODO-RAINCLAUDE: ends the erroring requests in an error that extends their streak, as surface_internal_error does, and gets the others through.
    struct ErroringDriver {
        erroring: BTreeSet<SledExpungeRequestUuid>,
    }

    impl RequestDriver for ErroringDriver {
        async fn drive(
            &self,
            request: &SledExpungeRequest,
            progress: &mut RequestProgress,
        ) -> SledExpungeRequestOutcome {
            if !self.erroring.contains(&request.id()) {
                return SledExpungeRequestOutcome::RemovalPreparing;
            }
            progress.internal_errors += 1;
            SledExpungeRequestOutcome::Error {
                error: "error".to_string(),
                last_error: None,
            }
        }
    }

    // TODO-RAINCLAUDE: an activation carries an erroring request's streak forward, and drops the streak of a request it gets through without an error and of one no longer listed.
    #[tokio::test]
    async fn test_sled_expunge_internal_error_streaks() {
        let log = slog::Logger::root(slog::Discard, o!());
        let erroring = test_request();
        let recovered = test_request();
        let streak = |request: &SledExpungeRequest, consecutive| {
            InternalErrorStreak { request_id: request.id(), consecutive }
        };
        let earlier = IdOrdMap::from_iter_unique([
            streak(&erroring, 1),
            streak(&recovered, 1),
            streak(&test_request(), 1),
        ])
        .expect("streaks are unique");
        let driver =
            ErroringDriver { erroring: BTreeSet::from([erroring.id()]) };

        let (_, internal_errors) = drive_requests_with_timeout(
            &log,
            vec![erroring.clone(), recovered],
            &driver,
            &test_activators(),
            &earlier,
        )
        .await;
        assert_eq!(
            internal_errors,
            IdOrdMap::from_iter_unique([streak(&erroring, 2)])
                .expect("one streak"),
        );
    }

    // TODO-RAINCLAUDE: the request is listed in powering_off with the internal-error last_error already set, then advances to expunging (which clears last_error) in the same activation and fails there; the generic last_error is compared against and written to the expunging row the advance returned, not the listed row, so it is written rather than skipped as unchanged.
    #[tokio::test]
    async fn test_sled_expunge_internal_error_uses_latest_row() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_internal_error_uses_latest_row",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &db.opctx().log;
        let rack = TestRack::new(&db).await;
        rack.propose_removal(&db).await;
        rack.commit(&db, 2).await;
        let inserted = rack
            .insert_request(&db, 2, SledExpungeRequestState::PoweringOff)
            .await;
        let listed = match db
            .datastore()
            .sled_expunge_request_set_last_error(
                db.opctx(),
                &authz_sled_expunge_request_from_id(inserted.id()),
                SledExpungeRequestUnfinishedState::PoweringOff,
                INTERNAL_ERROR_TEXT.to_string(),
            )
            .await
            .expect("seeded last_error")
        {
            SledExpungeRequestUpdateOutcome::Updated { request } => request,
            SledExpungeRequestUpdateOutcome::NotInExpectedState { current } => {
                panic!("seeded last_error in powering_off: {current:?}")
            }
        };
        set_sled_state(
            &db,
            rack.sled_id,
            nexus_db_model::SledState::Decommissioned,
        )
        .await;

        let mut progress = RequestProgress {
            internal_errors: INTERNAL_ERROR_ATTENTION_THRESHOLD - 1,
            ..RequestProgress::default()
        };
        let outcome = task
            .advance_to_expunging(db.opctx(), log, &listed, &mut progress)
            .await;
        let outcome = task
            .surface_internal_error(
                db.opctx(),
                log,
                &listed,
                outcome,
                &mut progress,
            )
            .await;
        let expunging = read(&db, &listed).await;
        let error = complete_error(&db, &expunging).await;
        assert_eq!(
            request_status(&listed, outcome, progress),
            SledExpungeRequestStatus {
                state_after: Some(SledExpungeTaskRequestState::Expunging),
                consecutive_internal_errors: INTERNAL_ERROR_ATTENTION_THRESHOLD,
                ..rack.status(
                    &listed,
                    SledExpungeRequestOutcome::Error {
                        error,
                        last_error: Some(INTERNAL_ERROR_TEXT.to_string()),
                    },
                )
            },
        );
        assert_eq!(
            expunging,
            SledExpungeRequest {
                state: SledExpungeRequestState::Expunging,
                power_off_outcome: Some(SledExpungePowerOffOutcome::PoweredOff),
                last_error: Some(INTERNAL_ERROR_TEXT.to_string()),
                time_modified: expunging.time_modified,
                ..listed.clone()
            },
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: a wait wakes inventory collection only if a newer collection may resolve it and none was taken during it, i.e. the loaded collection (if any) started before the request's time_modified.
    #[test]
    fn test_sled_expunge_wakes_inventory_collection() {
        let request = test_request();
        let collection_started_at = |time_started| {
            let mut collection = CollectionBuilder::new("test").build();
            collection.time_started = time_started;
            collection
        };
        let older = collection_started_at(
            request.time_modified - TimeDelta::seconds(1),
        );
        let during = collection_started_at(request.time_modified);
        let mgs: SocketAddrV6 =
            "[::1]:1".parse().expect("parsed an MGS address");
        let not_powered_off = |stages: &[MgsPowerOffFailure]| {
            SledExpungePowerOffWait::NotPoweredOff {
                slot: 3,
                failed_attempts: stages
                    .iter()
                    .zip(1..)
                    .map(|(stage, port)| SledExpungeMgsPowerOffFailure {
                        mgs: SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0),
                        stage: *stage,
                        error: "error".to_string(),
                    })
                    .collect(),
            }
        };
        for (wait, newer_collection_may_help) in [
            (SledExpungePowerOffWait::NoInventory, true),
            (SledExpungePowerOffWait::NotInInventory, true),
            (
                SledExpungePowerOffWait::SlotHoldsOtherBoard {
                    slot: 3,
                    mgs,
                    found: request.baseboard_id(),
                },
                true,
            ),
            (not_powered_off(&[MgsPowerOffFailure::RecheckFailed]), true),
            (not_powered_off(&[MgsPowerOffFailure::PowerOffFailed]), false),
            (
                not_powered_off(&[
                    MgsPowerOffFailure::PowerOffFailed,
                    MgsPowerOffFailure::RecheckFailed,
                ]),
                true,
            ),
            (
                SledExpungePowerOffWait::NotASled { sp_type: SpType::Switch },
                false,
            ),
            (
                SledExpungePowerOffWait::InAnotherRack {
                    rack_id: RackUuid::new_v4(),
                },
                false,
            ),
            (SledExpungePowerOffWait::SwitchAttachedCubby { slot: 14 }, false),
            (SledExpungePowerOffWait::NoMgs, false),
        ] {
            for (collection, taken_before_wait) in
                [(None, true), (Some(&older), true), (Some(&during), false)]
            {
                assert_eq!(
                    wakes_inventory_collection(&wait, collection, &request),
                    newer_collection_may_help && taken_before_wait,
                    "{wait:?} with collection started at {:?}",
                    collection.map(|collection| collection.time_started),
                );
            }
        }
    }
}
