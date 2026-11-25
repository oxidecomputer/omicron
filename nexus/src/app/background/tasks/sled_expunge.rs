// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: Background task that drives operator-requested sled expunges
// (RFD 747).
//
// Each activation runs two passes:
//
// * The trust quorum pass reads only the database. It advances each waiting
//   request whose trust quorum removal is past preparing.
// * The MGS pass then powers off each request's sled through MGS and expunges
//   it, in the same activation.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use itertools::Either;
use itertools::Itertools;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledExpungeRecordSpSlotOutcome;
use nexus_db_queries::db::datastore::SledExpungeRequestCompleteOutcome;
use nexus_db_queries::db::datastore::SledExpungeRequestUpdateOutcome;
use nexus_db_queries::db::datastore::SledExpungeTaskGateOutcome;
use nexus_db_queries::db::datastore::authz_sled_expunge_request_from_id;
use nexus_networking::GatewaysByRack;
use nexus_networking::LazyOnce;
use nexus_networking::LazyOnceState;
use nexus_networking::PowerOffAtSlotOutcome;
use nexus_networking::SledPowerOffGateways;
use nexus_networking::power_off_sled_at_slot;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::external_api::sled::SledProvisionPolicy;
use nexus_types::internal_api::background::SledExpungeCubbyLocated;
use nexus_types::internal_api::background::SledExpungeIgnitionGap;
use nexus_types::internal_api::background::SledExpungeIgnitionUnavailable;
use nexus_types::internal_api::background::SledExpungeMgsDiscovery;
use nexus_types::internal_api::background::SledExpungeMgsFailure;
use nexus_types::internal_api::background::SledExpungeMgsPowerOffFailure;
use nexus_types::internal_api::background::SledExpungeOtherBoard;
use nexus_types::internal_api::background::SledExpungePowerCheck;
use nexus_types::internal_api::background::SledExpungePowerOffWait;
use nexus_types::internal_api::background::SledExpungeRackAbsence;
use nexus_types::internal_api::background::SledExpungeRackSwitches;
use nexus_types::internal_api::background::SledExpungeRequestOutcome;
use nexus_types::internal_api::background::SledExpungeRequestStatus;
use nexus_types::internal_api::background::SledExpungeSearch;
use nexus_types::internal_api::background::SledExpungeStateAfter;
use nexus_types::internal_api::background::SledExpungeStatus;
use nexus_types::internal_api::background::SledExpungeTaskStep;
use nexus_types::internal_api::background::SledPoweredDown;
use nexus_types::sled_expunge::CubbyUnidentified;
use nexus_types::sled_expunge::MgsPowerOffFailure;
use nexus_types::sled_expunge::SledExpungeCubbyPowerOffOutcome;
use nexus_types::sled_expunge::SledExpungeLivePresence;
use nexus_types::sled_expunge::SledExpungePowerOff;
use nexus_types::sled_expunge::SledExpungeRecordedCubby;
use nexus_types::sled_expunge::SledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeRequestAdvance;
use nexus_types::sled_expunge::SledExpungeRequestStateKind;
use nexus_types::sled_expunge::SledExpungeRequestUnfinishedState;
use nexus_types::sled_expunge::SledExpungeSpSlotChange;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use omicron_common::api::external::Error;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackUuid;
use serde_json::json;
use sled_agent_types::early_networking::SwitchSlot;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use trust_quorum_types::types::Epoch;

// TODO-RAINCLAUDE: How long a removal may stay preparing before `last_error`
// tells the operator that it is stuck. This is measured from the request's
// `time_created`.
const REMOVAL_PREPARING_ATTENTION_THRESHOLD: TimeDelta = TimeDelta::minutes(10);

// TODO-RAINCLAUDE: The most requests driven at once within a pass. Requests are
// driven concurrently so that one slow request doesn't hold up the others.
const MAX_CONCURRENT_REQUESTS: usize = 16;

// TODO-RAINCLAUDE: How long one request may be driven within one pass of an
// activation. Nothing else bounds an activation as a whole.
//
// This is a backstop. It is set well above the worst case in which every MGS
// call runs to its client timeout, so a stalled MGS normally ends in a
// `last_error` and not in this timeout.
//
// A request that does time out is abandoned mid-step. That is safe because
// every step is either a compare-and-swap or idempotent, so the next activation
// picks the request up from whatever state it reached.
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

pub struct SledExpunge {
    datastore: Arc<DataStore>,
    resolver: internal_dns_resolver::Resolver,
    activators: SledExpungeActivators,
}

enum MgsDiscovery {
    Failed(String),
    Discovered {
        racks: IdOrdMap<RackMgs>,
        rack_id_unknown: IdOrdMap<SledExpungeMgsFailure>,
    },
}

enum RackMgsLookup<'a> {
    DiscoveryFailed,
    NoMgsForRack,
    Found(&'a SledPowerOffGateways),
}

struct RackMgs {
    rack_id: RackUuid,
    gateways: SledPowerOffGateways,
}

impl IdOrdItem for RackMgs {
    type Key<'a> = RackUuid;

    fn key(&self) -> Self::Key<'_> {
        self.rack_id
    }

    id_upcast!();
}

impl MgsDiscovery {
    fn from_gateways(gateways: GatewaysByRack) -> Self {
        let rack_id_unknown = gateways
            .unknown()
            .iter()
            .map(|(gateway, error)| SledExpungeMgsFailure {
                mgs: gateway.addr,
                error: InlineErrorChain::new(error).to_string(),
            })
            .collect();
        let racks = gateways
            .into_discovered()
            .map(|rack| RackMgs {
                rack_id: rack.rack_id,
                // TODO-RAINCLAUDE: We pass no cubbies as already identified,
                // because only MGS reads made after the removal reached
                // `Committing` may decide a request's outcome. An inventory
                // collection may be older than that.
                gateways: SledPowerOffGateways::new(
                    &rack.gateways,
                    IdOrdMap::new(),
                ),
            })
            .collect();
        MgsDiscovery::Discovered { racks, rack_id_unknown }
    }

    // TODO-RAINCLAUDE: Returns the MGSes discovered for `rack_id`. A rack that
    // is found always has at least one MGS.
    fn gateways_for(&self, rack_id: RackUuid) -> RackMgsLookup<'_> {
        match self {
            MgsDiscovery::Failed(_) => RackMgsLookup::DiscoveryFailed,
            MgsDiscovery::Discovered { racks, rack_id_unknown: _ } => {
                match racks.get(&rack_id) {
                    Some(rack) => RackMgsLookup::Found(&rack.gateways),
                    None => RackMgsLookup::NoMgsForRack,
                }
            }
        }
    }

    fn status(&self) -> SledExpungeMgsDiscovery {
        match self {
            MgsDiscovery::Failed(error) => {
                SledExpungeMgsDiscovery::Failed { error: error.clone() }
            }
            MgsDiscovery::Discovered { racks, rack_id_unknown } => {
                SledExpungeMgsDiscovery::Discovered {
                    rack_id_unknown: rack_id_unknown.clone(),
                    racks: racks
                        .iter()
                        .filter_map(|rack| {
                            rack.gateways.switch_status().map(|switches| {
                                SledExpungeRackSwitches {
                                    rack_id: rack.rack_id,
                                    switches,
                                }
                            })
                        })
                        .collect(),
                }
            }
        }
    }
}

// TODO-RAINCLAUDE: The MGS state that every request in one activation's MGS
// pass shares.
//
// Discovery is lazy. The first request that needs MGS starts it, so it runs at
// most once per activation, and not at all if no request needs MGS. Each
// discovered rack's `RackMgs` is shared in the same way, so the requests in one
// rack share its switch mapping, its ignition lists and its cubby scan.
struct ActivationMgs<'r> {
    log: &'r Logger,
    resolver: &'r internal_dns_resolver::Resolver,
    discovery: LazyOnce<MgsDiscovery>,
}

impl<'r> ActivationMgs<'r> {
    fn new(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
    ) -> Self {
        Self { log, resolver, discovery: LazyOnce::new() }
    }

    #[cfg(test)]
    fn with_discovery(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
        discovery: MgsDiscovery,
    ) -> Self {
        Self { log, resolver, discovery: LazyOnce::new_with(discovery) }
    }

    async fn get(&self) -> &MgsDiscovery {
        self.discovery
            .get_or_init(|| discover_gateways(self.log, self.resolver))
            .await
    }

    fn status(&self) -> SledExpungeMgsDiscovery {
        match self.discovery.state() {
            LazyOnceState::Done(discovery) => discovery.status(),
            LazyOnceState::Unfinished => SledExpungeMgsDiscovery::Unfinished,
            LazyOnceState::NotStarted => SledExpungeMgsDiscovery::NotNeeded,
        }
    }
}

// TODO-RAINCLAUDE: What driving one request has found so far, besides its
// outcome. This is kept outside the per-request timeout so that a request that
// times out still reports the step it was in and what it had found.
#[derive(Default)]
struct RequestProgress {
    step: SledExpungeTaskStep,
    latest_state: Option<SledExpungeRequestStateKind>,
    power_check: Option<SledExpungePowerCheck>,
}

impl RequestProgress {
    fn dispatching() -> Self {
        Self {
            step: SledExpungeTaskStep::DispatchingListedRequest,
            ..Self::default()
        }
    }
}

use mgs_pass::AdvancedRowNotPoweringOff;
use mgs_pass::MgsPassRequest;
use mgs_pass::MgsPassStart;

// TODO-RAINCLAUDE: This is a module of its own so that an `MgsPassRequest` can
// only be built through `as_listed` or `advanced`. Both make sure that `start`
// matches the row's state.
mod mgs_pass {
    use super::PER_REQUEST_TIMEOUT;
    use super::RequestDriver;
    use super::RequestProgress;
    use super::SledExpungeActivators;
    use super::listed_request_status;
    use super::request_log;
    use super::timed_out_outcome;
    use nexus_types::internal_api::background::SledExpungeRequestStatus;
    use nexus_types::sled_expunge::SledExpungeRequest;
    use nexus_types::sled_expunge::SledExpungeRequestStateKind;
    use slog::Logger;

    #[derive(Clone, Copy, Debug)]
    pub(super) enum MgsPassStart {
        PoweringOff,
        Expunging,
    }

    // TODO-RAINCLAUDE: A request handed to the MGS pass. `current` is the
    // freshest row we have, which is either the row as listed or the row that
    // the trust quorum pass's advance returned. `listed_state` is kept
    // separately for the status.
    pub(super) struct MgsPassRequest {
        listed_state: SledExpungeRequestStateKind,
        current: SledExpungeRequest,
        start: MgsPassStart,
        progress: RequestProgress,
    }

    // TODO-RAINCLAUDE: The error for an advance in the trust quorum pass that
    // returned a row not in `powering_off`. The advance's compare-and-swap only
    // returns a `powering_off` row, so this means there is a bug in it.
    pub(super) struct AdvancedRowNotPoweringOff {
        pub(super) state: SledExpungeRequestStateKind,
        pub(super) progress: RequestProgress,
    }

    impl MgsPassRequest {
        pub(super) fn as_listed(listed: &SledExpungeRequest) -> Option<Self> {
            let listed_state = listed.state.kind();
            let start = match listed_state {
                SledExpungeRequestStateKind::PoweringOff => {
                    MgsPassStart::PoweringOff
                }
                SledExpungeRequestStateKind::Expunging => {
                    MgsPassStart::Expunging
                }
                SledExpungeRequestStateKind::WaitingForTrustQuorum
                | SledExpungeRequestStateKind::Done
                | SledExpungeRequestStateKind::Failed => return None,
            };
            Some(Self {
                listed_state,
                current: listed.clone(),
                start,
                progress: RequestProgress::default(),
            })
        }

        pub(super) fn advanced(
            listed_state: SledExpungeRequestStateKind,
            powering_off: SledExpungeRequest,
            progress: RequestProgress,
        ) -> Result<Self, AdvancedRowNotPoweringOff> {
            match powering_off.state.kind() {
                SledExpungeRequestStateKind::PoweringOff => Ok(Self {
                    listed_state,
                    current: powering_off,
                    start: MgsPassStart::PoweringOff,
                    progress,
                }),
                state @ (SledExpungeRequestStateKind::WaitingForTrustQuorum
                | SledExpungeRequestStateKind::Expunging
                | SledExpungeRequestStateKind::Done
                | SledExpungeRequestStateKind::Failed) => {
                    Err(AdvancedRowNotPoweringOff { state, progress })
                }
            }
        }

        pub(super) async fn drive_with_timeout<D: RequestDriver>(
            self,
            log: &Logger,
            driver: &D,
            activators: &SledExpungeActivators,
        ) -> SledExpungeRequestStatus {
            let Self { listed_state, current, start, mut progress } = self;
            let outcome = match tokio::time::timeout(
                PER_REQUEST_TIMEOUT,
                driver.drive(&current, start, &mut progress),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_) => timed_out_outcome(
                    &request_log(log, &current),
                    progress.step,
                    activators,
                ),
            };
            listed_request_status(&current, listed_state, outcome, progress)
        }
    }
}

struct RackWideCheck {
    search: RackWideSearch,
    presence: Result<SledExpungeLivePresence, SledExpungeIgnitionUnavailable>,
}

// TODO-RAINCLAUDE: The task's own copy of `SledExpungeSearch`. It keeps the
// recorded cubby exactly as the row holds it, so that a request settled from
// this search sends the advance the same cubby the row has.
#[derive(Debug)]
enum RackWideSearch {
    GoneAtCreation,
    NotInRecordedCubby {
        cubby: SledExpungeRecordedCubby,
        other_board: Option<SledExpungeOtherBoard>,
        failed_attempts: IdOrdMap<SledExpungeMgsPowerOffFailure>,
    },
}

impl RackWideSearch {
    fn into_status(self) -> SledExpungeSearch {
        match self {
            RackWideSearch::GoneAtCreation => SledExpungeSearch::GoneAtCreation,
            RackWideSearch::NotInRecordedCubby {
                cubby,
                other_board,
                failed_attempts,
            } => SledExpungeSearch::NotInRecordedCubby {
                slot: cubby.slot,
                source: cubby.source,
                other_board,
                failed_attempts,
            },
        }
    }
}

// TODO-RAINCLAUDE: How the `powering_off` step settled a request. The sled was
// either powered down in a cubby or ruled out of the rack, and in both cases
// the request then advances to `expunging`.
// TODO-RAINCLAUDE: Each variant carries the request's cubby as the row holds
// it, rather than one rebuilt from the task status. The advance compares the
// whole cubby, slot and source, so a rebuilt cubby that drifted from the row
// would lose that compare every time and leave the request stuck.
// TODO-RAINCLAUDE: `clippy::large_enum_variant` is expected here and on
// `NeedsAttention`. Each value is built at most once per request per activation
// and consumed right away, so boxing the large variant would buy nothing.
#[expect(clippy::large_enum_variant)]
enum PowerOffSettled {
    PoweredDown {
        cubby: SledExpungeRecordedCubby,
        located: SledExpungeCubbyLocated,
        powered_down: SledPoweredDown,
    },
    Gone {
        search: RackWideSearch,
        absence: SledExpungeRackAbsence,
    },
}

impl PowerOffSettled {
    fn power_off(&self) -> SledExpungePowerOff {
        match self {
            PowerOffSettled::PoweredDown {
                cubby,
                located: _,
                powered_down,
            } => {
                let outcome = match powered_down {
                    SledPoweredDown::PoweredOff { mgs: _ } => {
                        SledExpungeCubbyPowerOffOutcome::PoweredOff
                    }
                    SledPoweredDown::AlreadyOff { mgs: _ } => {
                        SledExpungeCubbyPowerOffOutcome::AlreadyOff
                    }
                };
                SledExpungePowerOff::Cubby { cubby: *cubby, outcome }
            }
            PowerOffSettled::Gone {
                search: RackWideSearch::GoneAtCreation,
                absence: _,
            } => SledExpungePowerOff::AbsentAtCreation,
            PowerOffSettled::Gone {
                search:
                    RackWideSearch::NotInRecordedCubby {
                        cubby,
                        other_board,
                        failed_attempts: _,
                    },
                absence: _,
            } => {
                let outcome = match other_board {
                    Some(_) => {
                        SledExpungeCubbyPowerOffOutcome::OtherBoardPresent
                    }
                    None => SledExpungeCubbyPowerOffOutcome::NotPresent,
                };
                SledExpungePowerOff::Cubby { cubby: *cubby, outcome }
            }
        }
    }

    fn into_power_check(self) -> SledExpungePowerCheck {
        match self {
            PowerOffSettled::PoweredDown { cubby, located, powered_down } => {
                SledExpungePowerCheck::PoweredDown {
                    slot: cubby.slot,
                    located,
                    powered_down,
                }
            }
            PowerOffSettled::Gone { search, absence } => {
                SledExpungePowerCheck::Gone {
                    search: search.into_status(),
                    absence,
                }
            }
        }
    }
}

enum GateOutcome {
    AdvancedToPoweringOff(SledExpungeRequest),
    Stopped(SledExpungeRequestOutcome),
}

// TODO-RAINCLAUDE: Drives one request through the MGS pass, recording its
// progress as it goes. This is a trait and not a closure so that the future it
// returns can borrow the request and the progress, and so that tests can drive
// requests that stall.
trait RequestDriver {
    async fn drive(
        &self,
        request: &SledExpungeRequest,
        start: MgsPassStart,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome;
}

struct ActivationDriver<'x> {
    task: &'x SledExpunge,
    opctx: &'x OpContext,
    mgs: &'x ActivationMgs<'x>,
}

impl RequestDriver for ActivationDriver<'_> {
    async fn drive(
        &self,
        request: &SledExpungeRequest,
        start: MgsPassStart,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        self.task
            .drive_request(self.opctx, self.mgs, request, start, progress)
            .await
    }
}

impl SledExpunge {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns_resolver::Resolver,
        activators: SledExpungeActivators,
    ) -> Self {
        Self { datastore, resolver, activators }
    }

    // TODO-RAINCLAUDE: Runs one activation. `now` is passed in so that tests
    // can move the clock past the stuck-preparing threshold without sleeping.
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

        let mgs = ActivationMgs::new(&opctx.log, &self.resolver);
        let requests = self.run_passes(opctx, &mgs, requests, now).await;
        SledExpungeStatus::Listed { requests, mgs_discovery: mgs.status() }
    }

    // TODO-RAINCLAUDE: Runs both passes of one activation: the trust quorum
    // pass, and then the MGS pass. `actually_activate` and the tests both go
    // through this function.
    //
    // The order of the passes is what keeps power-off decisions safe. MGS
    // evidence may decide a request's outcome only if it was read after the
    // request's removal reached `Committing`, because from then on the removal
    // can no longer abort. So the trust quorum pass takes no MGS context, and
    // `mgs` is first used only once that pass has finished.
    //
    // `mgs` is lazy, so a caller may create it before calling this function.
    // (The tests do, to inject discovery.) What matters is when it is first
    // used.
    async fn run_passes(
        &self,
        opctx: &OpContext,
        mgs: &ActivationMgs<'_>,
        requests: Vec<SledExpungeRequest>,
        now: DateTime<Utc>,
    ) -> IdOrdMap<SledExpungeRequestStatus> {
        let (mut statuses, to_mgs_pass) =
            self.trust_quorum_pass(opctx, requests, now).await;
        let driver = ActivationDriver { task: self, opctx, mgs };
        statuses.extend(
            drive_requests_with_timeout(
                &opctx.log,
                to_mgs_pass,
                &driver,
                &self.activators,
            )
            .await,
        );
        IdOrdMap::from_iter_unique(statuses).expect(
            "list_unfinished pages by sled_id, which a request never changes, so each request is listed once, and the trust quorum pass either finishes a listed request or hands it to the MGS pass, never both",
        )
    }

    // TODO-RAINCLAUDE: Runs the trust quorum pass. Returns the statuses of the
    // requests it finished, and the requests it hands on to the MGS pass.
    async fn trust_quorum_pass(
        &self,
        opctx: &OpContext,
        requests: Vec<SledExpungeRequest>,
        now: DateTime<Utc>,
    ) -> (Vec<SledExpungeRequestStatus>, Vec<MgsPassRequest>) {
        let mut statuses = Vec::new();
        let mut to_mgs_pass = Vec::new();
        let mut waiting = Vec::new();
        for request in requests {
            let kind = request.state.kind();
            match kind {
                SledExpungeRequestStateKind::WaitingForTrustQuorum => {
                    waiting.push(request);
                }
                SledExpungeRequestStateKind::PoweringOff
                | SledExpungeRequestStateKind::Expunging => {
                    match MgsPassRequest::as_listed(&request) {
                        Some(mgs_request) => to_mgs_pass.push(mgs_request),
                        None => statuses.push(internal_error_status(
                            &request_log(&opctx.log, &request),
                            &request,
                            format!(
                                "the MGS pass refused a request listed in \
                                 state {kind}",
                            ),
                            RequestProgress::dispatching(),
                        )),
                    }
                }
                SledExpungeRequestStateKind::Done
                | SledExpungeRequestStateKind::Failed => {
                    statuses.push(internal_error_status(
                        &request_log(&opctx.log, &request),
                        &request,
                        format!("listed as unfinished, but in state {kind}",),
                        RequestProgress::dispatching(),
                    ));
                }
            }
        }

        let (gate_statuses, advanced): (Vec<_>, Vec<_>) = stream::iter(waiting)
            .map(|request| self.check_gate_with_timeout(opctx, request, now))
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .partition_map(|gated| gated);
        statuses.extend(gate_statuses);
        to_mgs_pass.extend(advanced);
        (statuses, to_mgs_pass)
    }

    // TODO-RAINCLAUDE: Checks one waiting request's gate under
    // `PER_REQUEST_TIMEOUT`. Returns `Left` with the request's status if the
    // request stops here, and `Right` if it advanced to `powering_off` and goes
    // on to the MGS pass.
    async fn check_gate_with_timeout(
        &self,
        opctx: &OpContext,
        request: SledExpungeRequest,
        now: DateTime<Utc>,
    ) -> Either<SledExpungeRequestStatus, MgsPassRequest> {
        let log = &request_log(&opctx.log, &request);
        let mut progress = RequestProgress::default();
        let gate = match tokio::time::timeout(
            PER_REQUEST_TIMEOUT,
            self.drive_waiting_for_trust_quorum(
                opctx,
                log,
                &request,
                now,
                &mut progress,
            ),
        )
        .await
        {
            Ok(gate) => gate,
            Err(_) => GateOutcome::Stopped(timed_out_outcome(
                log,
                progress.step,
                &self.activators,
            )),
        };
        match gate {
            GateOutcome::AdvancedToPoweringOff(powering_off) => {
                match MgsPassRequest::advanced(
                    request.state.kind(),
                    powering_off,
                    progress,
                ) {
                    Ok(mgs_request) => Either::Right(mgs_request),
                    Err(AdvancedRowNotPoweringOff { state, progress }) => {
                        Either::Left(internal_error_status(
                            log,
                            &request,
                            format!(
                                "the advance to powering_off returned the \
                                 request in state {state}",
                            ),
                            progress,
                        ))
                    }
                }
            }
            GateOutcome::Stopped(outcome) => {
                Either::Left(request_status(&request, outcome, progress))
            }
        }
    }

    async fn drive_request(
        &self,
        opctx: &OpContext,
        mgs: &ActivationMgs<'_>,
        request: &SledExpungeRequest,
        start: MgsPassStart,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let log = &request_log(&opctx.log, request);
        match start {
            MgsPassStart::PoweringOff => {
                self.drive_powering_off(opctx, log, mgs, request, progress)
                    .await
            }
            MgsPassStart::Expunging => {
                self.complete(opctx, log, request, progress).await
            }
        }
    }

    async fn drive_waiting_for_trust_quorum(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
        progress: &mut RequestProgress,
    ) -> GateOutcome {
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
                return GateOutcome::Stopped(
                    self.fail(
                        opctx,
                        log,
                        request,
                        from,
                        FailureReason::RemovalConfigMissing,
                        progress,
                    )
                    .await,
                );
            }
            Err(error) => {
                return GateOutcome::Stopped(error_outcome(
                    log,
                    progress.step,
                    &error,
                ));
            }
        };
        let outcome = match gate {
            SledExpungeTaskGate::SledRemoved => {
                progress.step = SledExpungeTaskStep::AdvancingToPoweringOff;
                let result = self
                    .datastore
                    .sled_expunge_request_advance(
                        opctx,
                        &authz_sled_expunge_request_from_id(request.id),
                        SledExpungeRequestAdvance::ToPoweringOff,
                    )
                    .await;
                match require_updated(log, progress, result) {
                    Ok(powering_off) => {
                        info!(
                            log,
                            "trust quorum removed the sled; powering it off \
                             in this activation's MGS pass"
                        );
                        return GateOutcome::AdvancedToPoweringOff(
                            powering_off,
                        );
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
                        NeedsAttention::RemovalStuckPreparing,
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
        };
        GateOutcome::Stopped(outcome)
    }

    async fn drive_powering_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        mgs: &ActivationMgs<'_>,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::ResolvingPower;
        let rack_id = request.rack_id;
        // TODO-RAINCLAUDE: Discovery can fail outright, or it can find no MGS
        // for this rack. Both cases wait with the same `NoMgs` `last_error`,
        // because the activation's `mgs_discovery` status already says which
        // one happened.
        let gateways = match mgs.get().await.gateways_for(rack_id) {
            RackMgsLookup::Found(gateways) => gateways,
            // TODO-RAINCLAUDE: `discover_gateways` already logged this failure
            // once for the activation at error level, so each request only
            // notes it at debug level.
            RackMgsLookup::DiscoveryFailed => {
                debug!(
                    log,
                    "cannot power off the sled: MGS discovery failed";
                    "rack_id" => %rack_id,
                );
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        request,
                        SledExpungePowerOffWait::NoMgs,
                        progress,
                    )
                    .await;
            }
            RackMgsLookup::NoMgsForRack => {
                warn!(
                    log,
                    "cannot power off the sled: no discovered MGS reported \
                     the request's rack";
                    "rack_id" => %rack_id,
                );
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        request,
                        SledExpungePowerOffWait::NoMgs,
                        progress,
                    )
                    .await;
            }
        };
        self.power_off(opctx, log, request, gateways, progress).await
    }

    // TODO-RAINCLAUDE: Runs RFD 747's `powering_off` step, which locates the
    // sled and then acts on it.
    //
    // The recorded cubby is only a hint, and it is tried first. If no MGS reads
    // the sled's board there, or if there is no recorded cubby, the rack-wide
    // check runs. It either rules the sled out of the rack, which settles the
    // request, or finds the sled in another sled cubby, which is then recorded
    // and tried.
    // TODO-RAINCLAUDE: A sled that was ruled out of the rack could still come
    // back. If it restarts, it can reconstruct its secret once it has shares
    // from `threshold - 1` other members of the old configuration that haven't
    // committed the new epoch. (Members that have committed answer with
    // `Expunged`.) Sleds removed in the same reconfiguration never commit, so
    // they always count toward that number.
    //
    // v1 accepts this window for the `AbsentAtCreation`, `NotPresent` and
    // `OtherBoardPresent` outcomes. The alternative is to wait until every
    // member of the new configuration has committed, and that would never
    // finish if one of them is dead.
    // TODO-RAINCLAUDE: The live queries reach only this rack's MGSes, so a sled
    // that was moved to another rack looks gone here. This is acceptable for
    // single-rack deployments.
    async fn power_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        gateways: &SledPowerOffGateways,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        // TODO-RAINCLAUDE: Re-read the request before any power-off, so that a
        // stale row can't power off a sled whose request another Nexus has
        // already moved on. For example, the request may have failed and the
        // sled may have been added back since. Even a row that this Nexus
        // advanced a moment ago can be stale in this way. The recorded cubby
        // comes from this read too.
        progress.step = SledExpungeTaskStep::ReReadingRequest;
        let current = match self
            .datastore
            .sled_expunge_request_get(opctx, request.id)
            .await
        {
            Ok(current) => current,
            Err(error) => return error_outcome(log, progress.step, &error),
        };
        match current.state.kind() {
            SledExpungeRequestStateKind::PoweringOff => {}
            SledExpungeRequestStateKind::WaitingForTrustQuorum
            | SledExpungeRequestStateKind::Expunging
            | SledExpungeRequestStateKind::Done
            | SledExpungeRequestStateKind::Failed => {
                return not_in_expected_state_outcome(
                    log,
                    progress.step,
                    &current,
                );
            }
        }
        let target = &current.baseboard_id;

        let search = match current.state.recorded_cubby() {
            Some(cubby) => {
                let slot = cubby.slot;
                let source = cubby.source;
                progress.step = SledExpungeTaskStep::SendingPowerOff;
                match power_off_sled_at_slot(log, gateways, target, slot).await
                {
                    PowerOffAtSlotOutcome::PoweredDown(powered_down) => {
                        return self
                            .advance_settled(
                                opctx,
                                log,
                                &current,
                                PowerOffSettled::PoweredDown {
                                    cubby,
                                    located:
                                        SledExpungeCubbyLocated::Recorded {
                                            source,
                                        },
                                    powered_down,
                                },
                                progress,
                            )
                            .await;
                    }
                    PowerOffAtSlotOutcome::NotPoweredOff {
                        failed_attempts,
                    } => {
                        return self
                            .wait_to_power_off(
                                opctx,
                                log,
                                &current,
                                SledExpungePowerOffWait::NotPoweredOff {
                                    slot,
                                    located:
                                        SledExpungeCubbyLocated::Recorded {
                                            source,
                                        },
                                    failed_attempts,
                                },
                                progress,
                            )
                            .await;
                    }
                    PowerOffAtSlotOutcome::NotLocated {
                        other_board,
                        failed_attempts,
                    } => {
                        info!(
                            log,
                            "no MGS found the sled in its recorded cubby; \
                             checking the whole rack";
                            "slot" => slot,
                            "other_board" => ?other_board,
                        );
                        RackWideSearch::NotInRecordedCubby {
                            cubby,
                            other_board,
                            failed_attempts,
                        }
                    }
                }
            }
            None => RackWideSearch::GoneAtCreation,
        };

        progress.step = SledExpungeTaskStep::ResolvingPower;
        let presence = gateways.live_sled_presence(log, target).await;
        self.act_on_presence(
            opctx,
            log,
            &current,
            gateways,
            RackWideCheck { search, presence },
            progress,
        )
        .await
    }

    // TODO-RAINCLAUDE: Acts on the result of the rack-wide check. This is split
    // out of `power_off` so that tests can inject a result the simulator can't
    // produce, such as a sled in a switch-attached cubby.
    async fn act_on_presence(
        &self,
        opctx: &OpContext,
        log: &Logger,
        current: &SledExpungeRequest,
        gateways: &SledPowerOffGateways,
        check: RackWideCheck,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let RackWideCheck { search, presence } = check;
        let target = &current.baseboard_id;
        let found = match presence {
            Ok(SledExpungeLivePresence::Gone { absence }) => {
                info!(
                    log,
                    "every sled cubby either switch lists powered, or only \
                     one lists, holds another board; the sled cannot be \
                     running in any sled cubby";
                    "search" => ?search,
                );
                return self
                    .advance_settled(
                        opctx,
                        log,
                        current,
                        PowerOffSettled::Gone { search, absence },
                        progress,
                    )
                    .await;
            }
            Ok(SledExpungeLivePresence::Unidentified { cubbies }) => {
                warn!(
                    log,
                    "cannot rule the sled out of the rack: some sled cubbies \
                     could not be identified";
                    "slots" => ?cubbies.iter().map(|c| c.slot).collect::<Vec<_>>(),
                );
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        current,
                        SledExpungePowerOffWait::CubbiesUnidentified {
                            search: search.into_status(),
                            cubbies,
                        },
                        progress,
                    )
                    .await;
            }
            Err(switches) => {
                warn!(
                    log,
                    "cannot rule the sled out of the rack: a switch's \
                     ignition state is unavailable";
                    "switches" => ?switches,
                );
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        current,
                        SledExpungePowerOffWait::IgnitionUnavailable {
                            search: search.into_status(),
                            switches,
                        },
                        progress,
                    )
                    .await;
            }
            Ok(SledExpungeLivePresence::InCubby { slot }) => slot,
        };

        let (located, change) = match search {
            RackWideSearch::GoneAtCreation => (
                SledExpungeCubbyLocated::FoundAfterGoneAtCreation,
                SledExpungeSpSlotChange::found_after_absent(found),
            ),
            RackWideSearch::NotInRecordedCubby {
                cubby,
                other_board: _,
                failed_attempts,
            } => {
                match SledExpungeSpSlotChange::relocated(cubby.slot, found) {
                    Some(change) => (
                        SledExpungeCubbyLocated::Relocated {
                            recorded_slot: cubby.slot,
                        },
                        change,
                    ),
                    // TODO-RAINCLAUDE: The rack-wide check found the sled in its
                    // recorded cubby, even though this activation's attempt there
                    // did not. There's nothing new to record, and the sled is
                    // located but not powered off, so the request waits.
                    None => {
                        return self
                            .wait_to_power_off(
                                opctx,
                                log,
                                current,
                                SledExpungePowerOffWait::NotPoweredOff {
                                    slot: cubby.slot,
                                    located:
                                        SledExpungeCubbyLocated::Recorded {
                                            source: cubby.source,
                                        },
                                    failed_attempts,
                                },
                                progress,
                            )
                            .await;
                    }
                }
            }
        };
        info!(
            log,
            "the rack-wide check found the sled in a sled cubby";
            "slot" => found,
            "located" => ?located,
        );

        progress.step = SledExpungeTaskStep::RecordingSpSlot;
        let result = self
            .datastore
            .sled_expunge_request_record_sp_slot(
                opctx,
                &authz_sled_expunge_request_from_id(current.id),
                change,
            )
            .await;
        let recorded = match result {
            Ok(SledExpungeRecordSpSlotOutcome::Updated {
                request: updated,
            }) => {
                info!(log, "recorded the sled's cubby"; "slot" => found);
                progress.latest_state = Some(updated.state.kind());
                updated
            }
            Ok(SledExpungeRecordSpSlotOutcome::NotInExpectedState {
                current: concurrent,
            }) => match concurrent.state.kind() {
                // TODO-RAINCLAUDE: Another Nexus already wrote exactly what
                // this call would have. The source must match too, so the
                // status below doesn't contradict a row that reached this cubby
                // another way.
                SledExpungeRequestStateKind::PoweringOff
                    if concurrent.state.recorded_cubby()
                        == Some(change.recorded_cubby()) =>
                {
                    info!(
                        log,
                        "the sled's cubby was already recorded";
                        "slot" => found,
                    );
                    concurrent
                }
                SledExpungeRequestStateKind::PoweringOff
                | SledExpungeRequestStateKind::WaitingForTrustQuorum
                | SledExpungeRequestStateKind::Expunging
                | SledExpungeRequestStateKind::Done
                | SledExpungeRequestStateKind::Failed => {
                    return not_in_expected_state_outcome(
                        log,
                        progress.step,
                        &concurrent,
                    );
                }
            },
            // TODO-RAINCLAUDE: A sled that is, or might be, in a
            // switch-attached cubby counts as a scrimlet. We neither power it
            // off nor rule it out, and the request waits for support.
            Ok(SledExpungeRecordSpSlotOutcome::SwitchAttachedCubby {
                sp_slot,
            }) => {
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        current,
                        SledExpungePowerOffWait::SwitchAttachedCubby {
                            slot: sp_slot,
                        },
                        progress,
                    )
                    .await;
            }
            Err(error) => {
                return error_outcome(log, progress.step, &error);
            }
        };

        // TODO-RAINCLAUDE: We follow the sled to a newly recorded cubby only
        // once. If the sled isn't powered down there, the request waits and
        // doesn't run the rack-wide check again. The next activation starts
        // from the newly recorded cubby.
        progress.step = SledExpungeTaskStep::SendingPowerOff;
        match power_off_sled_at_slot(log, gateways, target, found).await {
            PowerOffAtSlotOutcome::PoweredDown(powered_down) => {
                self.advance_settled(
                    opctx,
                    log,
                    &recorded,
                    PowerOffSettled::PoweredDown {
                        cubby: change.recorded_cubby(),
                        located,
                        powered_down,
                    },
                    progress,
                )
                .await
            }
            PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts }
            | PowerOffAtSlotOutcome::NotLocated {
                other_board: _,
                failed_attempts,
            } => {
                self.wait_to_power_off(
                    opctx,
                    log,
                    &recorded,
                    SledExpungePowerOffWait::NotPoweredOff {
                        slot: found,
                        located,
                        failed_attempts,
                    },
                    progress,
                )
                .await
            }
        }
    }

    async fn advance_settled(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        settled: PowerOffSettled,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let power_off = settled.power_off();
        progress.power_check = Some(settled.into_power_check());
        self.advance_to_expunging(opctx, log, request, power_off, progress)
            .await
    }

    async fn wait_to_power_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        wait: SledExpungePowerOffWait,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
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
        power_off: SledExpungePowerOff,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::AdvancingToExpunging;
        let result = self
            .datastore
            .sled_expunge_request_advance(
                opctx,
                &authz_sled_expunge_request_from_id(request.id),
                SledExpungeRequestAdvance::ToExpunging { power_off },
            )
            .await;
        let expunging = match require_updated(log, progress, result) {
            Ok(expunging) => expunging,
            Err(outcome) => return outcome,
        };
        info!(
            log,
            "the sled is powered down in its cubby or cannot be running in \
             any sled cubby; expunging it now";
            "power_off_outcome" => power_off.label(),
        );
        self.complete(opctx, log, &expunging, progress).await
    }

    // TODO-RAINCLAUDE: Runs RFD 747's `expunging` step. It doesn't resolve the
    // sled's identity again, because v1 never powers a sled back on, and a
    // request only reaches `expunging` once its sled was powered down or ruled
    // out of the rack.
    async fn complete(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let from = SledExpungeRequestUnfinishedState::Expunging;
        progress.step = SledExpungeTaskStep::Completing;
        match self.datastore.sled_expunge_request_complete(opctx, request).await
        {
            Ok(SledExpungeRequestCompleteOutcome::Completed {
                previous_sled_policy,
            }) => {
                info!(
                    log,
                    "expunged the sled; sled expunge request is done";
                    "previous_sled_policy" => ?previous_sled_policy,
                );
                // TODO-RAINCLAUDE: The lockstep `sled_expunge` does the same.
                // `instance_watcher` marks the VMMs on the expunged sled as
                // failed sooner, and the planner acts on the expungement
                // sooner.
                self.activators.activate();
                SledExpungeRequestOutcome::Completed { previous_sled_policy }
            }
            Ok(SledExpungeRequestCompleteOutcome::NotExpunging { current }) => {
                not_in_expected_state_outcome(log, progress.step, &current)
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
            // TODO-RAINCLAUDE: The expunge may have committed before the error
            // reached us (for example, on a timeout), so we activate the tasks
            // anyway.
            Err(error) => {
                self.activators.activate();
                error_outcome(log, progress.step, &error)
            }
        }
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
        // TODO-RAINCLAUDE: Skip the write when the message hasn't changed, so
        // that `time_modified` keeps saying when the request last changed.
        if request.state.last_error() == Some(last_error.as_str()) {
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
                &authz_sled_expunge_request_from_id(request.id),
                expected_state,
                last_error.clone(),
            )
            .await;
        match require_updated(log, progress, result) {
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
        let sled_id = request.sled_id;
        // TODO-RAINCLAUDE: The failure reason states the sled's policy as it is
        // now. If the sled's row is missing, the request still fails, and the
        // reason says that the policy couldn't be read. Any other read error
        // leaves the request for the next activation.
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
                return error_outcome(log, progress.step, &error);
            }
        };
        let failure_reason = reason.render(request, sled_policy);
        let result = self
            .datastore
            .sled_expunge_request_fail(
                opctx,
                &authz_sled_expunge_request_from_id(request.id),
                from,
                failure_reason.clone(),
            )
            .await;
        match require_updated(log, progress, result) {
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
        "sled_expunge_request_id" => request.id.to_string(),
        "sled_id" => request.sled_id.to_string(),
        "tq_epoch" => request.tq_epoch.0,
        "state" => request.state.kind().to_string(),
    ))
}

async fn drive_requests_with_timeout<D: RequestDriver>(
    log: &Logger,
    requests: Vec<MgsPassRequest>,
    driver: &D,
    activators: &SledExpungeActivators,
) -> Vec<SledExpungeRequestStatus> {
    stream::iter(requests)
        .map(|request| request.drive_with_timeout(log, driver, activators))
        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
        .collect()
        .await
}

// TODO-RAINCLAUDE: Reports a request that timed out in `step`. A timeout while
// advancing to `expunging` or while completing may come after the expunge
// committed, so those two steps activate `instance_watcher` and
// `blueprint_planner` anyway.
fn timed_out_outcome(
    log: &Logger,
    step: SledExpungeTaskStep,
    activators: &SledExpungeActivators,
) -> SledExpungeRequestOutcome {
    error!(
        log,
        "driving sled expunge request timed out; the next activation \
         retries it";
        "timeout" => ?PER_REQUEST_TIMEOUT,
        "step" => step.description(),
    );
    match step {
        SledExpungeTaskStep::AdvancingToExpunging
        | SledExpungeTaskStep::Completing => {
            activators.activate();
        }
        SledExpungeTaskStep::Starting
        | SledExpungeTaskStep::DispatchingListedRequest
        | SledExpungeTaskStep::CheckingTrustQuorum
        | SledExpungeTaskStep::ResolvingPower
        | SledExpungeTaskStep::ReReadingRequest
        | SledExpungeTaskStep::SendingPowerOff
        | SledExpungeTaskStep::RecordingSpSlot
        | SledExpungeTaskStep::AdvancingToPoweringOff
        | SledExpungeTaskStep::SettingLastError
        | SledExpungeTaskStep::Failing => {}
    }
    SledExpungeRequestOutcome::TimedOut { step }
}

fn internal_error_status(
    log: &Logger,
    request: &SledExpungeRequest,
    message: String,
    progress: RequestProgress,
) -> SledExpungeRequestStatus {
    let outcome =
        error_outcome(log, progress.step, &Error::internal_error(&message));
    request_status(request, outcome, progress)
}

fn request_status(
    request: &SledExpungeRequest,
    outcome: SledExpungeRequestOutcome,
    progress: RequestProgress,
) -> SledExpungeRequestStatus {
    listed_request_status(request, request.state.kind(), outcome, progress)
}

// TODO-RAINCLAUDE: Builds the status for one request. `request` may be a row
// read after the listing. Its `id` and `sled_id` never change, but its state
// may have, so the state the request was listed in is passed separately.
fn listed_request_status(
    request: &SledExpungeRequest,
    listed_state: SledExpungeRequestStateKind,
    outcome: SledExpungeRequestOutcome,
    progress: RequestProgress,
) -> SledExpungeRequestStatus {
    let RequestProgress { step: _, latest_state, power_check } = progress;
    let state_after = match &outcome {
        SledExpungeRequestOutcome::Completed { .. } => {
            SledExpungeStateAfter::Moved {
                state: SledExpungeRequestStateKind::Done,
            }
        }
        SledExpungeRequestOutcome::Failed { .. } => {
            SledExpungeStateAfter::Moved {
                state: SledExpungeRequestStateKind::Failed,
            }
        }
        SledExpungeRequestOutcome::TimedOut { .. }
        | SledExpungeRequestOutcome::Error { .. }
        | SledExpungeRequestOutcome::NotInExpectedState { .. } => {
            SledExpungeStateAfter::Unknown { last_confirmed: latest_state }
        }
        SledExpungeRequestOutcome::RemovalPreparing
        | SledExpungeRequestOutcome::LastErrorSet { .. }
        | SledExpungeRequestOutcome::LastErrorUnchanged { .. } => {
            match latest_state {
                Some(after) if after != listed_state => {
                    SledExpungeStateAfter::Moved { state: after }
                }
                Some(_) | None => SledExpungeStateAfter::Unchanged,
            }
        }
    };
    SledExpungeRequestStatus {
        request_id: request.id,
        sled_id: request.sled_id,
        state: listed_state,
        state_after,
        outcome,
        power_check,
    }
}

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
            MgsDiscovery::from_gateways(gateways)
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

fn require_updated(
    log: &Logger,
    progress: &mut RequestProgress,
    result: Result<SledExpungeRequestUpdateOutcome, Error>,
) -> Result<SledExpungeRequest, SledExpungeRequestOutcome> {
    let step = progress.step;
    match result {
        Ok(SledExpungeRequestUpdateOutcome::Updated { request }) => {
            progress.latest_state = Some(request.state.kind());
            Ok(request)
        }
        Ok(SledExpungeRequestUpdateOutcome::NotInExpectedState { current }) => {
            Err(not_in_expected_state_outcome(log, step, &current))
        }
        Err(error) => Err(error_outcome(log, step, &error)),
    }
}

fn not_in_expected_state_outcome(
    log: &Logger,
    step: SledExpungeTaskStep,
    current: &SledExpungeRequest,
) -> SledExpungeRequestOutcome {
    info!(
        log,
        "sled expunge request changed concurrently; leaving it to the next \
         activation";
        "step" => step.description(),
        "current_state" => current.state.kind().to_string(),
    );
    SledExpungeRequestOutcome::NotInExpectedState {
        current_state: current.state.kind(),
    }
}

// TODO-RAINCLAUDE: Logs a step's error and reports it in the task's status.
// Like other background tasks, we never write such an error to `last_error`,
// and the next activation retries the request. A `last_error` written earlier
// for a known wait stays on the row in the meantime.
fn error_outcome(
    log: &Logger,
    step: SledExpungeTaskStep,
    error: &Error,
) -> SledExpungeRequestOutcome {
    let step = step.description();
    if error.retryable() {
        warn!(log, "sled expunge request step failed"; "step" => step, error);
    } else {
        error!(log, "sled expunge request step failed"; "step" => step, error);
    }
    SledExpungeRequestOutcome::Error {
        error: format!("{step}: {}", InlineErrorChain::new(error)),
    }
}

#[expect(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
enum NeedsAttention {
    RemovalStuckPreparing,
    ReAddPreparing { re_add_epoch: Epoch },
    PowerOff(SledExpungePowerOffWait),
}

const KEEPS_RETRYING: &str =
    "Nexus will keep retrying; if this persists, contact Oxide support.";

impl NeedsAttention {
    // TODO-RAINCLAUDE: Renders the `last_error` text for this reason. Operators
    // see `last_error` as is, and this is the only place that produces it. It
    // takes only epochs, IDs and cubby numbers from the reason, so no internal
    // error text, MGS error text or MGS address reaches operators.
    fn render(&self, request: &SledExpungeRequest) -> String {
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
            NeedsAttention::PowerOff(wait) => power_off_wait_text(wait),
        }
    }
}

fn power_off_wait_text(wait: &SledExpungePowerOffWait) -> String {
    match wait {
        SledExpungePowerOffWait::SwitchAttachedCubby { slot } => format!(
            "This sled was found in switch-attached cubby {slot}, so it is, or \
             might be, a scrimlet (it manages a switch), and Nexus will not \
             power it off. \
             Expunging a scrimlet through this API is not supported. Contact \
             Oxide support."
        ),
        SledExpungePowerOffWait::NoMgs => format!(
            "Nexus could not find the management gateway of either switch in \
             this sled's rack, so it cannot reach the sled's cubby. \
             {KEEPS_RETRYING}"
        ),
        SledExpungePowerOffWait::NotPoweredOff {
            slot,
            located: _,
            failed_attempts,
        } => {
            let failures = failed_attempts
                .iter()
                .map(|attempt| power_off_failure_text(&attempt.failure))
                .collect::<BTreeSet<_>>()
                .iter()
                .join("; ");
            format!(
                "Nexus could not power off this sled in cubby {slot} \
                 ({failures}). {KEEPS_RETRYING}",
            )
        }
        SledExpungePowerOffWait::IgnitionUnavailable { search, switches } => {
            format!(
                "{}, which needs the ignition state of both switches: {}. \
                 {KEEPS_RETRYING}",
                search_text(search),
                switches
                    .gaps()
                    .into_iter()
                    .map(|(switch, gap)| ignition_gap_text(switch, gap))
                    .join("; "),
            )
        }
        SledExpungePowerOffWait::CubbiesUnidentified { search, cubbies } => {
            let slots = cubbies.iter().map(|cubby| cubby.slot).collect_vec();
            let unidentified = match slots.as_slice() {
                [slot] => format!(
                    "the board in sled cubby {slot} could not be identified, \
                     so the sled may still be running in it"
                ),
                [] | [_, _, ..] => format!(
                    "the boards in sled cubbies {} could not be identified, \
                     so the sled may still be running in one of them",
                    slots.iter().join(", "),
                ),
            };
            format!("{}: {unidentified}. {KEEPS_RETRYING}", search_text(search))
        }
    }
}

fn search_text(search: &SledExpungeSearch) -> String {
    match search {
        SledExpungeSearch::GoneAtCreation => {
            "This sled was not found when the request was created, and Nexus \
             cannot yet confirm that it is still gone"
                .to_string()
        }
        SledExpungeSearch::NotInRecordedCubby {
            slot,
            source: _,
            other_board: _,
            failed_attempts: _,
        } => {
            format!(
                "Nexus did not find this sled in cubby {slot}, where it was \
                 last known to be, and cannot yet confirm that it is gone \
                 from the rack"
            )
        }
    }
}

fn power_off_failure_text(failure: &MgsPowerOffFailure) -> &'static str {
    match failure {
        // TODO-RAINCLAUDE: `MgsRequestFailed` also covers an SP that did
        // answer, but with an error. So the text says that the request failed,
        // and not that nothing answered.
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::MgsRequestFailed { error: _ },
        } => "a management gateway's request for the sled's state failed",
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::SpUnreachable { error: _ },
        } => {
            "a management gateway could not reach the sled's service processor"
        }
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::EmptyIdentity { reported: _ },
        } => "the sled's cubby reported an empty baseboard identity",
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::UnexpectedSystemType { system_type: _ },
        } => {
            "ignition reports a system type that does not belong in a sled cubby"
        }
        MgsPowerOffFailure::OtherBoardPresent { found: _ } => {
            "a management gateway found another board in the cubby"
        }
        MgsPowerOffFailure::IgnitionPowerOffFailed { error: _ } => {
            "a management gateway did not confirm the power-off"
        }
    }
}

fn switch_number(switch: SwitchSlot) -> u8 {
    match switch {
        SwitchSlot::Switch0 => 0,
        SwitchSlot::Switch1 => 1,
    }
}

fn ignition_gap_text(
    switch: SwitchSlot,
    gap: SledExpungeIgnitionGap,
) -> String {
    let n = switch_number(switch);
    match gap {
        SledExpungeIgnitionGap::NoMgs => {
            format!("no management gateway was found for switch {n}")
        }
        SledExpungeIgnitionGap::MgsConflict => {
            format!("more than one management gateway claims switch {n}")
        }
        SledExpungeIgnitionGap::FetchFailed { mgs: _ } => {
            format!("switch {n}'s ignition state could not be read")
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
    // TODO-RAINCLAUDE: Renders the `failure_reason` text, which operators see
    // as is. A re-add can fail a request that is already in `expunging`, after
    // its sled was powered off, so in that case the text also says what Nexus
    // had done to the sled.
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
                // TODO-RAINCLAUDE: Only a request that was already `expunging`
                // has a power-off to report, so the text says what Nexus did to
                // the sled only in that case.
                //
                // A re-add can fail a request in two places:
                //
                // * While it is `waiting_for_trust_quorum`, when the task gate
                //   sees the re-add. The power-off step hasn't run yet, so
                //   `settled_power_off` returns `None` and nothing is added.
                // * While it is `expunging`, when
                //   `sled_expunge_request_complete` sees the re-add. The
                //   power-off step has settled, so we say how.
                //
                // `request` is the task's copy from before the failure is
                // written, so its state is still one of those two. A
                // `powering_off` request is never failed for a re-add, because
                // that step doesn't check the gate.
                let power_off = match request.state.settled_power_off() {
                    None => "",
                    Some(SledExpungePowerOff::Cubby {
                        cubby: _,
                        outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                    }) => " Nexus had powered the sled off.",
                    Some(SledExpungePowerOff::Cubby {
                        cubby: _,
                        outcome: SledExpungeCubbyPowerOffOutcome::AlreadyOff,
                    }) => " Nexus had found the sled already powered off.",
                    Some(SledExpungePowerOff::Cubby {
                        cubby: _,
                        outcome:
                            SledExpungeCubbyPowerOffOutcome::OtherBoardPresent,
                    }) => {
                        " Nexus had found another sled in its cubby, and this \
                         sled in no powered sled cubby of the rack (it may \
                         have been unpowered in one)."
                    }
                    Some(SledExpungePowerOff::Cubby {
                        cubby: _,
                        outcome: SledExpungeCubbyPowerOffOutcome::NotPresent,
                    }) => {
                        " Nexus had found it in no powered sled cubby of the \
                         rack (it may have been unpowered in one)."
                    }
                    Some(SledExpungePowerOff::AbsentAtCreation) => {
                        " Nexus had confirmed that the sled was still in no \
                         powered sled cubby of the rack (it may have been \
                         unpowered in one)."
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
    use gateway_types::component::SpType;
    use nexus_auth::authz;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::helpers::SledUpdateBuilder;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_expect_written;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::backdate_sled_expunge_request;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_with_sp_slot;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::set_sled_expunge_request_state;
    use nexus_db_queries::db::pub_test_utils::trust_quorum::TrustQuorumConfigDriver;
    use nexus_networking::GatewayClient;
    use nexus_types::identity::Asset;
    use nexus_types::internal_api::background::SledExpungeIgnitionSnapshot;
    use nexus_types::internal_api::background::SledExpungeIgnitionUnavailable;
    use nexus_types::internal_api::background::SledExpungeMgsPowerOffFailure;
    use nexus_types::internal_api::background::SledExpungeOtherBoard;
    use nexus_types::internal_api::background::SledExpungeSwitchIdentification;
    use nexus_types::internal_api::background::SledExpungeSwitchMgs;
    use nexus_types::sled_expunge::NonEmptyBaseboardId;
    use nexus_types::sled_expunge::SledCubbyBoard;
    use nexus_types::sled_expunge::SledExpungeRequestState;
    use nexus_types::sled_expunge::SledExpungeSpSlotSource;
    use nexus_types::sled_expunge::SledExpungeUnidentifiedCubbies;
    use nexus_types::sled_expunge::UnidentifiedCubby;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::tcp_proxy::ProxyTarget;
    use omicron_test_utils::dev::tcp_proxy::RetargetableTcpProxy;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledExpungeRequestUuid;
    use omicron_uuid_kinds::SledUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::net::SocketAddrV6;
    use trust_quorum_types::membership::TrustQuorumMembers;
    use uuid::Uuid;

    // TODO-RAINCLAUDE: A rack whose committed epoch 1 trust quorum has five
    // members. `target` has a sled row that is non-provisionable, as request
    // creation leaves it. The epoch 1 coordinator has an in-service sled row so
    // that later configurations can be proposed.
    struct TestRack {
        rack_id: RackUuid,
        members: BTreeSet<BaseboardId>,
        target: BaseboardId,
        sled_id: SledUuid,
    }

    impl TestRack {
        async fn new(db: &TestDatabase) -> Self {
            Self::with_target(db, None).await
        }

        // TODO-RAINCLAUDE: `target`, when given, becomes the sled's baseboard,
        // so that a request can name a simulated gimlet.
        async fn with_target(
            db: &TestDatabase,
            target: Option<&BaseboardId>,
        ) -> Self {
            let (opctx, datastore) = (db.opctx(), db.datastore());
            let rack_id = RackUuid::new_v4();
            let sled = match target {
                Some(target) => {
                    sled_upsert_for_baseboard(datastore, rack_id, target).await
                }
                None => {
                    sled_upsert_expect_written(
                        datastore,
                        SledUpdateBuilder::new().rack_id(rack_id).build(),
                    )
                    .await
                }
            };
            let sled_id = sled.id();
            let target = sled.baseboard_id();
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
                TrustQuorumMembers::new(members.clone())
                    .expect("test membership is within the size bounds"),
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

        // TODO-RAINCLAUDE: Inserts a `powering_off` request whose removal at
        // epoch 2 is committed. An `sp_slot` of `None` means the sled was gone
        // at creation.
        async fn insert_powering_off(
            &self,
            db: &TestDatabase,
            sp_slot: Option<u16>,
        ) -> SledExpungeRequest {
            self.propose_removal(db).await;
            self.tq(db).commit(2).await;
            let request = self
                .insert_request(db, 2, SledExpungeRequestStateKind::PoweringOff)
                .await;
            set_recorded_cubby(db, &request, sp_slot.map(inventory_cubby)).await
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

    const NO_MGS_TEXT: &str = "Nexus could not find the management gateway \
        of either switch in this sled's rack, so it cannot reach the sled's \
        cubby. Nexus will keep retrying; if this persists, contact Oxide \
        support.";

    // TODO-RAINCLAUDE: The status of a waiting request that advances to
    // `powering_off` and then finds no MGS, all in one activation.
    fn chained_no_mgs_status(
        request: &SledExpungeRequest,
    ) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            state_after: SledExpungeStateAfter::Moved {
                state: SledExpungeRequestStateKind::PoweringOff,
            },
            ..no_mgs_status(request)
        }
    }

    fn no_mgs_status(request: &SledExpungeRequest) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            power_check: Some(SledExpungePowerCheck::Waiting {
                wait: SledExpungePowerOffWait::NoMgs,
            }),
            ..status_for(request, no_mgs_outcome())
        }
    }

    fn no_mgs_outcome() -> SledExpungeRequestOutcome {
        SledExpungeRequestOutcome::LastErrorSet {
            last_error: NO_MGS_TEXT.to_string(),
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

    // TODO-RAINCLAUDE: Creates a task with no DNS servers, so its MGS discovery
    // always fails and a `powering_off` request waits with the no-MGS
    // `last_error`. Also returns handles to the task's activators so that tests
    // can check them.
    fn test_task(db: &TestDatabase) -> (SledExpunge, SledExpungeActivators) {
        let activators = test_activators();
        let task = SledExpunge::new(
            db.datastore().clone(),
            no_dns_servers_resolver(db),
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
            .sled_expunge_request_get(db.opctx(), request.id)
            .await
            .expect("read the sled expunge request")
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

    async fn drive_one(
        task: &SledExpunge,
        opctx: &OpContext,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
    ) -> SledExpungeRequestStatus {
        let mgs = ActivationMgs::new(&opctx.log, &task.resolver);
        task.run_passes(opctx, &mgs, vec![request.clone()], now)
            .await
            .into_iter()
            .exactly_one()
            .expect("one listed request yields one status")
    }

    async fn set_recorded_cubby(
        db: &TestDatabase,
        request: &SledExpungeRequest,
        cubby: Option<SledExpungeRecordedCubby>,
    ) -> SledExpungeRequest {
        use nexus_db_model::DbSledExpungeSpSlotSource;
        use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
        use nexus_db_model::SpMgsSlot;
        use nexus_db_model::to_db_typed_uuid;
        use nexus_db_schema::schema::sled_expunge_request::dsl;
        let conn = db
            .datastore()
            .pool_connection_for_tests()
            .await
            .expect("got a database connection");
        let row = diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(to_db_typed_uuid(request.id)))
            .set((
                dsl::sp_slot.eq(cubby.map(|cubby| SpMgsSlot::from(cubby.slot))),
                dsl::sp_slot_source.eq(cubby.map(|cubby| {
                    DbSledExpungeSpSlotSource::from(cubby.source)
                })),
            ))
            .returning(DbSledExpungeRequest::as_returning())
            .get_result_async(&*conn)
            .await
            .expect("set the request's sp_slot and sp_slot_source");
        SledExpungeRequest::try_from(row).unwrap_or_else(|error| {
            panic!("decoded the updated request row: {error:#}")
        })
    }

    fn recorded_cubby(
        slot: u16,
        source: SledExpungeSpSlotSource,
    ) -> SledExpungeRecordedCubby {
        SledExpungeRecordedCubby { slot, source }
    }

    fn inventory_cubby(slot: u16) -> SledExpungeRecordedCubby {
        recorded_cubby(slot, SledExpungeSpSlotSource::Inventory)
    }

    fn completed() -> SledExpungeRequestOutcome {
        SledExpungeRequestOutcome::Completed {
            previous_sled_policy: SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
        }
    }

    fn powered_down_check(
        slot: u16,
        powered_down: SledPoweredDown,
    ) -> SledExpungePowerCheck {
        SledExpungePowerCheck::PoweredDown {
            slot,
            located: SledExpungeCubbyLocated::Recorded {
                source: SledExpungeSpSlotSource::Inventory,
            },
            powered_down,
        }
    }

    fn both_switches_fetched(
        switch0: &GatewayClient,
        switch1: &GatewayClient,
    ) -> SledExpungeSwitchIdentification {
        let fetched = |mgs: &GatewayClient| SledExpungeSwitchMgs::Found {
            mgs: mgs.addr,
            ignition: SledExpungeIgnitionSnapshot::Fetched,
        };
        SledExpungeSwitchIdentification::Identified {
            switch_unknown: IdOrdMap::new(),
            switch0: fetched(switch0),
            switch1: fetched(switch1),
        }
    }

    fn discovered_status<I>(racks: I) -> SledExpungeMgsDiscovery
    where
        I: IntoIterator<Item = (RackUuid, SledExpungeSwitchIdentification)>,
    {
        SledExpungeMgsDiscovery::Discovered {
            rack_id_unknown: IdOrdMap::new(),
            racks: racks
                .into_iter()
                .map(|(rack_id, switches)| SledExpungeRackSwitches {
                    rack_id,
                    switches,
                })
                .collect(),
        }
    }

    const GONE_AT_CREATION_TEXT: &str = "This sled was not found when the \
        request was created, and Nexus cannot yet confirm that it is still \
        gone";

    fn not_in_recorded_cubby_text(slot: u16) -> String {
        format!(
            "Nexus did not find this sled in cubby {slot}, where it was last \
             known to be, and cannot yet confirm that it is gone from the \
             rack"
        )
    }

    fn ignition_unavailable_text(search: &str, switches: &str) -> String {
        format!(
            "{search}, which needs the ignition state of both switches: \
             {switches}. {KEEPS_RETRYING}"
        )
    }

    fn cubby_unidentified_text(search: &str, slot: u16) -> String {
        format!(
            "{search}: the board in sled cubby {slot} could not be \
             identified, so the sled may still be running in it. \
             {KEEPS_RETRYING}"
        )
    }

    fn cubbies_unidentified_text(search: &str, slots: &str) -> String {
        format!(
            "{search}: the boards in sled cubbies {slots} could not be \
             identified, so the sled may still be running in one of them. \
             {KEEPS_RETRYING}"
        )
    }

    fn non_empty(baseboard_id: &BaseboardId) -> NonEmptyBaseboardId {
        NonEmptyBaseboardId::new(baseboard_id.clone())
            .expect("sim gimlets report non-empty baseboards")
    }

    // TODO-RAINCLAUDE: The absence the rack-wide check reports for `sim`.
    // `boards` must list every powered sled cubby and the board in it.
    fn absence(
        sim: &SimRackTwoSwitches,
        boards: &[(u16, &BaseboardId)],
    ) -> SledExpungeRackAbsence {
        SledExpungeRackAbsence {
            switch0_mgs: sim.switch0.addr,
            switch1_mgs: sim.switch1.addr,
            other_boards: boards
                .iter()
                .map(|(slot, board)| SledCubbyBoard {
                    slot: *slot,
                    baseboard_id: non_empty(board),
                })
                .collect(),
        }
    }

    fn unidentified_slots(
        power_check: &Option<SledExpungePowerCheck>,
    ) -> (SledExpungeSearch, Vec<u16>) {
        match power_check {
            Some(SledExpungePowerCheck::Waiting {
                wait:
                    SledExpungePowerOffWait::CubbiesUnidentified { search, cubbies },
            }) => {
                let slots =
                    cubbies
                        .iter()
                        .map(|cubby| {
                            match &cubby.reason {
                        CubbyUnidentified::SpUnreachable { error: _ } => {
                            cubby.slot
                        }
                        reason @ (CubbyUnidentified::EmptyIdentity { .. }
                        | CubbyUnidentified::MgsRequestFailed { .. }
                        | CubbyUnidentified::UnexpectedSystemType {
                            ..
                        }) => {
                            panic!(
                                "expected cubby {} to be unreachable, found \
                                 {reason:?}",
                                cubby.slot,
                            )
                        }
                    }
                        })
                        .collect();
                (search_without_errors(search), slots)
            }
            other @ (None
            | Some(
                SledExpungePowerCheck::PoweredDown { .. }
                | SledExpungePowerCheck::Gone { .. }
                | SledExpungePowerCheck::Waiting {
                    wait:
                        SledExpungePowerOffWait::NoMgs
                        | SledExpungePowerOffWait::NotPoweredOff { .. }
                        | SledExpungePowerOffWait::IgnitionUnavailable { .. }
                        | SledExpungePowerOffWait::SwitchAttachedCubby { .. },
                },
            )) => panic!("expected unidentified cubbies, found {other:?}"),
        }
    }

    fn failure_without_error(
        failure: &MgsPowerOffFailure,
    ) -> MgsPowerOffFailure {
        match failure {
            MgsPowerOffFailure::Unidentified {
                reason: CubbyUnidentified::MgsRequestFailed { error: _ },
            } => mgs_request_failed(),
            MgsPowerOffFailure::Unidentified {
                reason: CubbyUnidentified::SpUnreachable { error: _ },
            } => sp_unreachable(),
            MgsPowerOffFailure::IgnitionPowerOffFailed { error: _ } => {
                MgsPowerOffFailure::IgnitionPowerOffFailed {
                    error: String::new(),
                }
            }
            failure @ (MgsPowerOffFailure::Unidentified {
                reason:
                    CubbyUnidentified::EmptyIdentity { .. }
                    | CubbyUnidentified::UnexpectedSystemType { .. },
            }
            | MgsPowerOffFailure::OtherBoardPresent { .. }) => failure.clone(),
        }
    }

    fn attempts_without_errors(
        attempts: &IdOrdMap<SledExpungeMgsPowerOffFailure>,
    ) -> IdOrdMap<SledExpungeMgsPowerOffFailure> {
        attempts
            .iter()
            .map(|attempt| SledExpungeMgsPowerOffFailure {
                mgs: attempt.mgs,
                failure: failure_without_error(&attempt.failure),
            })
            .collect()
    }

    fn search_without_errors(search: &SledExpungeSearch) -> SledExpungeSearch {
        match search {
            SledExpungeSearch::GoneAtCreation => {
                SledExpungeSearch::GoneAtCreation
            }
            SledExpungeSearch::NotInRecordedCubby {
                slot,
                source,
                other_board,
                failed_attempts,
            } => SledExpungeSearch::NotInRecordedCubby {
                slot: *slot,
                source: *source,
                other_board: other_board.clone(),
                failed_attempts: attempts_without_errors(failed_attempts),
            },
        }
    }

    fn gone_without_errors(
        mut status: SledExpungeRequestStatus,
    ) -> SledExpungeRequestStatus {
        status.power_check = match status.power_check {
            Some(SledExpungePowerCheck::Gone { search, absence }) => {
                Some(SledExpungePowerCheck::Gone {
                    search: search_without_errors(&search),
                    absence,
                })
            }
            check @ (None
            | Some(
                SledExpungePowerCheck::PoweredDown { .. }
                | SledExpungePowerCheck::Waiting { .. },
            )) => check,
        };
        status
    }

    fn failures<I>(entries: I) -> IdOrdMap<SledExpungeMgsPowerOffFailure>
    where
        I: IntoIterator<Item = (SocketAddrV6, MgsPowerOffFailure)>,
    {
        entries
            .into_iter()
            .map(|(mgs, failure)| SledExpungeMgsPowerOffFailure {
                mgs,
                failure,
            })
            .collect()
    }

    fn sp_unreachable() -> MgsPowerOffFailure {
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::SpUnreachable { error: String::new() },
        }
    }

    fn mgs_request_failed() -> MgsPowerOffFailure {
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::MgsRequestFailed {
                error: String::new(),
            },
        }
    }

    // TODO-RAINCLAUDE: This shows how sleds that were gone at creation are
    // checked live, against a simulated rack behind two MGSes.
    //
    // The request whose sled is found runs in an activation of its own.
    // Otherwise its power-off would race the other requests' reads against the
    // shared simulated rack.
    #[tokio::test]
    async fn test_sled_expunge_gone_at_creation_live() {
        use sp_sim::SimulatedSp;
        let logctx =
            dev::test_setup_log("test_sled_expunge_gone_at_creation_live");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &logctx.log;
        let sim = sim_rack_with_gimlets(
            "test_sled_expunge_gone_at_creation_live",
            log,
            3,
        )
        .await;
        let both = sim.both();
        let refusing = RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
            .await
            .expect("bound a refusing TCP proxy");
        let down = GatewayClient::from_addr(log, refusing.local_addr());
        let gimlet0 = sim_gimlet_baseboard(&sim, 0).await;
        let gimlet1 = sim_gimlet_baseboard(&sim, 1).await;
        let gimlet2 = sim_gimlet_baseboard(&sim, 2).await;
        sim.rack.simrack.gimlets[2]
            .set_responsiveness(sp_sim::Responsiveness::Unresponsive)
            .await;

        let reappeared = TestRack::with_target(&db, Some(&gimlet1)).await;
        let reappeared_request =
            reappeared.insert_powering_off(&db, None).await;
        let gone = TestRack::new(&db).await;
        let gone_request = gone.insert_powering_off(&db, None).await;
        let no_ignition = TestRack::new(&db).await;
        let no_ignition_request =
            no_ignition.insert_powering_off(&db, None).await;

        let mgs = discovered(log, &task.resolver, &[reappeared.rack_id], &both);
        assert_eq!(
            drive_activation(
                &task,
                db.opctx(),
                &mgs,
                vec![reappeared_request.clone()],
            )
            .await,
            statuses([SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::PoweredDown {
                    slot: 1,
                    located: SledExpungeCubbyLocated::FoundAfterGoneAtCreation,
                    powered_down: SledPoweredDown::PoweredOff {
                        mgs: sim.first().addr,
                    },
                }),
                ..reappeared.status(&reappeared_request, completed())
            }]),
        );

        let mgs = discovered_per_rack(
            log,
            &task.resolver,
            &[
                (gone.rack_id, &both),
                (no_ignition.rack_id, std::slice::from_ref(&down)),
            ],
        );
        let found = drive_activation(
            &task,
            db.opctx(),
            &mgs,
            vec![gone_request.clone(), no_ignition_request.clone()],
        )
        .await;
        let gone_status =
            found.get(&gone_request.id).expect("request was driven");
        assert_eq!(
            gone_status.outcome,
            SledExpungeRequestOutcome::LastErrorSet {
                last_error: cubby_unidentified_text(GONE_AT_CREATION_TEXT, 2),
            },
        );
        assert_eq!(
            unidentified_slots(&gone_status.power_check),
            (SledExpungeSearch::GoneAtCreation, vec![2]),
        );
        assert_eq!(
            found.get(&no_ignition_request.id),
            Some(&SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::Waiting {
                    wait: SledExpungePowerOffWait::IgnitionUnavailable {
                        search: SledExpungeSearch::GoneAtCreation,
                        switches: SledExpungeIgnitionUnavailable::Both {
                            switch0: SledExpungeIgnitionGap::NoMgs,
                            switch1: SledExpungeIgnitionGap::NoMgs,
                        },
                    },
                }),
                ..no_ignition.status(
                    &no_ignition_request,
                    SledExpungeRequestOutcome::LastErrorSet {
                        last_error: ignition_unavailable_text(
                            GONE_AT_CREATION_TEXT,
                            "no management gateway was found for switch 0; \
                             no management gateway was found for switch 1",
                        ),
                    },
                )
            }),
        );
        let recorded = read(&db, &reappeared_request).await;
        assert_eq!(
            (recorded.state.kind(), recorded.state.settled_power_off(),),
            (
                SledExpungeRequestStateKind::Done,
                Some(SledExpungePowerOff::Cubby {
                    cubby: recorded_cubby(
                        1,
                        SledExpungeSpSlotSource::FoundAfterAbsent,
                    ),
                    outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                }),
            ),
        );
        let waiting = read(&db, &gone_request).await;
        assert_eq!(
            (
                waiting.state.kind(),
                waiting.state.recorded_cubby(),
                waiting.state.last_error(),
            ),
            (
                SledExpungeRequestStateKind::PoweringOff,
                None,
                Some(
                    cubby_unidentified_text(GONE_AT_CREATION_TEXT, 2).as_str()
                ),
            ),
        );

        // TODO-RAINCLAUDE: Switch 1's MGS is down, so switch 1's ignition list
        // is missing and the sled can't be confirmed gone.
        sim.rack.simrack.gimlets[2]
            .set_responsiveness(sp_sim::Responsiveness::Responsive)
            .await;
        let switch1_proxy =
            RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
                .await
                .expect("bound a TCP proxy for switch 1's MGS");
        let switch1 = GatewayClient::from_addr(log, switch1_proxy.local_addr());
        let with_switch1 = [sim.switch0.clone(), switch1.clone()];
        let mgs =
            discovered(log, &task.resolver, &[gone.rack_id], &with_switch1);
        assert_eq!(
            drive_activation(&task, db.opctx(), &mgs, vec![waiting.clone()])
                .await,
            statuses([SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::Waiting {
                    wait: SledExpungePowerOffWait::IgnitionUnavailable {
                        search: SledExpungeSearch::GoneAtCreation,
                        switches: SledExpungeIgnitionUnavailable::Switch1 {
                            switch1: SledExpungeIgnitionGap::NoMgs,
                        },
                    },
                }),
                ..gone.status(
                    &waiting,
                    SledExpungeRequestOutcome::LastErrorSet {
                        last_error: ignition_unavailable_text(
                            GONE_AT_CREATION_TEXT,
                            "no management gateway was found for switch 1",
                        ),
                    },
                )
            }]),
        );
        let still_waiting = read(&db, &gone_request).await;
        assert_eq!(
            (still_waiting.state.kind(), still_waiting.state.recorded_cubby()),
            (SledExpungeRequestStateKind::PoweringOff, None),
        );

        // TODO-RAINCLAUDE: Switch 1's MGS is back. Cubby 1 was powered off
        // above, so only cubbies 0 and 2 are identified.
        switch1_proxy
            .set_target(ProxyTarget::Forward(sim.rack.mgs1.address().into()))
            .await;
        let mgs =
            discovered(log, &task.resolver, &[gone.rack_id], &with_switch1);
        assert_eq!(
            drive_activation(
                &task,
                db.opctx(),
                &mgs,
                vec![still_waiting.clone()]
            )
            .await,
            statuses([SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::Gone {
                    search: SledExpungeSearch::GoneAtCreation,
                    absence: SledExpungeRackAbsence {
                        switch0_mgs: sim.switch0.addr,
                        switch1_mgs: switch1.addr,
                        other_boards: [(0, &gimlet0), (2, &gimlet2)]
                            .into_iter()
                            .map(|(slot, board)| SledCubbyBoard {
                                slot,
                                baseboard_id: non_empty(board),
                            })
                            .collect(),
                    },
                }),
                ..gone.status(&still_waiting, completed())
            }]),
        );
        assert_eq!(
            mgs.status(),
            discovered_status([(
                gone.rack_id,
                both_switches_fetched(&sim.switch0, &switch1),
            )]),
        );
        let done = read(&db, &gone_request).await;
        assert_eq!(
            (done.state.kind(), done.state.settled_power_off()),
            (
                SledExpungeRequestStateKind::Done,
                Some(SledExpungePowerOff::AbsentAtCreation),
            ),
        );

        switch1_proxy.shutdown().await;
        refusing.shutdown().await;
        sim.teardown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: The two-MGS simulated rack, with a client for each MGS.
    // `first()` returns the client whose address sorts first, which is the MGS
    // a successful power-off goes through.
    struct SimRackTwoSwitches {
        rack: gateway_test_utils::setup::TwoSwitchSimRack,
        switch0: GatewayClient,
        switch1: GatewayClient,
    }

    impl SimRackTwoSwitches {
        fn both(&self) -> [GatewayClient; 2] {
            [self.switch0.clone(), self.switch1.clone()]
        }

        fn first(&self) -> &GatewayClient {
            if self.switch0.addr < self.switch1.addr {
                &self.switch0
            } else {
                &self.switch1
            }
        }

        async fn teardown(self) {
            self.rack
                .teardown()
                .await
                .expect("simulated rack was torn down cleanly");
        }
    }

    async fn sim_rack_with_gimlets(
        test_name: &str,
        log: &Logger,
        count: usize,
    ) -> SimRackTwoSwitches {
        let rack = gateway_test_utils::setup::TwoSwitchSimRack::start(
            test_name, log, count,
        )
        .await;
        let switch0 = GatewayClient::from_addr(log, rack.mgs0.address());
        let switch1 = GatewayClient::from_addr(log, rack.mgs1.address());
        SimRackTwoSwitches { rack, switch0, switch1 }
    }

    async fn sim_gimlet_baseboard(
        sim: &SimRackTwoSwitches,
        slot: usize,
    ) -> BaseboardId {
        use sp_sim::SimulatedSp;
        let state = sim.rack.simrack.gimlets[slot].state().await;
        BaseboardId {
            part_number: state.model,
            serial_number: state.serial_number,
        }
    }

    fn discovered<'r>(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
        racks: &[RackUuid],
        mgs: &[GatewayClient],
    ) -> ActivationMgs<'r> {
        let racks: Vec<_> =
            racks.iter().map(|rack_id| (*rack_id, mgs)).collect();
        discovered_per_rack(log, resolver, &racks)
    }

    fn discovered_per_rack<'r>(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
        racks: &[(RackUuid, &[GatewayClient])],
    ) -> ActivationMgs<'r> {
        let racks = racks
            .iter()
            .map(|(rack_id, mgs)| RackMgs {
                rack_id: *rack_id,
                gateways: SledPowerOffGateways::new(mgs, IdOrdMap::new()),
            })
            .collect();
        ActivationMgs::with_discovery(
            log,
            resolver,
            MgsDiscovery::Discovered {
                racks,
                rack_id_unknown: IdOrdMap::new(),
            },
        )
    }

    async fn drive_activation(
        task: &SledExpunge,
        opctx: &OpContext,
        mgs: &ActivationMgs<'_>,
        requests: Vec<SledExpungeRequest>,
    ) -> IdOrdMap<SledExpungeRequestStatus> {
        task.run_passes(opctx, mgs, requests, now_db_precision()).await
    }

    // TODO-RAINCLAUDE: This shows each way a request with a recorded cubby can
    // settle. Each request is in its own rack. The first activation changes no
    // cubby's power, so every rack-wide check in it sees the same rack.
    #[tokio::test]
    async fn test_sled_expunge_recorded_cubby_outcomes() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_recorded_cubby_outcomes");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &logctx.log;
        let sim = sim_rack_with_gimlets(
            "test_sled_expunge_recorded_cubby_outcomes",
            log,
            4,
        )
        .await;
        let up = sim.first().clone();
        let gimlets = [
            sim_gimlet_baseboard(&sim, 0).await,
            sim_gimlet_baseboard(&sim, 1).await,
            sim_gimlet_baseboard(&sim, 2).await,
            sim_gimlet_baseboard(&sim, 3).await,
        ];
        up.client
            .sp_power_state_set(
                &SpType::Sled,
                0,
                gateway_client::types::PowerState::A2,
            )
            .await
            .expect("put sim gimlet 0 in A2");
        up.client
            .ignition_command(
                &SpType::Sled,
                2,
                gateway_client::types::IgnitionCommand::PowerOff,
            )
            .await
            .expect("powered off cubby 2");

        let already_off = TestRack::with_target(&db, Some(&gimlets[0])).await;
        let other_board = TestRack::new(&db).await;
        let not_present = TestRack::with_target(&db, Some(&gimlets[2])).await;
        let powered_on_cubbies = absence(
            &sim,
            &[(0, &gimlets[0]), (1, &gimlets[1]), (3, &gimlets[3])],
        );
        let first_activation = [
            (
                &already_off,
                inventory_cubby(0),
                powered_down_check(
                    0,
                    SledPoweredDown::AlreadyOff { mgs: up.addr },
                ),
                SledExpungeCubbyPowerOffOutcome::AlreadyOff,
            ),
            (
                &other_board,
                inventory_cubby(0),
                SledExpungePowerCheck::Gone {
                    search: SledExpungeSearch::NotInRecordedCubby {
                        slot: 0,
                        source: SledExpungeSpSlotSource::Inventory,
                        other_board: Some(SledExpungeOtherBoard {
                            mgs: up.addr,
                            found: non_empty(&gimlets[0]),
                        }),
                        failed_attempts: IdOrdMap::new(),
                    },
                    absence: powered_on_cubbies.clone(),
                },
                SledExpungeCubbyPowerOffOutcome::OtherBoardPresent,
            ),
            (
                &not_present,
                recorded_cubby(2, SledExpungeSpSlotSource::FoundAfterAbsent),
                SledExpungePowerCheck::Gone {
                    search: SledExpungeSearch::NotInRecordedCubby {
                        slot: 2,
                        source: SledExpungeSpSlotSource::FoundAfterAbsent,
                        other_board: None,
                        failed_attempts: failures([
                            (sim.switch0.addr, sp_unreachable()),
                            (sim.switch1.addr, sp_unreachable()),
                        ]),
                    },
                    absence: powered_on_cubbies.clone(),
                },
                SledExpungeCubbyPowerOffOutcome::NotPresent,
            ),
        ];
        let mut requests = Vec::new();
        let mut expected = Vec::new();
        for (rack, cubby, check, _) in &first_activation {
            let request = rack.insert_powering_off(&db, None).await;
            let request = set_recorded_cubby(&db, &request, Some(*cubby)).await;
            expected.push(SledExpungeRequestStatus {
                power_check: Some(check.clone()),
                ..rack.status(&request, completed())
            });
            requests.push(request);
        }
        let racks: Vec<RackUuid> =
            first_activation.iter().map(|(rack, ..)| rack.rack_id).collect();
        let mgs = discovered(log, &task.resolver, &racks, &sim.both());
        assert_eq!(
            drive_activation(&task, db.opctx(), &mgs, requests.clone())
                .await
                .into_iter()
                .map(gone_without_errors)
                .collect::<IdOrdMap<_>>(),
            statuses(expected),
        );
        assert_eq!(
            mgs.status(),
            discovered_status([
                (
                    other_board.rack_id,
                    both_switches_fetched(&sim.switch0, &sim.switch1),
                ),
                (
                    not_present.rack_id,
                    both_switches_fetched(&sim.switch0, &sim.switch1),
                ),
            ]),
        );
        for (request, (_, cubby, _, outcome)) in
            requests.iter().zip(&first_activation)
        {
            let done = read(&db, request).await;
            assert_eq!(
                (done.state.kind(), done.state.settled_power_off(),),
                (
                    SledExpungeRequestStateKind::Done,
                    Some(SledExpungePowerOff::Cubby {
                        cubby: *cubby,
                        outcome: *outcome,
                    }),
                ),
            );
        }

        let powered_off = TestRack::with_target(&db, Some(&gimlets[1])).await;
        let relocated = TestRack::with_target(&db, Some(&gimlets[3])).await;
        let powered_off_request =
            powered_off.insert_powering_off(&db, None).await;
        let powered_off_request = set_recorded_cubby(
            &db,
            &powered_off_request,
            Some(recorded_cubby(1, SledExpungeSpSlotSource::LiveScan)),
        )
        .await;
        let relocated_request =
            relocated.insert_powering_off(&db, Some(0)).await;
        let mgs = discovered(
            log,
            &task.resolver,
            &[powered_off.rack_id, relocated.rack_id],
            &sim.both(),
        );
        assert_eq!(
            drive_activation(
                &task,
                db.opctx(),
                &mgs,
                vec![powered_off_request.clone(), relocated_request.clone()],
            )
            .await,
            statuses([
                SledExpungeRequestStatus {
                    power_check: Some(SledExpungePowerCheck::PoweredDown {
                        slot: 1,
                        located: SledExpungeCubbyLocated::Recorded {
                            source: SledExpungeSpSlotSource::LiveScan,
                        },
                        powered_down: SledPoweredDown::PoweredOff {
                            mgs: up.addr,
                        },
                    }),
                    ..powered_off.status(&powered_off_request, completed())
                },
                SledExpungeRequestStatus {
                    power_check: Some(SledExpungePowerCheck::PoweredDown {
                        slot: 3,
                        located: SledExpungeCubbyLocated::Relocated {
                            recorded_slot: 0,
                        },
                        powered_down: SledPoweredDown::PoweredOff {
                            mgs: up.addr,
                        },
                    }),
                    ..relocated.status(&relocated_request, completed())
                },
            ]),
        );
        let done = read(&db, &powered_off_request).await;
        assert_eq!(
            (done.state.kind(), done.state.settled_power_off()),
            (
                SledExpungeRequestStateKind::Done,
                Some(SledExpungePowerOff::Cubby {
                    cubby: recorded_cubby(1, SledExpungeSpSlotSource::LiveScan),
                    outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                }),
            ),
        );
        let done = read(&db, &relocated_request).await;
        assert_eq!(
            (done.state.kind(), done.state.settled_power_off()),
            (
                SledExpungeRequestStateKind::Done,
                Some(SledExpungePowerOff::Cubby {
                    cubby: recorded_cubby(
                        3,
                        SledExpungeSpSlotSource::Relocated
                    ),
                    outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                }),
            ),
        );

        let ignition = up
            .client
            .ignition_list()
            .await
            .expect("listed ignition state")
            .into_inner();
        let powered: Vec<(u16, bool)> = ignition
            .iter()
            .filter(|info| info.id.typ == SpType::Sled)
            .map(|info| match info.details {
                gateway_types::ignition::SpIgnition::Present {
                    power, ..
                } => (info.id.slot, power),
                gateway_types::ignition::SpIgnition::Absent => {
                    panic!("sim cubby {} is absent", info.id.slot)
                }
            })
            .collect();
        assert_eq!(
            powered,
            vec![(0, true), (1, false), (2, false), (3, false)],
            "only cubbies 1 and 3 were powered off by the task",
        );

        sim.teardown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: Three requests in one rack go through one shared
    // rack-wide check. Their SPs don't answer while their cubbies stay powered,
    // so each request waits, naming all three cubbies.
    #[tokio::test]
    async fn test_sled_expunge_shares_rack_wide_check() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_shares_rack_wide_check");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &logctx.log;
        let sim = sim_rack_with_gimlets(
            "test_sled_expunge_shares_rack_wide_check",
            log,
            3,
        )
        .await;
        let rack_id = RackUuid::new_v4();
        let mut requests = Vec::new();
        for slot in 0..3u16 {
            use sp_sim::SimulatedSp;
            sim.rack.simrack.gimlets[usize::from(slot)]
                .set_responsiveness(sp_sim::Responsiveness::Unresponsive)
                .await;
            requests.push(
                insert_sled_expunge_request_with_sp_slot(
                    db.datastore(),
                    rack_id,
                    &BaseboardId {
                        part_number: "test-part".to_string(),
                        serial_number: Uuid::new_v4().to_string(),
                    },
                    Some(slot),
                    Epoch(u64::from(slot) + 2),
                    SledExpungeRequestStateKind::PoweringOff,
                )
                .await,
            );
        }

        let mgs = discovered(log, &task.resolver, &[rack_id], &sim.both());
        let found =
            drive_activation(&task, db.opctx(), &mgs, requests.clone()).await;
        for request in &requests {
            let status = found.get(&request.id).expect("request was driven");
            let slot = request
                .state
                .recorded_cubby()
                .expect("request has a cubby")
                .slot;
            assert_eq!(
                status.outcome,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: cubbies_unidentified_text(
                        &not_in_recorded_cubby_text(slot),
                        "0, 1, 2",
                    ),
                },
            );
            assert_eq!(
                unidentified_slots(&status.power_check),
                (
                    SledExpungeSearch::NotInRecordedCubby {
                        slot,
                        source: SledExpungeSpSlotSource::Inventory,
                        other_board: None,
                        failed_attempts: failures([
                            (sim.switch0.addr, sp_unreachable()),
                            (sim.switch1.addr, sp_unreachable()),
                        ]),
                    },
                    vec![0, 1, 2],
                ),
            );
        }
        assert_eq!(
            mgs.status(),
            discovered_status([(
                rack_id,
                both_switches_fetched(&sim.switch0, &sim.switch1),
            )])
        );

        sim.teardown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: This shows that ruling a sled out needs both switches'
    // ignition lists. While switch 1's MGS is down, the request waits. Once it
    // is back, the request settles as `NotPresent`.
    #[tokio::test]
    async fn test_sled_expunge_not_present_needs_both_switches() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_not_present_needs_both_switches",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &logctx.log;
        let sim = sim_rack_with_gimlets(
            "test_sled_expunge_not_present_needs_both_switches",
            log,
            3,
        )
        .await;
        let gimlet0 = sim_gimlet_baseboard(&sim, 0).await;
        let gimlet1 = sim_gimlet_baseboard(&sim, 1).await;
        let gimlet2 = sim_gimlet_baseboard(&sim, 2).await;
        sim.switch0
            .client
            .ignition_command(
                &SpType::Sled,
                2,
                gateway_client::types::IgnitionCommand::PowerOff,
            )
            .await
            .expect("powered off cubby 2");
        let rack = TestRack::with_target(&db, Some(&gimlet2)).await;
        let request = rack.insert_powering_off(&db, Some(2)).await;

        let switch1_proxy =
            RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
                .await
                .expect("bound a TCP proxy for switch 1's MGS");
        let switch1 = GatewayClient::from_addr(log, switch1_proxy.local_addr());
        let with_switch1 = [sim.switch0.clone(), switch1.clone()];
        let mgs =
            discovered(log, &task.resolver, &[rack.rack_id], &with_switch1);
        let last_error = ignition_unavailable_text(
            &not_in_recorded_cubby_text(2),
            "no management gateway was found for switch 1",
        );
        let found =
            drive_activation(&task, db.opctx(), &mgs, vec![request.clone()])
                .await;
        let status = found.get(&request.id).expect("request was driven");
        assert_eq!(
            status.outcome,
            SledExpungeRequestOutcome::LastErrorSet {
                last_error: last_error.clone(),
            },
        );
        match &status.power_check {
            Some(SledExpungePowerCheck::Waiting {
                wait:
                    SledExpungePowerOffWait::IgnitionUnavailable {
                        search:
                            SledExpungeSearch::NotInRecordedCubby {
                                slot: 2,
                                source: SledExpungeSpSlotSource::Inventory,
                                other_board: None,
                                failed_attempts,
                            },
                        switches,
                    },
            }) => {
                assert_eq!(
                    attempts_without_errors(failed_attempts),
                    failures([
                        (sim.switch0.addr, sp_unreachable()),
                        (switch1.addr, mgs_request_failed()),
                    ]),
                );
                assert_eq!(
                    *switches,
                    SledExpungeIgnitionUnavailable::Switch1 {
                        switch1: SledExpungeIgnitionGap::NoMgs,
                    },
                );
            }
            other @ (None
            | Some(
                SledExpungePowerCheck::PoweredDown { .. }
                | SledExpungePowerCheck::Gone { .. }
                | SledExpungePowerCheck::Waiting {
                    wait:
                        SledExpungePowerOffWait::NoMgs
                        | SledExpungePowerOffWait::NotPoweredOff { .. }
                        | SledExpungePowerOffWait::IgnitionUnavailable { .. }
                        | SledExpungePowerOffWait::CubbiesUnidentified { .. }
                        | SledExpungePowerOffWait::SwitchAttachedCubby { .. },
                },
            )) => panic!("unexpected power check {other:?}"),
        }
        let waiting = read(&db, &request).await;
        assert_eq!(
            (
                waiting.state.kind(),
                waiting.state.recorded_cubby(),
                waiting.state.last_error(),
            ),
            (
                SledExpungeRequestStateKind::PoweringOff,
                Some(inventory_cubby(2)),
                Some(last_error.as_str()),
            ),
        );

        switch1_proxy
            .set_target(ProxyTarget::Forward(sim.rack.mgs1.address().into()))
            .await;
        let mgs =
            discovered(log, &task.resolver, &[rack.rack_id], &with_switch1);
        assert_eq!(
            drive_activation(&task, db.opctx(), &mgs, vec![waiting.clone()])
                .await
                .into_iter()
                .map(gone_without_errors)
                .collect::<IdOrdMap<_>>(),
            statuses([SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::Gone {
                    search: SledExpungeSearch::NotInRecordedCubby {
                        slot: 2,
                        source: SledExpungeSpSlotSource::Inventory,
                        other_board: None,
                        failed_attempts: failures([
                            (sim.switch0.addr, sp_unreachable()),
                            (switch1.addr, sp_unreachable()),
                        ]),
                    },
                    absence: SledExpungeRackAbsence {
                        switch0_mgs: sim.switch0.addr,
                        switch1_mgs: switch1.addr,
                        other_boards: [(0, &gimlet0), (1, &gimlet1)]
                            .into_iter()
                            .map(|(slot, board)| SledCubbyBoard {
                                slot,
                                baseboard_id: non_empty(board),
                            })
                            .collect(),
                    },
                }),
                ..rack.status(&waiting, completed())
            }]),
        );
        let done = read(&db, &request).await;
        assert_eq!(
            (done.state.kind(), done.state.settled_power_off()),
            (
                SledExpungeRequestStateKind::Done,
                Some(SledExpungePowerOff::Cubby {
                    cubby: inventory_cubby(2),
                    outcome: SledExpungeCubbyPowerOffOutcome::NotPresent,
                }),
            ),
        );

        switch1_proxy.shutdown().await;
        sim.teardown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: The simulator can't put a sled in a switch-attached
    // cubby, so this test injects the rack-wide check's result. A sled found in
    // a switch-attached cubby waits as a scrimlet.
    #[tokio::test]
    async fn test_sled_expunge_found_in_switch_attached_cubby() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_found_in_switch_attached_cubby",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let opctx = db.opctx();
        let gateways = SledPowerOffGateways::new(&[], IdOrdMap::new());
        for sp_slot in [None, Some(3)] {
            let rack = TestRack::new(&db).await;
            let request = rack.insert_powering_off(&db, sp_slot).await;
            let search = match sp_slot {
                None => RackWideSearch::GoneAtCreation,
                Some(slot) => RackWideSearch::NotInRecordedCubby {
                    cubby: inventory_cubby(slot),
                    other_board: None,
                    failed_attempts: IdOrdMap::new(),
                },
            };
            let mut progress = RequestProgress::default();
            let outcome = task
                .act_on_presence(
                    opctx,
                    &opctx.log,
                    &request,
                    &gateways,
                    RackWideCheck {
                        search,
                        presence: Ok(SledExpungeLivePresence::InCubby {
                            slot: 14,
                        }),
                    },
                    &mut progress,
                )
                .await;
            assert_eq!(
                outcome,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: switch_attached_cubby_text(14),
                },
                "recorded cubby {sp_slot:?}",
            );
            assert_eq!(
                progress.power_check,
                Some(SledExpungePowerCheck::Waiting {
                    wait: SledExpungePowerOffWait::SwitchAttachedCubby {
                        slot: 14,
                    },
                }),
                "recorded cubby {sp_slot:?}",
            );
            let waiting = read(&db, &request).await;
            assert_eq!(
                (waiting.state.kind(), waiting.state.recorded_cubby()),
                (
                    SledExpungeRequestStateKind::PoweringOff,
                    sp_slot.map(inventory_cubby),
                ),
                "recorded cubby {sp_slot:?}",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: The task passes a stale copy of a request that was
    // gone at creation, and the rack-wide check finds the sled in cubby 5. The
    // compare-and-swap loses to the stored row, and the loss counts as this
    // call's own write only when the row holds both the slot and the source
    // that this call would have written.
    #[tokio::test]
    async fn test_sled_expunge_record_sp_slot_lost_to_same_cubby() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_record_sp_slot_lost_to_same_cubby",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let opctx = db.opctx();
        let gateways = SledPowerOffGateways::new(&[], IdOrdMap::new());
        let own_write = SledExpungePowerOffWait::NotPoweredOff {
            slot: 5,
            located: SledExpungeCubbyLocated::FoundAfterGoneAtCreation,
            failed_attempts: IdOrdMap::new(),
        };
        let lost = SledExpungeRequestOutcome::NotInExpectedState {
            current_state: SledExpungeRequestStateKind::PoweringOff,
        };
        let cases = [
            (
                recorded_cubby(5, SledExpungeSpSlotSource::FoundAfterAbsent),
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: power_off_wait_text(&own_write),
                },
                Some(SledExpungePowerCheck::Waiting { wait: own_write }),
            ),
            (
                recorded_cubby(5, SledExpungeSpSlotSource::Relocated),
                lost.clone(),
                None,
            ),
            (
                recorded_cubby(6, SledExpungeSpSlotSource::FoundAfterAbsent),
                lost,
                None,
            ),
        ];
        for (row_cubby, expected_outcome, expected_power_check) in cases {
            let rack = TestRack::new(&db).await;
            let stale = rack.insert_powering_off(&db, None).await;
            set_recorded_cubby(&db, &stale, Some(row_cubby)).await;
            let mut progress = RequestProgress::default();
            let outcome = task
                .act_on_presence(
                    opctx,
                    &opctx.log,
                    &stale,
                    &gateways,
                    RackWideCheck {
                        search: RackWideSearch::GoneAtCreation,
                        presence: Ok(SledExpungeLivePresence::InCubby {
                            slot: 5,
                        }),
                    },
                    &mut progress,
                )
                .await;
            assert_eq!(outcome, expected_outcome, "row cubby {row_cubby:?}");
            assert_eq!(
                progress.power_check, expected_power_check,
                "row cubby {row_cubby:?}",
            );
            let after = read(&db, &stale).await;
            assert_eq!(
                (after.state.kind(), after.state.recorded_cubby()),
                (SledExpungeRequestStateKind::PoweringOff, Some(row_cubby)),
                "row cubby {row_cubby:?}",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
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
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
            )
            .await;
        let request =
            backdate_sled_expunge_request(db.datastore(), &inserted).await;
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
                state: SledExpungeRequestState::WaitingForTrustQuorum {
                    cubby: request.state.recorded_cubby(),
                    last_error: Some(stuck.clone()),
                },
                time_modified: flagged.time_modified,
                ..request.clone()
            },
        );
        assert!(flagged.time_modified > request.time_modified);

        // TODO-RAINCLAUDE: This activation also runs at `threshold_at`, which
        // shows that the threshold is measured from `time_created` and not from
        // `time_modified`. The message is the same as before, so it isn't
        // rewritten and `time_modified` stays put.
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

    // TODO-RAINCLAUDE: This drives one request for each way the task can act on
    // a request, all in one activation.
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
        removed.tq(&db).ack_all_prepares(2).await;
        let removed_request = removed
            .insert_request(
                &db,
                2,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
            )
            .await;
        expected.push(chained_no_mgs_status(&removed_request));

        let aborted = TestRack::new(&db).await;
        aborted.propose_removal(&db).await;
        aborted.tq(&db).abort(2).await;
        let aborted_request = aborted
            .insert_request(
                &db,
                2,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
            )
            .await;
        expected.push(aborted.status(
            &aborted_request,
            SledExpungeRequestOutcome::Failed {
                failure_reason: aborted.removal_aborted_text(),
            },
        ));

        let missing_config = TestRack::new(&db).await;
        let missing_config_request = missing_config
            .insert_request(
                &db,
                9,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
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
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
            )
            .await;
        expected.push(preparing.status(
            &preparing_request,
            SledExpungeRequestOutcome::RemovalPreparing,
        ));

        // TODO-RAINCLAUDE: Each re-add is proposed before the request row
        // exists, because the trust quorum refuses to add a sled back while it
        // has an unfinished request (see
        // `check_no_new_member_being_expunged_conn`).
        for (state, committing_power_off_sentence) in [
            (SledExpungeRequestStateKind::WaitingForTrustQuorum, None),
            (SledExpungeRequestStateKind::WaitingForTrustQuorum, Some("")),
            (SledExpungeRequestStateKind::Expunging, None),
            (
                SledExpungeRequestStateKind::Expunging,
                Some(" Nexus had powered the sled off."),
            ),
        ] {
            let rack = TestRack::new(&db).await;
            rack.propose_removal(&db).await;
            rack.tq(&db).commit(2).await;
            rack.tq(&db).propose(3, 2, &rack.members).await;
            let request = rack.insert_request(&db, 2, state).await;
            let outcome = match committing_power_off_sentence {
                None => SledExpungeRequestOutcome::LastErrorSet {
                    last_error: rack.re_add_preparing_text(),
                },
                Some(sentence) => {
                    rack.tq(&db).ack_all_prepares(3).await;
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
        powering_off.tq(&db).commit(2).await;
        let powering_off_request = powering_off
            .insert_request(&db, 2, SledExpungeRequestStateKind::PoweringOff)
            .await;
        expected.push(no_mgs_status(&powering_off_request));

        let expunging = TestRack::new(&db).await;
        expunging.propose_removal(&db).await;
        expunging.tq(&db).commit(2).await;
        let expunging_request = expunging
            .insert_request(&db, 2, SledExpungeRequestStateKind::Expunging)
            .await;
        expected.push(expunging.status(
            &expunging_request,
            SledExpungeRequestOutcome::Completed {
                previous_sled_policy: SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::NonProvisionable,
                },
            },
        ));

        // TODO-RAINCLAUDE: This request points at epoch 1, the committed RSS
        // configuration that still includes the sled. The gate reports that as
        // an internal error.
        let inconsistent = TestRack::new(&db).await;
        let inconsistent_request = inconsistent
            .insert_request(
                &db,
                1,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
            )
            .await;
        expected.push(inconsistent.status(
            &inconsistent_request,
            SledExpungeRequestOutcome::Error {
                error: format!(
                    "checking the trust quorum gate: Internal Error: sled \
                     expunge request {} for {} (rack {}, removal epoch 1): \
                     the removal configuration (epoch 1, state Committed) \
                     still includes the sled",
                    inconsistent_request.id,
                    inconsistent.target,
                    inconsistent.rack_id,
                ),
            },
        ));

        let missing_sled = TestRack::new(&db).await;
        missing_sled.propose_removal(&db).await;
        missing_sled.tq(&db).abort(2).await;
        let missing_sled_request = missing_sled
            .insert_request_for_sled(
                &db,
                SledUuid::new_v4(),
                2,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
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

        match task.actually_activate(db.opctx(), now_db_precision()).await {
            SledExpungeStatus::Listed {
                requests,
                mgs_discovery: SledExpungeMgsDiscovery::Failed { .. },
            } => {
                assert_eq!(requests, statuses(expected));
            }
            other @ (SledExpungeStatus::ListError { .. }
            | SledExpungeStatus::Listed {
                mgs_discovery:
                    SledExpungeMgsDiscovery::NotNeeded
                    | SledExpungeMgsDiscovery::Unfinished
                    | SledExpungeMgsDiscovery::Discovered { .. },
                ..
            }) => panic!("expected failed MGS discovery, got {other:?}"),
        }
        activators.instance_watcher.assert_activated(
            "completing the expunging request activates instance_watcher",
        );
        activators.blueprint_planner.assert_activated(
            "completing the expunging request activates blueprint_planner",
        );
        assert_eq!(read(&db, &preparing_request).await, preparing_request);
        assert_eq!(
            read(&db, &inconsistent_request).await,
            inconsistent_request
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: This shows two ways a stale row is caught:
    //
    // * The request is handed over as another Nexus listed it, after it was
    //   already moved on, so the advance's compare-and-swap loses.
    // * `power_off` re-reads the row and finds that the request has failed, so
    //   it returns before any MGS call.
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
        rack.tq(&db).ack_all_prepares(2).await;
        let stale = rack
            .insert_request(
                &db,
                2,
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
            )
            .await;
        set_sled_expunge_request_state(
            db.datastore(),
            &stale,
            SledExpungeRequestStateKind::PoweringOff,
        )
        .await;
        assert_eq!(
            drive_one(&task, db.opctx(), &stale, now_db_precision()).await,
            SledExpungeRequestStatus {
                state_after: SledExpungeStateAfter::Unknown {
                    last_confirmed: None,
                },
                ..rack.status(
                    &stale,
                    SledExpungeRequestOutcome::NotInExpectedState {
                        current_state: SledExpungeRequestStateKind::PoweringOff,
                    },
                )
            },
        );

        let powering_off = read(&db, &stale).await;
        set_sled_expunge_request_state(
            db.datastore(),
            &powering_off,
            SledExpungeRequestStateKind::Failed,
        )
        .await;
        let refusing = RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
            .await
            .expect("bound a refusing TCP proxy");
        let unreachable = GatewayClient::from_addr(log, refusing.local_addr());
        let gateways = SledPowerOffGateways::new(
            std::slice::from_ref(&unreachable),
            IdOrdMap::new(),
        );
        let mut progress = RequestProgress::default();
        let outcome = task
            .power_off(db.opctx(), log, &powering_off, &gateways, &mut progress)
            .await;
        assert_eq!(
            request_status(&powering_off, outcome, progress),
            rack.status(
                &powering_off,
                SledExpungeRequestOutcome::NotInExpectedState {
                    current_state: SledExpungeRequestStateKind::Failed,
                },
            ),
        );

        refusing.shutdown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_sled_expunge_failure_reason_policies() {
        let sled_id = SledUuid::new_v4();
        let request = SledExpungeRequest {
            sled_id,
            baseboard_id: BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: "test-serial".to_string(),
            },
            state: SledExpungeRequestState::WaitingForTrustQuorum {
                cubby: Some(inventory_cubby(3)),
                last_error: None,
            },
            ..test_request()
        };
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
        let attempts = |entries: Vec<(&str, MgsPowerOffFailure)>| {
            entries
                .into_iter()
                .map(|(mgs, failure)| SledExpungeMgsPowerOffFailure {
                    mgs: mgs.parse().expect("parsed an MGS address"),
                    failure,
                })
                .collect::<IdOrdMap<_>>()
        };
        let error = || "error".to_string();
        let board = BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: "BRM42220004".to_string(),
        };
        let empty_identity = || {
            NonEmptyBaseboardId::new(BaseboardId {
                serial_number: String::new(),
                ..board.clone()
            })
            .expect_err("an empty serial number is empty")
        };
        assert_eq!(
            NeedsAttention::PowerOff(SledExpungePowerOffWait::NotPoweredOff {
                slot: 3,
                located: SledExpungeCubbyLocated::Recorded {
                    source: SledExpungeSpSlotSource::Inventory,
                },
                failed_attempts: attempts(vec![
                    (
                        "[::1]:1",
                        MgsPowerOffFailure::IgnitionPowerOffFailed {
                            error: error(),
                        },
                    ),
                    (
                        "[::1]:2",
                        MgsPowerOffFailure::Unidentified {
                            reason: CubbyUnidentified::MgsRequestFailed {
                                error: error(),
                            },
                        },
                    ),
                    (
                        "[::1]:3",
                        MgsPowerOffFailure::IgnitionPowerOffFailed {
                            error: error(),
                        },
                    ),
                    (
                        "[::1]:4",
                        MgsPowerOffFailure::OtherBoardPresent {
                            found: NonEmptyBaseboardId::new(board.clone())
                                .expect("the board is non-empty"),
                        },
                    ),
                    (
                        "[::1]:5",
                        MgsPowerOffFailure::Unidentified {
                            reason: CubbyUnidentified::EmptyIdentity {
                                reported: empty_identity(),
                            },
                        },
                    ),
                    (
                        "[::1]:6",
                        MgsPowerOffFailure::Unidentified {
                            reason: CubbyUnidentified::SpUnreachable {
                                error: error(),
                            },
                        },
                    ),
                ]),
            })
            .render(&request),
            format!(
                "Nexus could not power off this sled in cubby 3 (a \
                 management gateway could not reach the sled's service \
                 processor; a management gateway did not confirm the \
                 power-off; a management gateway found another board in the \
                 cubby; a management gateway's request for the sled's state \
                 failed; the sled's cubby reported an empty baseboard \
                 identity). {KEEPS_RETRYING}"
            ),
        );
        let mgs: SocketAddrV6 = "[::1]:2".parse().expect("parsed an address");
        let not_in_recorded = SledExpungeSearch::NotInRecordedCubby {
            slot: 3,
            source: SledExpungeSpSlotSource::Inventory,
            other_board: None,
            failed_attempts: attempts(vec![(
                "[::1]:1",
                MgsPowerOffFailure::Unidentified {
                    reason: CubbyUnidentified::SpUnreachable { error: error() },
                },
            )]),
        };
        let ignition_cases = [
            (
                SledExpungeIgnitionUnavailable::Both {
                    switch0: SledExpungeIgnitionGap::NoMgs,
                    switch1: SledExpungeIgnitionGap::FetchFailed { mgs },
                },
                "no management gateway was found for switch 0; switch 1's \
                 ignition state could not be read",
            ),
            (
                SledExpungeIgnitionUnavailable::Switch0 {
                    switch0: SledExpungeIgnitionGap::MgsConflict,
                },
                "more than one management gateway claims switch 0",
            ),
            (
                SledExpungeIgnitionUnavailable::Switch1 {
                    switch1: SledExpungeIgnitionGap::NoMgs,
                },
                "no management gateway was found for switch 1",
            ),
        ];
        for (switches, gaps_text) in ignition_cases {
            for (search, search_text) in [
                (
                    SledExpungeSearch::GoneAtCreation,
                    GONE_AT_CREATION_TEXT.to_string(),
                ),
                (not_in_recorded.clone(), not_in_recorded_cubby_text(3)),
            ] {
                assert_eq!(
                    NeedsAttention::PowerOff(
                        SledExpungePowerOffWait::IgnitionUnavailable {
                            search,
                            switches,
                        }
                    )
                    .render(&request),
                    ignition_unavailable_text(&search_text, gaps_text),
                );
            }
        }
        let unreachable_cubby = UnidentifiedCubby {
            slot: 2,
            reason: CubbyUnidentified::SpUnreachable { error: error() },
        };
        let empty_cubby = UnidentifiedCubby {
            slot: 5,
            reason: CubbyUnidentified::EmptyIdentity {
                reported: empty_identity(),
            },
        };
        let unidentified = |cubbies: Vec<UnidentifiedCubby>| {
            SledExpungeUnidentifiedCubbies::new(cubbies.into_iter().collect())
                .expect("test cubbies are non-empty")
        };
        assert_eq!(
            NeedsAttention::PowerOff(
                SledExpungePowerOffWait::CubbiesUnidentified {
                    search: not_in_recorded.clone(),
                    cubbies: unidentified(vec![unreachable_cubby.clone()]),
                }
            )
            .render(&request),
            cubby_unidentified_text(&not_in_recorded_cubby_text(3), 2),
        );
        let cubbies = unidentified(vec![unreachable_cubby, empty_cubby]);
        assert_eq!(
            NeedsAttention::PowerOff(
                SledExpungePowerOffWait::CubbiesUnidentified {
                    search: not_in_recorded,
                    cubbies,
                }
            )
            .render(&request),
            cubbies_unidentified_text(&not_in_recorded_cubby_text(3), "2, 5"),
        );
        let policy = Some(SledPolicy::Expunged);
        let re_added = FailureReason::ReAdded { re_add_epoch: Epoch(3) };
        let expunging = |power_off| SledExpungeRequestState::Expunging {
            power_off,
            last_error: None,
        };
        let at_cubby = |outcome| SledExpungePowerOff::Cubby {
            cubby: inventory_cubby(3),
            outcome,
        };
        let cases = [
            (request.state.clone(), ""),
            (
                expunging(at_cubby(
                    SledExpungeCubbyPowerOffOutcome::PoweredOff,
                )),
                " Nexus had powered the sled off.",
            ),
            (
                expunging(at_cubby(
                    SledExpungeCubbyPowerOffOutcome::AlreadyOff,
                )),
                " Nexus had found the sled already powered off.",
            ),
            (
                expunging(at_cubby(
                    SledExpungeCubbyPowerOffOutcome::OtherBoardPresent,
                )),
                " Nexus had found another sled in its cubby, and this sled \
                 in no powered sled cubby of the rack (it may have been \
                 unpowered in one).",
            ),
            (
                expunging(at_cubby(
                    SledExpungeCubbyPowerOffOutcome::NotPresent,
                )),
                " Nexus had found it in no powered sled cubby of the rack (it \
                 may have been unpowered in one).",
            ),
            (
                expunging(SledExpungePowerOff::AbsentAtCreation),
                " Nexus had confirmed that the sled was still in no powered \
                 sled cubby of the rack (it may have been unpowered in one).",
            ),
        ];
        for (state, sentence) in cases {
            let request = SledExpungeRequest { state, ..request.clone() };
            assert_eq!(
                re_added.render(&request, policy),
                re_added_text(sentence, "the sled was expunged."),
            );
        }
    }

    fn re_added_text(power_off_sentence: &str, policy: &str) -> String {
        format!(
            "The sled was added back to the rack (rack membership version 3) \
             after its removal at version 2, so this request did not expunge \
             it.{power_off_sentence} Contact Oxide support. At the time this \
             request failed, {policy}"
        )
    }

    // TODO-RAINCLAUDE: A driver for the timeout tests. Requests in `stalled`
    // record some progress and then never answer. The rest answer at once.
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
                    .map(|request| request.id)
                    .collect(),
                stall_step,
                finished: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    fn stalled_power_check() -> SledExpungePowerCheck {
        powered_down_check(
            3,
            SledPoweredDown::PoweredOff {
                mgs: "[::1]:12225".parse().expect("parsed an MGS address"),
            },
        )
    }

    impl RequestDriver for StallingDriver {
        async fn drive(
            &self,
            request: &SledExpungeRequest,
            _start: MgsPassStart,
            progress: &mut RequestProgress,
        ) -> SledExpungeRequestOutcome {
            if self.stalled.contains(&request.id) {
                progress.step = self.stall_step;
                progress.power_check = Some(stalled_power_check());
                std::future::pending::<()>().await;
            }
            self.finished
                .lock()
                .expect("the finished list is not poisoned")
                .push((request.id, tokio::time::Instant::now()));
            no_mgs_outcome()
        }
    }

    // TODO-RAINCLAUDE: Time is paused to make the per-request timeout
    // deterministic. Tokio advances a paused clock only when every future is
    // idle, so the stalled request times out at exactly `PER_REQUEST_TIMEOUT`
    // while the healthy one finishes at once.
    #[tokio::test(start_paused = true)]
    async fn test_sled_expunge_stalled_request_does_not_block_others() {
        let log = slog::Logger::root(slog::Discard, o!());
        let stalled = test_request();
        let healthy = test_request();
        let driver =
            StallingDriver::new([&stalled], SledExpungeTaskStep::Completing);
        let started = tokio::time::Instant::now();

        let statuses_out = drive_requests_with_timeout(
            &log,
            vec![mgs_pass_request(&stalled), mgs_pass_request(&healthy)],
            &driver,
            &test_activators(),
        )
        .await;
        assert_eq!(
            driver
                .finished
                .into_inner()
                .expect("the finished list is not poisoned"),
            vec![(healthy.id, started)],
        );
        assert_eq!(tokio::time::Instant::now() - started, PER_REQUEST_TIMEOUT);
        assert_eq!(
            statuses(statuses_out),
            statuses([
                SledExpungeRequestStatus {
                    power_check: Some(stalled_power_check()),
                    state_after: SledExpungeStateAfter::Unknown {
                        last_confirmed: None,
                    },
                    ..status_for(
                        &stalled,
                        SledExpungeRequestOutcome::TimedOut {
                            step: SledExpungeTaskStep::Completing,
                        },
                    )
                },
                status_for(&healthy, no_mgs_outcome()),
            ]),
        );
    }

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

            let statuses_out = drive_requests_with_timeout(
                &log,
                vec![mgs_pass_request(&stalled)],
                &driver,
                &activators,
            )
            .await;

            assert_eq!(
                statuses(statuses_out),
                statuses([SledExpungeRequestStatus {
                    power_check: Some(stalled_power_check()),
                    ..status_for(
                        &stalled,
                        SledExpungeRequestOutcome::TimedOut { step },
                    )
                }]),
                "timed out in {step}",
            );
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
        let now = now_db_precision();
        SledExpungeRequest {
            id: SledExpungeRequestUuid::new_v4(),
            rack_id: RackUuid::new_v4(),
            sled_id: SledUuid::new_v4(),
            baseboard_id: BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            },
            tq_epoch: Epoch(2),
            state: SledExpungeRequestState::PoweringOff {
                cubby: Some(inventory_cubby(3)),
                last_error: None,
            },
            time_created: now,
            time_modified: now,
        }
    }

    fn mgs_pass_request(request: &SledExpungeRequest) -> MgsPassRequest {
        MgsPassRequest::as_listed(request)
            .expect("a powering_off or expunging row starts the MGS pass")
    }

    fn switch_attached_cubby_text(slot: u16) -> String {
        format!(
            "This sled was found in switch-attached cubby {slot}, so it is, or \
             might be, a scrimlet (it manages a switch), and Nexus will not \
             power it off. \
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

    // TODO-RAINCLAUDE: Sets the sled row's state directly, bypassing the
    // transition checks, so that the expunge in `complete` fails.
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
            .sled_expunge_request_complete(db.opctx(), request)
            .await
            .expect_err("a decommissioned sled refuses the expunge");
        format!("completing the request: {}", InlineErrorChain::new(&error))
    }

    // TODO-RAINCLAUDE: This shows what happens when the expunge fails right
    // after the request advances to `expunging`. The status reports the error
    // along with the state the advance left the request in, `last_error` isn't
    // written, and `instance_watcher` and `blueprint_planner` are still
    // activated.
    #[tokio::test]
    async fn test_sled_expunge_error_after_advance() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_error_after_advance");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, activators) = test_task(&db);
        let log = &db.opctx().log;
        let rack = TestRack::new(&db).await;
        rack.propose_removal(&db).await;
        rack.tq(&db).commit(2).await;
        let listed = rack
            .insert_request(&db, 2, SledExpungeRequestStateKind::PoweringOff)
            .await;
        set_sled_state(
            &db,
            rack.sled_id,
            nexus_db_model::SledState::Decommissioned,
        )
        .await;

        let mut progress = RequestProgress::default();
        let outcome = task
            .advance_to_expunging(
                db.opctx(),
                log,
                &listed,
                SledExpungePowerOff::Cubby {
                    cubby: listed
                        .state
                        .recorded_cubby()
                        .expect("the listed request records a cubby"),
                    outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                },
                &mut progress,
            )
            .await;
        let expunging = read(&db, &listed).await;
        let error = complete_error(&db, &expunging).await;
        assert_eq!(
            request_status(&listed, outcome, progress),
            SledExpungeRequestStatus {
                state_after: SledExpungeStateAfter::Unknown {
                    last_confirmed: Some(SledExpungeRequestStateKind::Expunging),
                },
                ..rack.status(
                    &listed,
                    SledExpungeRequestOutcome::Error { error },
                )
            },
        );
        assert_eq!(
            expunging,
            SledExpungeRequest {
                state: SledExpungeRequestState::Expunging {
                    power_off: SledExpungePowerOff::Cubby {
                        cubby: listed
                            .state
                            .recorded_cubby()
                            .expect("the listed request records a cubby"),
                        outcome: SledExpungeCubbyPowerOffOutcome::PoweredOff,
                    },
                    last_error: None,
                },
                time_modified: expunging.time_modified,
                ..listed.clone()
            },
        );
        activators.instance_watcher.assert_activated(
            "a failed complete may have committed, so it activates \
             instance_watcher",
        );
        activators.blueprint_planner.assert_activated(
            "a failed complete may have committed, so it activates \
             blueprint_planner",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
