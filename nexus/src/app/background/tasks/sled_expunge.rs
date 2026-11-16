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
use nexus_auth::authz;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledExpungeCubbyPowerOffOutcome;
use nexus_db_model::SledExpungePowerOff;
use nexus_db_model::SledExpungePowerOffOutcome;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestAdvance;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_model::SledExpungeRequestUnfinishedState;
use nexus_db_model::SpMgsSlot;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledExpungeRecordSpSlotOutcome;
use nexus_db_queries::db::datastore::SledExpungeRequestCompleteOutcome;
use nexus_db_queries::db::datastore::SledExpungeRequestUpdateOutcome;
use nexus_db_queries::db::datastore::SledExpungeTaskGateOutcome;
use nexus_db_queries::db::datastore::authz_sled_expunge_request_from_id;
use nexus_networking::CubbyPowerOffOutcome;
use nexus_networking::GatewaysByRack;
use nexus_networking::PowerOffAtSlotOutcome;
use nexus_networking::SledPowerOffGateways;
use nexus_networking::power_off_sled_at_slot;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::external_api::sled::SledProvisionPolicy;
use nexus_types::internal_api::background::SledExpungeCollectionWait;
use nexus_types::internal_api::background::SledExpungeMgsDiscovery;
use nexus_types::internal_api::background::SledExpungeMgsRackIdUnknown;
use nexus_types::internal_api::background::SledExpungePowerCheck;
use nexus_types::internal_api::background::SledExpungePowerOffWait;
use nexus_types::internal_api::background::SledExpungeRackIgnitionSnapshot;
use nexus_types::internal_api::background::SledExpungeRequestOutcome;
use nexus_types::internal_api::background::SledExpungeRequestStatus;
use nexus_types::internal_api::background::SledExpungeStatus;
use nexus_types::internal_api::background::SledExpungeTaskRequestState;
use nexus_types::internal_api::background::SledExpungeTaskStep;
use nexus_types::inventory::Collection;
use nexus_types::sled_expunge::MgsPowerOffFailure;
use nexus_types::trust_quorum::SledExpungeTaskGate;
use omicron_common::api::external::Error;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackUuid;
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

// TODO-RAINCLAUDE: value. Requests are driven concurrently, at most this many at once, so one slow request does not hold up the others' turn; each request is bounded by PER_REQUEST_TIMEOUT, so a stalled one holds a turn for at most that long, and a request queued behind MAX_CONCURRENT_REQUESTS stalled ones waits at most one timeout for its turn.
const MAX_CONCURRENT_REQUESTS: usize = 16;

// TODO-RAINCLAUDE: value. A backstop over all of one request's driving in one activation, well above the worst case with every MGS call at its 15s client timeout: the activation's MGS discovery, if this request is the one that starts it (DNS, then each MGS's rack ID, about 20s), re-reading the request row, then settling the recorded cubby through each MGS of the rack (normally two, one per switch) at about 30s per MGS (a re-check, then an ignition power-off or a GET /ignition), plus the rack's ignition snapshot if this request is the one that fetches it (about 15s per MGS), plus the database writes; so a stalled MGS normally ends in a failed power-off and a last_error rather than in this timeout. A timed-out request is abandoned mid-step and nothing more is written for it; every step is a compare-and-swap or idempotent, so the next activation picks it up from whatever state it reached.
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

// TODO-RAINCLAUDE: inventory_rx is the inventory loader's latest collection, read once per activation.
pub struct SledExpunge {
    datastore: Arc<DataStore>,
    resolver: internal_dns_resolver::Resolver,
    inventory_rx: watch::Receiver<Option<Arc<Collection>>>,
    inventory_collection: Activator,
    activators: SledExpungeActivators,
}

enum MgsDiscovery {
    Failed(String),
    Discovered {
        racks: IdOrdMap<RackMgs>,
        rack_id_unknown: IdOrdMap<SledExpungeMgsRackIdUnknown>,
    },
}

// TODO-RAINCLAUDE: one rack's discovered MGSes, with the rack's ignition snapshot that every request in the rack shares for the rest of the activation.
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
    // TODO-RAINCLAUDE: DNS can name one MGS more than once, so the status keeps one entry per address, with the last error seen for it.
    fn from_gateways(gateways: GatewaysByRack) -> Self {
        let mut rack_id_unknown = IdOrdMap::new();
        for (gateway, error) in gateways.unknown() {
            rack_id_unknown.insert_overwrite(SledExpungeMgsRackIdUnknown {
                mgs: gateway.addr,
                error: InlineErrorChain::new(error).to_string(),
            });
        }
        let racks = gateways
            .into_discovered()
            .map(|rack| RackMgs {
                rack_id: rack.rack_id,
                gateways: SledPowerOffGateways::new(&rack.gateways),
            })
            .collect();
        MgsDiscovery::Discovered { racks, rack_id_unknown }
    }

    fn gateways_for(&self, rack_id: RackUuid) -> Option<&SledPowerOffGateways> {
        match self {
            MgsDiscovery::Failed(_) => None,
            MgsDiscovery::Discovered { racks, rack_id_unknown: _ } => racks
                .get(&rack_id)
                .map(|rack| &rack.gateways)
                .filter(|gateways| !gateways.is_empty()),
        }
    }

    fn status(&self) -> SledExpungeMgsDiscovery {
        match self {
            MgsDiscovery::Failed(error) => {
                SledExpungeMgsDiscovery::Failed { error: error.clone() }
            }
            MgsDiscovery::Discovered { racks: _, rack_id_unknown } => {
                SledExpungeMgsDiscovery::Discovered {
                    rack_id_unknown: rack_id_unknown.clone(),
                }
            }
        }
    }

    fn ignition_snapshots(&self) -> IdOrdMap<SledExpungeRackIgnitionSnapshot> {
        match self {
            MgsDiscovery::Failed(_) => IdOrdMap::new(),
            MgsDiscovery::Discovered { racks, rack_id_unknown: _ } => racks
                .iter()
                .filter_map(|rack| {
                    rack.gateways.ignition_status().map(|snapshot| {
                        SledExpungeRackIgnitionSnapshot {
                            rack_id: rack.rack_id,
                            snapshot,
                        }
                    })
                })
                .collect(),
        }
    }
}

// TODO-RAINCLAUDE: what every request in one activation shares: MGS discovery, run lazily by the first request in powering_off (including one that advanced to it in this activation) with a recorded cubby, at most once per activation, and not at all if no request needs it; and, through it, each rack's ignition snapshot (see SledPowerOffGateways). The OnceCell is cancel-safe, so a request timing out mid-discovery leaves it to the next request that needs it; started records that it began, so the status can tell "never needed" from "cancelled before it finished".
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

    #[cfg(test)]
    fn with_discovery(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
        discovery: MgsDiscovery,
    ) -> Self {
        Self {
            log,
            resolver,
            started: AtomicBool::new(true),
            discovery: tokio::sync::OnceCell::new_with(Some(discovery)),
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

    fn ignition_snapshots(&self) -> IdOrdMap<SledExpungeRackIgnitionSnapshot> {
        match self.discovery.get() {
            Some(discovery) => discovery.ignition_snapshots(),
            None => IdOrdMap::new(),
        }
    }
}

// TODO-RAINCLAUDE: what driving one request found besides its outcome, kept outside the per-request timeout so that a timed-out request still reports the step it was in and whatever it had found. latest_state is the state this activation's last successful write left the request in, for the status's state_after.
#[derive(Default)]
struct RequestProgress {
    step: SledExpungeTaskStep,
    latest_state: Option<SledExpungeRequestState>,
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
        let statuses = drive_requests_with_timeout(
            &opctx.log,
            requests,
            &driver,
            &self.activators,
        )
        .await;
        let requests = IdOrdMap::from_iter_unique(statuses).expect(
            "list_unfinished pages by id, so each request is listed once",
        );
        SledExpungeStatus::Listed {
            requests,
            mgs_discovery: mgs.status(),
            ignition_snapshots: mgs.ignition_snapshots(),
        }
    }

    async fn drive_request(
        &self,
        opctx: &OpContext,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        now: DateTime<Utc>,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let log = &request_log(&opctx.log, request);
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
            Err(error) => {
                return error_outcome(
                    log,
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
        }
    }

    // TODO-RAINCLAUDE: RFD 747's powering_off step: a request with a recorded cubby settles that cubby through MGS; one whose sled was gone at creation waits for an inventory collection that confirms the sled is still gone, or records the cubby it reappeared in and settles that cubby in the same activation.
    async fn drive_powering_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::ResolvingPower;
        match request.sp_slot {
            Some(_) => {
                self.drive_recorded_cubby(opctx, log, inputs, request, progress)
                    .await
            }
            None => {
                self.drive_gone_at_creation(
                    opctx, log, inputs, request, progress,
                )
                .await
            }
        }
    }

    async fn drive_recorded_cubby(
        &self,
        opctx: &OpContext,
        log: &Logger,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let Some(gateways) = inputs
            .mgs
            .get()
            .await
            .gateways_for(RackUuid::from(request.rack_id))
        else {
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
        };
        self.power_off(
            opctx,
            log,
            request,
            inputs.collection,
            gateways,
            progress,
        )
        .await
    }

    // TODO-RAINCLAUDE: on a settled cubby the request advances to expunging and is expunged in this same activation; otherwise it waits with a last_error.
    async fn power_off(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
        collection: Option<&Collection>,
        gateways: &SledPowerOffGateways,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        // TODO-RAINCLAUDE: the request is re-read once before the MGS loop so that a Nexus whose listing is stale does not power off a sled whose request another Nexus has already moved on (for example, one that failed and whose sled was then added back and powered on); the window between this read and the power-off is accepted. The cubby comes from this re-read row too.
        let step = "re-reading the request before powering off";
        let current = match self
            .datastore
            .sled_expunge_request_get(opctx, request.id())
            .await
        {
            Ok(current) => current,
            Err(error) => return error_outcome(log, step, &error),
        };
        let slot = match (current.state, current.sp_slot) {
            (SledExpungeRequestState::PoweringOff, Some(slot)) => slot,
            (SledExpungeRequestState::PoweringOff, None)
            | (
                SledExpungeRequestState::WaitingForTrustQuorum
                | SledExpungeRequestState::Expunging
                | SledExpungeRequestState::Done
                | SledExpungeRequestState::Failed,
                _,
            ) => {
                return not_in_expected_state_outcome(log, step, &current);
            }
        };

        progress.step = SledExpungeTaskStep::SendingPowerOff;
        let outcome = power_off_sled_at_slot(
            log,
            gateways,
            &current.baseboard_id(),
            **slot,
        )
        .await;
        let settled = match outcome {
            PowerOffAtSlotOutcome::Settled(settled) => settled,
            PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts } => {
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        &current,
                        collection,
                        SledExpungePowerOffWait::NotPoweredOff {
                            slot: **slot,
                            failed_attempts,
                        },
                        progress,
                    )
                    .await;
            }
        };
        let (check, outcome) = cubby_power_check(**slot, settled);
        progress.power_check = Some(check);
        self.advance_to_expunging(
            opctx,
            log,
            &current,
            SledExpungePowerOff::Cubby { sp_slot: slot, outcome },
            progress,
        )
        .await
    }

    // TODO-RAINCLAUDE: a sled gone at creation is confirmed gone only by a collection that started after the removal committed, since only then can the sled no longer unlock its disks should it come back; a sled seen again gets its cubby recorded and goes straight on to the recorded-cubby path.
    async fn drive_gone_at_creation(
        &self,
        opctx: &OpContext,
        log: &Logger,
        inputs: PoweringOffInputs<'_>,
        request: &SledExpungeRequest,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        let Some(latest) = inputs.collection else {
            return self.wait_for_collection(
                log,
                SledExpungeCollectionWait::NoInventory,
                progress,
            );
        };
        let removal_committed_at =
            match self.removal_committed_at(opctx, log, request).await {
                Ok(at) => at,
                Err(outcome) => return outcome,
            };
        let found =
            match gone_at_creation_check(request, latest, removal_committed_at)
            {
                GoneAtCreationCheck::StillGone => {
                    info!(
                        log,
                        "a collection taken after the removal committed still \
                         has neither the sled's SP nor its sled agent";
                        "collection_id" => %latest.id,
                    );
                    progress.power_check =
                        Some(SledExpungePowerCheck::AbsentAtCreation {
                            collection_id: latest.id,
                        });
                    return self
                        .advance_to_expunging(
                            opctx,
                            log,
                            request,
                            SledExpungePowerOff::AbsentAtCreation,
                            progress,
                        )
                        .await;
                }
                GoneAtCreationCheck::CollectionWait(wait) => {
                    return self.wait_for_collection(log, wait, progress);
                }
                GoneAtCreationCheck::Wait(wait) => {
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
                GoneAtCreationCheck::SpFound { slot } => slot,
            };

        progress.step = SledExpungeTaskStep::RecordingSpSlot;
        let result = self
            .datastore
            .sled_expunge_request_record_sp_slot(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                SpMgsSlot::from(found),
            )
            .await;
        let recorded = match result {
            Ok(SledExpungeRecordSpSlotOutcome::Updated {
                request: updated,
            }) => {
                info!(
                    log,
                    "the sled's SP is back in inventory; recorded its cubby";
                    "slot" => found,
                    "collection_id" => %latest.id,
                );
                progress.latest_state = Some(updated.state);
                updated
            }
            // TODO-RAINCLAUDE: another Nexus (or a retried call) recorded the same cubby first.
            Ok(SledExpungeRecordSpSlotOutcome::NotInExpectedState {
                current,
            }) if current.state == SledExpungeRequestState::PoweringOff
                && current.sp_slot == Some(SpMgsSlot::from(found)) =>
            {
                info!(
                    log,
                    "the sled's cubby was already recorded";
                    "slot" => found,
                );
                current
            }
            Ok(SledExpungeRecordSpSlotOutcome::NotInExpectedState {
                current,
            }) => {
                return not_in_expected_state_outcome(
                    log,
                    "recording the sled's cubby",
                    &current,
                );
            }
            Ok(SledExpungeRecordSpSlotOutcome::SwitchAttachedCubby {
                sp_slot,
            }) => {
                return self
                    .wait_to_power_off(
                        opctx,
                        log,
                        request,
                        inputs.collection,
                        SledExpungePowerOffWait::SwitchAttachedCubby {
                            slot: **sp_slot,
                        },
                        progress,
                    )
                    .await;
            }
            Err(error) => {
                return error_outcome(
                    log,
                    "recording the sled's cubby",
                    &error,
                );
            }
        };
        progress.step = SledExpungeTaskStep::ResolvingPower;
        self.drive_recorded_cubby(opctx, log, inputs, &recorded, progress).await
    }

    // TODO-RAINCLAUDE: a normal wait for a newer inventory collection, like RemovalPreparing: logged at debug, reported in the status, and nothing written, so last_error and time_modified stay put. Inventory collection is woken on every such activation; the activator coalesces wakes while a collection runs, and each wait ends once a qualifying collection is loaded.
    fn wait_for_collection(
        &self,
        log: &Logger,
        wait: SledExpungeCollectionWait,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        debug!(log, "waiting for a newer inventory collection"; "wait" => ?wait);
        self.inventory_collection.activate();
        progress.power_check =
            Some(SledExpungePowerCheck::WaitingForCollection { wait });
        SledExpungeRequestOutcome::WaitingForCollection
    }

    // TODO-RAINCLAUDE: the removal config's time_committing, set in the same update that moves it out of preparing, which is what let the request into powering_off; its absence contradicts the request, so it is an error the next activation retries rather than a last_error.
    async fn removal_committed_at(
        &self,
        opctx: &OpContext,
        log: &Logger,
        request: &SledExpungeRequest,
    ) -> Result<DateTime<Utc>, SledExpungeRequestOutcome> {
        let step = "reading when the removal committed";
        let epoch =
            u64::try_from(request.tq_epoch).map(Epoch).map_err(|_| {
                error_outcome(
                    log,
                    step,
                    &Error::internal_error(&format!(
                        "request epoch {} is negative",
                        request.tq_epoch
                    )),
                )
            })?;
        let config = self
            .datastore
            .tq_get_config(
                opctx,
                authz::TrustQuorumConfig::for_rack_id(RackUuid::from(
                    request.rack_id,
                )),
                epoch,
            )
            .await
            .map_err(|error| error_outcome(log, step, &error))?;
        config.and_then(|config| config.time_committing).ok_or_else(|| {
            error_outcome(
                log,
                step,
                &Error::internal_error(&format!(
                    "the removal configuration (epoch {epoch}) has not left \
                     preparing, but the request is in powering_off"
                )),
            )
        })
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
        power_off: SledExpungePowerOff,
        progress: &mut RequestProgress,
    ) -> SledExpungeRequestOutcome {
        progress.step = SledExpungeTaskStep::AdvancingToExpunging;
        let result = self
            .datastore
            .sled_expunge_request_advance(
                opctx,
                &authz_sled_expunge_request_from_id(request.id()),
                SledExpungeRequestAdvance::ToExpunging { power_off },
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
        info!(
            log,
            "the sled cannot be running in its last known cubby; expunging \
             it now";
            "power_off_outcome" => %power_off.outcome(),
        );
        self.complete(opctx, log, &expunging, progress).await
    }

    // TODO-RAINCLAUDE: RFD 747's expunging step. It does not resolve the sled's identity again: power-on is out of scope for v1, and the request was only ever advanced once its sled could not be running in its last known cubby (see power_off_outcome).
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
            Err(error) => error_outcome(log, "completing the request", &error),
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
        // TODO-RAINCLAUDE: skipping an unchanged message keeps time_modified meaningful; callers pass the freshest row this activation has (as listed, as re-read before powering off, or as its last write returned it), so a concurrent writer can at worst cause one redundant write.
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

// TODO-RAINCLAUDE: a timed-out request reports TimedOut in the step it had reached, with whatever progress it recorded; if it may have just completed (it timed out while advancing to expunging or completing), instance_watcher and blueprint_planner are activated anyway, since an extra activation is harmless. Nothing bounds the activation as a whole beyond the per-request timeout.
async fn drive_requests_with_timeout<D: RequestDriver>(
    log: &Logger,
    requests: Vec<SledExpungeRequest>,
    driver: &D,
    activators: &SledExpungeActivators,
) -> Vec<SledExpungeRequestStatus> {
    stream::iter(requests)
        .map(|request| async move {
            let mut progress = RequestProgress::default();
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
                        | SledExpungeTaskStep::RecordingSpSlot
                        | SledExpungeTaskStep::AdvancingToPoweringOff
                        | SledExpungeTaskStep::SettingLastError
                        | SledExpungeTaskStep::Failing => {}
                    }
                    SledExpungeRequestOutcome::TimedOut { step: progress.step }
                }
            };
            request_status(&request, outcome, progress)
        })
        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
        .collect()
        .await
}

fn request_status(
    request: &SledExpungeRequest,
    outcome: SledExpungeRequestOutcome,
    progress: RequestProgress,
) -> SledExpungeRequestStatus {
    let RequestProgress { step: _, latest_state, power_check } = progress;
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
        | SledExpungeRequestOutcome::WaitingForCollection
        | SledExpungeRequestOutcome::TimedOut { .. }
        | SledExpungeRequestOutcome::Error { .. } => {
            latest_state.map(task_request_state)
        }
    }
    .filter(|after| *after != state);
    SledExpungeRequestStatus {
        request_id: request.id(),
        sled_id: SledUuid::from(request.sled_id),
        state,
        state_after,
        outcome,
        power_check,
    }
}

// TODO-RAINCLAUDE: discovery failing outright leaves every request with no MGS for its rack, so each waits with the no-MGS last_error.
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

fn cubby_power_check(
    slot: u16,
    settled: CubbyPowerOffOutcome,
) -> (SledExpungePowerCheck, SledExpungeCubbyPowerOffOutcome) {
    match settled {
        CubbyPowerOffOutcome::PoweredOff { mgs } => (
            SledExpungePowerCheck::PoweredOff { slot, mgs },
            SledExpungeCubbyPowerOffOutcome::PoweredOff,
        ),
        CubbyPowerOffOutcome::AlreadyOff { mgs } => (
            SledExpungePowerCheck::AlreadyOff { slot, mgs },
            SledExpungeCubbyPowerOffOutcome::AlreadyOff,
        ),
        CubbyPowerOffOutcome::OtherBoardPresent { mgs, found } => (
            SledExpungePowerCheck::OtherBoardPresent { slot, mgs, found },
            SledExpungeCubbyPowerOffOutcome::OtherBoardPresent,
        ),
        CubbyPowerOffOutcome::NotPresent { mgs, ignition } => (
            SledExpungePowerCheck::NotPresent { slot, mgs, ignition },
            SledExpungeCubbyPowerOffOutcome::NotPresent,
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum GoneAtCreationCheck {
    StillGone,
    SpFound { slot: u16 },
    CollectionWait(SledExpungeCollectionWait),
    Wait(SledExpungePowerOffWait),
}

// TODO-RAINCLAUDE: what the latest loaded collection says about a sled that was gone at creation. Only a collection started strictly after the removal committed counts, and its missing SP proves nothing unless it has SP records for the request's rack (so the rack's MGS answered it). An SP record that is ours but not a sled's, or in another rack, should not happen, so those wait for support rather than record a cubby; the switch-attached check is left to record_sp_slot.
fn gone_at_creation_check(
    request: &SledExpungeRequest,
    collection: &Collection,
    removal_committed_at: DateTime<Utc>,
) -> GoneAtCreationCheck {
    if collection.time_started <= removal_committed_at {
        return GoneAtCreationCheck::CollectionWait(
            SledExpungeCollectionWait::NoCollectionSinceRemoval {
                removal_committed_at,
                collection_started_at: collection.time_started,
            },
        );
    }
    match collection.sps.get(&request.baseboard_id()) {
        Some(sp) => {
            match sp.sp_type {
                SpType::Sled => {}
                SpType::Switch | SpType::Power => {
                    return GoneAtCreationCheck::Wait(
                        SledExpungePowerOffWait::NotASled {
                            sp_type: sp.sp_type,
                        },
                    );
                }
            }
            if sp.rack_id != RackUuid::from(request.rack_id) {
                return GoneAtCreationCheck::Wait(
                    SledExpungePowerOffWait::InAnotherRack {
                        rack_id: sp.rack_id,
                    },
                );
            }
            GoneAtCreationCheck::SpFound { slot: sp.sp_slot }
        }
        None => {
            let rack_id = RackUuid::from(request.rack_id);
            if !collection.sps.values().any(|sp| sp.rack_id == rack_id) {
                GoneAtCreationCheck::CollectionWait(
                    SledExpungeCollectionWait::NoRackSpData {
                        collection_id: collection.id,
                    },
                )
            } else if collection
                .sled_agents
                .get(&SledUuid::from(request.sled_id))
                .is_some()
            {
                GoneAtCreationCheck::Wait(
                    SledExpungePowerOffWait::SledAgentWithoutSp {
                        collection_id: collection.id,
                    },
                )
            } else {
                GoneAtCreationCheck::StillGone
            }
        }
    }
}

// TODO-RAINCLAUDE: whether a last_error wait should wake inventory collection: only for the one a newer collection may resolve (a sled gone at creation whose sled agent is back without its SP), and only if the loaded collection started before the request's time_modified, which marks when its current wait began (set_last_error leaves an unchanged last_error alone), so each wait wakes collection at most until a collection taken during it is loaded. A recorded cubby is settled through MGS alone, so none of its waits wake collection.
fn wakes_inventory_collection(
    wait: &SledExpungePowerOffWait,
    collection: Option<&Collection>,
    request: &SledExpungeRequest,
) -> bool {
    let newer_collection_may_help = match wait {
        SledExpungePowerOffWait::SledAgentWithoutSp { .. } => true,
        SledExpungePowerOffWait::NoMgs
        | SledExpungePowerOffWait::NotPoweredOff { .. }
        | SledExpungePowerOffWait::NotASled { .. }
        | SledExpungePowerOffWait::InAnotherRack { .. }
        | SledExpungePowerOffWait::SwitchAttachedCubby { .. } => false,
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

// TODO-RAINCLAUDE: the updated row, or the outcome to report: a lost compare-and-swap (another Nexus moved the request) or a logged error.
fn require_updated(
    log: &Logger,
    progress: &mut RequestProgress,
    step: &str,
    result: Result<SledExpungeRequestUpdateOutcome, Error>,
) -> Result<SledExpungeRequest, SledExpungeRequestOutcome> {
    match result {
        Ok(SledExpungeRequestUpdateOutcome::Updated { request }) => {
            progress.latest_state = Some(request.state);
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

// TODO-RAINCLAUDE: like other background tasks, a step's error is logged and reported in the task's status, never in last_error, and the next activation retries the request; retryable() picks only the log level. A known-wait last_error written earlier (for example RemovalStuckPreparing) stays on the row when a later step hits an error, until the request's next state or last_error write replaces it.
fn error_outcome(
    log: &Logger,
    step: &str,
    error: &Error,
) -> SledExpungeRequestOutcome {
    if error.retryable() {
        warn!(log, "sled expunge request step failed"; "step" => step, error);
    } else {
        error!(log, "sled expunge request step failed"; "step" => step, error);
    }
    SledExpungeRequestOutcome::Error {
        error: format!("{step}: {}", InlineErrorChain::new(error)),
    }
}

// TODO-RAINCLAUDE: why a request needs attention, carrying its typed reason; this module is the only writer of last_error.
#[derive(Clone, Debug)]
enum NeedsAttention {
    RemovalStuckPreparing,
    ReAddPreparing { re_add_epoch: Epoch },
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
            "This sled was found in switch-attached cubby {slot}, so it is a \
             scrimlet (it manages a switch), and Nexus will not power it off. \
             Expunging a scrimlet through this API is not supported. Contact \
             Oxide support."
        ),
        SledExpungePowerOffWait::NoMgs => format!(
            "Nexus could not find the management gateway of either switch in \
             this sled's rack, so it cannot reach the sled's cubby. \
             {KEEPS_RETRYING}"
        ),
        SledExpungePowerOffWait::NotPoweredOff { slot, failed_attempts } => {
            format!(
                "Nexus could not power off this sled in cubby {slot} ({}). \
                 {KEEPS_RETRYING}",
                failed_attempts
                    .iter()
                    .map(|attempt| power_off_failure_text(&attempt.failure))
                    .collect::<BTreeSet<_>>()
                    .iter()
                    .join("; "),
            )
        }
        SledExpungePowerOffWait::SledAgentWithoutSp { .. } => format!(
            "This sled was not found when the request was created, but the \
             latest inventory collection has its sled agent and not its \
             service processor, so Nexus cannot tell which cubby to power \
             off. {KEEPS_RETRYING}"
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
    }
}

fn power_off_failure_text(failure: &MgsPowerOffFailure) -> &'static str {
    match failure {
        MgsPowerOffFailure::IdentityUnknown => {
            "the sled's cubby reported an empty baseboard identity"
        }
        MgsPowerOffFailure::SpUnreachableCubbyUnpowered => {
            "a management gateway could not reach the sled's service \
             processor while ignition reported its cubby unpowered, but \
             another management gateway did not confirm that"
        }
        MgsPowerOffFailure::SpUnreachableCubbyPowered => {
            "the sled's service processor did not answer, but its cubby is \
             powered"
        }
        MgsPowerOffFailure::IgnitionStateUnknown => {
            "the sled's service processor did not answer, and its cubby's \
             power state could not be read"
        }
        MgsPowerOffFailure::IgnitionPowerOffFailed => {
            "a management gateway did not confirm the power-off"
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
                    Some(SledExpungePowerOffOutcome::AlreadyOff) => {
                        " Nexus had found the sled already powered off."
                    }
                    Some(SledExpungePowerOffOutcome::OtherBoardPresent) => {
                        " Nexus had found another sled in its cubby."
                    }
                    Some(SledExpungePowerOffOutcome::NotPresent) => {
                        " Nexus had found its cubby empty or unpowered."
                    }
                    Some(SledExpungePowerOffOutcome::AbsentAtCreation) => {
                        " Nexus had confirmed that the sled was gone."
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
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::helpers::SledUpdateBuilder;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_expect_written;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::TEST_SP_SLOT;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::backdate_sled_expunge_request;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_for_sled;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request_with_sp_slot;
    use nexus_db_queries::db::pub_test_utils::sled_expunge_request::set_sled_expunge_request_state;
    use nexus_db_queries::db::pub_test_utils::trust_quorum::TrustQuorumConfigDriver;
    use nexus_inventory::CollectionBuilder;
    use nexus_networking::GatewayClient;
    use nexus_types::identity::Asset;
    use nexus_types::internal_api::background::SledExpungeCubbyIgnition;
    use nexus_types::internal_api::background::SledExpungeIgnitionSnapshot;
    use nexus_types::internal_api::background::SledExpungeMgsPowerOffFailure;
    use nexus_types::inventory::ServiceProcessor;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledExpungeRequestUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use trust_quorum_types::membership::TrustQuorumMembers;
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
            Self::with_target(db, None).await
        }

        // TODO-RAINCLAUDE: target, when given, is the sled's baseboard, so that a request can name a sim gimlet.
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
            tq_epoch: i64,
            state: SledExpungeRequestState,
        ) -> SledExpungeRequest {
            self.insert_request_for_sled(db, self.sled_id, tq_epoch, state)
                .await
        }

        // TODO-RAINCLAUDE: a powering_off request at the committed epoch 2 removal, with the given recorded cubby (None: gone at creation).
        async fn insert_powering_off(
            &self,
            db: &TestDatabase,
            sp_slot: Option<u16>,
        ) -> SledExpungeRequest {
            self.propose_removal(db).await;
            self.tq(db).commit(2).await;
            let request = self
                .insert_request(db, 2, SledExpungeRequestState::PoweringOff)
                .await;
            set_sp_slot(db, &request, sp_slot.map(SpMgsSlot::from)).await
        }

        async fn removal_committed_at(
            &self,
            db: &TestDatabase,
        ) -> DateTime<Utc> {
            db.datastore()
                .tq_get_config(
                    db.opctx(),
                    authz::TrustQuorumConfig::for_rack_id(self.rack_id),
                    Epoch(2),
                )
                .await
                .expect("read the removal config")
                .expect("the removal config exists")
                .time_committing
                .expect("the removal config has committed")
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

    // TODO-RAINCLAUDE: a waiting request whose removal has committed, in a database-only test: it advances and goes on to powering_off, where MGS discovery finds no MGS for its recorded cubby.
    fn chained_no_mgs_status(
        request: &SledExpungeRequest,
    ) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            state_after: Some(SledExpungeTaskRequestState::PoweringOff),
            ..no_mgs_status(request)
        }
    }

    // TODO-RAINCLAUDE: a powering_off request with a recorded cubby in a database-only test, where there are no DNS servers, so MGS discovery fails and the request waits with a last_error.
    fn no_mgs_status(request: &SledExpungeRequest) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            power_check: Some(SledExpungePowerCheck::Waiting {
                wait: SledExpungePowerOffWait::NoMgs,
            }),
            ..status_for(
                request,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: NO_MGS_TEXT.to_string(),
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

    // TODO-RAINCLAUDE: a task with no DNS servers, so MGS discovery fails, and no inventory collection loaded; its activators are wired up, and handles to them are returned so tests can check them.
    fn test_task(db: &TestDatabase) -> (SledExpunge, SledExpungeActivators) {
        let (task, _, activators) = test_task_with_inventory(db);
        (task, activators)
    }

    struct TestInventory {
        tx: watch::Sender<Option<Arc<Collection>>>,
        collection_activator: Activator,
    }

    impl TestInventory {
        fn load(&self, collection: Collection) {
            self.tx.send_replace(Some(Arc::new(collection)));
        }
    }

    fn test_task_with_inventory(
        db: &TestDatabase,
    ) -> (SledExpunge, TestInventory, SledExpungeActivators) {
        let activators = test_activators();
        let (tx, rx) = watch::channel(None);
        let collection_activator = wired_up_activator();
        let task = SledExpunge::new(
            db.datastore().clone(),
            no_dns_servers_resolver(db),
            rx,
            collection_activator.clone(),
            activators.clone(),
        );
        (task, TestInventory { tx, collection_activator }, activators)
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
            ignition_snapshots: IdOrdMap::new(),
        }
    }

    async fn drive_one(
        task: &SledExpunge,
        opctx: &OpContext,
        request: &SledExpungeRequest,
        collection: Option<&Collection>,
        now: DateTime<Utc>,
    ) -> SledExpungeRequestStatus {
        let mgs = ActivationMgs::new(&opctx.log, &task.resolver);
        let mut progress = RequestProgress::default();
        let outcome = task
            .drive_request(
                opctx,
                PoweringOffInputs { mgs: &mgs, collection },
                request,
                now,
                &mut progress,
            )
            .await;
        request_status(request, outcome, progress)
    }

    async fn set_sp_slot(
        db: &TestDatabase,
        request: &SledExpungeRequest,
        sp_slot: Option<SpMgsSlot>,
    ) -> SledExpungeRequest {
        use nexus_db_schema::schema::sled_expunge_request::dsl;
        let conn = db
            .datastore()
            .pool_connection_for_tests()
            .await
            .expect("got a database connection");
        diesel::update(dsl::sled_expunge_request)
            .filter(dsl::id.eq(request.id))
            .set(dsl::sp_slot.eq(sp_slot))
            .returning(SledExpungeRequest::as_returning())
            .get_result_async(&*conn)
            .await
            .expect("set the request's sp_slot")
    }

    fn collection_started_at(time_started: DateTime<Utc>) -> Collection {
        let mut collection = CollectionBuilder::new("test").build();
        collection.time_started = time_started;
        collection
    }

    fn sled_sp(rack_id: RackUuid, sp_slot: u16) -> ServiceProcessor {
        ServiceProcessor {
            time_collected: now_db_precision(),
            source: "test".to_string(),
            sp_type: SpType::Sled,
            sp_slot,
            baseboard_revision: 0,
            hubris_archive: "test".to_string(),
            power_state: PowerState::A0,
            rack_id,
        }
    }

    fn completed() -> SledExpungeRequestOutcome {
        SledExpungeRequestOutcome::Completed {
            previous_sled_policy: SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
        }
    }

    fn requests_after_failed_discovery(
        status: SledExpungeStatus,
    ) -> IdOrdMap<SledExpungeRequestStatus> {
        match status {
            SledExpungeStatus::Listed {
                requests,
                mgs_discovery: SledExpungeMgsDiscovery::Failed { .. },
                ignition_snapshots,
            } => {
                assert_eq!(ignition_snapshots, IdOrdMap::new());
                requests
            }
            other => panic!("expected failed MGS discovery, got {other:?}"),
        }
    }

    fn waiting_for_collection(
        rack: &TestRack,
        request: &SledExpungeRequest,
        wait: SledExpungeCollectionWait,
    ) -> SledExpungeRequestStatus {
        SledExpungeRequestStatus {
            power_check: Some(SledExpungePowerCheck::WaitingForCollection {
                wait,
            }),
            ..rack.status(
                request,
                SledExpungeRequestOutcome::WaitingForCollection,
            )
        }
    }

    // TODO-RAINCLAUDE: a sled gone at creation waits, writing nothing and waking inventory collection, while the latest collection started before the removal committed, and again while a later collection has no SP records for its rack; a collection started after the removal with SP records for the rack but neither the sled's SP nor its sled agent advances it as absent_at_creation, and it is expunged in the same activation.
    #[tokio::test]
    async fn test_sled_expunge_gone_at_creation_confirmed() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_gone_at_creation_confirmed");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (mut task, inventory, _) = test_task_with_inventory(&db);
        let rack = TestRack::new(&db).await;
        let request = rack.insert_powering_off(&db, None).await;
        let committed_at = rack.removal_committed_at(&db).await;

        let before = committed_at - TimeDelta::seconds(1);
        inventory.load(collection_started_at(before));
        assert_eq!(
            task.actually_activate(db.opctx(), now_db_precision()).await,
            listed(vec![waiting_for_collection(
                &rack,
                &request,
                SledExpungeCollectionWait::NoCollectionSinceRemoval {
                    removal_committed_at: committed_at,
                    collection_started_at: before,
                },
            )]),
        );
        inventory.collection_activator.assert_activated(
            "waiting for a collection after the removal wakes inventory \
             collection",
        );
        assert_eq!(read(&db, &request).await, request, "nothing is written");

        let no_rack_sps = collection_started_at(now_db_precision());
        let no_rack_sps_id = no_rack_sps.id;
        inventory.load(no_rack_sps);
        assert_eq!(
            task.actually_activate(db.opctx(), now_db_precision()).await,
            listed(vec![waiting_for_collection(
                &rack,
                &request,
                SledExpungeCollectionWait::NoRackSpData {
                    collection_id: no_rack_sps_id,
                },
            )]),
        );
        inventory.collection_activator.assert_activated(
            "a collection without the rack's SPs wakes inventory collection",
        );
        assert_eq!(read(&db, &request).await, request, "nothing is written");

        let mut after = collection_started_at(now_db_precision());
        after.sps.insert(
            Arc::new(BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            }),
            sled_sp(rack.rack_id, 5),
        );
        let collection_id = after.id;
        inventory.load(after);
        assert_eq!(
            task.actually_activate(db.opctx(), now_db_precision()).await,
            listed(vec![SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::AbsentAtCreation {
                    collection_id,
                }),
                ..rack.status(&request, completed())
            }]),
        );
        let done = read(&db, &request).await;
        assert_eq!(
            (done.state, done.power_off_outcome, done.sp_slot),
            (
                SledExpungeRequestState::Done,
                Some(SledExpungePowerOffOutcome::AbsentAtCreation),
                None,
            ),
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: a sled gone at creation whose SP shows up again in a collection taken after the removal committed gets that cubby recorded and goes straight on to the recorded-cubby path (here finding no MGS). Driving the stale listed row again finds the same cubby already recorded and goes on the same way. An SP back in a switch-attached cubby is not recorded and waits.
    #[tokio::test]
    async fn test_sled_expunge_gone_at_creation_reappears() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_gone_at_creation_reappears");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (mut task, inventory, _) = test_task_with_inventory(&db);
        let rack = TestRack::new(&db).await;
        let request = rack.insert_powering_off(&db, None).await;
        let scrimlet = TestRack::new(&db).await;
        let scrimlet_request = scrimlet.insert_powering_off(&db, None).await;

        let mut collection = collection_started_at(now_db_precision());
        collection
            .sps
            .insert(Arc::new(rack.target.clone()), sled_sp(rack.rack_id, 3));
        collection.sps.insert(
            Arc::new(scrimlet.target.clone()),
            sled_sp(scrimlet.rack_id, 14),
        );
        inventory.load(collection.clone());
        assert_eq!(
            requests_after_failed_discovery(
                task.actually_activate(db.opctx(), now_db_precision()).await
            ),
            statuses([
                no_mgs_status(&request),
                SledExpungeRequestStatus {
                    power_check: Some(SledExpungePowerCheck::Waiting {
                        wait: SledExpungePowerOffWait::SwitchAttachedCubby {
                            slot: 14,
                        },
                    }),
                    ..scrimlet.status(
                        &scrimlet_request,
                        SledExpungeRequestOutcome::LastErrorSet {
                            last_error: switch_attached_cubby_text(14),
                        },
                    )
                },
            ]),
        );
        let recorded = read(&db, &request).await;
        assert_eq!(
            (recorded.state, recorded.sp_slot),
            (SledExpungeRequestState::PoweringOff, Some(SpMgsSlot::from(3))),
        );
        assert_eq!(read(&db, &scrimlet_request).await.sp_slot, None);

        assert_eq!(
            drive_one(
                &task,
                db.opctx(),
                &request,
                Some(&collection),
                now_db_precision()
            )
            .await,
            SledExpungeRequestStatus {
                power_check: Some(SledExpungePowerCheck::Waiting {
                    wait: SledExpungePowerOffWait::NoMgs,
                }),
                ..rack.status(
                    &request,
                    SledExpungeRequestOutcome::LastErrorUnchanged {
                        last_error: NO_MGS_TEXT.to_string(),
                    },
                )
            },
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: a sim rack with gimlets in cubbies 0 through 2, behind one MGS.
    async fn sim_rack_with_three_gimlets(
        test_name: &str,
    ) -> gateway_test_utils::setup::GatewayTestContext {
        let (mut mgs_config, mut sp_sim_config) =
            gateway_test_utils::setup::load_test_config(
                gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG.into(),
            );
        let added = gateway_test_utils::setup::add_simulated_gimlet_sps(
            &mut sp_sim_config,
            1,
        );
        gateway_test_utils::setup::add_simulated_gimlet_ports(
            &mut mgs_config,
            added,
        );
        gateway_test_utils::setup::test_setup_with_config(
            test_name,
            gateway_messages::SpPort::One,
            mgs_config,
            &sp_sim_config,
            None,
        )
        .await
    }

    async fn sim_gimlet_baseboard(
        sim: &gateway_test_utils::setup::GatewayTestContext,
        slot: usize,
    ) -> BaseboardId {
        use sp_sim::SimulatedSp;
        let state = sim.simrack.gimlets[slot].state().await;
        BaseboardId {
            part_number: state.model,
            serial_number: state.serial_number,
        }
    }

    fn discovered<'r>(
        log: &'r Logger,
        resolver: &'r internal_dns_resolver::Resolver,
        racks: &[RackUuid],
        mgs: &GatewayClient,
    ) -> ActivationMgs<'r> {
        let racks = racks
            .iter()
            .map(|rack_id| RackMgs {
                rack_id: *rack_id,
                gateways: SledPowerOffGateways::new(std::slice::from_ref(mgs)),
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
        let driver = ActivationDriver {
            task,
            opctx,
            inputs: PoweringOffInputs { mgs, collection: None },
            now: now_db_precision(),
        };
        statuses(
            drive_requests_with_timeout(
                &opctx.log,
                requests,
                &driver,
                &task.activators,
            )
            .await,
        )
    }

    // TODO-RAINCLAUDE: one request per recorded-cubby outcome the sim can produce, each in its own rack, driven by one activation: each advances with its outcome and is expunged; only the rack whose SP did not answer needed an ignition snapshot.
    #[tokio::test]
    async fn test_sled_expunge_recorded_cubby_outcomes() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_recorded_cubby_outcomes");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let sim = sim_rack_with_three_gimlets(
            "test_sled_expunge_recorded_cubby_outcomes",
        )
        .await;
        let log = &logctx.log;
        let up = GatewayClient::from_addr(log, sim.address());
        let gimlets = [
            sim_gimlet_baseboard(&sim, 0).await,
            sim_gimlet_baseboard(&sim, 1).await,
            sim_gimlet_baseboard(&sim, 2).await,
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

        let powered_off = TestRack::with_target(&db, Some(&gimlets[1])).await;
        let already_off = TestRack::with_target(&db, Some(&gimlets[0])).await;
        let other_board = TestRack::new(&db).await;
        let not_present = TestRack::with_target(&db, Some(&gimlets[2])).await;
        let cases = [
            (
                &powered_off,
                1,
                SledExpungePowerCheck::PoweredOff { slot: 1, mgs: up.addr },
                SledExpungePowerOffOutcome::PoweredOff,
            ),
            (
                &already_off,
                0,
                SledExpungePowerCheck::AlreadyOff { slot: 0, mgs: up.addr },
                SledExpungePowerOffOutcome::AlreadyOff,
            ),
            (
                &other_board,
                0,
                SledExpungePowerCheck::OtherBoardPresent {
                    slot: 0,
                    mgs: up.addr,
                    found: gimlets[0].clone(),
                },
                SledExpungePowerOffOutcome::OtherBoardPresent,
            ),
            (
                &not_present,
                2,
                SledExpungePowerCheck::NotPresent {
                    slot: 2,
                    mgs: up.addr,
                    ignition: SledExpungeCubbyIgnition::Unpowered,
                },
                SledExpungePowerOffOutcome::NotPresent,
            ),
        ];
        let mut requests = Vec::new();
        let mut expected = Vec::new();
        for (rack, slot, check, _) in &cases {
            let request = rack.insert_powering_off(&db, Some(*slot)).await;
            expected.push(SledExpungeRequestStatus {
                power_check: Some(check.clone()),
                ..rack.status(&request, completed())
            });
            requests.push(request);
        }

        let racks: Vec<RackUuid> =
            cases.iter().map(|(rack, ..)| rack.rack_id).collect();
        let mgs = discovered(log, &task.resolver, &racks, &up);
        assert_eq!(
            drive_activation(&task, db.opctx(), &mgs, requests.clone()).await,
            statuses(expected),
        );
        assert_eq!(
            mgs.ignition_snapshots(),
            [SledExpungeRackIgnitionSnapshot {
                rack_id: not_present.rack_id,
                snapshot: SledExpungeIgnitionSnapshot::Fetched {
                    mgs: up.addr,
                    failed_attempts: IdOrdMap::new(),
                },
            }]
            .into_iter()
            .collect::<IdOrdMap<_>>(),
        );
        for (request, (_, slot, _, outcome)) in requests.iter().zip(&cases) {
            let done = read(&db, request).await;
            assert_eq!(
                (done.state, done.sp_slot, done.power_off_outcome),
                (
                    SledExpungeRequestState::Done,
                    Some(SpMgsSlot::from(*slot)),
                    Some(*outcome),
                ),
            );
        }
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
            vec![(0, true), (1, false), (2, false)],
            "only cubby 1 was powered off by the task",
        );

        sim.teardown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: three requests in one rack whose SPs do not answer while their cubbies stay powered: each needs the rack's ignition snapshot, which one GET /ignition fetches for all of them, and each waits with a failed power-off.
    #[tokio::test]
    async fn test_sled_expunge_shares_ignition_snapshot() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_shares_ignition_snapshot");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let sim = sim_rack_with_three_gimlets(
            "test_sled_expunge_shares_ignition_snapshot",
        )
        .await;
        let log = &logctx.log;
        let up = GatewayClient::from_addr(log, sim.address());
        let rack_id = RackUuid::new_v4();
        let mut requests = Vec::new();
        for slot in 0..3u16 {
            use sp_sim::SimulatedSp;
            sim.simrack.gimlets[usize::from(slot)]
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
                    Some(SpMgsSlot::from(slot)),
                    i64::from(slot) + 2,
                    SledExpungeRequestState::PoweringOff,
                )
                .await,
            );
        }

        let mgs = discovered(log, &task.resolver, &[rack_id], &up);
        let found =
            drive_activation(&task, db.opctx(), &mgs, requests.clone()).await;
        for request in &requests {
            let status = found.get(&request.id()).expect("request was driven");
            let slot = **request.sp_slot.expect("request has a cubby");
            assert_eq!(
                status.outcome,
                SledExpungeRequestOutcome::LastErrorSet {
                    last_error: format!(
                        "Nexus could not power off this sled in cubby {slot} \
                         (the sled's service processor did not answer, but \
                         its cubby is powered). {KEEPS_RETRYING}"
                    ),
                },
            );
            match &status.power_check {
                Some(SledExpungePowerCheck::Waiting {
                    wait:
                        SledExpungePowerOffWait::NotPoweredOff {
                            slot: waited_slot,
                            failed_attempts,
                        },
                }) => {
                    assert_eq!(*waited_slot, slot);
                    assert_eq!(
                        failed_attempts
                            .iter()
                            .map(|attempt| (attempt.mgs, attempt.failure))
                            .collect::<Vec<_>>(),
                        vec![(
                            up.addr,
                            MgsPowerOffFailure::SpUnreachableCubbyPowered
                        )],
                    );
                }
                other => panic!("unexpected power check {other:?}"),
            }
        }
        assert_eq!(
            mgs.ignition_snapshots(),
            [SledExpungeRackIgnitionSnapshot {
                rack_id,
                snapshot: SledExpungeIgnitionSnapshot::Fetched {
                    mgs: up.addr,
                    failed_attempts: IdOrdMap::new(),
                },
            }]
            .into_iter()
            .collect::<IdOrdMap<_>>(),
        );

        sim.teardown().await;
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
                SledExpungeRequestState::WaitingForTrustQuorum,
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

    // TODO-RAINCLAUDE: one request per way the task can act on a request, all driven by one activation; each status that reports a write shows it landed, since a lost compare-and-swap reports NotInExpectedState, and the request whose records contradict it reports an error and is left unwritten.
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
                SledExpungeRequestState::WaitingForTrustQuorum,
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
                SledExpungeRequestState::WaitingForTrustQuorum,
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

        // TODO-RAINCLAUDE: each re-add is proposed before the request row exists; see check_no_new_member_being_expunged_conn. A re-add still preparing waits with a last_error, and one that has started committing fails the request, whose failure reason says whether Nexus had powered the sled off (it had, for a request in expunging).
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
            .insert_request(&db, 2, SledExpungeRequestState::PoweringOff)
            .await;
        expected.push(no_mgs_status(&powering_off_request));

        let expunging = TestRack::new(&db).await;
        expunging.propose_removal(&db).await;
        expunging.tq(&db).commit(2).await;
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

        // TODO-RAINCLAUDE: the request points at epoch 1, the committed RSS config that still includes the sled, which the gate reports as an internal error; last_error is not written.
        let inconsistent = TestRack::new(&db).await;
        let inconsistent_request = inconsistent
            .insert_request(
                &db,
                1,
                SledExpungeRequestState::WaitingForTrustQuorum,
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

        // TODO-RAINCLAUDE: the request's sled row does not exist, so it fails without the sled's policy.
        let missing_sled = TestRack::new(&db).await;
        missing_sled.propose_removal(&db).await;
        missing_sled.tq(&db).abort(2).await;
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

        match task.actually_activate(db.opctx(), now_db_precision()).await {
            SledExpungeStatus::Listed {
                requests,
                mgs_discovery: SledExpungeMgsDiscovery::Failed { .. },
                ignition_snapshots,
            } => {
                assert_eq!(requests, statuses(expected));
                assert_eq!(ignition_snapshots, IdOrdMap::new());
            }
            other => panic!("expected failed MGS discovery, got {other:?}"),
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
        rack.tq(&db).ack_all_prepares(2).await;
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
            drive_one(&task, db.opctx(), &stale, None, now_db_precision())
                .await,
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
        let gateways =
            SledPowerOffGateways::new(std::slice::from_ref(&unreachable));
        let mut progress = RequestProgress::default();
        let outcome = task
            .power_off(
                db.opctx(),
                log,
                &powering_off,
                None,
                &gateways,
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
            Some(SpMgsSlot::from(3)),
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
            NeedsAttention::PowerOff(SledExpungePowerOffWait::NotPoweredOff {
                slot: 3,
                failed_attempts: [
                    ("[::1]:1", MgsPowerOffFailure::IgnitionPowerOffFailed),
                    ("[::1]:2", MgsPowerOffFailure::SpUnreachableCubbyPowered),
                    ("[::1]:3", MgsPowerOffFailure::IgnitionPowerOffFailed),
                    ("[::1]:4", MgsPowerOffFailure::IgnitionStateUnknown),
                ]
                .into_iter()
                .map(|(mgs, failure)| SledExpungeMgsPowerOffFailure {
                    mgs: mgs.parse().expect("parsed an MGS address"),
                    failure,
                    error: "error".to_string(),
                })
                .collect(),
            })
            .render(&request),
            format!(
                "Nexus could not power off this sled in cubby 3 (a \
                 management gateway did not confirm the power-off; the \
                 sled's service processor did not answer, and its cubby's \
                 power state could not be read; the sled's service \
                 processor did not answer, but its cubby is powered). \
                 {KEEPS_RETRYING}"
            ),
        );
        let policy = Some(SledPolicy::Expunged);
        let re_added = FailureReason::ReAdded { re_add_epoch: Epoch(3) };
        let cases = [
            (None, ""),
            (
                Some(SledExpungePowerOffOutcome::PoweredOff),
                " Nexus had powered the sled off.",
            ),
            (
                Some(SledExpungePowerOffOutcome::AlreadyOff),
                " Nexus had found the sled already powered off.",
            ),
            (
                Some(SledExpungePowerOffOutcome::OtherBoardPresent),
                " Nexus had found another sled in its cubby.",
            ),
            (
                Some(SledExpungePowerOffOutcome::NotPresent),
                " Nexus had found its cubby empty or unpowered.",
            ),
            (
                Some(SledExpungePowerOffOutcome::AbsentAtCreation),
                " Nexus had confirmed that the sled was gone.",
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

    // TODO-RAINCLAUDE: one row per branch of the gone-at-creation decision; the representative collection has sled agents (one of which is made the request's) and SPs in one rack, which is made the request's, none of them the request's own until a row adds it.
    #[test]
    fn test_sled_expunge_gone_at_creation_check() {
        let representative = nexus_inventory::examples::representative();
        let sled_agent_id = representative.sled_agents[0];
        let base = representative.builder.build();
        let rack_id = base
            .sps
            .values()
            .next()
            .expect("the representative collection has SPs")
            .rack_id;
        let request = SledExpungeRequest {
            sp_slot: None,
            rack_id: rack_id.into(),
            ..test_request()
        };
        let other_rack = RackUuid::new_v4();
        let committed_at = base.time_started - TimeDelta::seconds(1);
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
        let with_sp = |sp| {
            let mut collection = base.clone();
            collection.sps.insert(Arc::new(request.baseboard_id()), sp);
            collection
        };

        assert_eq!(
            gone_at_creation_check(&request, &base, base.time_started),
            GoneAtCreationCheck::CollectionWait(
                SledExpungeCollectionWait::NoCollectionSinceRemoval {
                    removal_committed_at: base.time_started,
                    collection_started_at: base.time_started,
                }
            ),
            "a collection that started when the removal committed does not \
             count",
        );
        assert_eq!(
            gone_at_creation_check(&request, &base, committed_at),
            GoneAtCreationCheck::StillGone,
        );
        let in_rack_without_sps = SledExpungeRequest {
            rack_id: other_rack.into(),
            ..request.clone()
        };
        assert_eq!(
            gone_at_creation_check(&in_rack_without_sps, &base, committed_at),
            GoneAtCreationCheck::CollectionWait(
                SledExpungeCollectionWait::NoRackSpData {
                    collection_id: base.id,
                }
            ),
            "a collection with no SPs in the request's rack proves nothing",
        );
        let with_sled_agent = SledExpungeRequest {
            sled_id: sled_agent_id.into(),
            ..request.clone()
        };
        assert_eq!(
            gone_at_creation_check(&with_sled_agent, &base, committed_at),
            GoneAtCreationCheck::Wait(
                SledExpungePowerOffWait::SledAgentWithoutSp {
                    collection_id: base.id,
                }
            ),
        );
        let cases = [
            (
                sp(SpType::Sled, 3, rack_id),
                GoneAtCreationCheck::SpFound { slot: 3 },
            ),
            (
                sp(SpType::Switch, 0, rack_id),
                GoneAtCreationCheck::Wait(SledExpungePowerOffWait::NotASled {
                    sp_type: SpType::Switch,
                }),
            ),
            (
                sp(SpType::Sled, 3, other_rack),
                GoneAtCreationCheck::Wait(
                    SledExpungePowerOffWait::InAnotherRack {
                        rack_id: other_rack,
                    },
                ),
            ),
        ];
        for (sp, expected) in cases {
            assert_eq!(
                gone_at_creation_check(
                    &request,
                    &with_sp(sp.clone()),
                    committed_at
                ),
                expected,
                "{sp:?}",
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

        let statuses_out = drive_requests_with_timeout(
            &log,
            vec![stalled.clone(), healthy.clone()],
            &driver,
            &test_activators(),
        )
        .await;
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

    // TODO-RAINCLAUDE: a timeout after the request may have entered expunging activates instance_watcher and blueprint_planner, and one while settling the sled's cubby through MGS does not.
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
                vec![stalled.clone()],
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
        SledExpungeRequest::new(
            RackUuid::new_v4(),
            SledUuid::new_v4(),
            BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            },
            Some(SpMgsSlot::from(3)),
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

    // TODO-RAINCLAUDE: sets the sled row's state directly, bypassing the transition checks, to make the expunge in complete fail.
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

    // TODO-RAINCLAUDE: the request advances to expunging and the expunge then fails in the same activation; the error is reported in the status with the state the advance left the request in, and last_error is not written.
    #[tokio::test]
    async fn test_sled_expunge_error_after_advance() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_error_after_advance");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (task, _) = test_task(&db);
        let log = &db.opctx().log;
        let rack = TestRack::new(&db).await;
        rack.propose_removal(&db).await;
        rack.tq(&db).commit(2).await;
        let listed = rack
            .insert_request(&db, 2, SledExpungeRequestState::PoweringOff)
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
                    sp_slot: SpMgsSlot::from(TEST_SP_SLOT),
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
                state_after: Some(SledExpungeTaskRequestState::Expunging),
                ..rack.status(
                    &listed,
                    SledExpungeRequestOutcome::Error { error },
                )
            },
        );
        assert_eq!(
            expunging,
            SledExpungeRequest {
                state: SledExpungeRequestState::Expunging,
                power_off_outcome: Some(SledExpungePowerOffOutcome::PoweredOff),
                last_error: None,
                time_modified: expunging.time_modified,
                ..listed.clone()
            },
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

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
        let not_powered_off = |failures: &[MgsPowerOffFailure]| {
            SledExpungePowerOffWait::NotPoweredOff {
                slot: 3,
                failed_attempts: failures
                    .iter()
                    .zip(1..)
                    .map(|(failure, port)| SledExpungeMgsPowerOffFailure {
                        mgs: SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0),
                        failure: *failure,
                        error: "error".to_string(),
                    })
                    .collect(),
            }
        };
        for (wait, newer_collection_may_help) in [
            (
                SledExpungePowerOffWait::SledAgentWithoutSp {
                    collection_id: CollectionUuid::new_v4(),
                },
                true,
            ),
            (
                not_powered_off(&[
                    MgsPowerOffFailure::SpUnreachableCubbyPowered,
                    MgsPowerOffFailure::IgnitionStateUnknown,
                    MgsPowerOffFailure::IgnitionPowerOffFailed,
                ]),
                false,
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
