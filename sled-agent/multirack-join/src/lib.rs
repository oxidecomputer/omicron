// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multirack Join Service
//!
//! This is the bootstrap service that provisions a new rack on an existing
//! underlay network such that it can be adopted by an existing Nexus in a
//! cluster on the same network. It's vastly simpler than RSS because only
//! sled-agent is started. No other control plane zones, including DNS and NTP,
//! are started. Reconfigurator on the existing Nexuses will setup the rack post
//! cluster join.
//!
//! See RFD 680 for further details.

#[macro_use]
extern crate slog;
use bootstrap_agent_lockstep_types::{
    CommitState, MultirackJoinRequest, MultirackJoinServiceState,
    SledAgentInfo, SledAgentStartState, StartSledAgentsStatus,
};
use nexus_types::trust_quorum::TrustQuorumConfig;
use omicron_common::address::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use omicron_ledger::{self as ledger};
use omicron_uuid_kinds::RackUuid;
use sled_agent_bootstrap_common::sprockets::SprocketsClient;
use sled_agent_bootstrap_common::{RssContext, RunRssError};
use sled_agent_types::early_networking::EarlyNetworkConfigEnvelope;
use sled_agent_types::rack_init::rack_init_bootstore_generation;
use sled_agent_types::sled::{
    StartSledAgentRequest, StartSledAgentRequestBody,
};
use sled_agent_types::system_networking::SystemNetworkingConfig;
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info};
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::watch,
    task::{JoinError, JoinSet},
};
use trust_quorum::{NodeApiError, ProxyError};
use trust_quorum_types::{
    messages::ReconfigureMsg as TqReconfigureMsg, types::Epoch,
};

const INITIAL_TRUST_QUORUM_EPOCH: Epoch = Epoch(1);
const TRUST_QUORUM_RETRY_TIMEOUT: Duration = Duration::from_millis(500);

/// Describes errors which may occur while operating the multirack join service.
#[derive(Error, Debug, SlogInlineError)]
pub enum MultirackJoinServiceError {
    #[error("Rack already initialized")]
    RackAlreadyInitialized,

    #[error("Rack initialization was interrupted. Clean-slate required")]
    RackInitInterrupted,

    #[error("Trust quorum error")]
    TqApiError(#[from] NodeApiError),

    #[error("Trust quorum coordinator doesn't think it's a coordinator")]
    TqBadCoordinator,

    #[error("Trust quorum commit cannot complete: {0:?}")]
    TqCommitFailed(CommitState),

    #[error("Failed to receive input from bootstrap agent")]
    InputRx(#[from] watch::error::RecvError),

    #[error("Failed to join proxy commit task")]
    ProxyCommit(#[from] JoinError),

    #[error(
        "Sprockets connections not available for all sleds. Missing {0:#?}"
    )]
    MissingSledConnections(BTreeSet<BaseboardId>),

    #[error("Failed to access ledger")]
    Ledger(#[from] ledger::Error),

    #[error("Bootstore error")]
    Bootstore(#[from] bootstore::schemes::v0::NodeRequestError),
}

impl From<RunRssError> for MultirackJoinServiceError {
    fn from(value: RunRssError) -> Self {
        match value {
            RunRssError::RackAlreadyInitialized => Self::RackAlreadyInitialized,
            RunRssError::RackInitInterrupted => Self::RackInitInterrupted,
        }
    }
}

// The value returned from `MultirackJoinServiceTask::tq_prepare`
enum TqPrepareResult {
    Prepared,
    ReconfigurationNeeded {
        new_members: BTreeSet<BaseboardId>,
        new_epoch: Epoch,
    },
}

// The value returned from `MultirackJoinServiceTask::tq_commit`
enum TqCommitResult {
    Committed,
    ReconfigurationNeeded {
        new_members: BTreeSet<BaseboardId>,
        new_epoch: Epoch,
        just_committed_epoch: Epoch,
    },
}

/// All the information required to start a sled agent remotely over a sprockets
/// channel.
struct StartSledAgentInfo {
    baseboard_id: BaseboardId,
    bootstrap_ip: Ipv6Addr,
    req: StartSledAgentRequest,
}

impl StartSledAgentInfo {
    fn new(
        rack_id: RackUuid,
        bootstrap_ip: Ipv6Addr,
        info: SledAgentInfo,
    ) -> Self {
        StartSledAgentInfo {
            baseboard_id: info.baseboard_id.clone(),
            bootstrap_ip,
            req: StartSledAgentRequest {
                generation: 0,
                schema_version: 1,
                body: StartSledAgentRequestBody {
                    id: info.sled_id,
                    subnet: info.sled_subnet,
                    use_trust_quorum: true,
                    is_lrtq_learner: false,
                    rack_id,
                },
            },
        }
    }
}

/// The interface to the Multirack Join Service.
pub struct MultirackJoinServiceHandle {
    pub join_handle:
        tokio::task::JoinHandle<Result<(), MultirackJoinServiceError>>,
    pub input_tx: watch::Sender<MultirackJoinRequest>,
    pub output_rx: watch::Receiver<MultirackJoinServiceState>,

    // Once we've initialized trust quorum, we no longer allow membership
    // changes. This allows helpful responses to user requests in case a
    // membership change is attempted too late in the process.
    pub membership_change_still_possible: Arc<AtomicBool>,
}

impl MultirackJoinServiceHandle {
    pub fn spawn(ctx: RssContext, request: MultirackJoinRequest) -> Self {
        let (input_tx, input_rx) = watch::channel(request);
        let state = MultirackJoinServiceState::Requested;
        let (output_tx, output_rx) = watch::channel(state.clone());
        let membership_change_still_possible = Arc::new(AtomicBool::new(true));
        let mcsp = membership_change_still_possible.clone();
        let join_handle = tokio::task::spawn(async move {
            let log =
                ctx.base_log.new(o!("component" => "MultirackJoinService"));
            info!(log, "Starting Multirack Join Service");
            let mut task = MultirackJoinServiceTask {
                log,
                ctx,
                input_rx,
                output_tx,
                membership_change_still_possible: mcsp,
            };
            task.run().await
        });

        Self {
            join_handle,
            input_tx,
            output_rx,
            membership_change_still_possible,
        }
    }
}

/// The internal state of the main task running the join service
struct MultirackJoinServiceTask {
    log: Logger,
    ctx: RssContext,
    input_rx: watch::Receiver<MultirackJoinRequest>,
    output_tx: watch::Sender<MultirackJoinServiceState>,

    // Once we've initialized trust quorum, we no longer allow membership
    // changes. This allows helpful responses to user requests in case a
    // membership change is attempted too late in the process.
    pub membership_change_still_possible: Arc<AtomicBool>,
}

impl MultirackJoinServiceTask {
    /// The main loop of the Multirack Join Service
    pub async fn run(&mut self) -> Result<(), MultirackJoinServiceError> {
        self.output_tx
            .send_modify(|state| *state = MultirackJoinServiceState::Starting);

        // Check to see if we've already started RSS or multirack join
        self.ctx.is_rss_safe_to_run(&self.log).await?;
        info!(&self.log, "No RSS ledger found. Starting Multirack Join Setup");

        let rack_id = RackUuid::new_v4();
        info!(&self.log, "Created RackId {rack_id}");

        // Record that we have started RSS
        //
        // NOTE: This is a "point-of-no-return" -- before sending any requests
        // to neighboring sleds, we record that RSS has started.
        // This way, if the RSS power-cycles, it can be detected and we can
        // clean-slate and try again.
        self.ctx.write_rss_started_ledger(&self.log).await?;

        self.init_trust_quorum(rack_id).await?;

        self.start_sled_agents(rack_id).await?;

        self.configure_networking().await?;

        // We're done
        self.ctx.write_rss_completed_ledger(&self.log).await?;

        Ok(())
    }

    /// Publish this rack's network configuration to the bootstore.
    ///
    /// The scrimlet reconcilers read it from there and program the switch, so
    /// this is what gets a joining rack's front ports configured.
    async fn configure_networking(
        &mut self,
    ) -> Result<(), MultirackJoinServiceError> {
        let rack_network_config =
            self.input_rx.borrow_and_update().rack_network_config.clone();

        self.output_tx.send_modify(|state| {
            *state = MultirackJoinServiceState::ConfigureNetworking
        });

        // Only RSS places services, so a joining rack has no service zone NAT
        // entries to publish and stays at the initial generation.
        let config = SystemNetworkingConfig {
            rack_network_config,
            blueprint_external_networking_config: None,
        };

        info!(self.log, "Writing initial network configuration to bootstore");
        self.ctx
            .bootstore_node_handle
            .update_network_config(
                EarlyNetworkConfigEnvelope::from(&config)
                    .serialize_to_bootstore_with_generation(
                        rack_init_bootstore_generation::RSS_INITIAL,
                    ),
            )
            .await?;

        Ok(())
    }

    /// Try to start all sled agents on each sled with no other zones
    async fn start_sled_agents(
        &mut self,
        rack_id: RackUuid,
    ) -> Result<(), MultirackJoinServiceError> {
        info!(self.log, "Starting Sled agents");
        let req = self.input_rx.borrow_and_update().clone();
        let tq_members = req.trust_quorum_peers.clone();
        let status = StartSledAgentsStatus::assign_ids_and_subnets(req);
        self.output_tx.send_modify(|state| {
            *state = MultirackJoinServiceState::StartSledAgents(status.clone())
        });

        let mut bootstrap_ips: BTreeMap<_, _> = self
            .ctx
            .trust_quorum_handle
            .conn_mgr_status()
            .await?
            .connected_peers()
            .into_iter()
            .collect();

        // Insert this node into our map. Connected peers don't include ourself.
        bootstrap_ips.insert(
            self.ctx.trust_quorum_handle.baseboard_id().clone(),
            self.ctx.global_zone_bootstrap_ip,
        );

        let mut missing_sleds = BTreeSet::new();
        for baseboard_id in tq_members {
            if !bootstrap_ips.contains_key(&baseboard_id) {
                missing_sleds.insert(baseboard_id);
            }
        }
        // We have already initialized the trust quorum on all nodes at this
        // point, and so the number of bootstrap ips should match our expected
        // configuration.
        if !missing_sleds.is_empty() {
            return Err(MultirackJoinServiceError::MissingSledConnections(
                missing_sleds,
            ));
        }

        // Attempt to start all our sled agents in parallel
        let mut set = JoinSet::new();
        for info in status.sleds.iter().cloned() {
            // Unwrap is safe, because we constructed both bootstrap_ips and
            // status from trust_quorum_peers.
            let bootstrap_ip = *bootstrap_ips.get(&info.baseboard_id).unwrap();
            set.spawn(self.spawn_start_sled_agent_task(
                StartSledAgentInfo::new(rack_id, bootstrap_ip, info),
            ));
        }

        // Wait for each sled-agent to start
        while let Some(res) = set.join_next().await {
            // We unwrap because we build with `abort=panic`, and don't want to
            // handle join errors here.
            let baseboard_id = res.unwrap();
            info!(
                self.log,
                "Started sled agent";
                "baseboard_id" => %baseboard_id
            );
            self.output_tx.send_modify(|state| {
                let MultirackJoinServiceState::StartSledAgents(status) = state
                else {
                    panic!("MultirackJoinService in wrong state: {:#?}", state);
                };
                // Safe to unwrap since we only start tasks that return
                // baseboards that already exist in status.
                let mut info = status.sleds.get1_mut(&baseboard_id).unwrap();
                info.start_state = SledAgentStartState::Started;
            });
        }

        Ok(())
    }

    /// Spawn a task that connects to the remote sprockets server and sends a
    /// `StartSledAgentRequest`.
    ///
    /// Runs indefinitely until success.
    ///
    /// We assume all errors are transient and will correct themselves, or can
    /// be corrected by support. In the case this is not true, a clean-slate
    /// will be required.
    fn spawn_start_sled_agent_task(
        &mut self,
        info: StartSledAgentInfo,
    ) -> impl Future<Output = BaseboardId> + 'static {
        let log = self.log.new(o!(
            "baseboard_id" => info.baseboard_id.to_string(),
            "bootstrap_ip" => info.bootstrap_ip.to_string()
        ));

        info!(log, "Attempting to start sled agent";);

        let bootstrap_addr = SocketAddrV6::new(
            info.bootstrap_ip,
            BOOTSTRAP_AGENT_RACK_INIT_PORT,
            0,
            0,
        );

        let client = SprocketsClient::new(
            bootstrap_addr,
            self.ctx.sprockets_config.clone(),
            self.ctx.measurements.clone(),
            log.clone(),
        );

        let output_tx = self.output_tx.clone();
        let mut retry_count = 0;
        async move {
            // Retry indefinitely to start sled-agent.
            loop {
                match client.start_sled_agent(&info.req).await {
                    Ok(_) => {
                        return info.baseboard_id;
                    }
                    Err(err) => {
                        // Update state with the latest error seen.
                        output_tx.send_modify(|state| {
                            let MultirackJoinServiceState::StartSledAgents(status) =
                                state
                            else {
                                panic!(
                                    "MultirackJoinService in wrong state: {:#?}",
                                    state
                                );
                            };
                            let errstr = InlineErrorChain::new(&err).to_string();
                            // Safe to unwrap since we only start tasks that return
                            // baseboards that already exist in status.
                            let mut info =
                                status.sleds.get1_mut(&info.baseboard_id).unwrap();
                            info.start_state = SledAgentStartState::InProgress { last_error: Some(errstr) };

                        });
                        // Only log every minute, given the 6 second sleep below
                        if retry_count % 10 == 0 {
                            warn!(log, "Failed to start sled agent"; &err);
                        }
                        tokio::time::sleep(Duration::from_secs(6)).await;
                        retry_count += 1;
                    }
                }
            }
        }
    }

    /// Start initializing trust quorum given the the existing
    /// `MultirackJoinRequest` in input_rx.
    ///
    /// If we get an update on `input_rx` while trust quorum is stuck
    /// waiting for all nodes to prepare or commit, we will skip the existing
    /// configuration, or commit it and move on. If too many nodes are offline
    /// during commit, we will not be able to make progress. But that is true in
    /// general. Automatic cleanup is generally only valid for a few offline /
    /// misbehaving nodes once commit phase is entered.
    async fn init_trust_quorum(
        &mut self,
        rack_id: RackUuid,
    ) -> Result<(), MultirackJoinServiceError> {
        let members =
            self.input_rx.borrow_and_update().trust_quorum_peers.clone();
        let epoch = INITIAL_TRUST_QUORUM_EPOCH;
        let last_committed_epoch = None;

        self.tq_run(rack_id, members, epoch, last_committed_epoch).await
    }

    /// Start the reconfigure/prepare/commit process with the given values
    async fn tq_run(
        &mut self,
        rack_id: RackUuid,
        mut members: BTreeSet<BaseboardId>,
        mut epoch: Epoch,
        mut last_committed_epoch: Option<Epoch>,
    ) -> Result<(), MultirackJoinServiceError> {
        loop {
            // We put the check inside this loop, rather than in
            // `init_trust_quorum` because the operator can change the
            // membership at any time.
            if members.len() < 3 || members.len() > 32 {
                let msg = format!(
                    "Rack membership must be between 3 and 32 sleds \
                    (inclusive). Received {} sleds.",
                    members.len()
                );
                error!(self.log, "{msg}");

                // Inform the operator about the mess they've made
                self.output_tx.send_modify(|state| {
                    *state = MultirackJoinServiceState::InvalidMembershipSize {
                        message: msg,
                    }
                });

                // Wait for the operator to clean up said mess.
                self.input_rx.changed().await?;
                members = self
                    .input_rx
                    .borrow_and_update()
                    .trust_quorum_peers
                    .clone();

                // We don't need to bump the epoch because we never attempted a
                // reconfiguration with the invalid membership.
                continue;
            }

            self.tq_reconfigure(
                rack_id,
                members.clone(),
                epoch,
                last_committed_epoch,
            )
            .await?;

            if let TqPrepareResult::ReconfigurationNeeded {
                new_members,
                new_epoch,
            } = self.tq_prepare(members.clone(), epoch).await?
            {
                // Start over
                members = new_members;
                epoch = new_epoch;
                continue;
            };

            match self
                .tq_commit(rack_id, members, epoch, last_committed_epoch)
                .await?
            {
                TqCommitResult::Committed => {
                    // We are done.
                    break;
                }
                TqCommitResult::ReconfigurationNeeded {
                    new_members,
                    new_epoch,
                    just_committed_epoch,
                } => {
                    members = new_members;
                    epoch = new_epoch;
                    last_committed_epoch = Some(just_committed_epoch);
                }
            }
        }

        Ok(())
    }

    async fn tq_reconfigure(
        &mut self,
        rack_id: RackUuid,
        members: BTreeSet<BaseboardId>,
        epoch: Epoch,
        last_committed_epoch: Option<Epoch>,
    ) -> Result<(), MultirackJoinServiceError> {
        let threshold =
            TrustQuorumConfig::threshold(members.len().try_into().unwrap());

        info!(
            self.log,
            "Starting trust quorum reconfiguration";
            "epoch" => %epoch,
            "last_committed_epoch" => ?last_committed_epoch,
            "threshold" => %threshold
        );

        let msg = TqReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
        };

        self.output_tx.send_modify(|state| {
            *state =
                MultirackJoinServiceState::TrustQuorumReconfigure(msg.clone())
        });

        // Start the initial configuration with this node as coordinator
        self.ctx.trust_quorum_handle.reconfigure(msg).await?;

        info!(
            self.log,
            "Trust quorum reconfiguration started";
            "epoch" => %epoch,
            "last_committed_epoch" => ?last_committed_epoch,
            "threshold" => %threshold
        );

        Ok(())
    }

    async fn tq_prepare(
        &mut self,
        members: BTreeSet<BaseboardId>,
        epoch: Epoch,
    ) -> Result<TqPrepareResult, MultirackJoinServiceError> {
        // Read unconditionally from `input_rx` so that we can ensure we are up
        // to date with the latest membership. Then in the loop below, anytime
        // the watch channel indicates a change we know that it must have
        // changed. This makes our reasonining local to this function.
        let new_members =
            self.input_rx.borrow_and_update().trust_quorum_peers.clone();
        if new_members != members {
            return Ok(TqPrepareResult::ReconfigurationNeeded {
                new_members,
                new_epoch: epoch.next(),
            });
        }

        loop {
            let status = self
                .ctx
                .trust_quorum_handle
                .coordinator_status()
                .await?
                .ok_or(MultirackJoinServiceError::TqBadCoordinator)?;

            let all_nodes_prepared = status.acked_prepares == members;
            let still_waiting = itertools::join(
                members.difference(&status.acked_prepares),
                ",",
            );

            // Set the output state and notifiy receivers if it has changed
            self.output_tx.send_if_modified(|state| {
                let new_state =
                    MultirackJoinServiceState::TrustQuorumPreparing(status);
                if *state == new_state {
                    false
                } else {
                    *state = new_state;
                    true
                }
            });

            // We're done preparing. Let's move on to committing.
            if all_nodes_prepared {
                info!(
                    self.log,
                    "Trust quorum prepared at all nodes";
                    "epoch" => %epoch
                );
                break;
            }

            info!(
                self.log,
                "trust quorum coordinator waiting for PrepareAcks";
                "epoch" => %epoch,
                "waiting_for" => still_waiting
            );

            // Before we check our prepare status again let's see if we've
            // received an updated configuration from an operator.
            //
            // The prepare phase of the TQ protocol can be interrupted safely at
            // any time, even if all nodes have received the `Prepare` message.
            if let Some(new_members) =
                self.has_membership_changed(&members).await?
            {
                return Ok(TqPrepareResult::ReconfigurationNeeded {
                    new_members,
                    new_epoch: epoch.next(),
                });
            }

            tokio::time::sleep(TRUST_QUORUM_RETRY_TIMEOUT).await;
        }

        // Before we return, let's see if the operator has changed the
        // configuration one more time.
        if let Some(new_members) = self.has_membership_changed(&members).await?
        {
            Ok(TqPrepareResult::ReconfigurationNeeded {
                new_members,
                new_epoch: epoch.next(),
            })
        } else {
            Ok(TqPrepareResult::Prepared)
        }
    }

    // Attempt to commit the configuration at each node.
    //
    // If a new configuration comes in before commit is attempted or after the
    // minimum number of nodes has committed then return the new members, new
    // epoch, and last commmitted epoch.
    async fn tq_commit(
        &mut self,
        rack_id: RackUuid,
        members: BTreeSet<BaseboardId>,
        epoch: Epoch,
        last_committed_epoch: Option<Epoch>,
    ) -> Result<TqCommitResult, MultirackJoinServiceError> {
        info!(
            self.log,
            "Starting to commit trust quorum configuration";
            "epoch" => %epoch
        );

        // Commit at this node. This is the only node that we locally access
        // over the bootstrap agent. For security purposes, we proxy all other
        // requests over sprockets.
        //
        // Unfortunately, if we have a problem here, we need to stop what we're
        // doing and clean slate.
        self.ctx.trust_quorum_handle.commit(rack_id, epoch).await?;

        // Peers that must proxy commit over sprockets
        let mut remote_peers = members.clone();
        let this_sled = self.ctx.trust_quorum_handle.baseboard_id().clone();
        remote_peers.remove(&this_sled);
        let mut set = JoinSet::new();

        let threshold =
            TrustQuorumConfig::threshold(members.len().try_into().unwrap());
        let commit_crash_tolerance = TrustQuorumConfig::commit_crash_tolerance(
            members.len().try_into().unwrap(),
        );
        let min_acks_to_commit =
            (threshold.0 + commit_crash_tolerance) as usize;

        // Transient errors are updated inside proxy commit tasks
        let (transient_errors_tx, mut transient_errors_rx) =
            watch::channel(BTreeMap::new());

        // Update our state as we start
        let mut commit_state = CommitState {
            rack_id,
            members: members.clone(),
            epoch,
            last_committed_epoch,
            threshold,
            commit_crash_tolerance,
            acked: BTreeSet::from([this_sled]),
            fatal_errors: BTreeMap::new(),
            transient_errors: BTreeMap::new(),
        };
        self.output_tx.send_modify(|state| {
            *state = MultirackJoinServiceState::TrustQuorumCommitting(
                commit_state.clone(),
            )
        });

        for peer in remote_peers {
            let proxy = self.ctx.trust_quorum_handle.proxy();
            let transient_errors_tx = transient_errors_tx.clone();
            info!(
                self.log,
                "Attempting to proxy commit trust quorum";
                "epoch" => %epoch,
                "baseboard_id" => %peer
            );

            // Spawn a task to perform a proxy commit
            //
            // Success and fatal errors are returned from the task. Transient errors
            // are continuously retried.
            set.spawn(async move {
                loop {
                    let peer = peer.clone();
                    match proxy.commit(peer.clone(), rack_id, epoch).await {
                        Ok(
                            trust_quorum_types::status::CommitStatus::Committed,
                        ) => {
                            return Ok(peer);
                        }
                        Ok(
                            trust_quorum_types::status::CommitStatus::Pending,
                        ) => {
                            let s = "unexpected CommitStatus::Pending \
                            from prepared peer"
                                .to_string();
                            return Err((peer, s));
                        }
                        Err(e @ ProxyError::Inner(_))
                        | Err(e @ ProxyError::InvalidResponse(_))
                        | Err(e @ ProxyError::RecvError) => {
                            let s = InlineErrorChain::new(&e).to_string();
                            return Err((peer, s));
                        }
                        Err(e @ ProxyError::Disconnected)
                        | Err(e @ ProxyError::Busy) => {
                            let s = InlineErrorChain::new(&e).to_string();
                            transient_errors_tx.send_modify(|errs| {
                                errs.insert(peer, s);
                            });
                            tokio::time::sleep(TRUST_QUORUM_RETRY_TIMEOUT)
                                .await;
                        }
                    }
                }
            });
        }

        // Wait for the results of proxy commits, membership changes from an
        // operator, or transient errors.
        loop {
            // All arms are cancel-safe and we do not `.await` within the body
            // of any arm, avoiding any opportunity for futurelock.
            tokio::select! {
                Some(res) = set.join_next() => {
                    match res? {
                        Ok(peer_id) => {
                            info!(
                                self.log,
                                "Proxy commit acked";
                                "peer_id" => %peer_id
                            );
                            commit_state.acked.insert(peer_id);
                        }
                        Err((peer_id,err)) => {
                            error!(
                                self.log,
                                "Failed to proxy commit";
                                "peer_id" => %peer_id,
                                "err" => %err
                            );
                            // If we get here, the operator will notice that
                            // not all sleds have acked and must issue a member
                            // change wihout this sled to complete the setup.
                            //
                            // This is the same choice we make in RSS: All sleds
                            // are required to become full trust quorum members
                            // before we allow the rack to be considered ready
                            // for business.
                            commit_state.fatal_errors.insert(peer_id, err);

                        }
                    }
                    self.output_tx.send_modify(|state| {
                        *state = MultirackJoinServiceState::TrustQuorumCommitting(
                            commit_state.clone(),
                        )
                    });
                    if commit_state.acked == members {
                        info!(
                            self.log,
                            "Trust quorum committed at all nodes";
                            "epoch" => %epoch
                        );

                        break;
                    }

                }
                res = self.input_rx.changed() => {
                    res?;
                    // The operator changed the input
                    let new_members = self
                      .input_rx
                      .borrow_and_update()
                      .trust_quorum_peers.clone();

                    // Did the membership change?
                    if members != new_members {
                        if commit_state.acked.len() >= min_acks_to_commit {
                            // We can commit this configuration safely and then
                            // try a reconfiguration with the updated config.
                            let just_committed_epoch = epoch;
                            return Ok(TqCommitResult::ReconfigurationNeeded {
                                new_members,
                                new_epoch: epoch.next(),
                                just_committed_epoch
                            });
                        }
                        // We can't perform a reconfiguration if we haven't
                        // committed at enough nodes. Yet that's exactly what
                        // the operator asked us to do here.
                        //
                        // We could theoretically continue waiting for more
                        // acks, but presumably the operator decided they
                        // had waited long enough and desired to reconfigure.
                        // If we try to do this, how long should we wait to
                        // acknowledge the operator's choice?
                        //
                        // Instead we take the pragamatic path, where not being
                        // able to commit at enough nodes means that quite a lot
                        // of nodes are having trouble. For example, in clusters
                        // larger than 24 nodes, only 17 nodes have to ack
                        // commit. If we can't ack 17 nodes, a support call, and
                        // then a clean-slate, is likely warranted.
                        return Err(MultirackJoinServiceError::TqCommitFailed(
                            commit_state
                        ));
                    } else {
                        // Something other than membership changed. Ignore.
                    }
                }
                res = transient_errors_rx.changed() => {
                    res?;
                    let transient_errors =
                        transient_errors_rx.borrow_and_update().clone();
                    commit_state.transient_errors = transient_errors;
                    self.output_tx.send_modify(|state| {
                        *state =
                            MultirackJoinServiceState::TrustQuorumCommitting(
                                commit_state.clone()
                            )
                    });

                }
            }
        }

        // At this point all members have acked the commit and trust quorum
        // is fully commited. However, we still have to check one more time to
        // see if membership has changed. If we don't do this, we can silently
        // ignore a submission from an operator.
        //
        // We must hold the read lock while checking. If membership has not
        // changed we update our gate to prevent it from changing in the future.
        // Otherwise we start a reconfiguration.
        let guard = self.input_rx.borrow_and_update();
        if guard.trust_quorum_peers == members {
            // No membership change
            //
            // If an operator tries to change the set of peers for rack
            // membership after this point they will get an error response.
            self.membership_change_still_possible
                .store(false, Ordering::Relaxed);

            Ok(TqCommitResult::Committed)
        } else {
            // Membership change
            let new_members = guard.trust_quorum_peers.clone();
            let just_committed_epoch = epoch;
            Ok(TqCommitResult::ReconfigurationNeeded {
                new_members,
                new_epoch: epoch.next(),
                just_committed_epoch,
            })
        }
    }

    // Check if we have received an updated membership set from an operator.
    //
    // If we have received a new set, return it. Otherwise, return `None`.
    // Return an error if checking for the update fails.
    async fn has_membership_changed(
        &mut self,
        members: &BTreeSet<BaseboardId>,
    ) -> Result<Option<BTreeSet<BaseboardId>>, MultirackJoinServiceError> {
        if self.input_rx.has_changed()? {
            let new_members =
                self.input_rx.borrow_and_update().trust_quorum_peers.clone();
            if new_members != *members {
                return Ok(Some(new_members));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use sled_agent_types::early_networking::{
        LinkSpeed, PortConfig, RackNetworkConfig, SwitchSlot, UplinkPorts,
    };
    use sled_hardware_types::BaseboardId;

    fn rack_network_config() -> RackNetworkConfig {
        // RackNetworkConfig's ports must be nonempty.
        let ports = vec![PortConfig {
            routes: Vec::new(),
            addresses: Vec::new(),
            switch: SwitchSlot::Switch1,
            port: "qsfp0".to_owned(),
            uplink_port_speed: LinkSpeed::Speed100G,
            uplink_port_fec: None,
            bgp_peers: Vec::new(),
            autoneg: false,
            lldp: None,
            tx_eq: None,
            allow_ddm_traffic: false,
        }];

        RackNetworkConfig {
            rack_subnet: "fd00:abcd:ffff::/56".parse().unwrap(),
            infra_ip_first: "10.0.0.1".parse().unwrap(),
            infra_ip_last: "10.0.0.100".parse().unwrap(),
            ports: UplinkPorts::new(ports).unwrap(),
            bgp: Vec::new(),
            bfd: Vec::new(),
        }
    }

    fn trust_quorum_peers() -> BTreeSet<BaseboardId> {
        (0..32)
            .into_iter()
            .map(|i| BaseboardId {
                part_number: "FAKE_PART".to_string(),
                serial_number: format!("2FAKE{:03}", i),
            })
            .collect()
    }

    #[test]
    fn new_start_sled_agent_status() {
        let req = MultirackJoinRequest {
            trust_quorum_peers: trust_quorum_peers(),
            rack_network_config: rack_network_config(),
        };

        let status = StartSledAgentsStatus::assign_ids_and_subnets(req.clone());
        assert_eq!(status.sleds.len(), req.trust_quorum_peers.len());

        let actual_sled_subnets: BTreeSet<_> =
            status.sleds.iter().map(|s| s.sled_subnet.to_string()).collect();
        let expected_sled_subnets: BTreeSet<_> =
            (0..req.trust_quorum_peers.len())
                .into_iter()
                .map(|i| format!("fd00:abcd:ffff:{:x}::/64", i + 1))
                .collect();

        assert_eq!(actual_sled_subnets, expected_sled_subnets);
    }
}
