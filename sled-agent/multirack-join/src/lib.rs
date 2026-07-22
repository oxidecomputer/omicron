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
use bootstrap_agent_lockstep_types::MultirackJoinRequest;
use nexus_types::trust_quorum::TrustQuorumConfig;
use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_bootstrap_common::{RssContext, RunRssError};
use sled_hardware_types::BaseboardId;
use slog::{Logger, info};
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::watch,
    task::{JoinError, JoinSet},
};
use trust_quorum::{NodeApiError, ProxyError};
use trust_quorum_types::{
    messages::ReconfigureMsg as TqReconfigureMsg,
    status::CoordinatorStatus,
    types::{Epoch, Threshold},
};

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
}

impl From<RunRssError> for MultirackJoinServiceError {
    fn from(value: RunRssError) -> Self {
        match value {
            RunRssError::RackAlreadyInitialized => Self::RackAlreadyInitialized,
            RunRssError::RackInitInterrupted => Self::RackInitInterrupted,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct CommitState {
    rack_id: RackUuid,
    members: BTreeSet<BaseboardId>,
    epoch: Epoch,
    last_committed_epoch: Option<Epoch>,
    threshold: Threshold,
    commit_crash_tolerance: u8,
    acked: BTreeSet<BaseboardId>,
    fatal_errors: BTreeMap<BaseboardId, String>,
    transient_errors: BTreeMap<BaseboardId, String>,
}

/// The current state of the `MultirackJoinService` as retrieved from the
/// `output_rx` watch channel.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum MultirackJoinServiceState {
    Uninitialized,
    Requested,
    Starting,
    TrustQuorumReconfigure(TqReconfigureMsg),
    TrustQuorumPreparing(CoordinatorStatus),
    TrustQuorumCommitting(CommitState),
    Completed,
    Failed(String),
    TaskPanicked,
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

/// The interface to the Multirack Join Service.
pub struct MultirackJoinServiceHandle {
    pub join_handle:
        tokio::task::JoinHandle<Result<(), MultirackJoinServiceError>>,
    pub input_tx: watch::Sender<MultirackJoinRequest>,
    pub output_rx: watch::Receiver<MultirackJoinServiceState>,
}

impl MultirackJoinServiceHandle {
    pub fn spawn(ctx: RssContext, request: MultirackJoinRequest) -> Self {
        let (input_tx, input_rx) = watch::channel(request);
        let state = MultirackJoinServiceState::Requested;
        let (output_tx, output_rx) = watch::channel(state.clone());
        let join_handle = tokio::task::spawn(async move {
            let log =
                ctx.base_log.new(o!("component" => "MultirackJoinService"));
            info!(log, "Starting Multirack Join Service");
            let mut task =
                MultirackJoinServiceTask { log, ctx, input_rx, output_tx };
            task.run().await
        });

        Self { join_handle, input_tx, output_rx }
    }
}

/// The internal state of the main task running the join service
struct MultirackJoinServiceTask {
    log: Logger,
    ctx: RssContext,
    input_rx: watch::Receiver<MultirackJoinRequest>,
    output_tx: watch::Sender<MultirackJoinServiceState>,
}

impl MultirackJoinServiceTask {
    /// The main loop of the Multirack Join Service
    pub async fn run(&mut self) -> Result<(), MultirackJoinServiceError> {
        self.output_tx
            .send_modify(|state| *state = MultirackJoinServiceState::Starting);

        // Check to see if we've already finished RSS or multirack join
        self.ctx.is_rss_complete(&self.log).await?;
        info!(&self.log, "No RSS ledger found. Starting Multirack Join Setup");

        let rack_id = RackUuid::new_v4();
        info!(&self.log, "Created RackId {rack_id}");

        self.init_trust_quorum(rack_id).await?;

        // TODO:
        //   Start sled-agents
        //   Configure networking

        Ok(())
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
        let epoch = trust_quorum_types::types::Epoch(1);
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
            "epoch" => %epoch
        );

        Ok(())
    }

    async fn tq_prepare(
        &mut self,
        members: BTreeSet<BaseboardId>,
        epoch: Epoch,
    ) -> Result<TqPrepareResult, MultirackJoinServiceError> {
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
            self.output_tx.send_if_modified(|state| match state {
                MultirackJoinServiceState::TrustQuorumPreparing(
                    coordinator_status,
                ) => {
                    if &status != coordinator_status {
                        *state =
                            MultirackJoinServiceState::TrustQuorumPreparing(
                                status,
                            );
                        true
                    } else {
                        false
                    }
                }
                _ => {
                    *state =
                        MultirackJoinServiceState::TrustQuorumPreparing(status);
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

            tokio::time::sleep(Duration::from_millis(500)).await;
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
        // requests over sprockests.
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
        let transient_errors = Arc::new(Mutex::new(BTreeMap::new()));

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
            self.tq_spawn_proxy_commit_task(
                rack_id,
                peer,
                epoch,
                transient_errors.clone(),
                &mut set,
            );
        }

        loop {
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
                        return Err(MultirackJoinServiceError::TqCommitFailed(
                            commit_state
                        ));
                    } else {
                        // Something other than membership changed. Ignore.
                    }
                }
            }
        }

        Ok(TqCommitResult::Committed)
    }

    /// Spawn a task to perform a proxy commit
    ///
    /// Success and fatal errors are returned from the task. Transient errors
    /// are continuously retried.
    fn tq_spawn_proxy_commit_task(
        &mut self,
        rack_id: RackUuid,
        peer: BaseboardId,
        epoch: Epoch,
        transient_errors: Arc<Mutex<BTreeMap<BaseboardId, String>>>,
        set: &mut JoinSet<Result<BaseboardId, (BaseboardId, String)>>,
    ) {
        let proxy = self.ctx.trust_quorum_handle.proxy();
        info!(
            self.log,
            "Attempting to proxy commit trust quorum";
            "epoch" => %epoch,
            "baseboard_id" => %peer
        );
        set.spawn(async move {
            loop {
                let peer = peer.clone();
                match proxy.commit(peer.clone(), rack_id, epoch).await {
                    Ok(trust_quorum_types::status::CommitStatus::Committed) => {
                        return Ok(peer);
                    }
                    Ok(trust_quorum_types::status::CommitStatus::Pending) => {
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
                    Err(e) => {
                        let s = InlineErrorChain::new(&e).to_string();
                        transient_errors.lock().unwrap().insert(peer, s);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });
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
