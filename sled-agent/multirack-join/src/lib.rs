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
use bootstrap_agent_lockstep_types::{MultirackJoinRequest, MultirackJoinStep};
use omicron_uuid_kinds::RackUuid;
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
use tokio::{sync::watch, task::JoinSet};
use trust_quorum::{NodeApiError, ProxyError};
use trust_quorum_protocol::CommitError;
use trust_quorum_types::{
    messages::ReconfigureMsg as TqReconfigureMsg, status::CoordinatorStatus,
    types::Epoch,
};

/// Describes errors which may occur while operating the multirack join service.
#[derive(Error, Debug, SlogInlineError)]
pub enum MultirackJoinServiceError {
    #[error("Rack already initialized")]
    RackAlreadyInitialized,

    #[error("Rack initialization was interrupted. Clean-slate required")]
    RackInitInterrupted,

    #[error("Trust quorum error")]
    TrustQuorum(#[from] NodeApiError),

    #[error("Trust quorum coordinator doesn't think it's a coordinator")]
    TrustQuorumBadCoordinator,

    #[error("Failed to receive input from bootstrap agent")]
    InputRx(#[from] watch::error::RecvError),
}

impl From<RunRssError> for MultirackJoinServiceError {
    fn from(value: RunRssError) -> Self {
        match value {
            RunRssError::RackAlreadyInitialized => Self::RackAlreadyInitialized,
            RunRssError::RackInitInterrupted => Self::RackInitInterrupted,
        }
    }
}

/// The current state of the `MultirackJoinService` as retrieved from the `output`
/// watch channel.
#[derive(Debug, Clone)]
pub struct MultirackJoinServiceState {
    step: MultirackJoinStep,
    trust_quorum_coordinator_status: Option<CoordinatorStatus>,
}

impl MultirackJoinServiceState {
    pub fn new() -> Self {
        Self {
            step: MultirackJoinStep::Requested,
            trust_quorum_coordinator_status: None,
        }
    }
}

/// The interface to the Multirack Join Service.
pub struct MultirackJoinServiceHandle {
    handle: tokio::task::JoinHandle<Result<(), MultirackJoinServiceError>>,
    input_tx: watch::Sender<MultirackJoinRequest>,
    output_rx: watch::Receiver<MultirackJoinServiceState>,
}

impl MultirackJoinServiceHandle {
    pub fn spawn(ctx: RssContext, request: MultirackJoinRequest) -> Self {
        let (input_tx, input_rx) = watch::channel(request);
        let state = MultirackJoinServiceState::new();
        let (output_tx, output_rx) = watch::channel(state.clone());
        let handle = tokio::task::spawn(async move {
            let log =
                ctx.base_log.new(o!("component" => "MultirackJoinService"));
            info!(log, "Starting Multirack Join Service");
            let mut task =
                MultirackJoinServiceTask { log, ctx, input_rx, output_tx };
            task.run().await
        });

        Self { handle, input_tx, output_rx }
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
    fn step(&mut self, next: MultirackJoinStep) {
        self.output_tx.send_modify(|state| state.step = next);
    }
}

impl MultirackJoinServiceTask {
    /// The main loop of the Multirack Join Service
    pub async fn run(&mut self) -> Result<(), MultirackJoinServiceError> {
        self.step(MultirackJoinStep::Starting);

        // Check to see if we've already finished RSS or multirack join
        self.ctx.is_rss_complete(&self.log).await?;
        info!(&self.log, "No RSS ledger found. Starting Multirack Join Setup");

        let rack_id = RackUuid::new_v4();
        info!(&self.log, "Created RackId {rack_id}");

        self.init_trust_quorum(rack_id).await?;

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
        self.step(MultirackJoinStep::InitTrustQuorum);

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
        last_committed_epoch: Option<Epoch>,
    ) -> Result<(), MultirackJoinServiceError> {
        loop {
            self.tq_reconfigure(
                rack_id,
                members.clone(),
                epoch,
                last_committed_epoch,
            )
            .await?;

            if let Some((new_members, new_epoch)) =
                self.tq_prepare(members.clone(), epoch).await?
            {
                // We need to reconfigure
                members = new_members;
                epoch = new_epoch;
                continue;
            };

            if let Some((new_members, new_epoch)) =
                self.tq_commit(rack_id, members, epoch).await?
            {
                members = new_members;
                epoch = new_epoch;
            } else {
                break;
            };
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
        let threshold = trust_quorum_types::types::Threshold(
            u8::try_from(members.len()).unwrap() / 2 + 1,
        );

        let msg = TqReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
        };

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
    ) -> Result<Option<(BTreeSet<BaseboardId>, Epoch)>, MultirackJoinServiceError>
    {
        loop {
            let status = self
                .ctx
                .trust_quorum_handle
                .coordinator_status()
                .await?
                .ok_or(MultirackJoinServiceError::TrustQuorumBadCoordinator)?;

            let all_nodes_prepared = status.acked_prepares == members;
            let still_waiting = itertools::join(
                members.difference(&status.acked_prepares),
                ",",
            );

            // Set the output state and notifiy receivers if it has changed
            self.output_tx.send_if_modified(|state| {
                let status = Some(status);
                let changed = state.trust_quorum_coordinator_status == status;
                state.trust_quorum_coordinator_status = status;
                changed
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
                // Returning the new members will trigger a reconfiguration.
                return Ok(Some((new_members, epoch.next())));
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // All members have prepared
        Ok(None)
    }

    async fn tq_commit(
        &mut self,
        rack_id: RackUuid,
        members: BTreeSet<BaseboardId>,
        epoch: Epoch,
    ) -> Result<Option<(BTreeSet<BaseboardId>, Epoch)>, MultirackJoinServiceError>
    {
        // Before we attempt to commit, let's see if the operator has changed
        // the configuration.
        if let Some(new_members) = self.has_membership_changed(&members).await?
        {
            return Ok(Some((new_members, epoch.next())));
        }

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
        remote_peers.remove(self.ctx.trust_quorum_handle.baseboard_id());
        let mut set = JoinSet::new();

        // Track and report the status of each remote peer
        let mut acked = BTreeSet::<BaseboardId>::new();
        let mut fatal_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let mut transient_errors = Arc::new(Mutex::new(BTreeMap::new()));

        for peer in remote_peers {
            let proxy = self.ctx.trust_quorum_handle.proxy();
            info!(
                self.log,
                "Attempting to proxy commit trust quorum";
                "epoch" => %epoch,
                "baseboard_id" => %peer
            );
            let fatal_errors = fatal_errors.clone();
            let transient_errors = transient_errors.clone();
            set.spawn(async move {
                match proxy.commit(peer.clone(), rack_id, epoch).await {
                    Ok(trust_quorum_types::status::CommitStatus::Committed) => {
                    }
                    Ok(trust_quorum_types::status::CommitStatus::Pending) => {}
                    Err(e @ ProxyError::Inner(_))
                    | Err(e @ ProxyError::InvalidResponse(_))
                    | Err(e @ ProxyError::RecvError) => {
                        fatal_errors.lock().unwrap().insert(peer, e);
                    }
                    Err(transient_err) => {
                        transient_errors
                            .lock()
                            .unwrap()
                            .insert(peer, transient_err);
                    }
                }
            });
        }

        // TODO: Await each peer and also check for updates from `input_rx`
        // TODO: Ensure that output_tx contains the threshold being waited for as well
        // as safety factor, etc...

        Ok(None)
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
