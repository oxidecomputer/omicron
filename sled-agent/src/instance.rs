// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::instance::{
    InstanceStates, ObservedPropolisState, PublishedVmmState,
};
use crate::instance_manager::{
    Error as ManagerError, InstanceManagerServices, InstanceTicket,
};
use crate::metrics::MetricsRequestQueue;
use crate::nexus::NexusClient;
use crate::port_manager::SledAgentPortManager;
use crate::profile::*;
use crate::zone_bundle::ZoneBundler;
use chrono::Utc;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::{DhcpCfg, PortCreateParams};
use illumos_utils::running_zone::{RunningZone, ZoneBuilderFactory};
use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
use illumos_utils::zpool::ZpoolOrRamdisk;
use omicron_common::api::internal::nexus::{SledVmmState, VmmRuntimeState};
use omicron_common::api::internal::shared::{
    NetworkInterface, ResolvedVpcFirewallRule, SledIdentifiers, SourceNatConfig,
};
use omicron_common::backoff;
use omicron_common::backoff::BackoffError;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, OmicronZoneUuid, PropolisUuid,
};
use propolis_api_types::ErrorCode as PropolisErrorCode;
use propolis_client::Client as PropolisClient;
use propolis_client::instance_spec::{ComponentV0, SpecKey};
use rand::SeedableRng;
use rand::prelude::IteratorRandom;
use sled_agent_config_reconciler::AvailableDatasetsReceiver;
use sled_agent_types::instance::*;
use sled_agent_types::zone_bundle::ZoneBundleCause;
use sled_agent_zone_images::ramdisk_file_source;
use slog::Logger;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// The depth of the request queue for the instance.
const QUEUE_SIZE: usize = 32;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to wait for service: {0}")]
    Timeout(String),

    #[error("Failed to create VNIC: {0}")]
    VnicCreation(#[from] illumos_utils::dladm::CreateVnicError),

    #[error("Failure from Propolis Client: {0}")]
    Propolis(#[from] PropolisClientError),

    // TODO: Remove this error; prefer to retry notifications.
    #[error("Notifying Nexus failed: {0}")]
    Notification(nexus_client::Error<nexus_client::types::Error>),

    // TODO: This error type could become more specific
    #[error("Error performing a state transition: {0}")]
    Transition(omicron_common::api::external::Error),

    // TODO: Add more specific errors
    #[error("Failure during migration: {0}")]
    Migration(anyhow::Error),

    #[error("requested NIC {0} has no virtio network backend in Propolis spec")]
    NicNotInPropolisSpec(Uuid),

    #[error(transparent)]
    ZoneCommand(#[from] illumos_utils::running_zone::RunCommandError),

    #[error(transparent)]
    ZoneBoot(#[from] illumos_utils::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] illumos_utils::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] illumos_utils::running_zone::InstallZoneError),

    #[error("serde_json failure: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    Opte(#[from] illumos_utils::opte::Error),

    /// Issued by `impl TryFrom<&[u8]> for oxide_vpc::api::DomainName`
    #[error("Invalid hostname: {0}")]
    InvalidHostname(&'static str),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns_resolver::ResolveError),

    #[error("Propolis job with ID {0} is registered but not running")]
    VmNotRunning(PropolisUuid),

    #[error("Propolis job with ID {0} already registered")]
    PropolisAlreadyRegistered(PropolisUuid),

    #[error("No U.2 devices found")]
    U2NotFound,

    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("Failed to send request to Instance: Channel closed")]
    FailedSendChannelClosed,

    #[error(
        "Failed to send request to Instance: channel at capacity \
         ({QUEUE_SIZE})"
    )]
    FailedSendChannelFull,

    #[error(
        "Failed to send request from Instance Runner: Client Channel closed"
    )]
    FailedSendClientClosed,

    #[error("Instance dropped our request")]
    RequestDropped(#[from] oneshot::error::RecvError),

    #[error("Instance is terminating")]
    Terminating,
}

type PropolisClientError =
    propolis_client::Error<propolis_client::types::Error>;

// Issues read-only, idempotent HTTP requests at propolis until it responds with
// an acknowledgement. This provides a hacky mechanism to "wait until the HTTP
// server is serving requests".
//
// TODO: Plausibly we could use SMF to accomplish this goal in a less hacky way.
async fn wait_for_http_server(
    log: &Logger,
    client: &PropolisClient,
) -> Result<(), Error> {
    let log_notification_failure = |error, delay| {
        warn!(
            log,
            "failed to await http server ({}), will retry in {:?}", error, delay;
            "error" => ?error
        );
    };

    backoff::retry_notify(
        backoff::retry_policy_local(),
        || async {
            // This request is nonsensical - we don't expect an instance to
            // exist - but getting a response that isn't a connection-based
            // error informs us the HTTP server is alive.
            match client.instance_get().send().await {
                Ok(_) => return Ok(()),
                Err(value) => {
                    if value.status().is_some() {
                        // This means the propolis server responded to our
                        // request, instead of a connection error.
                        return Ok(());
                    }
                    return Err(backoff::BackoffError::transient(value));
                }
            }
        },
        log_notification_failure,
    )
    .await
    .map_err(|_| Error::Timeout("Propolis".to_string()))
}

fn service_name() -> &'static str {
    "svc:/system/illumos/propolis-server"
}

fn fmri_name() -> String {
    format!("{}:default", service_name())
}

/// Return the expected name of a Propolis zone managing an instance with the
/// provided ID.
pub fn propolis_zone_name(id: &PropolisUuid) -> String {
    format!("{}{}", PROPOLIS_ZONE_PREFIX, id)
}

// State associated with a running instance.
struct RunningState {
    // Connection to Propolis.
    client: Arc<PropolisClient>,
    // Handle to the zone.
    running_zone: RunningZone,
}

// Named type for values returned during propolis zone creation
struct PropolisSetup {
    client: Arc<PropolisClient>,
    running_zone: RunningZone,
}

// Requests that can be made of instances
#[derive(strum::Display)]
enum InstanceRequest {
    GetFilesystemPool {
        tx: oneshot::Sender<Result<Option<ZpoolName>, ManagerError>>,
    },
    CurrentState {
        tx: oneshot::Sender<Result<SledVmmState, ManagerError>>,
    },
    PutState {
        state: VmmStateRequested,
        tx: oneshot::Sender<Result<VmmPutStateResponse, ManagerError>>,
    },
    IssueSnapshotRequest {
        disk_id: Uuid,
        snapshot_id: Uuid,
        tx: oneshot::Sender<Result<(), ManagerError>>,
    },
    AddExternalIp {
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), ManagerError>>,
    },
    DeleteExternalIp {
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), ManagerError>>,
    },
    RefreshExternalIps {
        tx: oneshot::Sender<Result<(), ManagerError>>,
    },
}

impl InstanceRequest {
    /// Handle an error returned by [`mpsc::Sender::try_send`] when attempting
    /// to send a request to the instance.
    ///
    /// This is a bit complex: the returned [`mpsc::error::TrySendError`] will
    /// contain the [`InstanceRequest`] we were trying to send, and thus the
    /// [`oneshot::Sender`] for that request's response. This function handles
    /// the `TrySendError` by inspecting the error to determine whether the
    /// channel has closed or is full, constructing the relevant [`Error`], and
    /// extracting the response oneshot channel from the request, and then
    /// sending back the error over that channel.
    ///
    /// If sending the error back to the client fails, this function returns an
    /// error, so that the client having given up can be logged; otherwise, it returns `Ok(())`.
    fn fail_try_send(
        err: mpsc::error::TrySendError<Self>,
    ) -> Result<(), Error> {
        let (error, this) = match err {
            mpsc::error::TrySendError::Closed(this) => {
                (Error::FailedSendChannelClosed, this)
            }
            mpsc::error::TrySendError::Full(this) => {
                (Error::FailedSendChannelFull, this)
            }
        };

        match this {
            Self::GetFilesystemPool { tx } => tx
                .send(Err(error.into()))
                .map_err(|_| Error::FailedSendClientClosed),
            Self::CurrentState { tx } => tx
                .send(Err(error.into()))
                .map_err(|_| Error::FailedSendClientClosed),
            Self::PutState { tx, .. } => tx
                .send(Err(error.into()))
                .map_err(|_| Error::FailedSendClientClosed),
            Self::IssueSnapshotRequest { tx, .. }
            | Self::AddExternalIp { tx, .. }
            | Self::DeleteExternalIp { tx, .. }
            | Self::RefreshExternalIps { tx } => tx
                .send(Err(error.into()))
                .map_err(|_| Error::FailedSendClientClosed),
        }
    }
}

/// Identifies the component that's responsible for updating a specific VMM's
/// state.
///
/// Every VMM in the system has a database record (in Nexus) that contains the
/// VMM's current state. Nexus and Sled Agent work together to ensure that only
/// one entity in the system drives an individual VMM's state machine at once.
/// (If this weren't true, updates from multiple sources could race in ways that
/// produce invalid state transitions.) The general rules are:
///
/// - Nexus owns the state machines for newly-defined VMMs that it has not yet
///   asked to start.
/// - Once Nexus asks Sled Agent to start a VM in a Propolis instance, the
///   [`InstanceRunner`] takes over the state machine. The runner can change the
///   VM's state in response to updates from Propolis or other commands issued
///   to Sled Agent.
/// - Some Nexus commands (e.g., forcibly deregistering an instance from Sled
///   Agent) imply that Nexus also wants to reclaim responsibility for driving
///   the VMM state machine. For example, an unwinding instance start saga will
///   want the instance runner to exit without further updating the relevant
///   VMM's state machine (since this will be updated by saga unwind).
///
/// Each instance runner tracks who owns the state machine so that it can avoid
/// publishing unwanted state updates if Nexus reclaims control of a VMM's
/// state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VmmStateOwner {
    /// The [`InstanceRunner`] that manages the VMM is also responsible for
    /// updating its state.
    Runner,

    /// Nexus has indicated to this runner that it (Nexus) will drive future
    /// state changes to this VM.
    Nexus,
}

/// A request to immediately shut down an instance runner and terminate its
/// Propolis zone.
struct TerminateRequest {
    /// A channel to which to send the result of this request.
    tx: oneshot::Sender<Result<VmmUnregisterResponse, ManagerError>>,

    /// The logical owner of the VMM state once this request is successfully
    /// processed.
    new_state_owner: VmmStateOwner,
}

// A small task which tracks the state of the instance, by constantly querying
// the state of Propolis for updates.
//
// This task communicates with the "InstanceRunner" task to report status.
struct InstanceMonitorRunner {
    client: Arc<PropolisClient>,
    zone_name: String,
    tx_monitor: mpsc::Sender<InstanceMonitorMessage>,
    zones_api: Arc<dyn illumos_utils::zone::Api>,
    log: slog::Logger,
}

impl InstanceMonitorRunner {
    async fn run(self) -> Result<(), anyhow::Error> {
        let mut generation = 0;
        loop {
            let update = backoff::retry_notify(
                backoff::retry_policy_local(),
                || self.monitor(generation),
                |error, delay| {
                    warn!(
                        self.log,
                        "Failed to poll Propolis state";
                        "error" => %error,
                        "retry_in" => ?delay,
                        "generation" => generation,
                    );
                },
            )
            .await?;

            // Update the state generation for the next poll.
            if let InstanceMonitorUpdate::State(ref state) = update {
                generation = state.r#gen + 1;
            }

            // Now that we have the response from Propolis' HTTP server, we
            // forward that to the InstanceRunner.
            //
            // It will decide the new state, provide that info to Nexus,
            // and possibly identify if we should terminate.
            let (tx, rx) = oneshot::channel();
            self.tx_monitor.send(InstanceMonitorMessage { update, tx }).await?;

            if rx.await?.is_break() {
                return Ok(());
            }
        }
    }

    async fn monitor(
        &self,
        generation: u64,
    ) -> Result<InstanceMonitorUpdate, BackoffError<PropolisClientError>> {
        // State monitoring always returns the most recent state/gen pair
        // known to Propolis.
        let result = self
            .client
            .instance_state_monitor()
            .body(propolis_client::types::InstanceStateMonitorRequest {
                r#gen: generation,
            })
            .send()
            .await;
        match result {
            Ok(response) => {
                let state = response.into_inner();

                Ok(InstanceMonitorUpdate::State(state))
            }
            // If the channel has closed, then there's nothing left for us to do
            // here. Go die.
            Err(e) if self.tx_monitor.is_closed() => {
                Err(BackoffError::permanent(e))
            }
            // If we couldn't communicate with propolis-server, let's make sure
            // the zone is still there...
            Err(e @ PropolisClientError::CommunicationError(_)) => {
                match self.zones_api.find(&self.zone_name).await {
                    Ok(None) => {
                        // Oh it's GONE!
                        info!(
                            self.log,
                            "Propolis zone is Way Gone!";
                            "zone" => %self.zone_name,
                        );
                        Ok(InstanceMonitorUpdate::ZoneGone)
                    }
                    Ok(Some(zone)) if zone.state() == zone::State::Running => {
                        warn!(
                            self.log,
                            "communication error checking up on Propolis, but \
                             the zone is still running...";
                            "error" => %e,
                            "zone" => %self.zone_name,
                        );
                        Err(BackoffError::transient(e))
                    }
                    Ok(Some(zone)) => {
                        info!(
                            self.log,
                            "Propolis zone is no longer running!";
                            "error" => %e,
                            "zone" => %self.zone_name,
                            "zone_state" => ?zone.state(),
                        );
                        Ok(InstanceMonitorUpdate::ZoneGone)
                    }
                    Err(zoneadm_error) => {
                        // If we couldn't figure out whether the zone is still
                        // running, just keep retrying
                        error!(
                            self.log,
                            "error checking if Propolis zone still exists after \
                             commuication error";
                            "error" => %zoneadm_error,
                            "zone" => %self.zone_name,
                        );
                        Err(BackoffError::transient(e))
                    }
                }
            }
            // Otherwise, was there a known error code from Propolis?
            Err(e) => propolis_error_code(&self.log, &e)
                // If we were able to parse a known error code, send it along to
                // the instance runner task.
                .map(InstanceMonitorUpdate::Error)
                // Otherwise, just keep trying until we see a good state or
                // known error code.
                .ok_or_else(|| BackoffError::transient(e)),
        }
    }
}

enum InstanceMonitorUpdate {
    State(propolis_client::types::InstanceStateMonitorResponse),
    ZoneGone,
    Error(PropolisErrorCode),
}

/// A message sent by an instance monitor to its runner.
struct InstanceMonitorMessage {
    /// The body of the instance monitor's message.
    update: InstanceMonitorUpdate,

    /// After handling an update, the instance runner uses this channel to tell
    /// the monitor whether to continue running.
    tx: oneshot::Sender<ControlFlow<()>>,
}

struct InstanceRunner {
    log: Logger,

    // A signal the InstanceRunner should shut down.
    // This is currently only activated by the runner itself.
    should_terminate: bool,

    // Request channel on which most instance requests are made.
    rx: mpsc::Receiver<InstanceRequest>,

    // Request channel on which monitor requests are made.
    tx_monitor: mpsc::Sender<InstanceMonitorMessage>,
    rx_monitor: mpsc::Receiver<InstanceMonitorMessage>,
    monitor_handle: Option<tokio::task::JoinHandle<()>>,

    // Properties visible to Propolis
    properties: propolis_client::types::InstanceProperties,

    // The ID of the Propolis server (and zone) running this instance
    propolis_id: PropolisUuid,

    // The socket address of the Propolis server running this instance
    propolis_addr: SocketAddr,

    propolis_spec: VmmSpec,

    // NIC-related properties
    vnic_allocator: VnicAllocator<Etherstub>,

    // Reference to the port manager for creating OPTE ports when starting the
    // instance
    port_manager: SledAgentPortManager,

    // Guest NIC and OPTE port information
    requested_nics: Vec<NetworkInterface>,
    source_nat: SourceNatConfig,
    ephemeral_ip: Option<IpAddr>,
    floating_ips: Vec<IpAddr>,
    firewall_rules: Vec<ResolvedVpcFirewallRule>,
    dhcp_config: DhcpCfg,

    // Internal State management
    state: InstanceStates,
    running_state: Option<RunningState>,

    // Connection to Nexus
    nexus_client: NexusClient,

    // Available datasets for choosing zone roots
    available_datasets_rx: AvailableDatasetsReceiver,

    // Used to create propolis zones
    zone_builder_factory: ZoneBuilderFactory,

    // Object used to collect zone bundles from this instance when terminated.
    zone_bundler: ZoneBundler,

    // Queue to notify the sled agent's metrics task about our VNICs.
    metrics_queue: MetricsRequestQueue,
}

impl InstanceRunner {
    /// How long to wait for VMM shutdown to complete before forcefully
    /// terminating the zone.
    const STOP_GRACE_PERIOD: Duration = Duration::from_secs(60 * 10);

    async fn run(
        mut self,
        mut terminate_rx: mpsc::Receiver<TerminateRequest>,
        mut ticket: InstanceTicket,
    ) {
        use InstanceRequest::*;

        let mut state_owner = VmmStateOwner::Runner;

        // Timeout for stopping the instance gracefully.
        //
        // When we send Propolis a put-state request to transition to
        // Stopped`, we start this timer. If Propolis does not report back
        // that it has stopped the instance within `STOP_GRACE_PERIOD`, we
        // will forcibly terminate the VMM. This is to ensure that a totally
        // stuck VMM does not prevent the Propolis zone from being cleaned up.
        let mut stop_timeout = None;
        async fn stop_timeout_completed(
            stop_timeout: &mut Option<Pin<Box<tokio::time::Sleep>>>,
        ) {
            if let Some(ref mut timeout) = stop_timeout {
                timeout.await
            } else {
                std::future::pending().await
            }
        }

        while !self.should_terminate {
            tokio::select! {
                biased;

                // Handle messages from our own "Monitor the VMM" task.
                request = self.rx_monitor.recv() => {
                    self.handle_monitor_message(request).await;
                },

                // We are waiting for the VMM to stop, and the grace period has
                // elapsed without us hearing back from Propolis that it has
                // stopped the instance. Time to terminate it violently!
                //
                // Note that this must be a lower priority in the `select!` than
                // instance monitor requests, as we would prefer to honor a
                // message from the instance monitor indicating a successful
                // stop, even if we have reached the timeout.
                _ = stop_timeout_completed(&mut stop_timeout) => {
                    warn!(
                        self.log,
                        "Instance failed to stop within the grace period, \
                         terminating it violently!",
                    );

                    // This looks kind of like a failure condition (the VMM
                    // didn't do what it was asked), but if the instance goes
                    // to Failed as a result of this timeout, Nexus might
                    // automatically restart it even though the user asked for
                    // it to be stopped. To avoid this, move the VMM to
                    // Destroyed here.
                    //
                    // Note that this timeout is only implicated when Nexus
                    // asks to stop an instance (it is not enabled when someone
                    // forcibly terminates a VM).
                    self.state.force_state_to_destroyed();
                    self.terminate().await;
                    break;
                }

                // Requests to terminate the instance take priority over any
                // other request to the instance.
                request = terminate_rx.recv() => {
                    state_owner = self.handle_termination_request(
                        request,
                        None
                    ).await;
                    break;
                }

                // Handle external requests to act upon the instance.
                request = self.rx.recv() => {
                    let request = match request {
                        Some(r) => r,
                        None => {
                            // This shouldn't happen: it indicates that the
                            // manager has dropped the tx side of this channel
                            // before a final instance state was published! Try
                            // to recover by marking the VMM as Failed and
                            // publishing the corresponding state update.
                            error!(
                                self.log,
                                "instance request channel unexpectedly closed"
                            );

                            self.fail_vmm_and_terminate().await;
                            break;
                        }
                    };

                    let request_variant = request.to_string();
                    // Okay, this is a little bit wacky: if we are waiting for
                    // one of the instance operations we run here to come back,
                    // and a termination request comes in, we want to give up on
                    // the outstanding operation and honor the termination
                    // request immediately. This is in case the instance
                    // operation has gotten stuck: we don't want it to prevent
                    // the instance from terminating because something else is
                    // wedged.
                    //
                    // Therefore, we're going to select between the future that
                    // actually performs the instance op and receiving another
                    // request from the termination  channel.
                    let op = async {
                        match request {
                            GetFilesystemPool { tx } => {
                                tx.send(Ok(self.get_filesystem_zpool()))
                                    .map_err(|_| Error::FailedSendClientClosed)
                            },
                            CurrentState{ tx } => {
                                tx.send(Ok(self.current_state()))
                                    .map_err(|_| Error::FailedSendClientClosed)
                            },
                            PutState { state, tx } => {
                                // If we're going to stop the instance, start
                                // the timeout after which we will forcefully
                                // terminate the VMM.
                                if let VmmStateRequested::Stopped = state {
                                    // Only start the stop timeout if there
                                    // isn't one already, so that additional
                                    // requests to stop coming in don't reset
                                    // the clock.
                                    if stop_timeout.is_none() {
                                        stop_timeout = Some(Box::pin(tokio::time::sleep(
                                            Self::STOP_GRACE_PERIOD
                                        )));
                                    }
                                }

                                tx.send(self.put_state(state).await
                                    .map(|r| VmmPutStateResponse { updated_runtime: Some(r) })
                                    .map_err(|e| e.into()))
                                    .map_err(|_| Error::FailedSendClientClosed)
                            },
                            IssueSnapshotRequest { disk_id, snapshot_id, tx } => {
                                tx.send(
                                    self.issue_snapshot_request(
                                        disk_id,
                                        snapshot_id
                                    ).await.map_err(|e| e.into())
                                )
                                .map_err(|_| Error::FailedSendClientClosed)
                            },
                            AddExternalIp { ip, tx } => {
                                tx.send(self.add_external_ip(&ip).await.map_err(|e| e.into()))
                                .map_err(|_| Error::FailedSendClientClosed)
                            },
                            DeleteExternalIp { ip, tx } => {
                                tx.send(self.delete_external_ip(&ip).await.map_err(|e| e.into()))
                                .map_err(|_| Error::FailedSendClientClosed)
                            },
                            RefreshExternalIps { tx } => {
                                tx.send(self.refresh_external_ips().map_err(|e| e.into()))
                                .map_err(|_| Error::FailedSendClientClosed)
                            }
                        }
                    };
                    tokio::select! {
                        biased;

                        request = terminate_rx.recv() => {
                            state_owner = self.handle_termination_request(
                                request,
                                Some(&request_variant),
                            ).await;
                            break;
                        }

                        result = op => {
                            if let Err(err) = result {
                                warn!(
                                    self.log,
                                    "Error handling request";
                                    "request" => request_variant,
                                    "err" => ?err,
                                );
                            }
                        }
                    };
                }

            }
        }

        // This is the last opportunity to publish VMM state to Nexus for this
        // VMM, so the VMM had better be in a state that allows Nexus to
        // dissociate it from its instance.
        //
        // It should be possible to assert this property here: all the paths out
        // of the previous loop are supposed either to set a terminal state
        // themselves or ensure that one was observed before setting the "loop
        // should exit" flag. But program defensively anyway, since an assertion
        // failure that takes out sled agent can affect multiple instances,
        // whereas incorrectly removing one VMM from the table will affect only
        // that VMM (and even that VMM's instance will get back on track when
        // Nexus realizes what has happened).
        if !self.state.vmm_is_halted() {
            error!(
                self.log,
                "instance runner exiting with VMM in non-terminal state";
                "state" => ?self.current_state()
            );

            // Assert in tests, at least.
            debug_assert!(self.state.vmm_is_halted());
        } else {
            info!(self.log, "instance runner exited main loop");
        }

        if state_owner == VmmStateOwner::Runner {
            self.publish_state_to_nexus().await;
        }

        // Deregister this instance from the instance manager. This ensures that
        // no more requests will be queued to the runner, which allows this
        // routine to send replies to those requests in an orderly fashion.
        //
        // This needs to be done only after publishing the final VMM state to
        // Nexus so that Nexus is guaranteed to have received notice of the
        // VMM's terminal state before it begins to see "no such VMM" errors
        // from the instance manager. Otherwise it is possible for Nexus to
        // conclude that a cleanly-stopped instance has gone missing and should
        // be restarted.
        ticket.deregister();

        // Okay, now that we've terminated the instance, drain any outstanding
        // requests in the queue, so that they see an error indicating that the
        // instance is going away.
        while let Some(request) = self.rx.recv().await {
            // If the receiver for this request has been dropped, ignore it
            // instead of bailing out, since we still need to drain the rest of
            // the queue,
            let _ = match request {
                GetFilesystemPool { tx } => tx.send(Ok(None)).map_err(|_| ()),
                CurrentState { tx } => {
                    tx.send(Ok(self.current_state())).map_err(|_| ())
                }
                PutState { tx, .. } => {
                    tx.send(Err(Error::Terminating.into())).map_err(|_| ())
                }
                IssueSnapshotRequest { tx, .. } => {
                    tx.send(Err(Error::Terminating.into())).map_err(|_| ())
                }
                AddExternalIp { tx, .. } => {
                    tx.send(Err(Error::Terminating.into())).map_err(|_| ())
                }
                DeleteExternalIp { tx, .. } => {
                    tx.send(Err(Error::Terminating.into())).map_err(|_| ())
                }
                RefreshExternalIps { tx } => {
                    tx.send(Err(Error::Terminating.into())).map_err(|_| ())
                }
            };
        }

        // Anyone else who was trying to ask us to go die will be happy to learn
        // that we have now done so!
        while let Some(TerminateRequest { tx, .. }) = terminate_rx.recv().await
        {
            let _ = tx.send(Ok(VmmUnregisterResponse {
                updated_runtime: Some(self.current_state()),
            }));
        }
    }

    /// Yields this instance's ID.
    fn instance_id(&self) -> InstanceUuid {
        InstanceUuid::from_untyped_uuid(self.properties.id)
    }

    async fn publish_state_to_nexus(&self) {
        // Retry until Nexus acknowledges that it has applied this state update.
        // Note that Nexus may receive this call but then fail while reacting
        // to it. If that failure is transient, Nexus expects this routine to
        // retry the state update.
        let result = backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            || async {
                let state = self.state.sled_instance_state();
                info!(self.log, "Publishing instance state update to Nexus";
                    "instance_id" => %self.instance_id(),
                    "propolis_id" => %self.propolis_id,
                    "state" => ?state,
                );

                self.nexus_client
                    .cpapi_instances_put(&self.propolis_id, &state.into())
                    .await
                    .map_err(|err| -> backoff::BackoffError<Error> {
                        match &err {
                            nexus_client::Error::CommunicationError(_) => {
                                BackoffError::transient(Error::Notification(
                                    err,
                                ))
                            }
                            nexus_client::Error::InvalidRequest(_)
                            | nexus_client::Error::InvalidResponsePayload(..)
                            | nexus_client::Error::UnexpectedResponse(_)
                            | nexus_client::Error::InvalidUpgrade(_)
                            | nexus_client::Error::ResponseBodyError(_)
                            | nexus_client::Error::PreHookError(_)
                            | nexus_client::Error::PostHookError(_) => {
                                BackoffError::permanent(Error::Notification(
                                    err,
                                ))
                            }
                            nexus_client::Error::ErrorResponse(
                                response_value,
                            ) => {
                                let status = response_value.status();

                                // TODO(#3238): The call to `cpapi_instance_put`
                                // flattens some transient errors into 500
                                // Internal Server Error, so this path must be
                                // careful not to treat a "masked" transient
                                // error as a permanent error. Treat explicit
                                // client errors (like Not Found) as permanent,
                                // but don't be too discerning with server
                                // errors that may have been generated by
                                // flattening.
                                if status.is_server_error() {
                                    BackoffError::transient(
                                        Error::Notification(err),
                                    )
                                } else {
                                    BackoffError::permanent(
                                        Error::Notification(err),
                                    )
                                }
                            }
                        }
                    })
            },
            |err: Error, delay| {
                warn!(self.log,
                      "Failed to publish instance state to Nexus: {}",
                      err.to_string();
                      "instance_id" => %self.instance_id(),
                      "propolis_id" => %self.propolis_id,
                      "retry_after" => ?delay);
            },
        )
        .await;

        if let Err(e) = result {
            error!(
                self.log,
                "Failed to publish state to Nexus, will not retry: {:?}", e;
                "instance_id" => %self.instance_id(),
                "propolis_id" => %self.propolis_id,
            );
        }
    }

    async fn handle_monitor_message(
        &mut self,
        request: Option<InstanceMonitorMessage>,
    ) {
        let Some(request) = request else {
            unreachable!("runner keeps a copy of the monitor tx");
        };

        let InstanceMonitorMessage { update, tx } = request;
        let response = match update {
            InstanceMonitorUpdate::State(state) => {
                self.observe_state(&ObservedPropolisState::new(&state));

                // If Propolis still has an active VM, just publish this state
                // update and resume normal operation. Otherwise, go through the
                // Propolis teardown sequence.
                if !self.state.vmm_is_halted() {
                    self.publish_state_to_nexus().await;
                    ControlFlow::Continue(())
                } else {
                    self.terminate().await;
                    ControlFlow::Break(())
                }
            }
            InstanceMonitorUpdate::ZoneGone => {
                warn!(self.log, "Propolis zone is gone, marking VMM as failed");

                // There's no way to restore the zone at this point, so the
                // runner is now the sole driver of the VMM state machine. Drive
                // the VMM to Failed and go through the termination sequence.
                self.fail_vmm_and_terminate().await;
                ControlFlow::Break(())
            }
            InstanceMonitorUpdate::Error(PropolisErrorCode::NoInstance) => {
                warn!(
                    self.log,
                    "Propolis monitor unexpectedly reported no instance, \
                    marking as failed"
                );

                // This error indicates that Propolis crashed and restarted and
                // lost whatever VM was previously created there. Nothing to do
                // but mark the VMM as failed and tear things down.
                self.fail_vmm_and_terminate().await;
                ControlFlow::Break(())
            }
            InstanceMonitorUpdate::Error(
                code @ PropolisErrorCode::AlreadyRunning
                | code @ PropolisErrorCode::AlreadyInitialized
                | code @ PropolisErrorCode::CreateFailed,
            ) => {
                // These error codes are lifecycle-related errors that the
                // monitor is never expected to return. Put up a warning here
                // but let the monitor continue to see if it recovers.
                warn!(
                    self.log,
                    "Propolis monitor returned unexpected error code";
                    "error_code" => ?code
                );

                ControlFlow::Continue(())
            }
        };

        if tx.send(response).is_err() {
            warn!(
                self.log,
                "failed to send control flow response to instance state monitor"
            );
        }
    }

    /// Processes a Propolis state change observed by the Propolis monitoring
    /// task.
    fn observe_state(&mut self, state: &ObservedPropolisState) {
        info!(self.log, "observed new Propolis state"; "state" => ?state);

        // This shouldn't happen: the monitor can't observe anything before a
        // Propolis zone comes into being, and once the runner decides to
        // discard the active zone, it should stop listening for new requests
        // from the monitor.
        // if self.running_state.is_none() {
        //     error!(
        //         self.log,
        //         "received Propolis state observation without a zone"
        //     );
        //     return;
        // }

        self.state.apply_propolis_observation(state);
        info!(
            self.log,
            "updated state after observing Propolis state change";
            "propolis_id" => %self.propolis_id,
            "new_vmm_state" => ?self.state.vmm()
        );
    }

    /// Sends an instance state PUT request to this instance's Propolis.
    async fn propolis_state_put(
        &self,
        request: propolis_client::types::InstanceStateRequested,
    ) -> Result<(), PropolisClientError> {
        let res = self
            .running_state
            .as_ref()
            .expect("Propolis client should be initialized before usage")
            .client
            .instance_state_put()
            .body(request)
            .send()
            .await;

        if let Err(e) = &res {
            error!(self.log, "Error from Propolis client: {:?}", e;
               "status" => ?e.status());
        }

        res.map(|_| ())
    }

    /// Sends an instance ensure request to this instance's Propolis using the
    /// information contained in this instance's Propolis VM spec.
    async fn send_propolis_instance_ensure(
        &self,
        client: &PropolisClient,
        running_zone: &RunningZone,
        migrate: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        use propolis_client::{
            instance_spec::ReplacementComponent,
            types::InstanceInitializationMethod,
        };

        let mut spec = self.propolis_spec.clone();
        self.amend_propolis_network_backends(&mut spec, running_zone)?;

        // When a VM migrates, the migration target inherits most of its
        // configuration directly from the migration source. The exceptions are
        // cases where the target VM needs new parameters in order to interface
        // with services on its sled or in the rest of the rack. These include
        //
        // - Crucible disks: the target needs to connect to its downstairs
        //   instances with new generation numbers supplied from Nexus
        // - OPTE ports: the target needs to bind its VNICs to the correct
        //   devices for its new host; those devices may have different names
        //   than their counterparts on the source host
        //
        // If this is a request to migrate, construct a list of source component
        // specifications that this caller intends to replace. Otherwise,
        // construct a complete instance specification for a new VM.
        let request = if let Some(params) = migrate {
            // TODO(#6073): The migration ID is sent in the VMM registration
            // request and isn't part of the migration target params (despite
            // being known when the migration-start request is sent). If it were
            // sent here it would no longer be necessary to read the ID back
            // from the saved VMM/instance state.
            //
            // In practice, Nexus should (by the construction of the instance
            // migration saga) always initialize a migration-destination VMM
            // with a valid migration ID. But since that invariant is
            // technically part of a different component, return an error here
            // instead of unwrapping if it's violated.
            let migration_id = self
                .state
                .migration_in()
                .ok_or_else(|| {
                    Error::Migration(anyhow::anyhow!(
                        "migration requested but no migration ID was \
                            supplied when this VMM was registered"
                    ))
                })?
                .migration_id;

            // Add the new Crucible backends to the list of source instance spec
            // elements that should be replaced in the target's spec.
            let mut elements_to_replace: std::collections::HashMap<_, _> = spec
                .crucible_backends()
                .map(|(id, backend)| {
                    (
                        id.to_string(),
                        ReplacementComponent::CrucibleStorageBackend(
                            backend.clone(),
                        ),
                    )
                })
                .collect();

            // Add new OPTE backend configuration to the replacement list.
            elements_to_replace.extend(spec.viona_backends().map(
                |(id, backend)| {
                    (
                        id.to_string(),
                        ReplacementComponent::VirtioNetworkBackend(
                            backend.clone(),
                        ),
                    )
                },
            ));

            propolis_client::types::InstanceEnsureRequest {
                properties: self.properties.clone(),
                init: InstanceInitializationMethod::MigrationTarget {
                    migration_id,
                    replace_components: elements_to_replace,
                    src_addr: params.src_propolis_addr.to_string(),
                },
            }
        } else {
            propolis_client::types::InstanceEnsureRequest {
                properties: self.properties.clone(),
                init: InstanceInitializationMethod::Spec { spec: spec.0 },
            }
        };

        debug!(self.log, "Sending ensure request to propolis: {:?}", request);
        let result = client.instance_ensure().body(request).send().await;
        info!(self.log, "result of instance_ensure call is {:?}", result);
        result?;
        Ok(())
    }

    /// Amends the network backend entries in the supplied Propolis VM `spec` so
    /// that they map to the correct OPTE ports in the supplied `running_zone`.
    fn amend_propolis_network_backends(
        &self,
        spec: &mut VmmSpec,
        running_zone: &RunningZone,
    ) -> Result<(), Error> {
        for port in running_zone.opte_ports() {
            let nic = self
                .requested_nics
                .iter()
                .find(|nic| nic.slot == port.slot())
                .ok_or(Error::Opte(
                    illumos_utils::opte::Error::NoNicforPort(
                        port.name().into(),
                        port.slot().into(),
                    ),
                ))?;

            // The caller is presumed to have arranged things so that NICs in
            // the requested NIC list appear with the same IDs in the instance
            // spec. Bail if this isn't the case.
            let Some(backend) =
                spec.0.components.get_mut(&SpecKey::Uuid(nic.id))
            else {
                return Err(Error::NicNotInPropolisSpec(nic.id));
            };

            let ComponentV0::VirtioNetworkBackend(be) = backend else {
                return Err(Error::NicNotInPropolisSpec(nic.id));
            };

            be.vnic_name = port.name().to_string();
        }

        Ok(())
    }

    /// Given a freshly-created Propolis process, sends an ensure request to
    /// that Propolis and launches all of the tasks needed to monitor the
    /// resulting Propolis VM.
    ///
    /// # Panics
    ///
    /// Panics if this routine is called more than once for a given Instance.
    async fn install_running_state(
        &mut self,
        PropolisSetup { client, running_zone }: PropolisSetup,
    ) {
        // Monitor propolis for state changes in the background.
        //
        // This task exits after its associated Propolis has been terminated
        // (either because the task observed a message from Propolis saying that
        // it exited or because the Propolis server was terminated by other
        // means).
        let runner = InstanceMonitorRunner {
            zone_name: running_zone.name().to_string(),
            client: client.clone(),
            tx_monitor: self.tx_monitor.clone(),
            zones_api: self.zone_builder_factory.zones_api().clone(),
            log: self.log.clone(),
        };
        let log = self.log.clone();
        let monitor_handle = tokio::task::spawn(async move {
            match runner.run().await {
                Err(e) => warn!(log, "State monitoring task failed: {}", e),
                Ok(()) => info!(log, "State monitoring task complete"),
            }
        });
        self.monitor_handle = Some(monitor_handle);
        self.running_state = Some(RunningState { client, running_zone });
    }

    /// Immediately terminates this instance's Propolis zone and cleans up any
    /// runtime objects associated with the instance.
    ///
    /// This routine is safe to call even if the instance's zone was never
    /// started. It is also safe to call multiple times on a single instance.
    async fn remove_propolis_zone(&mut self) {
        let zname = propolis_zone_name(&self.propolis_id);

        // First fetch the running state.
        //
        // If there is nothing here, then there is no `RunningZone`, and so
        // there's no zone or resources to clean up at all.
        let mut running_state = if let Some(state) = self.running_state.take() {
            state
        } else {
            debug!(
                self.log,
                "Instance::terminate() called with no running state"
            );

            return;
        };

        // Ask the sled-agent's metrics task to stop tracking statistics for our
        // control VNIC and any OPTE ports in the zone as well.
        match self.metrics_queue.untrack_zone_links(&running_state.running_zone)
        {
            Ok(_) => debug!(
                self.log,
                "stopped tracking zone datalinks";
                "zone_name" => &zname,
            ),
            Err(errors) => error!(
                self.log,
                "failed to stop tracking zone datalinks";
                "zone_name" => &zname,
                "errors" => ?errors,
            ),
        }

        // Take a zone bundle whenever this instance stops.
        if let Err(e) = self
            .zone_bundler
            .create(
                &running_state.running_zone,
                ZoneBundleCause::TerminatedInstance,
            )
            .await
        {
            error!(
                self.log,
                "Failed to take zone bundle for terminated instance";
                "zone_name" => &zname,
                "reason" => ?e,
            );
        }

        // Ensure that no zone exists. This succeeds even if no zone was ever
        // created.
        // NOTE: we call`Zones::halt_and_remove_logged` directly instead of
        // `RunningZone::stop` in case we're called between creating the
        // zone and assigning `running_state`.
        warn!(self.log, "Halting and removing zone: {}", zname);
        let result = tokio::time::timeout(
            Duration::from_secs(60 * 5),
            omicron_common::backoff::retry(
                omicron_common::backoff::retry_policy_local(),
                || async {
                    self.zone_builder_factory
                        .zones_api()
                        .halt_and_remove_logged(&self.log, &zname)
                        .await
                        .map_err(|e| {
                            if e.is_invalid_state() {
                                BackoffError::transient(e)
                            } else {
                                BackoffError::permanent(e)
                            }
                        })
                },
            ),
        )
        .await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("{e}"),
            Err(_) => {
                panic!("Zone {zname:?} could not be halted within 5 minutes")
            }
        }

        // See if there are any runtime objects to clean up.
        //
        // We already removed the zone above but mark it as stopped
        running_state.running_zone.stop().await.unwrap();

        // Remove any OPTE ports from the port manager.
        running_state.running_zone.release_opte_ports();
    }

    async fn add_external_ip_inner(
        &mut self,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        // v4 + v6 handling is delegated to `external_ips_ensure`.
        // If OPTE is unhappy, we undo at `Instance` level.

        match ip {
            // For idempotency of add/delete, we want to return
            // success on 'already done'.
            InstanceExternalIpBody::Ephemeral(ip)
                if Some(ip) == self.ephemeral_ip.as_ref() =>
            {
                return Ok(());
            }
            InstanceExternalIpBody::Floating(ip)
                if self.floating_ips.contains(ip) =>
            {
                return Ok(());
            }
            // New Ephemeral IP while current exists -- error without
            // explicit delete.
            InstanceExternalIpBody::Ephemeral(ip)
                if self.ephemeral_ip.is_some() =>
            {
                return Err(Error::Opte(
                    illumos_utils::opte::Error::ImplicitEphemeralIpDetach(
                        *ip,
                        self.ephemeral_ip.unwrap(),
                    ),
                ));
            }
            // Not found, proceed with OPTE update.
            InstanceExternalIpBody::Ephemeral(ip) => {
                self.ephemeral_ip = Some(*ip);
            }
            InstanceExternalIpBody::Floating(ip) => {
                self.floating_ips.push(*ip);
            }
        }

        let Some(primary_nic) = self.primary_nic() else {
            return Err(Error::Opte(illumos_utils::opte::Error::NoPrimaryNic));
        };

        self.port_manager.external_ips_ensure(
            primary_nic.id,
            primary_nic.kind,
            Some(self.source_nat),
            self.ephemeral_ip,
            &self.floating_ips,
        )?;

        Ok(())
    }

    fn refresh_external_ips_inner(&mut self) -> Result<(), Error> {
        let Some(primary_nic) = self.primary_nic() else {
            return Err(Error::Opte(illumos_utils::opte::Error::NoPrimaryNic));
        };

        self.port_manager.external_ips_ensure(
            primary_nic.id,
            primary_nic.kind,
            Some(self.source_nat),
            self.ephemeral_ip,
            &self.floating_ips,
        )?;

        Ok(())
    }

    async fn delete_external_ip_inner(
        &mut self,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        // v4 + v6 handling is delegated to `external_ips_ensure`.
        // If OPTE is unhappy, we undo at `Instance` level.

        match ip {
            // For idempotency of add/delete, we want to return
            // success on 'already done'.
            // IP Mismatch and 'deleted in past' can't really be
            // disambiguated here.
            InstanceExternalIpBody::Ephemeral(ip)
                if self.ephemeral_ip != Some(*ip) =>
            {
                return Ok(());
            }
            InstanceExternalIpBody::Ephemeral(_) => {
                self.ephemeral_ip = None;
            }
            InstanceExternalIpBody::Floating(ip) => {
                let floating_index =
                    self.floating_ips.iter().position(|v| v == ip);
                if let Some(pos) = floating_index {
                    // Swap remove is valid here, OPTE is not sensitive
                    // to Floating Ip ordering.
                    self.floating_ips.swap_remove(pos);
                } else {
                    return Ok(());
                }
            }
        }

        let Some(primary_nic) = self.primary_nic() else {
            return Err(Error::Opte(illumos_utils::opte::Error::NoPrimaryNic));
        };

        self.port_manager.external_ips_ensure(
            primary_nic.id,
            primary_nic.kind,
            Some(self.source_nat),
            self.ephemeral_ip,
            &self.floating_ips,
        )?;

        Ok(())
    }

    fn primary_nic(&self) -> Option<&NetworkInterface> {
        self.requested_nics.iter().find(|nic| nic.primary)
    }
}

fn propolis_error_code(
    log: &slog::Logger,
    error: &PropolisClientError,
) -> Option<PropolisErrorCode> {
    // Is this a structured error response from the Propolis server?
    let propolis_client::Error::ErrorResponse(ref rv) = &error else {
        return None;
    };

    let code = rv.error_code.as_deref()?;
    match code.parse::<PropolisErrorCode>() {
        Err(parse_error) => {
            warn!(log, "Propolis returned an unknown error code: {code:?}";
                "status" => ?error.status(),
                "error" => %error,
                "code" => ?code,
                "parse_error" => ?parse_error);
            None
        }
        Ok(code) => Some(code),
    }
}

/// Describes a single Propolis server that incarnates a specific instance.
#[derive(Clone)]
pub struct Instance {
    id: InstanceUuid,

    /// Request channel for communicating with the instance task.
    ///
    /// # Extremely Serious Warning
    ///
    /// This channel is used by the `InstanceManager` task to communicate to the
    /// instance task corresponding to each instance on this sled. Note that all
    /// of the methods on this type which send [`InstanceRequest`]s over this
    /// channel use [`mpsc::Sender::try_send`], which fails if the channel is at
    /// capacity, and *not* [`mpsc::Sender::send`], which is an async method
    /// that *waits* until capacity is available. THIS IS VERY IMPORTANT.
    ///
    /// This is because the `InstanceManager` task will call these methods in
    /// its request-processing loop as it receives requests from clients, in
    /// order to forward the request to the relevant instance. If the instance's
    /// channel has filled up because the instance is currently processing a
    /// slow request, `await`ing a call to [`mpsc::Sender::send`] will block the
    /// `InstanceManager`'s main loop from proceeding until the instance task
    /// has finished what it's doing and drained the next request from channel.
    /// Critically, this means that requests to *other, unrelated instances* on
    /// this sled would have to wait until this instance has finished what it's
    /// doing. That means a single deadlocked instance task, which is waiting
    /// for something that never completes, can render *all* instances on this
    /// sled inaccessible.
    ///
    /// Therefore, any time we send requests to the `Instance` over this channel
    /// from code that's called in the `InstanceManager`'s run loop MUST use
    /// [`mpsc::Sender::try_send`] rather than [`mpsc::Sender::send`]. Should
    /// the channel be at capacity, we return an
    /// [`Error::FailedSendChannelFull`], which eventually becomes a 503 Service
    /// Unavailable error when returned to the client. It is acceptable to call
    /// [`mpsc::Sender::send`] on this channel ONLY from code which runs
    /// exclusively in tasks that are not blocking the `InstanceManager`'s run
    /// loop.
    tx: mpsc::Sender<InstanceRequest>,

    /// Sender for requests to terminate the instance.
    ///
    /// These are sent over a separate channel so that they can be prioritized
    /// over all other requests to the instance.
    terminate_tx: mpsc::Sender<TerminateRequest>,

    /// This is reference-counted so that the `Instance` struct may be cloned.
    #[allow(dead_code)]
    runner_handle: Arc<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) struct InstanceInitialState {
    pub vmm_spec: VmmSpec,
    pub local_config: InstanceSledLocalConfig,
    pub vmm_runtime: VmmRuntimeState,
    pub propolis_addr: SocketAddr,
    /// UUID of the migration in to this VMM, if the VMM is being created as the
    /// target of an active migration.
    pub migration_id: Option<Uuid>,
}

impl Instance {
    /// Creates a new (not yet running) instance object.
    ///
    /// # Arguments
    ///
    /// * `log`: Logger for dumping debug information.
    /// * `id`: UUID of the instance to be created.
    /// * `propolis_id`: UUID for the VMM to be created.
    /// * `ticket`: A ticket that ensures this instance is a member of its
    ///   instance manager's tracking table.
    /// * `state`: The initial state of this instance.
    /// * `services`: A set of instance manager-provided services.
    /// * `sled_identifiers`: Sled-related metadata used to track statistics.
    /// * `metadata`: Instance-related metadata used to track statistics.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        log: Logger,
        id: InstanceUuid,
        propolis_id: PropolisUuid,
        ticket: InstanceTicket,
        state: InstanceInitialState,
        services: InstanceManagerServices,
        sled_identifiers: SledIdentifiers,
        metadata: InstanceMetadata,
    ) -> Result<Self, Error> {
        info!(log, "initializing new Instance";
              "instance_id" => %id,
              "propolis_id" => %propolis_id,
              "migration_id" => ?state.migration_id,
              "state" => ?state);

        let InstanceInitialState {
            vmm_spec,
            local_config,
            vmm_runtime,
            propolis_addr,
            migration_id,
        } = state;

        let InstanceManagerServices {
            nexus_client,
            vnic_allocator,
            port_manager,
            zone_bundler,
            zone_builder_factory,
            metrics_queue,
            available_datasets_rx,
        } = services;

        let mut dhcp_config = DhcpCfg {
            hostname: Some(
                local_config
                    .hostname
                    .as_str()
                    .parse()
                    .map_err(Error::InvalidHostname)?,
            ),
            host_domain: local_config
                .dhcp_config
                .host_domain
                .map(|domain| domain.parse())
                .transpose()
                .map_err(Error::InvalidHostname)?,
            domain_search_list: local_config
                .dhcp_config
                .search_domains
                .into_iter()
                .map(|domain| domain.parse())
                .collect::<Result<_, _>>()
                .map_err(Error::InvalidHostname)?,
            dns4_servers: Vec::new(),
            dns6_servers: Vec::new(),
        };
        for ip in local_config.dhcp_config.dns_servers {
            match ip {
                IpAddr::V4(ip) => dhcp_config.dns4_servers.push(ip.into()),
                IpAddr::V6(ip) => dhcp_config.dns6_servers.push(ip.into()),
            }
        }

        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let (tx_monitor, rx_monitor) = mpsc::channel(1);

        // Request channel for terminating the instance.
        //
        // This is a separate channel from the main request channel (`self.rx`)
        // because we would like to be able to prioritize requests to terminate, and
        // handle them even when the instance's main request channel may have filled
        // up.
        //
        // Note also that this is *not* part of the `InstanceRunner` struct,
        // because it's necessary to split mutable borrows in order to allow
        // selecting between the actual instance operation (which must mutate
        // the `InstanceRunner`) and awaiting a termination request.
        let (terminate_tx, terminate_rx) = mpsc::channel(QUEUE_SIZE);

        let metadata = propolis_client::types::InstanceMetadata {
            project_id: metadata.project_id,
            silo_id: metadata.silo_id,
            sled_id: sled_identifiers.sled_id,
            sled_model: sled_identifiers.model,
            sled_revision: sled_identifiers.revision,
            sled_serial: sled_identifiers.serial,
        };

        let runner = InstanceRunner {
            log: log.new(o!("instance_id" => id.to_string())),
            should_terminate: false,
            rx,
            tx_monitor,
            rx_monitor,
            monitor_handle: None,
            properties: propolis_client::types::InstanceProperties {
                id: id.into_untyped_uuid(),
                name: local_config.hostname.to_string(),
                description: "Omicron-managed VM".to_string(),
                metadata,
            },
            propolis_spec: vmm_spec,
            propolis_id,
            propolis_addr,
            vnic_allocator,
            port_manager,
            requested_nics: local_config.nics,
            source_nat: local_config.source_nat,
            ephemeral_ip: local_config.ephemeral_ip,
            floating_ips: local_config.floating_ips,
            firewall_rules: local_config.firewall_rules,
            dhcp_config,
            state: InstanceStates::new(vmm_runtime, migration_id),
            running_state: None,
            nexus_client,
            available_datasets_rx,
            zone_builder_factory,
            zone_bundler,
            metrics_queue,
        };

        let runner_handle = tokio::task::spawn(async move {
            runner.run(terminate_rx, ticket).await
        });

        Ok(Instance {
            id,
            tx,
            runner_handle: Arc::new(runner_handle),
            terminate_tx,
        })
    }

    pub fn id(&self) -> InstanceUuid {
        self.id
    }

    pub fn get_filesystem_zpool(
        &self,
        tx: oneshot::Sender<Result<Option<ZpoolName>, ManagerError>>,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::GetFilesystemPool { tx })
            .or_else(InstanceRequest::fail_try_send)
    }

    pub fn current_state(
        &self,
        tx: oneshot::Sender<Result<SledVmmState, ManagerError>>,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::CurrentState { tx })
            .or_else(InstanceRequest::fail_try_send)
    }

    /// Attempts to update the current state of the instance by launching a
    /// Propolis process for the instance (if needed) and issuing an appropriate
    /// request to Propolis to change state.
    ///
    /// Returns the instance's state after applying any changes required by this
    /// call. Note that if the instance's Propolis is in the middle of its own
    /// state transition, it may publish states that supersede the state
    /// published by this routine in perhaps-surprising ways. For example, if an
    /// instance begins to stop when Propolis has just begun to handle a prior
    /// request to reboot, the instance's state may proceed from Stopping to
    /// Rebooting to Running to Stopping to Stopped.
    pub fn put_state(
        &self,
        tx: oneshot::Sender<Result<VmmPutStateResponse, ManagerError>>,
        state: VmmStateRequested,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::PutState { state, tx })
            .or_else(InstanceRequest::fail_try_send)
    }

    /// Rudely terminates this instance's Propolis (if it has one) and
    /// immediately transitions the instance to the Destroyed state.
    pub fn terminate(
        &self,
        tx: oneshot::Sender<Result<VmmUnregisterResponse, ManagerError>>,
        new_state_owner: VmmStateOwner,
    ) -> Result<(), Error> {
        self.terminate_tx
            .try_send(TerminateRequest { tx, new_state_owner })
            .or_else(|err| match err {
                mpsc::error::TrySendError::Closed(TerminateRequest {
                    tx,
                    ..
                }) => tx.send(Err(Error::FailedSendChannelClosed.into())),
                mpsc::error::TrySendError::Full(TerminateRequest {
                    tx,
                    ..
                }) => tx.send(Err(Error::FailedSendChannelFull.into())),
            })
            .map_err(|_| Error::FailedSendClientClosed)
    }

    pub fn issue_snapshot_request(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::IssueSnapshotRequest {
                disk_id,
                snapshot_id,
                tx,
            })
            .or_else(InstanceRequest::fail_try_send)
    }

    pub fn add_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::AddExternalIp { ip: *ip, tx })
            .or_else(InstanceRequest::fail_try_send)
    }

    pub fn delete_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::DeleteExternalIp { ip: *ip, tx })
            .or_else(InstanceRequest::fail_try_send)
    }

    /// Reinstalls an instance's set of external IPs within OPTE, using
    /// up-to-date IP<->IGW mappings. This will not disrupt existing flows.
    pub fn refresh_external_ips(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
    ) -> Result<(), Error> {
        self.tx
            .try_send(InstanceRequest::RefreshExternalIps { tx })
            .or_else(InstanceRequest::fail_try_send)
    }
}

// TODO: Move this implementation higher. I'm just keeping it here to make the
// incremental diff smaller.
impl InstanceRunner {
    fn get_filesystem_zpool(&self) -> Option<ZpoolName> {
        let Some(run_state) = &self.running_state else {
            return None;
        };
        match run_state.running_zone.root_zpool() {
            ZpoolOrRamdisk::Zpool(zpool_name) => Some(*zpool_name),
            ZpoolOrRamdisk::Ramdisk => None,
        }
    }

    fn current_state(&self) -> SledVmmState {
        self.state.sled_instance_state()
    }

    /// Idempotently ensures that there is an active Propolis zone and that it
    /// has been asked to create the VM described by this instance struct and
    /// the supplied `migration_params`.
    async fn propolis_ensure(
        &mut self,
        migration_params: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        // If there's already a Propolis zone, send the request there. This
        // allows Propolis to reject requests to initialize a VM with parameters
        // that are incompatible with a previous initialization request.
        if let Some(running_state) = self.running_state.as_ref() {
            info!(
                &self.log,
                "ensuring instance which already has a Propolis zone"
            );

            return self
                .send_propolis_instance_ensure(
                    &running_state.client,
                    &running_state.running_zone,
                    migration_params,
                )
                .await;
        }

        // Otherwise, set up the zone first, then ask Propolis to create the VM.
        let setup = match self.setup_propolis_zone().await {
            Ok(setup) => setup,
            Err(e) => {
                error!(&self.log, "failed to set up Propolis zone"; "error" => ?e);
                return Err(e);
            }
        };

        if let Err(e) = self
            .send_propolis_instance_ensure(
                &setup.client,
                &setup.running_zone,
                migration_params,
            )
            .await
        {
            error!(&self.log, "failed to create Propolis VM"; "error" => ?e);
            return Err(e);
        }

        // Move ownership of the zone into the instance and set up state
        // monitoring. This prevents the zone from being shut down when this
        // routine returns.
        self.install_running_state(setup).await;
        Ok(())
    }

    async fn put_state(
        &mut self,
        state: VmmStateRequested,
    ) -> Result<SledVmmState, Error> {
        use propolis_client::types::InstanceStateRequested as PropolisRequest;
        let (propolis_request, next_published) = match state {
            VmmStateRequested::MigrationTarget(migration_params) => {
                if let Err(e) =
                    self.propolis_ensure(Some(migration_params)).await
                {
                    // If the ensure call didn't even install a Propolis zone,
                    // then VM creation has failed entirely and this VMM should
                    // just move to Failed so that the corresponding instance
                    // can be stopped.
                    if self.running_state.is_none() {
                        self.fail_vmm_and_terminate().await;
                        return Err(e);
                    }
                }

                (None, None)
            }
            VmmStateRequested::Running => {
                if let Err(e) = self.propolis_ensure(None).await {
                    // As above, if the ensure call didn't even install a
                    // Propolis zone, then VM creation has failed entirely and
                    // this VMM should just move to Failed so that the
                    // corresponding instance can be stopped.
                    if self.running_state.is_none() {
                        self.fail_vmm_and_terminate().await;
                        return Err(e);
                    }
                }

                (Some(PropolisRequest::Run), None)
            }
            VmmStateRequested::Stopped => {
                // If there's no running Propolis yet, unregister this instance
                // straightaway. Since nothing will ever send a state update in
                // this case, force the VMM into a terminal Destroyed state,
                // which is what it would have reached if it *had* existed and
                // then stopped in an orderly manner.
                if self.running_state.is_none() {
                    self.state.force_state_to_destroyed();
                    self.terminate().await;
                    (None, None)
                } else {
                    (
                        Some(PropolisRequest::Stop),
                        Some(PublishedVmmState::Stopping),
                    )
                }
            }
            VmmStateRequested::Reboot => {
                if self.running_state.is_none() {
                    return Err(Error::VmNotRunning(self.propolis_id));
                }
                (
                    Some(PropolisRequest::Reboot),
                    Some(PublishedVmmState::Rebooting),
                )
            }
        };

        // All the arms above should either create a Propolis zone on success or
        // check that one already exists. Note that the calls that create the
        // zone also send a VM creation request to the new Propolis process, but
        // this is trickier to assert without actually calling the Propolis API.
        assert!(
            self.running_state.is_some(),
            "should have an active Propolis zone by now"
        );

        // Since there's an active Propolis zone with an extant VM, it's
        // possible to ask Propolis to drive the VM state machine.
        if let Some(p) = propolis_request {
            if let Err(e) = self.propolis_state_put(p).await {
                match propolis_error_code(&self.log, &e) {
                    Some(
                        code @ PropolisErrorCode::NoInstance
                        | code @ PropolisErrorCode::CreateFailed,
                    ) => {
                        // Propolis is alive but thinks there's no VM here,
                        // which implies that it restarted and lost the
                        // previously-created VM. This is grounds to mark the
                        // VMM as Failed.
                        error!(self.log,
                            "Propolis VM is missing, can't change its state";
                            "code" => ?code,
                        );

                        self.fail_vmm_and_terminate().await;

                        // Return the newly-installed Failed state here so it
                        // doesn't get clobbered by the published VMM state
                        // chosen above. (Note that returning `Ok` here means it
                        // is possible for a state change operation to "succeed"
                        // even though the outcome is a failed instance.)
                        return Ok(self.state.sled_instance_state());
                    }
                    _ => {
                        return Err(Error::Propolis(e));
                    }
                }
            }
        }
        if let Some(s) = next_published {
            self.state.transition_vmm(s, Utc::now());
        }
        Ok(self.state.sled_instance_state())
    }

    /// Sets up the Propolis zone that will host this instance's virtual
    /// machine.
    async fn setup_propolis_zone(&mut self) -> Result<PropolisSetup, Error> {
        // Create OPTE ports for the instance. We also store the names of all
        // those ports to notify the metrics task to start collecting statistics
        // for them.
        let mut opte_ports = Vec::with_capacity(self.requested_nics.len());
        let mut opte_port_names = Vec::with_capacity(self.requested_nics.len());
        for nic in self.requested_nics.iter() {
            let (snat, ephemeral_ip, floating_ips) = if nic.primary {
                (
                    Some(self.source_nat),
                    self.ephemeral_ip,
                    &self.floating_ips[..],
                )
            } else {
                (None, None, &[][..])
            };
            let port = self.port_manager.create_port(PortCreateParams {
                nic,
                source_nat: snat,
                ephemeral_ip,
                floating_ips,
                firewall_rules: &self.firewall_rules,
                dhcp_config: self.dhcp_config.clone(),
            })?;
            opte_port_names.push(port.0.name().to_string());
            opte_ports.push(port);
        }

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(&self.propolis_id);
        let mut rng = rand::rngs::StdRng::from_os_rng();

        let root = self
            .available_datasets_rx
            .all_mounted_zone_root_datasets()
            .into_iter()
            .choose(&mut rng)
            .ok_or_else(|| Error::U2NotFound)?;
        let installed_zone = self
            .zone_builder_factory
            .builder()
            .with_log(self.log.clone())
            .with_underlay_vnic_allocator(&self.vnic_allocator)
            .with_zone_root_path(root)
            .with_file_source(&ramdisk_file_source("propolis-server"))
            .with_zone_type("propolis-server")
            .with_unique_name(OmicronZoneUuid::from_untyped_uuid(
                self.propolis_id.into_untyped_uuid(),
            ))
            .with_datasets(&[])
            .with_filesystems(&[])
            .with_data_links(&[])
            .with_devices(&[
                zone::Device { name: "/dev/vmm/*".to_string() },
                zone::Device { name: "/dev/vmmctl".to_string() },
                zone::Device { name: "/dev/viona".to_string() },
            ])
            .with_opte_ports(opte_ports)
            .with_links(vec![])
            .with_limit_priv(vec![])
            .install()
            .await?;

        let gateway = self.port_manager.underlay_ip();
        let config = PropertyGroupBuilder::new("config")
            .add_property(
                "datalink",
                "astring",
                installed_zone.get_control_vnic_name(),
            )
            .add_property("gateway", "astring", &gateway.to_string())
            .add_property(
                "listen_addr",
                "astring",
                &self.propolis_addr.ip().to_string(),
            )
            .add_property(
                "listen_port",
                "astring",
                &self.propolis_addr.port().to_string(),
            )
            // Allow Propolis's `oximeter_producer::Server` to use DNS, based on
            // the underlay IP address supplied in `listen_addr` above.
            .add_property("metric_addr", "astring", "dns");

        let profile = ProfileBuilder::new("omicron").add_service(
            ServiceBuilder::new("system/illumos/propolis-server").add_instance(
                ServiceInstanceBuilder::new("default")
                    .add_property_group(config),
            ),
        );
        profile.add_to_zone(&self.log, &installed_zone).await?;

        let running_zone = RunningZone::boot(installed_zone).await?;
        info!(self.log, "Started propolis in zone: {}", zname);

        // This isn't strictly necessary - we wait for the HTTP server below -
        // but it helps distinguish "online in SMF" from "responding to HTTP
        // requests".
        let fmri = fmri_name();
        self.zone_builder_factory
            .zones_api()
            .wait_for_service(Some(&zname), &fmri, self.log.clone())
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;
        info!(self.log, "Propolis SMF service is online");

        // Notify the metrics task about the instance zone's datalinks.
        match self.metrics_queue.track_zone_links(&running_zone) {
            Ok(_) => debug!(
                self.log,
                "Started tracking datalinks";
                "zone_name" => running_zone.name(),
            ),
            Err(errors) => error!(
                self.log,
                "Failed to track one or more datalinks in the zone, \
                some metrics will not be produced";
                "errors" => ?errors,
                "zone_name" => running_zone.name(),
            ),
        }

        // We use a custom client builder here because the default progenitor
        // one has a timeout of 15s but we want to be able to wait indefinitely.
        let reqwest_client = reqwest::ClientBuilder::new().build().unwrap();
        let client = Arc::new(PropolisClient::new_with_client(
            &format!("http://{}", &self.propolis_addr),
            reqwest_client,
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&self.log, &client).await?;
        info!(self.log, "Propolis HTTP server online");

        Ok(PropolisSetup { client, running_zone })
    }

    /// Handles a request to rudely and immediately terminate a running
    /// Propolis.
    async fn handle_termination_request(
        &mut self,
        req: Option<TerminateRequest>,
        current_req: Option<&str>,
    ) -> VmmStateOwner {
        match req {
            Some(TerminateRequest { tx, new_state_owner }) => {
                if let Some(request) = current_req {
                    info!(
                        self.log,
                        "Received request to terminate instance while waiting \
                         on an ongoing request";
                        "request" => %request,
                    );
                } else {
                    info!(
                        self.log,
                        "Received request to terminate instance";
                    );
                }

                self.fail_vmm_and_terminate().await;
                let result = tx
                    .send(Ok(VmmUnregisterResponse {
                        updated_runtime: Some(self.state.sled_instance_state()),
                    }))
                    .map_err(|_| Error::FailedSendClientClosed);
                if let Err(err) = result {
                    warn!(
                        self.log,
                        "Error handling request to terminate instance";
                        "err" => ?err,
                    );
                }

                new_state_owner
            }
            None => {
                // This path shouldn't be reachable (as of this writing): it
                // requires the sender side of the runner's `terminate_rx` to be
                // dropped; this is owned by the runner's corresponding
                // Instance; the instance is only removed from its
                // InstanceManager in response to the instance ticket being
                // dropped; and the instance ticket isn't dropped until the
                // runner exits, which by definition hasn't happened if the
                // runner is still selecting on its termination receiver.
                //
                // This logic relies on an assumption about non-local code
                // (specifically that the instance manager has no way to drop an
                // Instance without its ticket being dropped), so defensively
                // drive the instance into a terminal state here anyway. (If the
                // instance manager shutdown sequence wants different behavior
                // it can send an explicit termination request.)
                if let Some(request) = current_req {
                    warn!(
                        self.log,
                        "Instance termination request channel closed while \
                         waiting on an ongoing request; shutting down";
                        "request" => %request,
                    );
                } else {
                    warn!(
                        self.log,
                        "Instance termination request channel closed; \
                         shutting down";
                    );
                }

                self.fail_vmm_and_terminate().await;
                VmmStateOwner::Runner
            }
        }
    }

    /// Forcibly moves this VMM to the Failed state, then goes through the
    /// runner termination sequence.
    async fn fail_vmm_and_terminate(&mut self) {
        self.state.force_state_to_failed();
        self.terminate().await;
    }

    /// Ensures that no Propolis zone exists for this instance runner and sets
    /// its `should_terminate` flag so that the runner will shut down.
    async fn terminate(&mut self) {
        self.remove_propolis_zone().await;
        self.should_terminate = true;
    }

    async fn issue_snapshot_request(
        &self,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        if let Some(running_state) = &self.running_state {
            running_state
                .client
                .instance_issue_crucible_snapshot_request()
                .id(disk_id)
                .snapshot_id(snapshot_id)
                .send()
                .await?;

            Ok(())
        } else {
            Err(Error::VmNotRunning(self.propolis_id))
        }
    }

    async fn add_external_ip(
        &mut self,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        // The internal call can either fail on adding the IP
        // to the list, or on the OPTE step.
        // Be cautious and reset state if either fails.
        // Note we don't need to re-ensure port manager/OPTE state
        // since that's the last call we make internally.
        let old_eph = self.ephemeral_ip;
        let out = self.add_external_ip_inner(ip).await;

        if out.is_err() {
            self.ephemeral_ip = old_eph;
            if let InstanceExternalIpBody::Floating(ip) = ip {
                self.floating_ips.retain(|v| v != ip);
            }
        }
        out
    }

    async fn delete_external_ip(
        &mut self,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        // Similar logic to `add_external_ip`, except here we
        // need to readd the floating IP if it was removed.
        // OPTE doesn't care about the order of floating IPs.
        let old_eph = self.ephemeral_ip;
        let out = self.delete_external_ip_inner(ip).await;

        if out.is_err() {
            self.ephemeral_ip = old_eph;
            if let InstanceExternalIpBody::Floating(ip) = ip {
                if !self.floating_ips.contains(ip) {
                    self.floating_ips.push(*ip);
                }
            }
        }
        out
    }

    fn refresh_external_ips(&mut self) -> Result<(), Error> {
        self.refresh_external_ips_inner()
    }
}

#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use super::*;
    use crate::fakes::nexus::{FakeNexusServer, ServerContext};
    use crate::metrics;
    use crate::nexus::make_nexus_client_with_port;
    use crate::vmm_reservoir::VmmReservoirManagerHandle;
    use camino_tempfile::Utf8TempDir;
    use dns_server::TransientServer;
    use dropshot::HttpServer;
    use internal_dns_resolver::Resolver;
    use omicron_common::FileKv;
    use omicron_common::api::external::{Generation, Hostname};
    use omicron_common::api::internal::nexus::VmmState;
    use omicron_common::api::internal::shared::{DhcpConfig, SledIdentifiers};
    use omicron_common::disk::DiskIdentity;
    use omicron_uuid_kinds::InternalZpoolUuid;
    use oximeter_instruments::kstat::KstatSemaphore;
    use propolis_client::types::{
        InstanceMigrateStatusResponse, InstanceStateMonitorResponse,
    };
    use sled_agent_config_reconciler::{
        CurrentlyManagedZpoolsReceiver, InternalDiskDetails,
        InternalDisksReceiver,
    };
    use sled_agent_types::zone_bundle::CleanupContext;
    use sled_storage::config::MountConfig;
    use std::net::SocketAddrV6;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4};
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::sync::watch::Receiver;
    use tokio::time::timeout;

    const TIMEOUT_DURATION: tokio::time::Duration =
        tokio::time::Duration::from_secs(30);

    // Make the Propolis ID const, so we can refer to it in tests that check the
    // zone name is included in requests to track the zone's links.
    const PROPOLIS_ID: Uuid =
        uuid::uuid!("e8e95a60-2aaf-4453-90e4-e0e58f126762");

    #[derive(Default, Clone, Debug)]
    enum ReceivedInstanceState {
        #[default]
        None,
        InstancePut(SledVmmState),
    }

    struct NexusServer {
        observed_runtime_state:
            tokio::sync::watch::Sender<ReceivedInstanceState>,
    }
    impl FakeNexusServer for NexusServer {
        fn cpapi_instances_put(
            &self,
            _propolis_id: PropolisUuid,
            new_runtime_state: SledVmmState,
        ) -> Result<(), omicron_common::api::external::Error> {
            self.observed_runtime_state
                .send(ReceivedInstanceState::InstancePut(new_runtime_state))
                .map_err(|_| {
                    omicron_common::api::external::Error::internal_error(
                        "couldn't send SledInstanceState to test driver",
                    )
                })
        }
    }

    struct FakeNexusParts {
        nexus_client: NexusClient,
        _nexus_server: HttpServer<ServerContext>,
        state_rx: Receiver<ReceivedInstanceState>,
        _dns_server: TransientServer,
    }

    impl FakeNexusParts {
        async fn new(log: &Logger) -> Self {
            let (state_tx, state_rx) =
                tokio::sync::watch::channel(ReceivedInstanceState::None);

            let _nexus_server = crate::fakes::nexus::start_test_server(
                log.new(o!("component" => "FakeNexusServer")),
                Box::new(NexusServer { observed_runtime_state: state_tx }),
            );

            let _dns_server =
                crate::fakes::nexus::start_dns_server(&log, &_nexus_server)
                    .await;

            let resolver = Arc::new(
                Resolver::new_from_addrs(
                    log.clone(),
                    &[_dns_server.dns_server.local_address()],
                )
                .unwrap(),
            );

            let nexus_client = make_nexus_client_with_port(
                &log,
                resolver,
                _nexus_server.local_addr().port(),
            );

            Self { nexus_client, _nexus_server, state_rx, _dns_server }
        }
    }

    // note the "mock" here is different from the vnic/zone contexts above.
    // this is actually running code for a dropshot server from propolis.
    // TODO: factor out, this is also in sled-agent-sim.
    fn propolis_mock_server(
        log: &Logger,
    ) -> (propolis_mock_server::Server, PropolisClient) {
        let propolis_bind_address =
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0); // allocate port
        let dropshot_config = propolis_mock_server::Config {
            bind_address: propolis_bind_address,
            ..Default::default()
        };
        info!(log, "Starting mock propolis-server...");

        let srv = propolis_mock_server::start(dropshot_config, log.clone())
            .expect("couldn't create mock propolis-server");
        let client = propolis_client::Client::new(&format!(
            "http://{}",
            srv.local_addr()
        ));

        (srv, client)
    }

    async fn propolis_mock_set_mode(
        client: &PropolisClient,
        mode: propolis_mock_server::MockMode,
    ) {
        let url = format!("{}/mock/mode", client.baseurl());
        client
            .client()
            .put(url)
            .json(&mode)
            .send()
            .await
            .expect("setting mock mode failed unexpectedly");
    }

    async fn propolis_mock_step(client: &PropolisClient) {
        let url = format!("{}/mock/step", client.baseurl());
        client
            .client()
            .put(url)
            .send()
            .await
            .expect("single-stepping mock server failed unexpectedly");
    }

    async fn instance_struct(
        log: &Logger,
        propolis_addr: SocketAddr,
        nexus_client: NexusClient,
        available_datasets_rx: AvailableDatasetsReceiver,
        temp_dir: &str,
    ) -> (Instance, MetricsRx) {
        let id = InstanceUuid::new_v4();
        let propolis_id = PropolisUuid::from_untyped_uuid(PROPOLIS_ID);

        let ticket = InstanceTicket::new_without_manager_for_test(propolis_id);

        let initial_state = fake_instance_initial_state(propolis_addr);

        let (services, rx) = fake_instance_manager_services(
            log,
            available_datasets_rx,
            nexus_client,
            temp_dir,
        )
        .await;

        let metadata = InstanceMetadata {
            silo_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
        };
        let sled_identifiers = SledIdentifiers {
            rack_id: Uuid::new_v4(),
            sled_id: Uuid::new_v4(),
            model: "fake-model".into(),
            revision: 1,
            serial: "fake-serial".into(),
        };

        let instance = Instance::new(
            log.new(o!("component" => "Instance")),
            id,
            propolis_id,
            ticket,
            initial_state,
            services,
            sled_identifiers,
            metadata,
        )
        .unwrap();
        (instance, rx)
    }

    fn fake_instance_initial_state(
        propolis_addr: SocketAddr,
    ) -> InstanceInitialState {
        use propolis_client::instance_spec::{Board, InstanceSpecV0};
        let spec = VmmSpec(InstanceSpecV0 {
            board: Board {
                cpus: 1,
                memory_mb: 1024,
                chipset: Default::default(),
                guest_hv_interface: Default::default(),
                cpuid: None,
            },
            components: Default::default(),
        });

        let local_config = InstanceSledLocalConfig {
            hostname: Hostname::from_str("bert").unwrap(),
            nics: vec![],
            source_nat: SourceNatConfig::new(
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                0,
                16383,
            )
            .unwrap(),
            ephemeral_ip: None,
            floating_ips: vec![],
            firewall_rules: vec![],
            dhcp_config: DhcpConfig {
                dns_servers: vec![],
                host_domain: None,
                search_domains: vec![],
            },
        };

        InstanceInitialState {
            vmm_spec: spec,
            local_config,
            vmm_runtime: VmmRuntimeState {
                state: VmmState::Starting,
                gen: Generation::new(),
                time_updated: Default::default(),
            },
            propolis_addr,
            migration_id: None,
        }
    }

    // Helper alias for the receive-side of the metrics request queue.
    type MetricsRx = mpsc::Receiver<metrics::Message>;

    async fn fake_instance_manager_services(
        log: &Logger,
        available_datasets_rx: AvailableDatasetsReceiver,
        nexus_client: NexusClient,
        temp_dir: &str,
    ) -> (InstanceManagerServices, MetricsRx) {
        let vnic_allocator = VnicAllocator::new(
            "Instance",
            Etherstub("mystub".to_string()),
            illumos_utils::fakes::dladm::Dladm::new(),
        );
        let port_manager = SledAgentPortManager::new(
            log.new(o!("component" => "PortManager")),
            KstatSemaphore::new(),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );

        let cleanup_context = CleanupContext::default();
        let zone_bundler = ZoneBundler::new(
            log.new(o!("component" => "ZoneBundler")),
            InternalDisksReceiver::fake_static(
                Arc::new(MountConfig::default()),
                [InternalDiskDetails::fake_details(
                    DiskIdentity {
                        vendor: "test-vendor".to_string(),
                        model: "test-model".to_string(),
                        serial: "test-serial".to_string(),
                    },
                    InternalZpoolUuid::new_v4(),
                    true, // is_boot_disk
                    None,
                    None,
                )]
                .into_iter(),
            ),
            available_datasets_rx.clone(),
            cleanup_context,
        )
        .await;

        let (metrics_queue, rx) = MetricsRequestQueue::for_test();
        let services = InstanceManagerServices {
            nexus_client,
            vnic_allocator,
            port_manager,
            available_datasets_rx,
            zone_bundler,
            zone_builder_factory: ZoneBuilderFactory::fake(
                Some(temp_dir),
                illumos_utils::fakes::zone::Zones::new(),
            ),
            metrics_queue,
        };
        (services, rx)
    }

    /// Holds assorted objects that are needed to test an Instance and its
    /// interactions with other parts of the system (e.g. Nexus and metrics).
    #[allow(dead_code)]
    struct InstanceTestObjects {
        nexus: FakeNexusParts,
        _temp_guard: Utf8TempDir,
        instance_manager: crate::instance_manager::InstanceManager,
        metrics_rx: MetricsRx,
    }

    impl InstanceTestObjects {
        async fn new(log: &slog::Logger) -> Self {
            let nexus = FakeNexusParts::new(&log).await;
            let temp_guard = Utf8TempDir::new().unwrap();
            let (services, metrics_rx) = fake_instance_manager_services(
                log,
                AvailableDatasetsReceiver::fake_in_tempdir_for_tests(
                    ZpoolOrRamdisk::Ramdisk,
                ),
                nexus.nexus_client.clone(),
                temp_guard.path().as_str(),
            )
            .await;

            let InstanceManagerServices {
                nexus_client,
                vnic_allocator,
                port_manager,
                available_datasets_rx,
                zone_bundler,
                zone_builder_factory,
                metrics_queue,
            } = services;

            let vmm_reservoir_manager =
                VmmReservoirManagerHandle::stub_for_test();
            let instance_manager =
                crate::instance_manager::InstanceManager::new_inner(
                    log.new(o!("component" => "InstanceManager")),
                    nexus_client,
                    vnic_allocator,
                    port_manager,
                    CurrentlyManagedZpoolsReceiver::fake_static(
                        std::iter::empty(),
                    ),
                    available_datasets_rx,
                    zone_bundler,
                    zone_builder_factory,
                    vmm_reservoir_manager,
                    metrics_queue,
                )
                .unwrap();

            Self {
                nexus,
                _temp_guard: temp_guard,
                instance_manager,
                metrics_rx,
            }
        }
    }

    #[tokio::test]
    async fn test_instance_create_events_normal() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_create_events_normal",
        );
        let log = logctx.log.new(o!(FileKv));

        let (propolis_server, _propolis_client) = propolis_mock_server(&log);
        let propolis_addr = propolis_server.local_addr();

        let FakeNexusParts {
            nexus_client,
            mut state_rx,
            _dns_server,
            _nexus_server,
        } = FakeNexusParts::new(&log).await;

        let temp_guard = Utf8TempDir::new().unwrap();

        let (inst, mut metrics_rx) = timeout(
            TIMEOUT_DURATION,
            instance_struct(
                &log,
                propolis_addr,
                nexus_client,
                AvailableDatasetsReceiver::fake_in_tempdir_for_tests(
                    ZpoolOrRamdisk::Ramdisk,
                ),
                temp_guard.path().as_str(),
            ),
        )
        .await
        .expect("timed out creating Instance struct");

        let (put_tx, put_rx) = oneshot::channel();

        // pretending we're InstanceManager::ensure_state, start our "instance"
        // (backed by fakes and propolis_mock_server)
        inst.put_state(put_tx, VmmStateRequested::Running)
            .expect("failed to send Instance::put_state");

        // even though we ignore this result at instance creation time in
        // practice (to avoid request timeouts), in this test let's make sure
        // it actually completes.
        timeout(TIMEOUT_DURATION, put_rx)
            .await
            .expect("timed out waiting for Instance::put_state result")
            .expect("failed to receive Instance::put_state result")
            .expect("Instance::put_state failed");

        timeout(
            TIMEOUT_DURATION,
            state_rx.wait_for(|maybe_state| match maybe_state {
                ReceivedInstanceState::InstancePut(sled_inst_state) => {
                    sled_inst_state.vmm_state.state == VmmState::Running
                }
                _ => false,
            }),
        )
        .await
        .expect("timed out waiting for InstanceState::Running in FakeNexus")
        .expect("failed to receive FakeNexus' InstanceState");

        // We should have received exactly one message on the metrics request
        // queue, for the control VNIC. The instance has no OPTE ports.
        let message =
            metrics_rx.try_recv().expect("Should have received a message");
        let zone_name =
            propolis_zone_name(&PropolisUuid::from_untyped_uuid(PROPOLIS_ID));
        assert_eq!(
            message,
            metrics::Message::TrackVnic {
                zone_name,
                name: "oxControlInstance0".into(),
            },
            "Expected instance zone to send a message on its metrics \
            request queue, asking to track its control VNIC",
        );
        metrics_rx
            .try_recv()
            .expect_err("The metrics request queue should have one message");

        logctx.cleanup_successful();
    }

    // tests around dropshot request timeouts during the blocking propolis setup
    #[tokio::test]
    async fn test_instance_create_timeout_while_starting_propolis() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_create_timeout_while_starting_propolis",
        );
        let log = logctx.log.new(o!(FileKv));

        let FakeNexusParts {
            nexus_client,
            state_rx,
            _dns_server,
            _nexus_server,
        } = FakeNexusParts::new(&log).await;

        let temp_guard = Utf8TempDir::new().unwrap();

        let (inst, _) = timeout(
            TIMEOUT_DURATION,
            instance_struct(
                &log,
                // we want to test propolis not ever coming up
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0, 0)),
                nexus_client,
                AvailableDatasetsReceiver::fake_in_tempdir_for_tests(
                    ZpoolOrRamdisk::Ramdisk,
                ),
                temp_guard.path().as_str(),
            ),
        )
        .await
        .expect("timed out creating Instance struct");

        let (put_tx, put_rx) = oneshot::channel();

        tokio::time::pause();

        // pretending we're InstanceManager::ensure_state, try in vain to start
        // our "instance", but no propolis server is running
        inst.put_state(put_tx, VmmStateRequested::Running)
            .expect("failed to send Instance::put_state");

        let timeout_fut = timeout(TIMEOUT_DURATION, put_rx);

        tokio::time::advance(TIMEOUT_DURATION).await;

        tokio::time::resume();

        timeout_fut
            .await
            .expect_err("*should've* timed out waiting for Instance::put_state, but didn't?");

        if let ReceivedInstanceState::InstancePut(SledVmmState {
            vmm_state: VmmRuntimeState { state: VmmState::Running, .. },
            ..
        }) = state_rx.borrow().to_owned()
        {
            panic!(
                "Nexus's InstanceState should never have reached running if zone creation timed out"
            );
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_manager_creation() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_manager_creation",
        );
        let log = logctx.log.new(o!(FileKv));

        let mut test_objects = InstanceTestObjects::new(&log).await;

        let (propolis_server, _propolis_client) =
            propolis_mock_server(&logctx.log);
        let propolis_addr = propolis_server.local_addr();

        let instance_id = InstanceUuid::new_v4();
        let propolis_id = PropolisUuid::from_untyped_uuid(PROPOLIS_ID);
        let InstanceInitialState {
            vmm_spec,
            local_config,
            vmm_runtime,
            propolis_addr,
            migration_id: _,
        } = fake_instance_initial_state(propolis_addr);

        let metadata = InstanceMetadata {
            silo_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
        };
        let sled_identifiers = SledIdentifiers {
            rack_id: Uuid::new_v4(),
            sled_id: Uuid::new_v4(),
            model: "fake-model".into(),
            revision: 1,
            serial: "fake-serial".into(),
        };

        test_objects
            .instance_manager
            .ensure_registered(
                propolis_id,
                InstanceEnsureBody {
                    vmm_spec,
                    local_config,
                    instance_id,
                    migration_id: None,
                    vmm_runtime,
                    propolis_addr,
                    metadata,
                },
                sled_identifiers,
            )
            .await
            .unwrap();

        test_objects
            .instance_manager
            .ensure_state(propolis_id, VmmStateRequested::Running)
            .await
            .unwrap();

        timeout(
            TIMEOUT_DURATION,
            test_objects.nexus.state_rx.wait_for(
                |maybe_state| match maybe_state {
                    ReceivedInstanceState::InstancePut(sled_inst_state) => {
                        sled_inst_state.vmm_state.state == VmmState::Running
                    }
                    _ => false,
                },
            ),
        )
        .await
        .expect("timed out waiting for InstanceState::Running in FakeNexus")
        .expect("failed to receive FakeNexus' InstanceState");

        // We should have received exactly one message on the metrics request
        // queue, for the control VNIC. The instance has no OPTE ports.
        let message = test_objects
            .metrics_rx
            .try_recv()
            .expect("Should have received a message");
        let zone_name = propolis_zone_name(&propolis_id);
        assert_eq!(
            message,
            metrics::Message::TrackVnic {
                zone_name,
                name: "oxControlInstance0".into(),
            },
            "Expected instance zone to send a message on its metrics \
            request queue, asking to track its control VNIC",
        );
        test_objects
            .metrics_rx
            .try_recv()
            .expect_err("The metrics request queue should have one message");

        logctx.cleanup_successful();
    }

    // Tests a scenario in which a propolis-server process fails to stop in a
    // timely manner (i.e. it's gotten stuck somehow). This test asserts that
    // the sled-agent will eventually forcibly terminate the VMM process, tear
    // down the zone, and tell Nexus that the VMM has been destroyed, even
    // if Propolis can't shut itself down nicely.
    #[tokio::test]
    async fn test_instance_manager_stop_timeout() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_manager_stop_timeout",
        );
        let log = logctx.log.new(o!(FileKv));

        let mut test_objects = InstanceTestObjects::new(&log).await;

        let (propolis_server, propolis_client) =
            propolis_mock_server(&logctx.log);
        let propolis_addr = propolis_server.local_addr();

        let instance_id = InstanceUuid::new_v4();
        let propolis_id = PropolisUuid::from_untyped_uuid(PROPOLIS_ID);
        let InstanceInitialState {
            vmm_spec,
            local_config,
            vmm_runtime,
            propolis_addr,
            migration_id: _,
        } = fake_instance_initial_state(propolis_addr);

        let metadata = InstanceMetadata {
            silo_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
        };
        let sled_identifiers = SledIdentifiers {
            rack_id: Uuid::new_v4(),
            sled_id: Uuid::new_v4(),
            model: "fake-model".into(),
            revision: 1,
            serial: "fake-serial".into(),
        };

        test_objects
            .instance_manager
            .ensure_registered(
                propolis_id,
                InstanceEnsureBody {
                    vmm_spec,
                    local_config,
                    instance_id,
                    migration_id: None,
                    vmm_runtime,
                    propolis_addr,
                    metadata,
                },
                sled_identifiers,
            )
            .await
            .unwrap();

        test_objects
            .instance_manager
            .ensure_state(propolis_id, VmmStateRequested::Running)
            .await
            .unwrap();

        timeout(
            TIMEOUT_DURATION,
            test_objects.nexus.state_rx.wait_for(
                |maybe_state| match maybe_state {
                    ReceivedInstanceState::InstancePut(sled_inst_state) => {
                        sled_inst_state.vmm_state.state == VmmState::Running
                    }
                    _ => false,
                },
            ),
        )
        .await
        .expect("timed out waiting for InstanceState::Running in FakeNexus")
        .expect("failed to receive VmmState' InstanceState");

        // Place the mock propolis in single-step mode.
        propolis_mock_set_mode(
            &propolis_client,
            propolis_mock_server::MockMode::SingleStep,
        )
        .await;

        // Request the VMM stop
        test_objects
            .instance_manager
            .ensure_state(propolis_id, VmmStateRequested::Stopped)
            .await
            .unwrap();

        // Single-step once.
        propolis_mock_step(&propolis_client).await;

        timeout(
            TIMEOUT_DURATION,
            test_objects.nexus.state_rx.wait_for(
                |maybe_state| match maybe_state {
                    ReceivedInstanceState::InstancePut(sled_inst_state) => {
                        sled_inst_state.vmm_state.state == VmmState::Stopping
                    }
                    _ => false,
                },
            ),
        )
        .await
        .expect("timed out waiting for VmmState::Stopping in FakeNexus")
        .expect("failed to receive FakeNexus' InstanceState");

        // NOW WE STOP ADVANCING THE MOCK PROPOLIS STATE MACHINE --- IT WILL
        // NEVER REACH `Stopped`.

        // Now, pause time and advance the Tokio clock past the stop grace
        // period. This should cause the stop timeout to fire, without requiring
        // the test to actually wait for ten minutes.
        tokio::time::pause();
        tokio::time::advance(
            super::InstanceRunner::STOP_GRACE_PERIOD + Duration::from_secs(1),
        )
        .await;
        tokio::time::resume();

        // The timeout should now fire and sled-agent will murder propolis,
        // allowing the zone to be destroyed.

        timeout(
            TIMEOUT_DURATION,
            test_objects.nexus.state_rx.wait_for(
                |maybe_state| match maybe_state {
                    ReceivedInstanceState::InstancePut(sled_inst_state) => {
                        sled_inst_state.vmm_state.state == VmmState::Destroyed
                    }
                    _ => false,
                },
            ),
        )
        .await
        .expect("timed out waiting for VmmState::Stopped in FakeNexus")
        .expect("failed to receive FakeNexus' InstanceState");

        logctx.cleanup_successful();
    }

    impl InstanceRunner {
        fn new_for_test(
            log: &slog::Logger,
            propolis_id: PropolisUuid,
            initial_state: InstanceInitialState,
            services: InstanceManagerServices,
            cmd_rx: mpsc::Receiver<InstanceRequest>,
            monitor_tx: mpsc::Sender<InstanceMonitorMessage>,
            monitor_rx: mpsc::Receiver<InstanceMonitorMessage>,
        ) -> Self {
            let metadata = InstanceMetadata {
                silo_id: Uuid::new_v4(),
                project_id: Uuid::new_v4(),
            };
            let sled_identifiers = SledIdentifiers {
                rack_id: Uuid::new_v4(),
                sled_id: Uuid::new_v4(),
                model: "fake-model".into(),
                revision: 1,
                serial: "fake-serial".into(),
            };

            let metadata = propolis_client::types::InstanceMetadata {
                project_id: metadata.project_id,
                silo_id: metadata.silo_id,
                sled_id: sled_identifiers.sled_id,
                sled_model: sled_identifiers.model,
                sled_revision: sled_identifiers.revision,
                sled_serial: sled_identifiers.serial,
            };

            let InstanceInitialState {
                vmm_spec,
                local_config,
                vmm_runtime,
                propolis_addr,
                migration_id,
            } = initial_state;

            let InstanceManagerServices {
                nexus_client,
                vnic_allocator,
                port_manager,
                available_datasets_rx,
                zone_bundler,
                zone_builder_factory,
                metrics_queue,
            } = services;

            let dhcp_config = DhcpCfg::default();

            Self {
                log: log.new(o!("component" => "TestInstanceRunner")),
                should_terminate: false,
                rx: cmd_rx,
                tx_monitor: monitor_tx,
                rx_monitor: monitor_rx,
                monitor_handle: None,
                properties: propolis_client::types::InstanceProperties {
                    id: propolis_id.into_untyped_uuid(),
                    name: "test instance".to_string(),
                    description: "test instance".to_string(),
                    metadata,
                },
                propolis_spec: vmm_spec,
                propolis_id,
                propolis_addr,
                vnic_allocator,
                port_manager,
                requested_nics: local_config.nics,
                source_nat: local_config.source_nat,
                ephemeral_ip: local_config.ephemeral_ip,
                floating_ips: local_config.floating_ips,
                firewall_rules: local_config.firewall_rules,
                dhcp_config,
                state: InstanceStates::new(vmm_runtime, migration_id),
                running_state: None,
                nexus_client,
                available_datasets_rx,
                zone_builder_factory,
                zone_bundler,
                metrics_queue,
            }
        }
    }

    struct TestInstanceRunner {
        runner_task: tokio::task::JoinHandle<()>,
        state_rx: tokio::sync::watch::Receiver<ReceivedInstanceState>,
        terminate_tx: mpsc::Sender<TerminateRequest>,
        monitor_tx: mpsc::Sender<InstanceMonitorMessage>,
        cmd_tx: mpsc::Sender<InstanceRequest>,
        remove_rx: mpsc::UnboundedReceiver<
            crate::instance_manager::InstanceDeregisterRequest,
        >,

        // Hold onto all of the test doubles that the instance runner will end
        // up communicating with, even though the tests may not interact with
        // them directly.
        _nexus_server: HttpServer<ServerContext>,
        _dns_server: TransientServer,
    }

    impl TestInstanceRunner {
        async fn new(log: &slog::Logger) -> Self {
            let FakeNexusParts {
                nexus_client,
                _nexus_server,
                state_rx,
                _dns_server,
            } = FakeNexusParts::new(&log).await;

            let temp_guard = Utf8TempDir::new().unwrap();
            let (services, _metrics_rx) = fake_instance_manager_services(
                &log,
                AvailableDatasetsReceiver::fake_in_tempdir_for_tests(
                    ZpoolOrRamdisk::Ramdisk,
                ),
                nexus_client,
                temp_guard.path().as_str(),
            )
            .await;
            let propolis_id = PropolisUuid::new_v4();
            let propolis_addr = SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, 1),
                12400,
            ));

            let initial_state = fake_instance_initial_state(propolis_addr);

            let (terminate_tx, terminate_rx) = mpsc::channel(1);
            let (monitor_tx, monitor_rx) = mpsc::channel(1);
            let (cmd_tx, cmd_rx) = mpsc::channel(QUEUE_SIZE);
            let (remove_tx, remove_rx) = mpsc::unbounded_channel();
            let ticket = InstanceTicket::new(propolis_id, remove_tx);

            let runner = InstanceRunner::new_for_test(
                &log.new(o!("component" => "InstanceRunner")),
                propolis_id,
                initial_state,
                services,
                cmd_rx,
                monitor_tx.clone(),
                monitor_rx,
            );

            let runner_task = tokio::spawn(async move {
                runner.run(terminate_rx, ticket).await;
            });

            Self {
                runner_task,
                state_rx,
                terminate_tx,
                monitor_tx,
                cmd_tx,
                remove_rx,
                _nexus_server,
                _dns_server,
            }
        }
    }

    #[tokio::test]
    async fn test_destroy_published_when_instance_removed() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_destroy_published_before_instanced_removed",
        );
        let log = logctx.log.new(o!(FileKv));

        let TestInstanceRunner {
            runner_task,
            state_rx,
            terminate_tx,
            monitor_tx,
            cmd_tx,
            mut remove_rx,
            _nexus_server,
            _dns_server,
        } = TestInstanceRunner::new(&log).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        monitor_tx
            .send(InstanceMonitorMessage {
                update: InstanceMonitorUpdate::State(
                    InstanceStateMonitorResponse {
                        gen: 5,
                        migration: InstanceMigrateStatusResponse {
                            migration_in: None,
                            migration_out: None,
                        },
                        state: propolis_client::types::InstanceState::Destroyed,
                    },
                ),
                tx: resp_tx,
            })
            .await
            .unwrap();

        // Since the fake VMM is gone, the monitor should be told to exit...
        let resp = resp_rx.await.unwrap();
        assert!(resp.is_break());

        // ...and the runner should advise the instance manager to remove this
        // instance from its table before it tries to return.
        assert!(remove_rx.recv().await.is_some());

        // By the time the "remove this instance" message arrives, the runner
        // should have published the Destroyed VM state.
        //
        // NOTE: Strictly speaking the expected behavior is that the runner will
        // publish state to Nexus *before* it removes the instance from the
        // table, so that there is no window where the instance is gone but
        // Nexus has yet to be told about the terminal VMM state. But these
        // messages are sent to different channels, so it's possible here that
        // the runner requested removal and then published the Destroyed state
        // quickly enough to satisfy this check. But this at least tests that
        // this window, if it exists, is very small.
        let state = state_rx.borrow().clone();
        let ReceivedInstanceState::InstancePut(state) = state else {
            panic!("unexpected ReceivedInstanceState variant {state:?}");
        };

        assert_eq!(state.vmm_state.state, VmmState::Destroyed);

        // Make sure the runner actually runs to completion once its command
        // channels are dropped. (This simulates what happens when the "real"
        // instance manager is asked to remove a record from its VMM table.)
        drop(cmd_tx);
        drop(terminate_tx);
        let _ = runner_task.await;

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_failed_published_when_zone_gone() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_failed_published_when_zone_gone",
        );
        let log = logctx.log.new(o!(FileKv));

        let TestInstanceRunner {
            runner_task: _rt,
            state_rx,
            terminate_tx: _tt,
            monitor_tx,
            cmd_tx: _ct,
            mut remove_rx,
            _nexus_server,
            _dns_server,
        } = TestInstanceRunner::new(&log).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        monitor_tx
            .send(InstanceMonitorMessage {
                update: InstanceMonitorUpdate::ZoneGone,
                tx: resp_tx,
            })
            .await
            .unwrap();

        // The "zone gone" message should cause the VMM to be moved to Failed
        // and removed from the instance table.
        let resp = resp_rx.await.unwrap();
        assert!(resp.is_break());
        assert!(remove_rx.recv().await.is_some());
        let state = state_rx.borrow().clone();
        let ReceivedInstanceState::InstancePut(state) = state else {
            panic!("unexpected ReceivedInstanceState variant {state:?}");
        };

        assert_eq!(state.vmm_state.state, VmmState::Failed);
        logctx.cleanup_successful();
    }
}
