// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::instance::{
    Action as InstanceAction, InstanceStates, ObservedPropolisState,
    PublishedVmmState,
};
use crate::instance_manager::{
    Error as ManagerError, InstanceManagerServices, InstanceTicket,
};
use crate::metrics::MetricsRequestQueue;
use crate::nexus::NexusClient;
use crate::profile::*;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use anyhow::anyhow;
use backoff::BackoffError;
use chrono::Utc;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::{DhcpCfg, PortCreateParams, PortManager};
use illumos_utils::running_zone::{RunningZone, ZoneBuilderFactory};
use illumos_utils::svc::wait_for_service;
use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
use omicron_common::api::internal::nexus::{SledVmmState, VmmRuntimeState};
use omicron_common::api::internal::shared::{
    NetworkInterface, ResolvedVpcFirewallRule, SledIdentifiers, SourceNatConfig,
};
use omicron_common::backoff;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid};
use propolis_client::Client as PropolisClient;
use rand::prelude::IteratorRandom;
use rand::SeedableRng;
use sled_agent_types::instance::*;
use sled_agent_types::zone_bundle::{ZoneBundleCause, ZoneBundleMetadata};
use sled_storage::dataset::ZONE_DATASET;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[cfg(test)]
use illumos_utils::zone::MockZones as Zones;
#[cfg(not(test))]
use illumos_utils::zone::Zones;

// The depth of the request queue for the instance.
const QUEUE_SIZE: usize = 32;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to wait for service: {0}")]
    Timeout(String),

    #[error("Failed to create VNIC: {0}")]
    VnicCreation(#[from] illumos_utils::dladm::CreateVnicError),

    #[error("Failure from Propolis Client: {0}")]
    Propolis(#[from] propolis_client::Error<propolis_client::types::Error>),

    // TODO: Remove this error; prefer to retry notifications.
    #[error("Notifying Nexus failed: {0}")]
    Notification(nexus_client::Error<nexus_client::types::Error>),

    // TODO: This error type could become more specific
    #[error("Error performing a state transition: {0}")]
    Transition(omicron_common::api::external::Error),

    // TODO: Add more specific errors
    #[error("Failure during migration: {0}")]
    Migration(anyhow::Error),

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
    ResolveError(#[from] internal_dns::resolver::ResolveError),

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
        "Failed to send request from Instance Runner: Client Channel closed"
    )]
    FailedSendClientClosed,

    #[error("Instance dropped our request")]
    RequestDropped(#[from] oneshot::error::RecvError),

    #[error("Instance is terminating")]
    Terminating,
}

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

// Action to be taken by the Sled Agent after monitoring Propolis for
// state changes.
enum Reaction {
    Continue,
    Terminate,
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
    RequestZoneBundle {
        tx: oneshot::Sender<Result<ZoneBundleMetadata, BundleError>>,
    },
    GetFilesystemPool {
        tx: oneshot::Sender<Option<ZpoolName>>,
    },
    CurrentState {
        tx: oneshot::Sender<SledVmmState>,
    },
    PutState {
        state: VmmStateRequested,
        tx: oneshot::Sender<Result<VmmPutStateResponse, ManagerError>>,
    },
    Terminate {
        mark_failed: bool,
        tx: oneshot::Sender<Result<VmmUnregisterResponse, ManagerError>>,
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
}

// A small task which tracks the state of the instance, by constantly querying
// the state of Propolis for updates.
//
// This task communicates with the "InstanceRunner" task to report status.
struct InstanceMonitorRunner {
    client: Arc<PropolisClient>,
    tx_monitor: mpsc::Sender<InstanceMonitorRequest>,
}

impl InstanceMonitorRunner {
    async fn run(self) -> Result<(), anyhow::Error> {
        let mut gen = 0;
        loop {
            // State monitoring always returns the most recent state/gen pair
            // known to Propolis.
            let response = self
                .client
                .instance_state_monitor()
                .body(propolis_client::types::InstanceStateMonitorRequest {
                    gen,
                })
                .send()
                .await?
                .into_inner();
            let observed_gen = response.gen;

            // Now that we have the response from Propolis' HTTP server, we
            // forward that to the InstanceRunner.
            //
            // It will decide the new state, provide that info to Nexus,
            // and possibly identify if we should terminate.
            let (tx, rx) = oneshot::channel();
            self.tx_monitor
                .send(InstanceMonitorRequest::Update { state: response, tx })
                .await?;

            if let Reaction::Terminate = rx.await? {
                return Ok(());
            }

            // Update the generation number we're asking for, to ensure the
            // Propolis will only return more recent values.
            gen = observed_gen + 1;
        }
    }
}

enum InstanceMonitorRequest {
    Update {
        state: propolis_client::types::InstanceStateMonitorResponse,
        tx: oneshot::Sender<Reaction>,
    },
}

struct InstanceRunner {
    log: Logger,

    // A signal the InstanceRunner should shut down.
    // This is currently only activated by the runner itself.
    should_terminate: bool,

    // Request channel on which most instance requests are made.
    rx: mpsc::Receiver<InstanceRequest>,

    // Request channel on which monitor requests are made.
    tx_monitor: mpsc::Sender<InstanceMonitorRequest>,
    rx_monitor: mpsc::Receiver<InstanceMonitorRequest>,
    monitor_handle: Option<tokio::task::JoinHandle<()>>,

    // Properties visible to Propolis
    properties: propolis_client::types::InstanceProperties,

    // The ID of the Propolis server (and zone) running this instance
    propolis_id: PropolisUuid,

    // The socket address of the Propolis server running this instance
    propolis_addr: SocketAddr,

    // NIC-related properties
    vnic_allocator: VnicAllocator<Etherstub>,

    // Reference to the port manager for creating OPTE ports when starting the
    // instance
    port_manager: PortManager,

    // Guest NIC and OPTE port information
    requested_nics: Vec<NetworkInterface>,
    source_nat: SourceNatConfig,
    ephemeral_ip: Option<IpAddr>,
    floating_ips: Vec<IpAddr>,
    firewall_rules: Vec<ResolvedVpcFirewallRule>,
    dhcp_config: DhcpCfg,

    // Disk related properties
    requested_disks: Vec<propolis_client::types::DiskRequest>,
    cloud_init_bytes: Option<String>,

    // Internal State management
    state: InstanceStates,
    running_state: Option<RunningState>,

    // Connection to Nexus
    nexus_client: NexusClient,

    // Storage resources
    storage: StorageHandle,

    // Used to create propolis zones
    zone_builder_factory: ZoneBuilderFactory,

    // Object used to collect zone bundles from this instance when terminated.
    zone_bundler: ZoneBundler,

    // Queue to notify the sled agent's metrics task about our VNICs.
    metrics_queue: MetricsRequestQueue,

    // Object representing membership in the "instance manager".
    instance_ticket: InstanceTicket,
}

impl InstanceRunner {
    async fn run(mut self) {
        use InstanceRequest::*;
        while !self.should_terminate {
            tokio::select! {
                biased;

                // Handle messages from our own "Monitor the VMM" task.
                request = self.rx_monitor.recv() => {
                    use InstanceMonitorRequest::*;
                    match request {
                        Some(Update { state, tx }) => {
                            let observed = ObservedPropolisState::new(&state);
                            let reaction = self.observe_state(&observed).await;
                            self.publish_state_to_nexus().await;

                            // NOTE: If we fail to send here, the
                            // InstanceMonitorRunner has stopped running for
                            // some reason. We'd presumably handle that on the
                            // next iteration of the loop.
                            if let Err(_) = tx.send(reaction) {
                                warn!(self.log, "InstanceRunner failed to send to InstanceMonitorRunner");
                            }
                        },
                        // NOTE: This case shouldn't really happen, as we keep a copy
                        // of the sender alive in "self.tx_monitor".
                        None => {
                            warn!(self.log, "Instance 'VMM monitor' channel closed; shutting down");
                            let mark_failed = true;
                            self.terminate(mark_failed).await;
                        },
                    }

                },
                // Handle external requests to act upon the instance.
                request = self.rx.recv() => {
                    let request_variant = request.as_ref().map(|r| r.to_string());
                    let result = match request {
                        Some(RequestZoneBundle { tx }) => {
                            tx.send(self.request_zone_bundle().await)
                                .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(GetFilesystemPool { tx } ) => {
                            tx.send(self.get_filesystem_zpool())
                                .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(CurrentState{ tx }) => {
                            tx.send(self.current_state())
                                .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(PutState{ state, tx }) => {
                             tx.send(self.put_state(state).await
                                .map(|r| VmmPutStateResponse { updated_runtime: Some(r) })
                                .map_err(|e| e.into()))
                                .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(Terminate { mark_failed, tx }) => {
                            tx.send(Ok(VmmUnregisterResponse {
                                updated_runtime: Some(self.terminate(mark_failed).await)
                            }))
                            .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(IssueSnapshotRequest { disk_id, snapshot_id, tx }) => {
                            tx.send(
                                self.issue_snapshot_request(
                                    disk_id,
                                    snapshot_id
                                ).await.map_err(|e| e.into())
                            )
                            .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(AddExternalIp { ip, tx }) => {
                            tx.send(self.add_external_ip(&ip).await.map_err(|e| e.into()))
                            .map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(DeleteExternalIp { ip, tx }) => {
                            tx.send(self.delete_external_ip(&ip).await.map_err(|e| e.into()))
                            .map_err(|_| Error::FailedSendClientClosed)
                        },
                        None => {
                            warn!(self.log, "Instance request channel closed; shutting down");
                            let mark_failed = false;
                            self.terminate(mark_failed).await;
                            break;
                        },
                    };

                    if let Err(err) = result {
                        warn!(
                            self.log,
                            "Error handling request";
                            "request" => request_variant.unwrap(),
                            "err" => ?err,

                        );
                    }
                }

            }
        }
        self.publish_state_to_nexus().await;

        // Okay, now that we've terminated the instance, drain any outstanding
        // requests in the queue, so that they see an error indicating that the
        // instance is going away.
        while let Some(request) = self.rx.recv().await {
            // If the receiver for this request has been dropped, ignore it
            // instead of bailing out, since we still need to drain the rest of
            // the queue,
            let _ = match request {
                RequestZoneBundle { tx } => tx
                    .send(Err(BundleError::InstanceTerminating))
                    .map_err(|_| ()),
                GetFilesystemPool { tx } => tx.send(None).map_err(|_| ()),
                CurrentState { tx } => {
                    tx.send(self.current_state()).map_err(|_| ())
                }
                PutState { tx, .. } => {
                    tx.send(Err(Error::Terminating.into())).map_err(|_| ())
                }
                Terminate { tx, .. } => {
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
            };
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
                            | nexus_client::Error::PreHookError(_) => {
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

    /// Processes a Propolis state change observed by the Propolis monitoring
    /// task.
    async fn observe_state(
        &mut self,
        state: &ObservedPropolisState,
    ) -> Reaction {
        info!(self.log, "Observing new propolis state: {:?}", state);

        // This instance may no longer have a Propolis zone if it was rudely
        // terminated between the time the call to Propolis returned and the
        // time this thread acquired the instance lock and entered this routine.
        // If the Propolis zone is gone, do not publish the Propolis state;
        // instead, maintain whatever state was published when the zone was
        // destroyed.
        if self.running_state.is_none() {
            info!(
                self.log,
                "Ignoring new propolis state: Propolis is already destroyed"
            );

            // Return the Terminate action so that the caller will cleanly
            // cease to monitor this Propolis. Note that terminating an instance
            // that's already terminated is a no-op.
            return Reaction::Terminate;
        }

        // Update the Sled Agent's internal state machine.
        let action = self.state.apply_propolis_observation(state);
        info!(
            self.log,
            "updated state after observing Propolis state change";
            "propolis_id" => %self.propolis_id,
            "new_vmm_state" => ?self.state.vmm()
        );

        // If the zone is now safe to terminate, tear it down and discard the
        // instance ticket before returning and publishing the new instance
        // state to Nexus. This ensures that the instance is actually gone from
        // the sled when Nexus receives the state update saying it's actually
        // destroyed.
        match action {
            Some(InstanceAction::Destroy) => {
                info!(self.log, "terminating VMM that has exited";
                      "instance_id" => %self.instance_id(),
                      "propolis_id" => %self.propolis_id);
                let mark_failed = false;
                self.terminate(mark_failed).await;
                Reaction::Terminate
            }
            None => Reaction::Continue,
        }
    }

    /// Sends an instance state PUT request to this instance's Propolis.
    async fn propolis_state_put(
        &self,
        request: propolis_client::types::InstanceStateRequested,
    ) -> Result<(), Error> {
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

        res?;
        Ok(())
    }

    /// Sends an instance ensure request to this instance's Propolis.
    async fn propolis_ensure_inner(
        &self,
        client: &PropolisClient,
        running_zone: &RunningZone,
        migrate: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        let nics = running_zone
            .opte_ports()
            .map(|port| propolis_client::types::NetworkInterfaceRequest {
                name: port.name().to_string(),
                slot: propolis_client::types::Slot(port.slot()),
            })
            .collect();

        let migrate = match migrate {
            Some(params) => {
                let migration_id = self.state
                    .migration_in()
                    // TODO(eliza): This is a bit of an unfortunate dance: the
                    // initial instance-ensure-registered request is what sends
                    // the migration ID, but it's the subsequent
                    // instance-ensure-state request (which we're handling here)
                    // that includes migration the source VMM's UUID and IP
                    // address. Because the API currently splits the migration
                    // IDs between the instance-ensure-registered and
                    // instance-ensure-state requests, we have to stash the
                    // migration ID in an `Option` and `expect()` it here,
                    // panicking if we get an instance-ensure-state request with
                    // a source Propolis ID if the instance wasn't registered
                    // with a migration in ID.
                    //
                    // This is kind of a shame. Eventually, we should consider
                    // reworking the API ensure-state request contains the
                    // migration ID, and we don't have to unwrap here. See:
                    // https://github.com/oxidecomputer/omicron/issues/6073
                    .expect("if we have migration target params, we should also have a migration in")
                    .migration_id;
                Some(propolis_client::types::InstanceMigrateInitiateRequest {
                    src_addr: params.src_propolis_addr.to_string(),
                    src_uuid: params.src_propolis_id,
                    migration_id,
                })
            }
            None => None,
        };

        let request = propolis_client::types::InstanceEnsureRequest {
            properties: self.properties.clone(),
            nics,
            disks: self
                .requested_disks
                .iter()
                .cloned()
                .map(Into::into)
                .collect(),
            migrate,
            cloud_init_bytes: self.cloud_init_bytes.clone(),
        };

        info!(self.log, "Sending ensure request to propolis: {:?}", request);
        let result = client.instance_ensure().body(request).send().await;
        info!(self.log, "result of instance_ensure call is {:?}", result);
        result?;
        Ok(())
    }

    /// Given a freshly-created Propolis process, sends an ensure request to
    /// that Propolis and launches all of the tasks needed to monitor the
    /// resulting Propolis VM.
    ///
    /// # Panics
    ///
    /// Panics if this routine is called more than once for a given Instance.
    async fn ensure_propolis_and_tasks(
        &mut self,
        setup: PropolisSetup,
        migrate: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        assert!(self.running_state.is_none());

        let PropolisSetup { client, running_zone } = setup;
        self.propolis_ensure_inner(&client, &running_zone, migrate).await?;

        // Monitor propolis for state changes in the background.
        //
        // This task exits after its associated Propolis has been terminated
        // (either because the task observed a message from Propolis saying that
        // it exited or because the Propolis server was terminated by other
        // means).
        let runner = InstanceMonitorRunner {
            client: client.clone(),
            tx_monitor: self.tx_monitor.clone(),
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

        Ok(())
    }

    /// Immediately terminates this instance's Propolis zone and cleans up any
    /// runtime objects associated with the instance.
    ///
    /// This routine is safe to call even if the instance's zone was never
    /// started. It is also safe to call multiple times on a single instance.
    async fn terminate_inner(&mut self) {
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

            // Ensure the instance is removed from the instance manager's table
            // so that a new instance can take its place.
            self.instance_ticket.deregister();
            return;
        };

        // Ask the sled-agent's metrics task to stop tracking statistics for our
        // control VNIC and any OPTE ports in the zone as well.
        self.metrics_queue
            .untrack_zone_links(&running_state.running_zone)
            .await;

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
        Zones::halt_and_remove_logged(&self.log, &zname).await.unwrap();

        // Remove ourselves from the instance manager's map of instances.
        self.instance_ticket.deregister();

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

        let Some(primary_nic) = self.requested_nics.get(0) else {
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

        let Some(primary_nic) = self.requested_nics.get(0) else {
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
}

/// Describes a single Propolis server that incarnates a specific instance.
pub struct Instance {
    id: InstanceUuid,

    tx: mpsc::Sender<InstanceRequest>,

    #[allow(dead_code)]
    runner_handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
pub(crate) struct InstanceInitialState {
    pub hardware: InstanceHardware,
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
            hardware,
            vmm_runtime,
            propolis_addr,
            migration_id,
        } = state;

        let InstanceManagerServices {
            nexus_client,
            vnic_allocator,
            port_manager,
            storage,
            zone_bundler,
            zone_builder_factory,
            metrics_queue,
        } = services;

        let mut dhcp_config = DhcpCfg {
            hostname: Some(
                hardware
                    .properties
                    .hostname
                    .as_str()
                    .parse()
                    .map_err(Error::InvalidHostname)?,
            ),
            host_domain: hardware
                .dhcp_config
                .host_domain
                .map(|domain| domain.parse())
                .transpose()
                .map_err(Error::InvalidHostname)?,
            domain_search_list: hardware
                .dhcp_config
                .search_domains
                .into_iter()
                .map(|domain| domain.parse())
                .collect::<Result<_, _>>()
                .map_err(Error::InvalidHostname)?,
            dns4_servers: Vec::new(),
            dns6_servers: Vec::new(),
        };
        for ip in hardware.dhcp_config.dns_servers {
            match ip {
                IpAddr::V4(ip) => dhcp_config.dns4_servers.push(ip.into()),
                IpAddr::V6(ip) => dhcp_config.dns6_servers.push(ip.into()),
            }
        }

        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let (tx_monitor, rx_monitor) = mpsc::channel(1);

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
            // NOTE: Mostly lies.
            properties: propolis_client::types::InstanceProperties {
                id: id.into_untyped_uuid(),
                name: hardware.properties.hostname.to_string(),
                description: "Test description".to_string(),
                image_id: Uuid::nil(),
                bootrom_id: Uuid::nil(),
                // TODO: Align the byte type w/propolis.
                memory: hardware.properties.memory.to_whole_mebibytes(),
                // TODO: we should probably make propolis aligned with
                // InstanceCpuCount here, to avoid any casting...
                vcpus: hardware.properties.ncpus.0 as u8,
                metadata,
            },
            propolis_id,
            propolis_addr,
            vnic_allocator,
            port_manager,
            requested_nics: hardware.nics,
            source_nat: hardware.source_nat,
            ephemeral_ip: hardware.ephemeral_ip,
            floating_ips: hardware.floating_ips,
            firewall_rules: hardware.firewall_rules,
            dhcp_config,
            requested_disks: hardware.disks,
            cloud_init_bytes: hardware.cloud_init_bytes,
            state: InstanceStates::new(vmm_runtime, migration_id),
            running_state: None,
            nexus_client,
            storage,
            zone_builder_factory,
            zone_bundler,
            metrics_queue,
            instance_ticket: ticket,
        };

        let runner_handle =
            tokio::task::spawn(async move { runner.run().await });

        Ok(Instance { id, tx, runner_handle })
    }

    pub fn id(&self) -> InstanceUuid {
        self.id
    }

    /// Create bundle from an instance zone.
    pub async fn request_zone_bundle(
        &self,
        tx: oneshot::Sender<Result<ZoneBundleMetadata, BundleError>>,
    ) -> Result<(), BundleError> {
        self.tx
            .send(InstanceRequest::RequestZoneBundle { tx })
            .await
            .map_err(|err| BundleError::FailedSend(anyhow!(err)))?;
        Ok(())
    }

    pub async fn get_filesystem_zpool(
        &self,
    ) -> Result<Option<ZpoolName>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(InstanceRequest::GetFilesystemPool { tx })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(rx.await?)
    }

    pub async fn current_state(&self) -> Result<SledVmmState, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(InstanceRequest::CurrentState { tx })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(rx.await?)
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
    pub async fn put_state(
        &self,
        tx: oneshot::Sender<Result<VmmPutStateResponse, ManagerError>>,
        state: VmmStateRequested,
    ) -> Result<(), Error> {
        self.tx
            .send(InstanceRequest::PutState { state, tx })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(())
    }

    /// Rudely terminates this instance's Propolis (if it has one) and
    /// immediately transitions the instance to the Destroyed state.
    pub async fn terminate(
        &self,
        tx: oneshot::Sender<Result<VmmUnregisterResponse, ManagerError>>,
        mark_failed: bool,
    ) -> Result<(), Error> {
        self.tx
            .send(InstanceRequest::Terminate { mark_failed, tx })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(())
    }

    pub async fn issue_snapshot_request(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.tx
            .send(InstanceRequest::IssueSnapshotRequest {
                disk_id,
                snapshot_id,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(())
    }

    pub async fn add_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.tx
            .send(InstanceRequest::AddExternalIp { ip: *ip, tx })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(())
    }

    pub async fn delete_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), ManagerError>>,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.tx
            .send(InstanceRequest::DeleteExternalIp { ip: *ip, tx })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        Ok(())
    }
}

// TODO: Move this implementation higher. I'm just keeping it here to make the
// incremental diff smaller.
impl InstanceRunner {
    async fn request_zone_bundle(
        &self,
    ) -> Result<ZoneBundleMetadata, BundleError> {
        let name = propolis_zone_name(&self.propolis_id);
        match &self.running_state {
            None => Err(BundleError::Unavailable { name }),
            Some(RunningState { ref running_zone, .. }) => {
                self.zone_bundler
                    .create(running_zone, ZoneBundleCause::ExplicitRequest)
                    .await
            }
        }
    }

    fn get_filesystem_zpool(&self) -> Option<ZpoolName> {
        let Some(run_state) = &self.running_state else {
            return None;
        };
        run_state.running_zone.root_zpool().map(|p| p.clone())
    }

    fn current_state(&self) -> SledVmmState {
        self.state.sled_instance_state()
    }

    /// Ensures that a Propolis process exists for this instance, then sends it
    /// an instance ensure request.
    async fn propolis_ensure(
        &mut self,
        migration_params: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        if let Some(running_state) = self.running_state.as_ref() {
            info!(
                &self.log,
                "Ensuring instance which already has a running state"
            );
            self.propolis_ensure_inner(
                &running_state.client,
                &running_state.running_zone,
                migration_params,
            )
            .await?;
        } else {
            let setup_result: Result<(), Error> = 'setup: {
                // Set up the Propolis zone and the objects associated with it.
                let setup = match self.setup_propolis_inner().await {
                    Ok(setup) => setup,
                    Err(e) => break 'setup Err(e),
                };

                // Direct the Propolis server to create its VM and the tasks
                // associated with it. On success, the zone handle moves into
                // this instance, preserving the zone.
                self.ensure_propolis_and_tasks(setup, migration_params).await
            };

            // If this instance started from scratch, and startup failed, move
            // the instance to the Failed state instead of leaking the Starting
            // state.
            //
            // Once again, migration targets don't do this, because a failure to
            // start a migration target simply leaves the VM running untouched
            // on the source.
            if migration_params.is_none() && setup_result.is_err() {
                error!(&self.log, "vmm setup failed: {:?}", setup_result);

                // This case is morally equivalent to starting Propolis and then
                // rudely terminating it before asking it to do anything. Update
                // the VMM and instance states accordingly.
                let mark_failed = false;
                self.state.terminate_rudely(mark_failed);
            }
            setup_result?;
        }
        Ok(())
    }

    async fn put_state(
        &mut self,
        state: VmmStateRequested,
    ) -> Result<SledVmmState, Error> {
        use propolis_client::types::InstanceStateRequested as PropolisRequest;
        let (propolis_state, next_published) = match state {
            VmmStateRequested::MigrationTarget(migration_params) => {
                self.propolis_ensure(Some(migration_params)).await?;
                (None, None)
            }
            VmmStateRequested::Running => {
                self.propolis_ensure(None).await?;
                (Some(PropolisRequest::Run), None)
            }
            VmmStateRequested::Stopped => {
                // If the instance has not started yet, unregister it
                // immediately. Since there is no Propolis to push updates when
                // this happens, generate an instance record bearing the
                // "Destroyed" state and return it to the caller.
                if self.running_state.is_none() {
                    let mark_failed = false;
                    self.terminate(mark_failed).await;
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

        if let Some(p) = propolis_state {
            self.propolis_state_put(p).await?;
        }
        if let Some(s) = next_published {
            self.state.transition_vmm(s, Utc::now());
        }
        Ok(self.state.sled_instance_state())
    }

    async fn setup_propolis_inner(&mut self) -> Result<PropolisSetup, Error> {
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
                is_service: false,
            })?;
            opte_port_names.push(port.0.name().to_string());
            opte_ports.push(port);
        }

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(&self.propolis_id);
        let mut rng = rand::rngs::StdRng::from_entropy();
        let latest_disks = self
            .storage
            .get_latest_disks()
            .await
            .all_u2_mountpoints(ZONE_DATASET);

        let root = latest_disks
            .into_iter()
            .choose(&mut rng)
            .ok_or_else(|| Error::U2NotFound)?;
        let installed_zone = self
            .zone_builder_factory
            .builder()
            .with_log(self.log.clone())
            .with_underlay_vnic_allocator(&self.vnic_allocator)
            .with_zone_root_path(root)
            .with_zone_image_paths(&["/opt/oxide".into()])
            .with_zone_type("propolis-server")
            .with_unique_name(self.propolis_id.into_untyped_uuid())
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
        wait_for_service(Some(&zname), &fmri, self.log.clone())
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;
        info!(self.log, "Propolis SMF service is online");

        // Notify the metrics task about the instance zone's datalinks.
        if !self.metrics_queue.track_zone_links(&running_zone).await {
            error!(
                self.log,
                "Failed to track one or more datalinks in the zone, \
                some metrics will not be produced";
                "zone_name" => running_zone.name(),
            );
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

    async fn terminate(&mut self, mark_failed: bool) -> SledVmmState {
        self.terminate_inner().await;
        self.state.terminate_rudely(mark_failed);

        // This causes the "run" task to exit on the next iteration.
        self.should_terminate = true;
        self.state.sled_instance_state()
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
    use illumos_utils::dladm::MockDladm;
    use illumos_utils::dladm::__mock_MockDladm::__create_vnic::Context as MockDladmCreateVnicContext;
    use illumos_utils::dladm::__mock_MockDladm::__delete_vnic::Context as MockDladmDeleteVnicContext;
    use illumos_utils::svc::__wait_for_service::Context as MockWaitForServiceContext;
    use illumos_utils::zone::MockZones;
    use illumos_utils::zone::__mock_MockZones::__boot::Context as MockZonesBootContext;
    use illumos_utils::zone::__mock_MockZones::__id::Context as MockZonesIdContext;
    use internal_dns::resolver::Resolver;
    use omicron_common::api::external::{
        ByteCount, Generation, Hostname, InstanceCpuCount,
    };
    use omicron_common::api::internal::nexus::{
        InstanceProperties, InstanceRuntimeState, VmmState,
    };
    use omicron_common::api::internal::shared::{DhcpConfig, SledIdentifiers};
    use omicron_common::FileKv;
    use sled_agent_types::zone_bundle::CleanupContext;
    use sled_storage::manager_test_harness::StorageManagerTestHarness;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
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

    #[derive(Default, Clone)]
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

    fn mock_vnic_contexts(
    ) -> (MockDladmCreateVnicContext, MockDladmDeleteVnicContext) {
        let create_vnic_ctx = MockDladm::create_vnic_context();
        let delete_vnic_ctx = MockDladm::delete_vnic_context();
        create_vnic_ctx.expect().return_once(
            |physical_link: &Etherstub, _, _, _, _| {
                assert_eq!(&physical_link.0, "mystub");
                Ok(())
            },
        );
        delete_vnic_ctx.expect().returning(|_| Ok(()));
        (create_vnic_ctx, delete_vnic_ctx)
    }

    // InstanceManager::ensure_state calls Instance::put_state(Running),
    //  which calls Instance::propolis_ensure,
    //   which spawns Instance::monitor_state_task,
    //    which calls cpapi_instances_put
    //   and calls Instance::setup_propolis_inner,
    //    which creates the zone (which isn't real in these tests, of course)
    fn mock_zone_contexts(
    ) -> (MockZonesBootContext, MockWaitForServiceContext, MockZonesIdContext)
    {
        let boot_ctx = MockZones::boot_context();
        boot_ctx.expect().return_once(|_| Ok(()));
        let wait_ctx = illumos_utils::svc::wait_for_service_context();
        wait_ctx.expect().times(..).returning(|_, _, _| Ok(()));
        let zone_id_ctx = MockZones::id_context();
        zone_id_ctx.expect().times(..).returning(|_| Ok(Some(1)));
        (boot_ctx, wait_ctx, zone_id_ctx)
    }

    // note the "mock" here is different from the vnic/zone contexts above.
    // this is actually running code for a dropshot server from propolis.
    // (might we want a locally-defined fake whose behavior we can control
    // more directly from the test driver?)
    // TODO: factor out, this is also in sled-agent-sim.
    fn propolis_mock_server(
        log: &Logger,
    ) -> (HttpServer<Arc<propolis_mock_server::Context>>, PropolisClient) {
        let propolis_bind_address =
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0); // allocate port
        let dropshot_config = dropshot::ConfigDropshot {
            bind_address: propolis_bind_address,
            ..Default::default()
        };
        let propolis_log = log.new(o!("component" => "propolis-server-mock"));
        let private =
            Arc::new(propolis_mock_server::Context::new(propolis_log));
        info!(log, "Starting mock propolis-server...");
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let mock_api = propolis_mock_server::api();

        let srv = dropshot::HttpServerStarter::new(
            &dropshot_config,
            mock_api,
            private,
            &dropshot_log,
        )
        .expect("couldn't create mock propolis-server")
        .start();

        let client = propolis_client::Client::new(&format!(
            "http://{}",
            srv.local_addr()
        ));

        (srv, client)
    }

    async fn setup_storage_manager(log: &Logger) -> StorageManagerTestHarness {
        let mut harness = StorageManagerTestHarness::new(log).await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        harness.handle().key_manager_ready().await;
        let config = harness.make_config(1, &raw_disks);
        let _ = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        harness
    }

    async fn instance_struct(
        log: &Logger,
        propolis_addr: SocketAddr,
        nexus_client: NexusClient,
        storage_handle: StorageHandle,
        temp_dir: &String,
    ) -> (Instance, MetricsRx) {
        let id = InstanceUuid::new_v4();
        let propolis_id = PropolisUuid::from_untyped_uuid(PROPOLIS_ID);

        let ticket = InstanceTicket::new_without_manager_for_test(propolis_id);

        let initial_state = fake_instance_initial_state(propolis_addr);

        let (services, rx) = fake_instance_manager_services(
            log,
            storage_handle,
            nexus_client,
            temp_dir,
        );

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
        let hardware = InstanceHardware {
            properties: InstanceProperties {
                ncpus: InstanceCpuCount(1),
                memory: ByteCount::from_gibibytes_u32(1),
                hostname: Hostname::from_str("bert").unwrap(),
            },
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
            disks: vec![],
            cloud_init_bytes: None,
        };

        InstanceInitialState {
            hardware,
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

    fn fake_instance_manager_services(
        log: &Logger,
        storage_handle: StorageHandle,
        nexus_client: NexusClient,
        temp_dir: &String,
    ) -> (InstanceManagerServices, MetricsRx) {
        let vnic_allocator =
            VnicAllocator::new("Foo", Etherstub("mystub".to_string()));
        let port_manager = PortManager::new(
            log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );

        let cleanup_context = CleanupContext::default();
        let zone_bundler = ZoneBundler::new(
            log.new(o!("component" => "ZoneBundler")),
            storage_handle.clone(),
            cleanup_context,
        );

        let (metrics_queue, rx) = MetricsRequestQueue::for_test();
        let services = InstanceManagerServices {
            nexus_client,
            vnic_allocator,
            port_manager,
            storage: storage_handle,
            zone_bundler,
            zone_builder_factory: ZoneBuilderFactory::fake(Some(temp_dir)),
            metrics_queue,
        };
        (services, rx)
    }

    #[tokio::test]
    async fn test_instance_create_events_normal() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_create_events_normal",
        );
        let log = logctx.log.new(o!(FileKv));

        let (propolis_server, _propolis_client) = propolis_mock_server(&log);
        let propolis_addr = propolis_server.local_addr();

        // automock'd things used during this test
        let _mock_vnic_contexts = mock_vnic_contexts();
        let _mock_zone_contexts = mock_zone_contexts();

        let FakeNexusParts {
            nexus_client,
            mut state_rx,
            _dns_server,
            _nexus_server,
        } = FakeNexusParts::new(&log).await;

        let mut storage_harness = setup_storage_manager(&log).await;
        let storage_handle = storage_harness.handle().clone();

        let temp_guard = Utf8TempDir::new().unwrap();
        let temp_dir = temp_guard.path().to_string();

        let (inst, mut metrics_rx) = timeout(
            TIMEOUT_DURATION,
            instance_struct(
                &log,
                propolis_addr,
                nexus_client,
                storage_handle,
                &temp_dir,
            ),
        )
        .await
        .expect("timed out creating Instance struct");

        let (put_tx, put_rx) = oneshot::channel();

        // pretending we're InstanceManager::ensure_state, start our "instance"
        // (backed by fakes and propolis_mock_server)
        inst.put_state(put_tx, VmmStateRequested::Running)
            .await
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
                name: "oxControlFoo0".into(),
            },
            "Expected instance zone to send a message on its metrics \
            request queue, asking to track its control VNIC",
        );
        metrics_rx
            .try_recv()
            .expect_err("The metrics request queue should have one message");

        storage_harness.cleanup().await;
        logctx.cleanup_successful();
    }

    // tests around dropshot request timeouts during the blocking propolis setup
    #[tokio::test]
    async fn test_instance_create_timeout_while_starting_propolis() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_create_timeout_while_starting_propolis",
        );
        let log = logctx.log.new(o!(FileKv));

        // automock'd things used during this test
        let _mock_vnic_contexts = mock_vnic_contexts();
        let _mock_zone_contexts = mock_zone_contexts();

        let FakeNexusParts {
            nexus_client,
            state_rx,
            _dns_server,
            _nexus_server,
        } = FakeNexusParts::new(&log).await;

        let mut storage_harness = setup_storage_manager(&logctx.log).await;
        let storage_handle = storage_harness.handle().clone();

        let temp_guard = Utf8TempDir::new().unwrap();
        let temp_dir = temp_guard.path().to_string();

        let (inst, _) = timeout(
            TIMEOUT_DURATION,
            instance_struct(
                &log,
                // we want to test propolis not ever coming up
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0, 0)),
                nexus_client,
                storage_handle,
                &temp_dir,
            ),
        )
        .await
        .expect("timed out creating Instance struct");

        let (put_tx, put_rx) = oneshot::channel();

        tokio::time::pause();

        // pretending we're InstanceManager::ensure_state, try in vain to start
        // our "instance", but no propolis server is running
        inst.put_state(put_tx, VmmStateRequested::Running)
            .await
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
            panic!("Nexus's InstanceState should never have reached running if zone creation timed out");
        }

        storage_harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_instance_create_timeout_while_creating_zone() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_create_timeout_while_creating_zone",
        );
        let log = logctx.log.new(o!(FileKv));

        // automock'd things used during this test
        let _mock_vnic_contexts = mock_vnic_contexts();

        // time out while booting zone, on purpose!
        let boot_ctx = MockZones::boot_context();
        const TIMEOUT: Duration = Duration::from_secs(1);
        let (boot_continued_tx, boot_continued_rx) =
            std::sync::mpsc::sync_channel(1);
        let boot_log = log.clone();
        boot_ctx.expect().times(1).return_once(move |_| {
            // We need a way to slow down zone boot, but that doesn't block the
            // entire Tokio runtime. Since this closure is synchronous, it also
            // has no way to await anything, all waits are blocking. That means
            // we cannot use a single-threaded runtime, which also means no
            // manually advancing time. The test has to take the full "slow boot
            // time".
            //
            // To do this, we use a multi-threaded runtime, and call
            // block_in_place so that we can just literally sleep for a while.
            // The sleep duration here is twice a timeout we set on the attempt
            // to actually set the instance running below.
            //
            // This boot method also directly signals the main test code to
            // continue when it's done sleeping to synchronize with it.
            tokio::task::block_in_place(move || {
                debug!(
                    boot_log,
                    "MockZones::boot() called, waiting for timeout"
                );
                std::thread::sleep(TIMEOUT * 2);
                debug!(
                    boot_log,
                    "MockZones::boot() waited for timeout, continuing"
                );
                boot_continued_tx.send(()).unwrap();
            });
            Ok(())
        });
        let wait_ctx = illumos_utils::svc::wait_for_service_context();
        wait_ctx.expect().times(..).returning(|_, _, _| Ok(()));
        let zone_id_ctx = MockZones::id_context();
        zone_id_ctx.expect().times(..).returning(|_| Ok(Some(1)));

        let FakeNexusParts {
            nexus_client,
            state_rx,
            _dns_server,
            _nexus_server,
        } = FakeNexusParts::new(&log).await;

        let mut storage_harness = setup_storage_manager(&logctx.log).await;
        let storage_handle = storage_harness.handle().clone();

        let temp_guard = Utf8TempDir::new().unwrap();
        let temp_dir = temp_guard.path().to_string();

        let (inst, _) = timeout(
            TIMEOUT_DURATION,
            instance_struct(
                &log,
                // isn't running because the "zone" never "boots"
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0, 0)),
                nexus_client,
                storage_handle,
                &temp_dir,
            ),
        )
        .await
        .expect("timed out creating Instance struct");

        let (put_tx, put_rx) = oneshot::channel();

        // pretending we're InstanceManager::ensure_state, try in vain to start
        // our "instance", but the zone never finishes installing
        inst.put_state(put_tx, VmmStateRequested::Running)
            .await
            .expect("failed to send Instance::put_state");

        // Timeout our future waiting for the instance-state-change at 1s. This
        // is much shorter than the actual `TIMEOUT_DURATION`, but the test
        // structure requires that we actually wait this period, since we cannot
        // advance time manually in a multi-threaded runtime.
        let timeout_fut = timeout(TIMEOUT, put_rx);
        debug!(log, "Awaiting zone-boot timeout");
        timeout_fut
            .await
            .expect_err("*should've* timed out waiting for Instance::put_state, but didn't?");
        debug!(log, "Zone-boot timeout awaited");

        if let ReceivedInstanceState::InstancePut(SledVmmState {
            vmm_state: VmmRuntimeState { state: VmmState::Running, .. },
            ..
        }) = state_rx.borrow().to_owned()
        {
            panic!("Nexus's InstanceState should never have reached running if zone creation timed out");
        }

        // Notify the "boot" closure that it can continue, and then wait to
        // ensure it's actually called.
        debug!(log, "Waiting for zone-boot to continue");
        tokio::task::spawn_blocking(move || boot_continued_rx.recv().unwrap())
            .await
            .unwrap();
        debug!(log, "Received continued message from MockZones::boot()");

        storage_harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_manager_creation() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_instance_manager_creation",
        );
        let log = logctx.log.new(o!(FileKv));

        // automock'd things used during this test
        let _mock_vnic_contexts = mock_vnic_contexts();
        let _mock_zone_contexts = mock_zone_contexts();

        let mut storage_harness = setup_storage_manager(&logctx.log).await;
        let storage_handle = storage_harness.handle().clone();

        let FakeNexusParts {
            nexus_client,
            mut state_rx,
            _dns_server,
            _nexus_server,
        } = FakeNexusParts::new(&log).await;

        let temp_guard = Utf8TempDir::new().unwrap();
        let temp_dir = temp_guard.path().to_string();

        let (services, mut metrics_rx) = fake_instance_manager_services(
            &log,
            storage_handle,
            nexus_client,
            &temp_dir,
        );
        let InstanceManagerServices {
            nexus_client,
            vnic_allocator: _,
            port_manager,
            storage,
            zone_bundler,
            zone_builder_factory,
            metrics_queue,
        } = services;

        let etherstub = Etherstub("mystub".to_string());

        let vmm_reservoir_manager = VmmReservoirManagerHandle::stub_for_test();

        let mgr = crate::instance_manager::InstanceManager::new(
            logctx.log.new(o!("component" => "InstanceManager")),
            nexus_client,
            etherstub,
            port_manager,
            storage,
            zone_bundler,
            zone_builder_factory,
            vmm_reservoir_manager,
            metrics_queue,
        )
        .unwrap();

        let (propolis_server, _propolis_client) =
            propolis_mock_server(&logctx.log);
        let propolis_addr = propolis_server.local_addr();

        let instance_id = InstanceUuid::new_v4();
        let propolis_id = PropolisUuid::from_untyped_uuid(PROPOLIS_ID);
        let InstanceInitialState {
            hardware,
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

        let instance_runtime = InstanceRuntimeState {
            propolis_id: Some(propolis_id),
            dst_propolis_id: None,
            migration_id: None,
            gen: Generation::new(),
            time_updated: Default::default(),
        };

        mgr.ensure_registered(
            instance_id,
            propolis_id,
            hardware,
            instance_runtime,
            vmm_runtime,
            propolis_addr,
            sled_identifiers,
            metadata,
        )
        .await
        .unwrap();

        mgr.ensure_state(propolis_id, VmmStateRequested::Running)
            .await
            .unwrap();

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
        metrics_rx
            .try_recv()
            .expect_err("The metrics request queue should have one message");

        storage_harness.cleanup().await;
        logctx.cleanup_successful();
    }
}
