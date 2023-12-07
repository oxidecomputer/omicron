// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::instance::{
    Action as InstanceAction, InstanceStates, ObservedPropolisState,
    PublishedVmmState,
};
use crate::instance_manager::{InstanceManagerServices, InstanceTicket};
use crate::nexus::NexusClientWithResolver;
use crate::params::ZoneBundleCause;
use crate::params::ZoneBundleMetadata;
use crate::params::{
    InstanceHardware, InstanceMigrationSourceParams,
    InstanceMigrationTargetParams, InstanceStateRequested, VpcFirewallRule,
};
use crate::profile::*;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use anyhow::anyhow;
use backoff::BackoffError;
use chrono::Utc;
use futures::lock::{Mutex, MutexGuard};
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::{DhcpCfg, PortManager};
use illumos_utils::running_zone::{RunningZone, ZoneBuilderFactory};
use illumos_utils::svc::wait_for_service;
use illumos_utils::zone::Zones;
use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::api::internal::nexus::{
    InstanceRuntimeState, SledInstanceState, VmmRuntimeState,
};
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use omicron_common::backoff;
use propolis_client::Client as PropolisClient;
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use sled_storage::dataset::ZONE_DATASET;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::net::IpAddr;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

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

    #[error("Instance {0} not running!")]
    InstanceNotRunning(Uuid),

    #[error("Instance already registered with Propolis ID {0}")]
    InstanceAlreadyRegistered(Uuid),

    #[error("No U.2 devices found")]
    U2NotFound,

    #[error("I/O error")]
    Io(#[from] std::io::Error),
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
pub fn propolis_zone_name(id: &Uuid) -> String {
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

struct InstanceInner {
    log: Logger,

    // Properties visible to Propolis
    properties: propolis_client::types::InstanceProperties,

    // The ID of the Propolis server (and zone) running this instance
    propolis_id: Uuid,

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
    firewall_rules: Vec<VpcFirewallRule>,
    dhcp_config: DhcpCfg,

    // Disk related properties
    requested_disks: Vec<propolis_client::types::DiskRequest>,
    cloud_init_bytes: Option<String>,

    // Internal State management
    state: InstanceStates,
    running_state: Option<RunningState>,

    // Connection to Nexus
    nexus_client: NexusClientWithResolver,

    // Storage resources
    storage: StorageHandle,

    // Used to create propolis zones
    zone_builder_factory: ZoneBuilderFactory,

    // Object used to collect zone bundles from this instance when terminated.
    zone_bundler: ZoneBundler,

    // Object representing membership in the "instance manager".
    instance_ticket: InstanceTicket,
}

impl InstanceInner {
    /// Yields this instance's ID.
    fn id(&self) -> &Uuid {
        &self.properties.id
    }

    /// Yields this instance's Propolis's ID.
    fn propolis_id(&self) -> &Uuid {
        &self.propolis_id
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
                    "instance_id" => %self.id(),
                    "state" => ?state,
                );

                self.nexus_client
                    .client()
                    .cpapi_instances_put(self.id(), &state.into())
                    .await
                    .map_err(|err| -> backoff::BackoffError<Error> {
                        match &err {
                            nexus_client::Error::CommunicationError(_) => {
                                BackoffError::transient(Error::Notification(
                                    err,
                                ))
                            }
                            nexus_client::Error::InvalidRequest(_)
                            | nexus_client::Error::InvalidResponsePayload(_)
                            | nexus_client::Error::UnexpectedResponse(_) => {
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
                      "instance_id" => %self.id(),
                      "retry_after" => ?delay);
            },
        )
        .await;

        if let Err(e) = result {
            error!(
                self.log,
                "Failed to publish state to Nexus, will not retry: {:?}", e;
                "instance_id" => %self.id()
            );
        }
    }

    /// Processes a Propolis state change observed by the Propolis monitoring
    /// task.
    async fn observe_state(
        &mut self,
        state: &ObservedPropolisState,
    ) -> Result<Reaction, Error> {
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
            return Ok(Reaction::Terminate);
        }

        // Update the Sled Agent's internal state machine.
        let action = self.state.apply_propolis_observation(state);
        info!(
            self.log,
            "updated state after observing Propolis state change";
            "propolis_id" => %self.state.propolis_id(),
            "new_instance_state" => ?self.state.instance(),
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
                      "instance_id" => %self.id());

                self.terminate().await?;
                Ok(Reaction::Terminate)
            }
            None => Ok(Reaction::Continue),
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
    async fn propolis_ensure(
        &self,
        client: &PropolisClient,
        running_zone: &RunningZone,
        migrate: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        let nics = running_zone
            .opte_ports()
            .map(|port| propolis_client::types::NetworkInterfaceRequest {
                // TODO-correctness: Remove `.vnic()` call when we use the port
                // directly.
                name: port.vnic_name().to_string(),
                slot: propolis_client::types::Slot(port.slot()),
            })
            .collect();

        let migrate = match migrate {
            Some(params) => {
                let migration_id =
                    self.state.instance().migration_id.ok_or_else(|| {
                        Error::Migration(anyhow!("Missing Migration UUID"))
                    })?;
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
        instance: Instance,
        setup: PropolisSetup,
        migrate: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        assert!(self.running_state.is_none());

        let PropolisSetup { client, running_zone } = setup;
        self.propolis_ensure(&client, &running_zone, migrate).await?;

        // Monitor propolis for state changes in the background.
        //
        // This task exits after its associated Propolis has been terminated
        // (either because the task observed a message from Propolis saying that
        // it exited or because the Propolis server was terminated by other
        // means).
        let monitor_client = client.clone();
        let _monitor_task = tokio::task::spawn(async move {
            let r = instance.monitor_state_task(monitor_client).await;
            let log = &instance.inner.lock().await.log;
            match r {
                Err(e) => warn!(log, "State monitoring task failed: {}", e),
                Ok(()) => info!(log, "State monitoring task complete"),
            }
        });

        self.running_state = Some(RunningState { client, running_zone });

        Ok(())
    }

    /// Immediately terminates this instance's Propolis zone and cleans up any
    /// runtime objects associated with the instance.
    ///
    /// This routine is safe to call even if the instance's zone was never
    /// started. It is also safe to call multiple times on a single instance.
    async fn terminate(&mut self) -> Result<(), Error> {
        let zname = propolis_zone_name(self.propolis_id());

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
            self.instance_ticket.terminate();
            return Ok(());
        };

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
        self.instance_ticket.terminate();

        // See if there are any runtime objects to clean up.
        //
        // We already removed the zone above but mark it as stopped
        running_state.running_zone.stop().await.unwrap();

        // Remove any OPTE ports from the port manager.
        running_state.running_zone.release_opte_ports();

        Ok(())
    }
}

/// A reference to a single instance running a running Propolis server.
///
/// Cloning this object clones the reference - it does not create another
/// instance.
#[derive(Clone)]
pub struct Instance {
    inner: Arc<Mutex<InstanceInner>>,
}

#[derive(Debug)]
pub(crate) struct InstanceInitialState {
    pub hardware: InstanceHardware,
    pub instance_runtime: InstanceRuntimeState,
    pub vmm_runtime: VmmRuntimeState,
    pub propolis_addr: SocketAddr,
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
    pub(crate) fn new(
        log: Logger,
        id: Uuid,
        propolis_id: Uuid,
        ticket: InstanceTicket,
        state: InstanceInitialState,
        services: InstanceManagerServices,
    ) -> Result<Self, Error> {
        info!(log, "initializing new Instance";
              "instance_id" => %id,
              "propolis_id" => %propolis_id,
              "state" => ?state);

        let InstanceInitialState {
            hardware,
            instance_runtime,
            vmm_runtime,
            propolis_addr,
        } = state;

        let InstanceManagerServices {
            nexus_client,
            vnic_allocator,
            port_manager,
            storage,
            zone_bundler,
            zone_builder_factory,
        } = services;

        let mut dhcp_config = DhcpCfg {
            hostname: Some(
                hardware
                    .properties
                    .hostname
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

        let instance = InstanceInner {
            log: log.new(o!("instance_id" => id.to_string())),
            // NOTE: Mostly lies.
            properties: propolis_client::types::InstanceProperties {
                id,
                name: hardware.properties.hostname.clone(),
                description: "Test description".to_string(),
                image_id: Uuid::nil(),
                bootrom_id: Uuid::nil(),
                // TODO: Align the byte type w/propolis.
                memory: hardware.properties.memory.to_whole_mebibytes(),
                // TODO: we should probably make propolis aligned with
                // InstanceCpuCount here, to avoid any casting...
                vcpus: hardware.properties.ncpus.0 as u8,
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
            state: InstanceStates::new(
                instance_runtime,
                vmm_runtime,
                propolis_id,
            ),
            running_state: None,
            nexus_client,
            storage,
            zone_builder_factory,
            zone_bundler,
            instance_ticket: ticket,
        };

        let inner = Arc::new(Mutex::new(instance));

        Ok(Instance { inner })
    }

    /// Create bundle from an instance zone.
    pub async fn request_zone_bundle(
        &self,
    ) -> Result<ZoneBundleMetadata, BundleError> {
        let inner = self.inner.lock().await;
        let name = propolis_zone_name(inner.propolis_id());
        match &*inner {
            InstanceInner { running_state: None, .. } => {
                Err(BundleError::Unavailable { name })
            }
            InstanceInner {
                running_state: Some(RunningState { ref running_zone, .. }),
                ..
            } => {
                inner
                    .zone_bundler
                    .create(running_zone, ZoneBundleCause::ExplicitRequest)
                    .await
            }
        }
    }

    pub async fn current_state(&self) -> SledInstanceState {
        let inner = self.inner.lock().await;
        inner.state.sled_instance_state()
    }

    /// Ensures that a Propolis process exists for this instance, then sends it
    /// an instance ensure request.
    async fn propolis_ensure(
        &self,
        inner: &mut MutexGuard<'_, InstanceInner>,
        migration_params: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        if let Some(running_state) = inner.running_state.as_ref() {
            info!(
                &inner.log,
                "Ensuring instance which already has a running state"
            );
            inner
                .propolis_ensure(
                    &running_state.client,
                    &running_state.running_zone,
                    migration_params,
                )
                .await?;
        } else {
            let setup_result: Result<(), Error> = 'setup: {
                // Set up the Propolis zone and the objects associated with it.
                let setup = match self.setup_propolis_locked(inner).await {
                    Ok(setup) => setup,
                    Err(e) => break 'setup Err(e),
                };

                // Direct the Propolis server to create its VM and the tasks
                // associated with it. On success, the zone handle moves into
                // this instance, preserving the zone.
                inner
                    .ensure_propolis_and_tasks(
                        self.clone(),
                        setup,
                        migration_params,
                    )
                    .await
            };

            // If this instance started from scratch, and startup failed, move
            // the instance to the Failed state instead of leaking the Starting
            // state.
            //
            // Once again, migration targets don't do this, because a failure to
            // start a migration target simply leaves the VM running untouched
            // on the source.
            if migration_params.is_none() && setup_result.is_err() {
                error!(&inner.log, "vmm setup failed: {:?}", setup_result);

                // This case is morally equivalent to starting Propolis and then
                // rudely terminating it before asking it to do anything. Update
                // the VMM and instance states accordingly.
                inner.state.terminate_rudely();
            }
            setup_result?;
        }
        Ok(())
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
        state: crate::params::InstanceStateRequested,
    ) -> Result<SledInstanceState, Error> {
        use propolis_client::types::InstanceStateRequested as PropolisRequest;
        let mut inner = self.inner.lock().await;
        let (propolis_state, next_published) = match state {
            InstanceStateRequested::MigrationTarget(migration_params) => {
                self.propolis_ensure(&mut inner, Some(migration_params))
                    .await?;
                (None, None)
            }
            InstanceStateRequested::Running => {
                self.propolis_ensure(&mut inner, None).await?;
                (Some(PropolisRequest::Run), None)
            }
            InstanceStateRequested::Stopped => {
                // If the instance has not started yet, unregister it
                // immediately. Since there is no Propolis to push updates when
                // this happens, generate an instance record bearing the
                // "Destroyed" state and return it to the caller.
                if inner.running_state.is_none() {
                    inner.terminate().await?;
                    inner.state.terminate_rudely();
                    (None, None)
                } else {
                    (
                        Some(PropolisRequest::Stop),
                        Some(PublishedVmmState::Stopping),
                    )
                }
            }
            InstanceStateRequested::Reboot => {
                if inner.running_state.is_none() {
                    return Err(Error::InstanceNotRunning(*inner.id()));
                }
                (
                    Some(PropolisRequest::Reboot),
                    Some(PublishedVmmState::Rebooting),
                )
            }
        };

        if let Some(p) = propolis_state {
            inner.propolis_state_put(p).await?;
        }
        if let Some(s) = next_published {
            inner.state.transition_vmm(s, Utc::now());
        }
        Ok(inner.state.sled_instance_state())
    }

    pub async fn put_migration_ids(
        &self,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        let mut inner = self.inner.lock().await;

        // Check that the instance's current generation matches the one the
        // caller expects to transition from. This helps Nexus ensure that if
        // multiple migration sagas launch at Propolis generation N, then only
        // one of them will successfully set the instance's migration IDs.
        if inner.state.instance().gen != old_runtime.gen {
            // Allow this transition for idempotency if the instance is
            // already in the requested goal state.
            if inner.state.migration_ids_already_set(old_runtime, migration_ids)
            {
                return Ok(inner.state.sled_instance_state());
            }

            return Err(Error::Transition(
                omicron_common::api::external::Error::conflict(format!(
                    "wrong instance state generation: expected {}, got {}",
                    inner.state.instance().gen,
                    old_runtime.gen
                )),
            ));
        }

        inner.state.set_migration_ids(migration_ids, Utc::now());
        Ok(inner.state.sled_instance_state())
    }

    async fn setup_propolis_locked(
        &self,
        inner: &mut MutexGuard<'_, InstanceInner>,
    ) -> Result<PropolisSetup, Error> {
        // Create OPTE ports for the instance
        let mut opte_ports = Vec::with_capacity(inner.requested_nics.len());
        for nic in inner.requested_nics.iter() {
            let (snat, ephemeral_ip, floating_ips) = if nic.primary {
                (
                    Some(inner.source_nat),
                    inner.ephemeral_ip,
                    &inner.floating_ips[..],
                )
            } else {
                (None, None, &[][..])
            };
            let port = inner.port_manager.create_port(
                nic,
                snat,
                ephemeral_ip,
                floating_ips,
                &inner.firewall_rules,
                inner.dhcp_config.clone(),
            )?;
            opte_ports.push(port);
        }

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(inner.propolis_id());
        let mut rng = rand::rngs::StdRng::from_entropy();
        let root = inner
            .storage
            .get_latest_resources()
            .await
            .all_u2_mountpoints(ZONE_DATASET)
            .choose(&mut rng)
            .ok_or_else(|| Error::U2NotFound)?
            .clone();
        let installed_zone = inner
            .zone_builder_factory
            .builder()
            .with_log(inner.log.clone())
            .with_underlay_vnic_allocator(&inner.vnic_allocator)
            .with_zone_root_path(&root)
            .with_zone_image_paths(&["/opt/oxide".into()])
            .with_zone_type("propolis-server")
            .with_unique_name(*inner.propolis_id())
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

        let gateway = inner.port_manager.underlay_ip();

        // TODO: We should not be using the resolver here to lookup the Nexus IP
        // address. It would be preferable for Propolis, and through Propolis,
        // Oximeter, to access the Nexus internal interface using a progenitor
        // resolver that relies on a DNS resolver.
        //
        // - With the current implementation: if Nexus' IP address changes, this
        // breaks.
        // - With a DNS resolver: the metric producer would be able to continue
        // sending requests to new servers as they arise.
        let metric_ip = inner
            .nexus_client
            .resolver()
            .lookup_ipv6(internal_dns::ServiceName::Nexus)
            .await?;
        let metric_addr = SocketAddr::V6(SocketAddrV6::new(
            metric_ip,
            NEXUS_INTERNAL_PORT,
            0,
            0,
        ));

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
                &inner.propolis_addr.ip().to_string(),
            )
            .add_property(
                "listen_port",
                "astring",
                &inner.propolis_addr.port().to_string(),
            )
            .add_property("metric_addr", "astring", &metric_addr.to_string());

        let profile = ProfileBuilder::new("omicron").add_service(
            ServiceBuilder::new("system/illumos/propolis-server").add_instance(
                ServiceInstanceBuilder::new("default")
                    .add_property_group(config),
            ),
        );
        profile.add_to_zone(&inner.log, &installed_zone).await?;

        let running_zone = RunningZone::boot(installed_zone).await?;
        info!(inner.log, "Started propolis in zone: {}", zname);

        // This isn't strictly necessary - we wait for the HTTP server below -
        // but it helps distinguish "online in SMF" from "responding to HTTP
        // requests".
        let fmri = fmri_name();
        wait_for_service(Some(&zname), &fmri, inner.log.clone())
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;
        info!(inner.log, "Propolis SMF service is online");

        // We use a custom client builder here because the default progenitor
        // one has a timeout of 15s but we want to be able to wait indefinitely.
        let reqwest_client = reqwest::ClientBuilder::new().build().unwrap();
        let client = Arc::new(PropolisClient::new_with_client(
            &format!("http://{}", &inner.propolis_addr),
            reqwest_client,
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&inner.log, &client).await?;
        info!(inner.log, "Propolis HTTP server online");

        Ok(PropolisSetup { client, running_zone })
    }

    /// Rudely terminates this instance's Propolis (if it has one) and
    /// immediately transitions the instance to the Destroyed state.
    pub async fn terminate(&self) -> Result<SledInstanceState, Error> {
        let mut inner = self.inner.lock().await;
        inner.terminate().await?;

        // Rude termination is safe here because this routine took the lock
        // before terminating the zone, which will cause any pending
        // observations from the instance state monitor to be
        inner.state.terminate_rudely();
        Ok(inner.state.sled_instance_state())
    }

    // Monitors propolis until explicitly told to disconnect.
    //
    // Intended to be spawned in a tokio task within [`Instance::start`].
    async fn monitor_state_task(
        &self,
        client: Arc<PropolisClient>,
    ) -> Result<(), Error> {
        let mut gen = 0;
        loop {
            // State monitoring always returns the most recent state/gen pair
            // known to Propolis.
            let response = client
                .instance_state_monitor()
                .body(propolis_client::types::InstanceStateMonitorRequest {
                    gen,
                })
                .send()
                .await?
                .into_inner();

            let reaction = {
                // The observed state depends on what Propolis reported and on
                // the `Instance`'s stored state. Take the instance lock to
                // stabilize that state across this entire operation.
                let mut inner = self.inner.lock().await;
                let observed = ObservedPropolisState::new(
                    inner.state.instance(),
                    &response,
                );
                let reaction = inner.observe_state(&observed).await?;
                inner.publish_state_to_nexus().await;
                reaction
            };

            if let Reaction::Terminate = reaction {
                return Ok(());
            }

            // Update the generation number we're asking for, to ensure the
            // Propolis will only return more recent values.
            gen = response.gen + 1;
        }
    }

    pub async fn issue_snapshot_request(
        &self,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let inner = self.inner.lock().await;

        if let Some(running_state) = &inner.running_state {
            running_state
                .client
                .instance_issue_crucible_snapshot_request()
                .id(disk_id)
                .snapshot_id(snapshot_id)
                .send()
                .await?;

            Ok(())
        } else {
            Err(Error::InstanceNotRunning(inner.properties.id))
        }
    }
}
