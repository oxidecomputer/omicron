// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::instance::{Action as InstanceAction, InstanceStates};
use crate::instance_manager::InstanceTicket;
use crate::nexus::LazyNexusClient;
use crate::params::{
    InstanceHardware, InstanceMigrationSourceParams,
    InstanceMigrationTargetParams, InstanceStateRequested, VpcFirewallRule,
};
use anyhow::anyhow;
use futures::lock::{Mutex, MutexGuard};
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::PortManager;
use illumos_utils::running_zone::{
    InstalledZone, RunCommandError, RunningZone,
};
use illumos_utils::svc::wait_for_service;
use illumos_utils::zfs::ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT;
use illumos_utils::zone::{AddressRequest, PROPOLIS_ZONE_PREFIX};
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::PROPOLIS_PORT;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use omicron_common::backoff;
//use propolis_client::generated::DiskRequest;
use propolis_client::Client as PropolisClient;
use slog::Logger;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(test)]
use illumos_utils::zone::MockZones as Zones;
#[cfg(not(test))]
use illumos_utils::zone::Zones;

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

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns::resolver::ResolveError),

    #[error("Instance {0} not running!")]
    InstanceNotRunning(Uuid),

    #[error("Instance already registered with Propolis ID {0}")]
    InstanceAlreadyRegistered(Uuid),
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

fn instance_name(id: &Uuid) -> String {
    format!("vm-{}", id)
}

fn fmri_name(id: &Uuid) -> String {
    format!("{}:{}", service_name(), instance_name(id))
}

fn propolis_zone_name(id: &Uuid) -> String {
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
    // Handle to task monitoring for Propolis state changes.
    monitor_task: Option<JoinHandle<()>>,
    // Handle to the zone.
    running_zone: RunningZone,
}

impl Drop for RunningState {
    fn drop(&mut self) {
        if let Some(task) = self.monitor_task.take() {
            // NOTE: We'd prefer to actually await the task, since it
            // will be completed at this point, but async drop doesn't exist.
            //
            // At a minimum, this implementation ensures the background task
            // is not executing after RunningState terminates.
            //
            // "InstanceManager" contains...
            //      ... "Instance", which contains...
            //      ... "InstanceInner", which contains...
            //      ... "RunningState", which owns the "monitor_task".
            //
            // The "monitor_task" removes the instance from the
            // "InstanceManager", triggering it's eventual drop.
            // When this happens, the "monitor_task" exits anyway.
            task.abort()
        }
    }
}

// Named type for values returned during propolis zone creation
struct PropolisSetup {
    client: Arc<PropolisClient>,
    running_zone: RunningZone,
}

struct InstanceInner {
    log: Logger,

    // Properties visible to Propolis
    properties: propolis_client::api::InstanceProperties,

    // The ID of the Propolis server (and zone) running this instance
    propolis_id: Uuid,

    // The IP address of the Propolis server running this instance
    propolis_ip: IpAddr,

    // NIC-related properties
    vnic_allocator: VnicAllocator<Etherstub>,

    // Reference to the port manager for creating OPTE ports when starting the
    // instance
    port_manager: PortManager,

    // Guest NIC and OPTE port information
    requested_nics: Vec<NetworkInterface>,
    source_nat: SourceNatConfig,
    external_ips: Vec<IpAddr>,
    firewall_rules: Vec<VpcFirewallRule>,

    // Disk related properties
    // TODO: replace `propolis_client::handmade::*` with properly-modeled local types
    requested_disks: Vec<propolis_client::handmade::api::DiskRequest>,
    cloud_init_bytes: Option<String>,

    // Internal State management
    state: InstanceStates,
    running_state: Option<RunningState>,

    // Connection to Nexus
    lazy_nexus_client: LazyNexusClient,

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

    async fn publish_state_to_nexus(&self) -> Result<(), Error> {
        self.lazy_nexus_client
            .get()
            .await?
            .cpapi_instances_put(
                self.id(),
                &nexus_client::types::InstanceRuntimeState::from(
                    self.state.current().clone(),
                ),
            )
            .await
            .map_err(|e| Error::Notification(e))?;

        Ok(())
    }

    /// Processes a Propolis state change observed by the Propolis monitoring
    /// task.
    async fn observe_state(
        &mut self,
        state: propolis_client::api::InstanceState,
    ) -> Result<Reaction, Error> {
        info!(self.log, "Observing new propolis state: {:?}", state);

        // The instance might have been rudely terminated between the time the
        // call to Propolis returned and the time the instance lock was
        // acquired for this call. If that happened, do not publish the Propolis
        // state; simply remain in the Destroyed state.
        //
        // Returning the `Terminate` action is OK because terminating a
        // previously-terminated instance is a no-op.
        if self.state.current().run_state == InstanceState::Destroyed {
            info!(
                self.log,
                "Ignoring new propolis state: instance is already destroyed"
            );
            return Ok(Reaction::Terminate);
        }

        // Update the Sled Agent's internal state machine.
        let action = self.state.observe_transition(&state);
        info!(
            self.log,
            "New state: {:?}, action: {:?}",
            self.state.current().run_state,
            action
        );

        // TODO(#2727): Any failure here (even a transient failure) causes the
        // monitoring task to exit, marooning the instance. Decide where best
        // to handle this.
        self.publish_state_to_nexus().await?;

        // Take the next action, if any.
        if let Some(action) = action {
            self.take_action(action).await
        } else {
            Ok(Reaction::Continue)
        }
    }

    /// Sends an instance state PUT request to this instance's Propolis.
    async fn propolis_state_put(
        &self,
        request: propolis_client::api::InstanceStateRequested,
    ) -> Result<(), Error> {
        self.running_state
            .as_ref()
            .expect("Propolis client should be initialized before usage")
            .client
            .instance_state_put()
            .body(request)
            .send()
            .await?;
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
            .map(|port| propolis_client::api::NetworkInterfaceRequest {
                // TODO-correctness: Remove `.vnic()` call when we use the port
                // directly.
                name: port.vnic_name().to_string(),
                slot: propolis_client::api::Slot(port.slot()),
            })
            .collect();

        let migrate = match migrate {
            Some(params) => {
                let migration_id =
                    self.state.current().migration_id.ok_or_else(|| {
                        Error::Migration(anyhow!("Missing Migration UUID"))
                    })?;
                Some(propolis_client::api::InstanceMigrateInitiateRequest {
                    src_addr: params.src_propolis_addr.to_string(),
                    src_uuid: params.src_propolis_id,
                    migration_id,
                })
            }
            None => None,
        };

        let request = propolis_client::api::InstanceEnsureRequest {
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
        let monitor_client = client.clone();
        let monitor_task = Some(tokio::task::spawn(async move {
            let r = instance.monitor_state_task(monitor_client).await;
            let log = &instance.inner.lock().await.log;
            match r {
                Err(e) => warn!(log, "State monitoring task failed: {}", e),
                Ok(()) => info!(log, "State monitoring task complete"),
            }
        }));

        self.running_state =
            Some(RunningState { client, monitor_task, running_zone });

        Ok(())
    }

    async fn take_action(
        &self,
        action: InstanceAction,
    ) -> Result<Reaction, Error> {
        info!(self.log, "Taking action: {:#?}", action);
        let requested_state = match action {
            InstanceAction::Run => {
                propolis_client::api::InstanceStateRequested::Run
            }
            InstanceAction::Stop => {
                propolis_client::api::InstanceStateRequested::Stop
            }
            InstanceAction::Reboot => {
                propolis_client::api::InstanceStateRequested::Reboot
            }
            InstanceAction::Destroy => {
                // Unlike the other actions, which update the Propolis state,
                // the "destroy" action indicates that the service should be
                // terminated.
                info!(self.log, "take_action: Taking the Destroy action");
                return Ok(Reaction::Terminate);
            }
        };
        self.propolis_state_put(requested_state).await?;
        Ok(Reaction::Continue)
    }

    /// Immediately terminates this instance's Propolis zone and cleans up any
    /// runtime objects associated with the instance.
    ///
    /// This routine is safe to call even if the instance's zone was never
    /// started. It is also safe to call multiple times on a single instance.
    async fn terminate(&mut self) -> Result<(), Error> {
        // Ensure that no zone exists. This succeeds even if no zone was ever
        // created.
        // NOTE: we call`Zones::halt_and_remove_logged` directly instead of
        // `RunningZone::stop` in case we're called between creating the
        // zone and assigning `running_state`.
        let zname = propolis_zone_name(self.propolis_id());
        warn!(self.log, "Halting and removing zone: {}", zname);
        Zones::halt_and_remove_logged(&self.log, &zname).await.unwrap();

        // Remove ourselves from the instance manager's map of instances.
        self.instance_ticket.terminate();

        // See if there are any runtime objects to clean up.
        let mut running_state = if let Some(state) = self.running_state.take() {
            state
        } else {
            return Ok(());
        };

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

#[cfg(test)]
mockall::mock! {
    pub Instance {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            log: Logger,
            id: Uuid,
            ticket: InstanceTicket,
            initial: InstanceHardware,
            vnic_allocator: VnicAllocator<Etherstub>,
            port_manager: PortManager,
            lazy_nexus_client: LazyNexusClient,
        ) -> Result<Self, Error>;
        pub async fn current_state(&self) -> InstanceRuntimeState;
        pub async fn put_state(
            &self,
            state: InstanceStateRequested,
        ) -> Result<InstanceRuntimeState, Error>;
        pub async fn put_migration_ids(
            &self,
            old_runtime: &InstanceRuntimeState,
            migration_ids: &Option<InstanceMigrationSourceParams>
        ) -> Result<InstanceRuntimeState, Error>;
        pub async fn issue_snapshot_request(
            &self,
            disk_id: Uuid,
            snapshot_name: Uuid,
        ) -> Result<(), Error>;
        pub async fn terminate(&self) -> Result<InstanceRuntimeState, Error>;
    }
    impl Clone for Instance {
        fn clone(&self) -> Self;
    }
}

#[cfg_attr(test, allow(dead_code))]
impl Instance {
    /// Creates a new (not yet running) instance object.
    ///
    /// Arguments:
    /// * `log`: Logger for dumping debug information.
    /// * `id`: UUID of the instance to be created.
    /// * `initial`: State of the instance at initialization time.
    /// * `vnic_allocator`: A unique (to the sled) ID generator to
    /// refer to a VNIC. (This exists because of a restriction on VNIC name
    /// lengths, otherwise the UUID would be used instead).
    /// * `port_manager`: Handle to the object responsible for managing OPTE
    /// ports.
    /// * `lazy_nexus_client`: Connection to Nexus, used for sending notifications.
    // TODO: This arg list is getting a little long; can we clean this up?
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log: Logger,
        id: Uuid,
        ticket: InstanceTicket,
        initial: InstanceHardware,
        vnic_allocator: VnicAllocator<Etherstub>,
        port_manager: PortManager,
        lazy_nexus_client: LazyNexusClient,
    ) -> Result<Self, Error> {
        info!(log, "Instance::new w/initial HW: {:?}", initial);
        let instance = InstanceInner {
            log: log.new(o!("instance_id" => id.to_string())),
            // NOTE: Mostly lies.
            properties: propolis_client::api::InstanceProperties {
                id,
                name: initial.runtime.hostname.clone(),
                description: "Test description".to_string(),
                image_id: Uuid::nil(),
                bootrom_id: Uuid::nil(),
                // TODO: Align the byte type w/propolis.
                memory: initial.runtime.memory.to_whole_mebibytes(),
                // TODO: we should probably make propolis aligned with
                // InstanceCpuCount here, to avoid any casting...
                vcpus: initial.runtime.ncpus.0 as u8,
            },
            propolis_id: initial.runtime.propolis_id,
            propolis_ip: initial.runtime.propolis_addr.unwrap().ip(),
            vnic_allocator,
            port_manager,
            requested_nics: initial.nics,
            source_nat: initial.source_nat,
            external_ips: initial.external_ips,
            firewall_rules: initial.firewall_rules,
            requested_disks: initial.disks,
            cloud_init_bytes: initial.cloud_init_bytes,
            state: InstanceStates::new(initial.runtime),
            running_state: None,
            lazy_nexus_client,
            instance_ticket: ticket,
        };

        let inner = Arc::new(Mutex::new(instance));

        Ok(Instance { inner })
    }

    pub async fn current_state(&self) -> InstanceRuntimeState {
        let inner = self.inner.lock().await;
        inner.state.current().clone()
    }

    /// Ensures that a Propolis process exists for this instance, then sends it
    /// an instance ensure request.
    async fn propolis_ensure(
        &self,
        inner: &mut MutexGuard<'_, InstanceInner>,
        migration_params: Option<InstanceMigrationTargetParams>,
    ) -> Result<(), Error> {
        if let Some(running_state) = inner.running_state.as_ref() {
            inner
                .propolis_ensure(
                    &running_state.client,
                    &running_state.running_zone,
                    migration_params,
                )
                .await?;
        } else {
            let setup_result: Result<(), Error> = 'setup: {
                // If there's no Propolis yet, and this instance is not being
                // initialized via migration, immediately send a state update to
                // Nexus to reflect that the instance is starting (so that the
                // external API will display this state while the zone is being
                // started).
                //
                // Migration targets don't do this because the instance is still
                // logically running (on the source) while the target Propolis
                // is being launched.
                if migration_params.is_none() {
                    inner.state.transition(InstanceState::Starting);
                    if let Err(e) = inner.publish_state_to_nexus().await {
                        break 'setup Err(e);
                    }
                }

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
                inner.state.transition(InstanceState::Failed);
                inner.publish_state_to_nexus().await?;
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
    ) -> Result<InstanceRuntimeState, Error> {
        use propolis_client::api::InstanceStateRequested as PropolisRequest;
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
                    (None, Some(InstanceState::Destroyed))
                } else {
                    (Some(PropolisRequest::Stop), Some(InstanceState::Stopping))
                }
            }
            InstanceStateRequested::Reboot => {
                if inner.running_state.is_none() {
                    return Err(Error::InstanceNotRunning(*inner.id()));
                }
                (Some(PropolisRequest::Reboot), Some(InstanceState::Rebooting))
            }
        };

        if let Some(p) = propolis_state {
            inner.propolis_state_put(p).await?;
        }
        if let Some(s) = next_published {
            inner.state.transition(s);
        }
        Ok(inner.state.current().clone())
    }

    pub async fn put_migration_ids(
        &self,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<InstanceRuntimeState, Error> {
        let mut inner = self.inner.lock().await;

        // Check that the instance's current generation matches the one the
        // caller expects to transition from. This helps Nexus ensure that if
        // multiple migration sagas launch at Propolis generation N, then only
        // one of them will successfully set the instance's migration IDs.
        if inner.state.current().propolis_gen != old_runtime.propolis_gen {
            // Allow this transition for idempotency if the instance is
            // already in the requested goal state.
            if inner.state.migration_ids_already_set(old_runtime, migration_ids)
            {
                return Ok(inner.state.current().clone());
            }

            return Err(Error::Transition(
                omicron_common::api::external::Error::InvalidRequest {
                    message: format!(
                        "wrong Propolis ID generation: expected {}, got {}",
                        inner.state.current().propolis_gen,
                        old_runtime.propolis_gen
                    ),
                },
            ));
        }

        inner.state.set_migration_ids(migration_ids);
        Ok(inner.state.current().clone())
    }

    async fn setup_propolis_locked(
        &self,
        inner: &mut MutexGuard<'_, InstanceInner>,
    ) -> Result<PropolisSetup, Error> {
        // Create OPTE ports for the instance
        let mut opte_ports = Vec::with_capacity(inner.requested_nics.len());
        for nic in inner.requested_nics.iter() {
            let (snat, external_ips) = if nic.primary {
                (Some(inner.source_nat), &inner.external_ips[..])
            } else {
                (None, &[][..])
            };
            let port = inner.port_manager.create_port(
                nic,
                snat,
                external_ips,
                &inner.firewall_rules,
            )?;
            opte_ports.push(port);
        }

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(inner.propolis_id());
        let root = std::path::Path::new(ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT);
        let installed_zone = InstalledZone::install(
            &inner.log,
            &inner.vnic_allocator,
            &root,
            "propolis-server",
            Some(&inner.propolis_id().to_string()),
            // dataset=
            &[],
            // filesystems=
            &[],
            &[
                zone::Device { name: "/dev/vmm/*".to_string() },
                zone::Device { name: "/dev/vmmctl".to_string() },
                zone::Device { name: "/dev/viona".to_string() },
            ],
            opte_ports,
            // physical_nic=
            None,
            vec![],
            vec![],
        )
        .await?;

        let running_zone = RunningZone::boot(installed_zone).await?;
        let addr_request = AddressRequest::new_static(inner.propolis_ip, None);
        let network = running_zone.ensure_address(addr_request).await?;
        info!(inner.log, "Created address {} for zone: {}", network, zname);

        let gateway = inner.port_manager.underlay_ip();
        running_zone.add_default_route(*gateway).await?;

        // Run Propolis in the Zone.
        let smf_service_name = "svc:/system/illumos/propolis-server";
        let instance_name = format!("vm-{}", inner.propolis_id());
        let smf_instance_name =
            format!("{}:{}", smf_service_name, instance_name);
        let server_addr = SocketAddr::new(inner.propolis_ip, PROPOLIS_PORT);

        // We intentionally do not import the service - it is placed under
        // `/var/svc/manifest`, and should automatically be imported by
        // configd.
        //
        // Insteady, we re-try adding the instance until it succeeds.
        // This implies that the service was added successfully.
        info!(
            inner.log, "Adding service"; "smf_name" => &smf_instance_name
        );
        backoff::retry_notify(
            backoff::retry_policy_local(),
            || async {
                running_zone
                    .run_cmd(&[
                        illumos_utils::zone::SVCCFG,
                        "-s",
                        smf_service_name,
                        "add",
                        &instance_name,
                    ])
                    .map_err(|e| backoff::BackoffError::transient(e))
            },
            |err: RunCommandError, delay| {
                warn!(
                    inner.log,
                    "Failed to add {} as a service (retrying in {:?}): {}",
                    instance_name,
                    delay,
                    err.to_string()
                );
            },
        )
        .await?;

        info!(inner.log, "Adding service property group 'config'");
        running_zone.run_cmd(&[
            illumos_utils::zone::SVCCFG,
            "-s",
            &smf_instance_name,
            "addpg",
            "config",
            "astring",
        ])?;

        info!(inner.log, "Setting server address property"; "address" => &server_addr);
        running_zone.run_cmd(&[
            illumos_utils::zone::SVCCFG,
            "-s",
            &smf_instance_name,
            "setprop",
            &format!("config/server_addr={}", server_addr),
        ])?;

        let metric_addr = inner.lazy_nexus_client.get_ip().await.unwrap();
        info!(
            inner.log,
            "Setting metric address property address [{}]:{}",
            metric_addr,
            NEXUS_INTERNAL_PORT,
        );
        running_zone.run_cmd(&[
            illumos_utils::zone::SVCCFG,
            "-s",
            &smf_instance_name,
            "setprop",
            &format!(
                "config/metric_addr=[{}]:{}",
                metric_addr, NEXUS_INTERNAL_PORT
            ),
        ])?;

        info!(inner.log, "Refreshing instance");
        running_zone.run_cmd(&[
            illumos_utils::zone::SVCCFG,
            "-s",
            &smf_instance_name,
            "refresh",
        ])?;

        info!(inner.log, "Enabling instance");
        running_zone.run_cmd(&[
            illumos_utils::zone::SVCADM,
            "enable",
            "-t",
            &smf_instance_name,
        ])?;

        info!(inner.log, "Started propolis in zone: {}", zname);

        // This isn't strictly necessary - we wait for the HTTP server below -
        // but it helps distinguish "online in SMF" from "responding to HTTP
        // requests".
        let fmri = fmri_name(inner.propolis_id());
        wait_for_service(Some(&zname), &fmri)
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;

        inner.state.current_mut().propolis_addr = Some(server_addr);

        // We use a custom client builder here because the default progenitor
        // one has a timeout of 15s but we want to be able to wait indefinitely.
        let reqwest_client = reqwest::ClientBuilder::new().build().unwrap();
        let client = Arc::new(PropolisClient::new_with_client(
            &format!("http://{}", server_addr),
            reqwest_client,
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&inner.log, &client).await?;

        Ok(PropolisSetup { client, running_zone })
    }

    /// Rudely terminates this instance's Propolis (if it has one) and
    /// immediately transitions the instance to the Destroyed state.
    pub async fn terminate(&self) -> Result<InstanceRuntimeState, Error> {
        let mut inner = self.inner.lock().await;
        inner.terminate().await?;
        inner.state.transition(InstanceState::Destroyed);
        Ok(inner.state.current().clone())
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
                .body(propolis_client::api::InstanceStateMonitorRequest { gen })
                .send()
                .await?;
            let reaction =
                self.inner.lock().await.observe_state(response.state).await?;

            match reaction {
                Reaction::Continue => {}
                Reaction::Terminate => {
                    return self.terminate().await.map(|_| ());
                }
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::instance_manager::InstanceManager;
    use crate::nexus::LazyNexusClient;
    use crate::params::InstanceStateRequested;
    use chrono::Utc;
    use illumos_utils::dladm::Etherstub;
    use illumos_utils::opte::PortManager;
    use macaddr::MacAddr6;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState,
    };
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
    use omicron_common::api::internal::shared::SourceNatConfig;
    use omicron_test_utils::dev::test_setup_log;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;

    static INST_UUID_STR: &str = "e398c5d5-5059-4e55-beac-3a1071083aaa";
    static PROPOLIS_UUID_STR: &str = "ed895b13-55d5-4e0b-88e9-3f4e74d0d936";

    fn test_uuid() -> Uuid {
        INST_UUID_STR.parse().unwrap()
    }

    fn test_propolis_uuid() -> Uuid {
        PROPOLIS_UUID_STR.parse().unwrap()
    }

    fn new_initial_instance() -> InstanceHardware {
        InstanceHardware {
            runtime: InstanceRuntimeState {
                run_state: InstanceState::Creating,
                sled_id: Uuid::new_v4(),
                propolis_id: test_propolis_uuid(),
                dst_propolis_id: None,
                propolis_addr: Some("[fd00:1de::74]:12400".parse().unwrap()),
                migration_id: None,
                propolis_gen: Generation::new(),
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_mebibytes_u32(512),
                hostname: "myvm".to_string(),
                gen: Generation::new(),
                time_updated: Utc::now(),
            },
            nics: vec![],
            source_nat: SourceNatConfig {
                ip: IpAddr::from(Ipv4Addr::new(10, 0, 0, 1)),
                first_port: 0,
                last_port: 16_384,
            },
            external_ips: vec![],
            firewall_rules: vec![],
            disks: vec![],
            cloud_init_bytes: None,
        }
    }

    // Due to the usage of global mocks, we use "serial_test" to avoid
    // parellizing test invocations.
    //
    // From https://docs.rs/mockall/0.10.1/mockall/index.html#static-methods
    //
    //   Mockall can also mock static methods. But be careful! The expectations
    //   are global. If you want to use a static method in multiple tests, you
    //   must provide your own synchronization. For ordinary methods,
    //   expectations are set on the mock object. But static methods don’t have
    //   any mock object. Instead, you must create a Context object just to set
    //   their expectations.

    #[tokio::test]
    #[serial_test::serial]
    async fn transition_before_start() {
        let logctx = test_setup_log("transition_before_start");
        let log = &logctx.log;
        let vnic_allocator =
            VnicAllocator::new("Test", Etherstub("mylink".to_string()));
        let underlay_ip = std::net::Ipv6Addr::new(
            0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        );
        let mac = MacAddr6::from([0u8; 6]);
        let port_manager =
            PortManager::new(log.new(slog::o!()), underlay_ip, Some(mac));
        let lazy_nexus_client =
            LazyNexusClient::new(log.clone(), std::net::Ipv6Addr::LOCALHOST)
                .unwrap();
        let instance_manager = InstanceManager::new(
            log.clone(),
            lazy_nexus_client.clone(),
            Etherstub("mylink".to_string()),
            port_manager.clone(),
        )
        .unwrap();

        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            instance_manager.test_instance_ticket(test_uuid()),
            new_initial_instance(),
            vnic_allocator,
            port_manager,
            lazy_nexus_client,
        )
        .unwrap();

        // Pick a state transition that requires the instance to have started.
        assert!(inst.put_state(InstanceStateRequested::Reboot).await.is_err());

        logctx.cleanup_successful();
    }
}
