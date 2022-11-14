// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::instance::{Action as InstanceAction, InstanceStates};
use crate::illumos::dladm::Etherstub;
use crate::illumos::link::VnicAllocator;
use crate::illumos::running_zone::{
    InstalledZone, RunCommandError, RunningZone,
};
use crate::illumos::svc::wait_for_service;
use crate::illumos::zone::{AddressRequest, PROPOLIS_ZONE_PREFIX};
use crate::instance_manager::InstanceTicket;
use crate::nexus::LazyNexusClient;
use crate::opte::PortManager;
use crate::opte::PortTicket;
use crate::params::NetworkInterface;
use crate::params::SourceNatConfig;
use crate::params::VpcFirewallRule;
use crate::params::{
    InstanceHardware, InstanceMigrateParams, InstanceRuntimeStateRequested,
    InstanceSerialConsoleData,
};
use crate::serial::{ByteOffset, SerialConsoleBuffer};
use anyhow::anyhow;
use futures::lock::{Mutex, MutexGuard};
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::PROPOLIS_PORT;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
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
use crate::illumos::zone::MockZones as Zones;
#[cfg(not(test))]
use crate::illumos::zone::Zones;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to wait for service: {0}")]
    Timeout(String),

    #[error("Failed to create VNIC: {0}")]
    VnicCreation(#[from] crate::illumos::dladm::CreateVnicError),

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
    ZoneCommand(#[from] crate::illumos::running_zone::RunCommandError),

    #[error(transparent)]
    ZoneBoot(#[from] crate::illumos::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] crate::illumos::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] crate::illumos::running_zone::InstallZoneError),

    #[error("serde_json failure: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    Opte(#[from] crate::opte::Error),

    #[error("Serial console buffer: {0}")]
    Serial(#[from] crate::serial::Error),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns_client::multiclient::ResolveError),

    #[error("Instance {0} not running!")]
    InstanceNotRunning(Uuid),
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
        backoff::internal_service_policy(),
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
    // Object representing membership in the "instance manager".
    instance_ticket: InstanceTicket,
    // Objects representing the instance's OPTE ports in the port manager
    port_tickets: Option<Vec<PortTicket>>,
    // Handle to task monitoring for Propolis state changes.
    monitor_task: Option<JoinHandle<()>>,
    // Handle to the zone.
    _running_zone: RunningZone,
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
    port_tickets: Option<Vec<PortTicket>>,
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

    // Task buffering the instance's serial console
    serial_tty_task: Option<SerialConsoleBuffer>,

    // Connection to Nexus
    lazy_nexus_client: LazyNexusClient,
}

impl InstanceInner {
    fn id(&self) -> &Uuid {
        &self.properties.id
    }

    /// UUID of the underlying propolis-server process
    fn propolis_id(&self) -> &Uuid {
        &self.propolis_id
    }

    async fn observe_state(
        &mut self,
        state: propolis_client::api::InstanceState,
    ) -> Result<Reaction, Error> {
        info!(self.log, "Observing new propolis state: {:?}", state);

        // Update the Sled Agent's internal state machine.
        let action = self.state.observe_transition(&state);
        info!(
            self.log,
            "New state: {:?}, action: {:?}",
            self.state.current().run_state,
            action
        );

        // Notify Nexus of the state change.
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

        // Take the next action, if any.
        if let Some(action) = action {
            self.take_action(action).await
        } else {
            Ok(Reaction::Continue)
        }
    }

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

    async fn ensure(
        &mut self,
        instance: Instance,
        instance_ticket: InstanceTicket,
        setup: PropolisSetup,
        migrate: Option<InstanceMigrateParams>,
    ) -> Result<(), Error> {
        let PropolisSetup { client, running_zone, port_tickets } = setup;

        let nics = running_zone
            .opte_ports()
            .iter()
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

        if self.serial_tty_task.is_none() {
            self.serial_tty_task = Some(SerialConsoleBuffer::new(
                Arc::downgrade(&client),
                self.log.clone(),
            ));
        }

        self.running_state = Some(RunningState {
            client,
            instance_ticket,
            port_tickets,
            monitor_task,
            _running_zone: running_zone,
        });

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
        pub fn new(
            log: Logger,
            id: Uuid,
            initial: InstanceHardware,
            vnic_allocator: VnicAllocator<Etherstub>,
            port_manager: PortManager,
            lazy_nexus_client: LazyNexusClient,
        ) -> Result<Self, Error>;
        pub async fn start(
            &self,
            instance_ticket: InstanceTicket,
            migrate: Option<InstanceMigrateParams>,
        ) -> Result<(), Error>;
        pub async fn transition(
            &self,
            target: InstanceRuntimeStateRequested,
        ) -> Result<InstanceRuntimeState, Error>;
        pub async fn serial_console_buffer_data(
            &self,
            byte_offset: ByteOffset,
            max_bytes: Option<usize>,
        ) -> Result<InstanceSerialConsoleData, Error>;
        pub async fn issue_snapshot_request(
            &self,
            disk_id: Uuid,
            snapshot_name: Uuid,
        ) -> Result<(), Error>;
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
    pub fn new(
        log: Logger,
        id: Uuid,
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
            serial_tty_task: None,
        };

        let inner = Arc::new(Mutex::new(instance));

        Ok(Instance { inner })
    }

    async fn setup_propolis_locked(
        &self,
        inner: &mut MutexGuard<'_, InstanceInner>,
    ) -> Result<PropolisSetup, Error> {
        // Update nexus with an in-progress state while we set up the instance.
        let desired = inner.state.desired().clone();
        inner
            .state
            .transition(InstanceState::Starting, desired.map(|d| d.run_state));
        inner
            .lazy_nexus_client
            .get()
            .await?
            .cpapi_instances_put(
                inner.id(),
                &nexus_client::types::InstanceRuntimeState::from(
                    inner.state.current().clone(),
                ),
            )
            .await
            .map_err(|e| Error::Notification(e))?;

        // Create OPTE ports for the instance
        let mut opte_ports = Vec::with_capacity(inner.requested_nics.len());
        let mut port_tickets = Vec::with_capacity(inner.requested_nics.len());
        for nic in inner.requested_nics.iter() {
            let (snat, external_ips) = if nic.primary {
                (Some(inner.source_nat), Some(inner.external_ips.clone()))
            } else {
                (None, None)
            };
            let (port, port_ticket) = inner.port_manager.create_port(
                *inner.id(),
                nic,
                snat,
                external_ips,
                &inner.firewall_rules,
            )?;
            opte_ports.push(port);
            port_tickets.push(port_ticket);
        }

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(inner.propolis_id());

        let installed_zone = InstalledZone::install(
            &inner.log,
            &inner.vnic_allocator,
            "propolis-server",
            Some(&inner.propolis_id().to_string()),
            // dataset=
            &[],
            &[
                zone::Device { name: "/dev/vmm/*".to_string() },
                zone::Device { name: "/dev/vmmctl".to_string() },
                zone::Device { name: "/dev/viona".to_string() },
            ],
            opte_ports,
            // physical_nic=
            None,
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
            backoff::internal_service_policy(),
            || async {
                running_zone
                    .run_cmd(&[
                        crate::illumos::zone::SVCCFG,
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
            crate::illumos::zone::SVCCFG,
            "-s",
            &smf_instance_name,
            "addpg",
            "config",
            "astring",
        ])?;

        info!(inner.log, "Setting server address property"; "address" => &server_addr);
        running_zone.run_cmd(&[
            crate::illumos::zone::SVCCFG,
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
            crate::illumos::zone::SVCCFG,
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
            crate::illumos::zone::SVCCFG,
            "-s",
            &smf_instance_name,
            "refresh",
        ])?;

        info!(inner.log, "Enabling instance");
        running_zone.run_cmd(&[
            crate::illumos::zone::SVCADM,
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

        Ok(PropolisSetup {
            client,
            running_zone,
            port_tickets: Some(port_tickets),
        })
    }

    /// Begins the execution of the instance's service (Propolis).
    pub async fn start(
        &self,
        instance_ticket: InstanceTicket,
        migrate: Option<InstanceMigrateParams>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;

        // Create the propolis zone and resources
        let setup = self.setup_propolis_locked(&mut inner).await?;

        // Ensure the instance exists in the Propolis Server before we start
        // using it.
        inner.ensure(self.clone(), instance_ticket, setup, migrate).await?;

        Ok(())
    }

    // Terminate the Propolis service.
    async fn stop(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;

        let zname = propolis_zone_name(inner.propolis_id());
        warn!(inner.log, "Halting and removing zone: {}", zname);
        Zones::halt_and_remove_logged(&inner.log, &zname).await.unwrap();

        // Remove ourselves from the instance manager's map of instances.
        let running_state = inner.running_state.as_mut().unwrap();
        running_state.instance_ticket.terminate();

        // And remove the OPTE ports from the port manager
        let mut result = Ok(());
        if let Some(tickets) = running_state.port_tickets.as_mut() {
            for ticket in tickets.iter_mut() {
                // Release the port from the manager, and store any error. We
                // don't return immediately so that we can try to clean up all
                // ports, even if early ones fail. Return the last error, which
                // is OK for now.
                if let Err(e) = ticket.release() {
                    result = Err(e.into());
                }
            }
        }
        result
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
                    return self.stop().await;
                }
            }

            // Update the generation number we're asking for, to ensure the
            // Propolis will only return more recent values.
            gen = response.gen + 1;
        }
    }

    /// Transitions an instance object to a new state, taking any actions
    /// necessary to perform state transitions.
    ///
    /// Returns the new state after starting the transition.
    ///
    /// # Panics
    ///
    /// This method may panic if it has been invoked before [`Instance::start`].
    pub async fn transition(
        &self,
        target: InstanceRuntimeStateRequested,
    ) -> Result<InstanceRuntimeState, Error> {
        let mut inner = self.inner.lock().await;
        if let Some(action) = inner
            .state
            .request_transition(&target)
            .map_err(|e| Error::Transition(e))?
        {
            info!(
                &inner.log,
                "transition to {:?}; action: {:#?}", target, action
            );
            inner.take_action(action).await?;
        }
        Ok(inner.state.current().clone())
    }

    pub async fn serial_console_buffer_data(
        &self,
        byte_offset: ByteOffset,
        max_bytes: Option<usize>,
    ) -> Result<InstanceSerialConsoleData, Error> {
        let inner = self.inner.lock().await;
        if let Some(ttybuf) = &inner.serial_tty_task {
            let (data, last_byte_offset) =
                ttybuf.contents(byte_offset, max_bytes).await?;
            Ok(InstanceSerialConsoleData {
                data,
                last_byte_offset: last_byte_offset as u64,
            })
        } else {
            Err(crate::serial::Error::Existential.into())
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
    use crate::illumos::dladm::Etherstub;
    use crate::nexus::LazyNexusClient;
    use crate::opte::PortManager;
    use crate::params::InstanceStateRequested;
    use crate::params::SourceNatConfig;
    use chrono::Utc;
    use macaddr::MacAddr6;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState,
    };
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
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
    #[should_panic(
        expected = "Propolis client should be initialized before usage"
    )]
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
            PortManager::new(log.new(slog::o!()), underlay_ip, mac);
        let lazy_nexus_client =
            LazyNexusClient::new(log.clone(), std::net::Ipv6Addr::LOCALHOST)
                .unwrap();

        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            new_initial_instance(),
            vnic_allocator,
            port_manager,
            lazy_nexus_client,
        )
        .unwrap();

        // Remove the logfile before we expect to panic, or it'll never be
        // cleaned up.
        logctx.cleanup_successful();

        // Trying to transition before the instance has been initialized will
        // result in a panic.
        inst.transition(InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Running,
            migration_params: None,
        })
        .await
        .unwrap();
    }
}
