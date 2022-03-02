// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::{
    instance::{Action as InstanceAction, InstanceStates, PROPOLIS_PORT},
    vlan::VlanID,
};
use crate::illumos::svc::wait_for_service;
use crate::illumos::zone::PROPOLIS_ZONE_PREFIX;
use crate::instance_manager::InstanceTicket;
use crate::nexus::NexusClient;
use crate::vnic::{interface_name, IdAllocator, Vnic};
use anyhow::anyhow;
use futures::lock::{Mutex, MutexGuard};
use omicron_common::api::external::NetworkInterface;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceHardware;
use omicron_common::api::internal::sled_agent::InstanceMigrateParams;
use omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested;
use omicron_common::backoff;
use propolis_client::Client as PropolisClient;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zone::Zones};
#[cfg(test)]
use crate::illumos::{dladm::MockDladm as Dladm, zone::MockZones as Zones};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to wait for service: {0}")]
    Timeout(String),

    #[error("Failure accessing data links: {0}")]
    Datalink(#[from] crate::illumos::dladm::Error),

    #[error("Error accessing zones: {0}")]
    Zone(#[from] crate::illumos::zone::Error),

    #[error("Failure from Propolis Client: {0}")]
    Propolis(#[from] propolis_client::Error),

    // TODO: Remove this error; prefer to retry notifications.
    #[error("Notifying Nexus failed: {0}")]
    Notification(nexus_client::Error<()>),

    // TODO: This error type could become more specific
    #[error("Error performing a state transition: {0}")]
    Transition(omicron_common::api::external::Error),

    // TODO: Add more specific errors
    #[error("Failure during migration: {0}")]
    Migration(anyhow::Error),
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
            // This request is nonsensical - we don't expect an instance to be
            // using the nil UUID - but getting a response that isn't a
            // connection-based error informs us the HTTP server is alive.
            match client.instance_get(Uuid::nil()).await {
                Ok(_) => return Ok(()),
                Err(value) => {
                    if let propolis_client::Error::Status(_) = &value {
                        // This means the propolis server responded to our garbage
                        // request, instead of a connection error.
                        return Ok(());
                    }
                    return Err(backoff::BackoffError::Transient(value));
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
    ticket: InstanceTicket,
    // Handle to task monitoring for Propolis state changes.
    monitor_task: Option<JoinHandle<()>>,
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
    control_nic: Vnic,
    guest_nics: Vec<Vnic>,
}

struct InstanceInner {
    id: Uuid,

    log: Logger,

    // Properties visible to Propolis
    properties: propolis_client::api::InstanceProperties,

    // NIC-related properties
    nic_id_allocator: IdAllocator,
    requested_nics: Vec<NetworkInterface>,
    allocated_nics: Vec<Vnic>,
    vlan: Option<VlanID>,

    // Internal State management
    state: InstanceStates,
    running_state: Option<RunningState>,

    // Connection to Nexus
    nexus_client: Arc<NexusClient>,
}

impl InstanceInner {
    fn id(&self) -> &Uuid {
        &self.id
    }

    /// UUID of the underlying propolis-server process
    fn propolis_id(&self) -> &Uuid {
        &self.properties.id
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
        self.nexus_client
            .cpapi_instances_put(
                self.id(),
                &nexus_client::types::InstanceRuntimeState::from(
                    self.state.current(),
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
            .instance_state_put(*self.propolis_id(), request)
            .await?;
        Ok(())
    }

    async fn ensure(
        &mut self,
        instance: Instance,
        ticket: InstanceTicket,
        setup: PropolisSetup,
        migrate: Option<InstanceMigrateParams>,
    ) -> Result<(), Error> {
        let PropolisSetup { client, control_nic, guest_nics } = setup;

        // TODO: Store slot in NetworkInterface, make this more stable.
        let nics = self
            .requested_nics
            .iter()
            .enumerate()
            .map(|(i, _)| propolis_client::api::NetworkInterfaceRequest {
                name: guest_nics[i].name().to_string(),
                slot: propolis_client::api::Slot(i as u8),
            })
            .collect();

        let migrate = match migrate {
            Some(params) => {
                let migration_id =
                    self.state.current().migration_uuid.ok_or_else(|| {
                        Error::Migration(anyhow!("Missing Migration UUID"))
                    })?;
                Some(propolis_client::api::InstanceMigrateInitiateRequest {
                    src_addr: params.src_propolis_addr,
                    src_uuid: params.src_propolis_uuid,
                    migration_id,
                })
            }
            None => None,
        };

        let request = propolis_client::api::InstanceEnsureRequest {
            properties: self.properties.clone(),
            nics,
            // TODO: Actual disks need to be wired up here.
            disks: vec![],
            migrate,
        };

        info!(self.log, "Sending ensure request to propolis: {:?}", request);
        client.instance_ensure(&request).await?;

        // Monitor propolis for state changes in the background.
        let monitor_task = Some(tokio::task::spawn(async move {
            let r = instance.monitor_state_task().await;
            let log = &instance.inner.lock().await.log;
            match r {
                Err(e) => warn!(log, "State monitoring task failed: {}", e),
                Ok(()) => info!(log, "State monitoring task complete"),
            }
        }));

        self.running_state =
            Some(RunningState { client, ticket, monitor_task });

        // Store the VNICs while the instance is running.
        self.allocated_nics = guest_nics
            .into_iter()
            .chain(std::iter::once(control_nic))
            .collect();

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
            nic_id_allocator: IdAllocator,
            initial: InstanceHardware,
            vlan: Option<VlanID>,
            nexus_client: Arc<NexusClient>,
        ) -> Result<Self, Error>;
        pub async fn start(
            &self,
            ticket: InstanceTicket,
            migrate: Option<InstanceMigrateParams>,
        ) -> Result<(), Error>;
        pub async fn transition(
            &self,
            target: InstanceRuntimeStateRequested,
        ) -> Result<InstanceRuntimeState, Error>;
    }
    impl Clone for Instance {
        fn clone(&self) -> Self;
    }
}

impl Instance {
    /// Creates a new (not yet running) instance object.
    ///
    /// Arguments:
    /// * `log`: Logger for dumping debug information.
    /// * `id`: UUID of the instance to be created.
    /// * `nic_id_allocator`: A unique (to the sled) ID generator to
    /// refer to a VNIC. (This exists because of a restriction on VNIC name
    /// lengths, otherwise the UUID would be used instead).
    /// * `initial`: State of the instance at initialization time.
    /// * `nexus_client`: Connection to Nexus, used for sending notifications.
    /// * `vlan`: An optional VLAN ID for tagging guest VNICs.
    // TODO: This arg list is getting a little long; can we clean this up?
    pub fn new(
        log: Logger,
        id: Uuid,
        nic_id_allocator: IdAllocator,
        initial: InstanceHardware,
        vlan: Option<VlanID>,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, Error> {
        info!(log, "Instance::new w/initial HW: {:?}", initial);
        let instance = InstanceInner {
            log: log.new(o!("instance id" => id.to_string())),
            id,
            // NOTE: Mostly lies.
            properties: propolis_client::api::InstanceProperties {
                id: initial.runtime.propolis_uuid,
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
            nic_id_allocator,
            requested_nics: initial.nics,
            allocated_nics: vec![],
            vlan,
            state: InstanceStates::new(initial.runtime),
            running_state: None,
            nexus_client,
        };

        let inner = Arc::new(Mutex::new(instance));

        Ok(Instance { inner })
    }

    async fn setup_propolis_locked(
        &self,
        inner: &mut MutexGuard<'_, InstanceInner>,
    ) -> Result<PropolisSetup, Error> {
        // Create the VNIC which will be attached to the zone.
        //
        // It would be preferable to use the UUID of the instance as a component
        // of the "per-Zone, control plane VNIC", but VNIC names are somewhat
        // restrictive. They must end with numerics, and they must be less than
        // 32 characters.
        //
        // Instead, we just use a per-agent incrementing number. We do the same
        // for the guest-accessible NICs too.
        let physical_dl = Dladm::find_physical()?;
        let control_nic =
            Vnic::new_control(&inner.nic_id_allocator, &physical_dl, None)?;

        // Instantiate all guest-requested VNICs.
        //
        // TODO: Ideally, we'd allocate VNICs directly within the Zone.
        // However, this seems to have been a SmartOS feature which
        // doesn't exist in illumos.
        //
        // https://github.com/illumos/ipd/blob/master/ipd/0003/README.md
        let guest_nics = inner
            .requested_nics
            .clone()
            .into_iter()
            .map(|nic| {
                Vnic::new_guest(
                    &inner.nic_id_allocator,
                    &physical_dl,
                    Some(nic.mac),
                    inner.vlan,
                )
                .map_err(|e| e.into())
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(inner.propolis_id());

        let nics_to_put_in_zone: Vec<String> = guest_nics
            .iter()
            .map(|nic| nic.name().to_string())
            .chain(std::iter::once(control_nic.name().to_string()))
            .collect();

        Zones::configure_propolis_zone(
            &inner.log,
            &zname,
            nics_to_put_in_zone,
        )?;
        info!(inner.log, "Configured propolis zone: {}", zname);

        // Clone the zone from a base zone (faster than installing) and
        // boot it up.
        Zones::clone_from_base_propolis(&zname)?;
        info!(inner.log, "Cloned child zone: {}", zname);
        Zones::boot(&zname)?;
        info!(inner.log, "Booted zone: {}", zname);

        // Wait for the network services to come online, then create an address.
        let fmri = "svc:/milestone/network:default";
        wait_for_service(Some(&zname), fmri)
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;
        info!(inner.log, "Network milestone ready for {}", zname);

        let network = Zones::create_address(
            &zname,
            &interface_name(&control_nic.name()),
        )?;
        info!(inner.log, "Created address {} for zone: {}", network, zname);

        // Run Propolis in the Zone.
        let server_addr = SocketAddr::new(network.ip(), PROPOLIS_PORT);
        Zones::run_propolis(&zname, inner.propolis_id(), &server_addr)?;
        info!(inner.log, "Started propolis in zone: {}", zname);

        // This isn't strictly necessary - we wait for the HTTP server below -
        // but it helps distinguish "online in SMF" from "responding to HTTP
        // requests".
        let fmri = fmri_name(inner.propolis_id());
        wait_for_service(Some(&zname), &fmri)
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;

        inner.state.current_mut().propolis_addr = Some(server_addr);

        let client = Arc::new(PropolisClient::new(
            server_addr,
            inner.log.new(o!("component" => "propolis-client")),
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&inner.log, &client).await?;

        Ok(PropolisSetup { client, control_nic, guest_nics })
    }

    /// Begins the execution of the instance's service (Propolis).
    pub async fn start(
        &self,
        ticket: InstanceTicket,
        migrate: Option<InstanceMigrateParams>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;

        // Create the propolis zone and resources
        let setup = self.setup_propolis_locked(&mut inner).await?;

        // Ensure the instance exists in the Propolis Server before we start
        // using it.
        inner.ensure(self.clone(), ticket, setup, migrate).await?;

        Ok(())
    }

    // Terminate the Propolis service.
    async fn stop(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;

        let zname = propolis_zone_name(inner.propolis_id());
        warn!(inner.log, "Halting and removing zone: {}", zname);
        Zones::halt_and_remove(&inner.log, &zname).unwrap();

        // Explicitly remove NICs.
        //
        // The NICs would self-delete on drop anyway, but this allows us
        // to explicitly record errors.
        let mut nics = vec![];
        std::mem::swap(&mut inner.allocated_nics, &mut nics);
        for mut nic in nics {
            if let Err(e) = nic.delete() {
                error!(inner.log, "Failed to delete NIC {:?}: {}", nic, e);
            }
        }
        inner.running_state.as_mut().unwrap().ticket.terminate();

        Ok(())
    }

    // Monitors propolis until explicitly told to disconnect.
    //
    // Intended to be spawned in a tokio task within [`Instance::start`].
    async fn monitor_state_task(&self) -> Result<(), Error> {
        // Grab the UUID and Propolis Client before we start looping, so we
        // don't need to contend the lock to access them in steady state.
        //
        // They aren't modified after being initialized, so it's fine to grab
        // a copy.
        let (propolis_id, client) = {
            let inner = self.inner.lock().await;
            let id = *inner.propolis_id();
            let client = inner.running_state.as_ref().unwrap().client.clone();
            (id, client)
        };

        let mut gen = 0;
        loop {
            // State monitoring always returns the most recent state/gen pair
            // known to Propolis.
            let response =
                client.instance_state_monitor(propolis_id, gen).await?;
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::illumos::{
        dladm::{MockDladm, PhysicalLink},
        zone::MockZones,
    };
    use crate::mocks::MockNexusClient;
    use crate::vnic::control_vnic_name;
    use chrono::Utc;
    use dropshot::{
        endpoint, ApiDescription, ConfigDropshot, ConfigLogging,
        ConfigLoggingLevel, HttpError, HttpResponseCreated, HttpResponseOk,
        HttpResponseUpdatedNoContent, HttpServer, HttpServerStarter, Path,
        RequestContext, TypedBody,
    };
    use futures::future::FutureExt;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState,
    };
    use omicron_common::api::internal::{
        nexus::InstanceRuntimeState, sled_agent::InstanceStateRequested,
    };
    use propolis_client::api;
    use tokio::sync::watch;

    static INST_UUID_STR: &str = "e398c5d5-5059-4e55-beac-3a1071083aaa";
    static PROPOLIS_UUID_STR: &str = "ed895b13-55d5-4e0b-88e9-3f4e74d0d936";

    fn test_uuid() -> Uuid {
        INST_UUID_STR.parse().unwrap()
    }

    fn test_propolis_uuid() -> Uuid {
        PROPOLIS_UUID_STR.parse().unwrap()
    }

    // Endpoints for a fake Propolis server.
    //
    // We intercept all traffic to Propolis via these endpoints.

    #[endpoint {
        method = PUT,
        path = "/instances/{instance_id}",
    }]
    async fn instance_ensure(
        rqctx: Arc<RequestContext<PropolisContext>>,
        path_params: Path<api::InstancePathParams>,
        _request: TypedBody<api::InstanceEnsureRequest>,
    ) -> Result<HttpResponseCreated<api::InstanceEnsureResponse>, HttpError>
    {
        let id = path_params.into_inner().instance_id;

        let mut server = rqctx.context().inner.lock().await;
        if server.is_some() {
            return Err(HttpError::for_internal_error(
                "Instance already initialized".to_string(),
            ));
        } else {
            *server = Some(FakeInstance { id });
            return Ok(HttpResponseCreated(api::InstanceEnsureResponse {
                migrate: None,
            }));
        }
    }

    #[endpoint {
        method = GET,
        path = "/instances/{instance_id}",
    }]
    async fn instance_get(
        rqctx: Arc<RequestContext<PropolisContext>>,
        path_params: Path<api::InstancePathParams>,
    ) -> Result<HttpResponseOk<api::InstanceGetResponse>, HttpError> {
        let id = path_params.into_inner().instance_id;
        let ctx = rqctx.context();
        let server = ctx.inner.lock().await;

        if let Some(server) = server.as_ref() {
            if server.id == id {
                // TODO: Patch this up with real values
                let instance_info = api::Instance {
                    properties: api::InstanceProperties {
                        id,
                        name: "Test Name".to_string(),
                        description: "Test Description".to_string(),
                        image_id: Uuid::new_v4(),
                        bootrom_id: Uuid::new_v4(),
                        memory: 0,
                        vcpus: 0,
                    },
                    state: ctx.state_receiver.borrow().state,
                    disks: vec![],
                    nics: vec![],
                };

                return Ok(HttpResponseOk(api::InstanceGetResponse {
                    instance: instance_info,
                }));
            }
        }
        Err(HttpError::for_internal_error("No matching instance".to_string()))
    }

    #[endpoint {
        method = GET,
        path = "/instances/{instance_id}/state-monitor",
    }]
    async fn instance_state_monitor(
        rqctx: Arc<RequestContext<PropolisContext>>,
        _path_params: Path<api::InstancePathParams>,
        request: TypedBody<api::InstanceStateMonitorRequest>,
    ) -> Result<HttpResponseOk<api::InstanceStateMonitorResponse>, HttpError>
    {
        let ctx = rqctx.context();
        let server_guard = ctx.inner.lock().await;
        let gen = request.into_inner().gen;
        if server_guard.is_some() {
            drop(server_guard);
            let mut receiver = ctx.state_receiver.clone();
            loop {
                let last = receiver.borrow().clone();
                if gen <= last.gen {
                    return Ok(HttpResponseOk(last));
                }
                receiver.changed().await.unwrap();
            }
        }
        Err(HttpError::for_internal_error(
            "Server not initialized (no instance)".to_string(),
        ))
    }

    #[endpoint {
        method = PUT,
        path = "/instances/{instance_id}/state",
    }]
    async fn instance_state_put(
        rqctx: Arc<RequestContext<PropolisContext>>,
        _path_params: Path<api::InstancePathParams>,
        request: TypedBody<api::InstanceStateRequested>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let server_guard = ctx.inner.lock().await;

        let state = match request.into_inner() {
            api::InstanceStateRequested::Run => api::InstanceState::Running,
            api::InstanceStateRequested::Stop => api::InstanceState::Destroyed,
            api::InstanceStateRequested::Reboot => {
                api::InstanceState::Rebooting
            }
        };

        if server_guard.is_some() {
            let last = (*ctx.state_sender.borrow()).clone();
            let _ = ctx.state_sender.send(api::InstanceStateMonitorResponse {
                gen: last.gen + 1,
                state,
            });
            Ok(HttpResponseUpdatedNoContent {})
        } else {
            Err(HttpError::for_internal_error(
                "No matching instance".to_string(),
            ))
        }
    }

    // Server context that is only valid once an instance has been ensured.
    struct FakeInstance {
        id: Uuid,
    }

    // Server context for the fake Propolis server.
    struct PropolisContext {
        inner: Mutex<Option<FakeInstance>>,
        state_sender: watch::Sender<api::InstanceStateMonitorResponse>,
        state_receiver: watch::Receiver<api::InstanceStateMonitorResponse>,
    }

    // Creates a fake propolis server on port 12400.
    fn fake_propolis_server() -> HttpServer<PropolisContext> {
        let config_dropshot = ConfigDropshot {
            bind_address: "127.0.0.1:12400".parse().unwrap(),
            ..Default::default()
        };
        let config_logging =
            ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info };
        let log = config_logging
            .to_logger("fake_propolis_server")
            .map_err(|error| format!("failed to create logger: {}", error))
            .unwrap();

        let mut api = ApiDescription::new();
        api.register(instance_ensure).unwrap();
        api.register(instance_get).unwrap();
        api.register(instance_state_monitor).unwrap();
        api.register(instance_state_put).unwrap();
        let (tx, rx) = watch::channel(api::InstanceStateMonitorResponse {
            gen: 0,
            state: api::InstanceState::Creating,
        });
        let api_context = PropolisContext {
            inner: Mutex::new(None),
            state_sender: tx,
            state_receiver: rx,
        };

        HttpServerStarter::new(&config_dropshot, api, api_context, &log)
            .map_err(|error| format!("failed to create server: {}", error))
            .unwrap()
            .start()
    }

    // Sets the expectation for the invocations to the underlying system made on
    // behalf of instance creation.
    //
    // Spawns a task which acts as a fake propolis server - this server acts in
    // lieu of the "real" propolis server which would be launched.
    async fn execute_instance_start(inst: &Instance, ticket: InstanceTicket) {
        let mut seq = mockall::Sequence::new();
        let dladm_find_physical_ctx = MockDladm::find_physical_context();
        dladm_find_physical_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(PhysicalLink("physical".to_string())));

        let dladm_create_vnic_ctx = MockDladm::create_vnic_context();
        dladm_create_vnic_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|phys, vnic, _maybe_mac, _maybe_vlan| {
                assert_eq!(phys.0, "physical");
                assert_eq!(vnic, control_vnic_name(0));
                Ok(())
            });

        let zone_configure_propolis_ctx =
            MockZones::configure_propolis_zone_context();
        zone_configure_propolis_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, zone, vnics| {
                assert_eq!(zone, propolis_zone_name(&test_propolis_uuid()));
                assert_eq!(vnics.len(), 1);
                assert_eq!(vnics[0], control_vnic_name(0));
                Ok(())
            });

        let zone_clone_from_base_propolis_ctx =
            MockZones::clone_from_base_propolis_context();
        zone_clone_from_base_propolis_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|zone| {
                assert_eq!(zone, propolis_zone_name(&test_propolis_uuid()));
                Ok(())
            });

        let zone_boot_ctx = MockZones::boot_context();
        zone_boot_ctx.expect().times(1).in_sequence(&mut seq).returning(
            |zone| {
                assert_eq!(zone, propolis_zone_name(&test_propolis_uuid()));
                Ok(())
            },
        );

        let wait_for_service_ctx =
            crate::illumos::svc::wait_for_service_context();
        wait_for_service_ctx.expect().times(1).in_sequence(&mut seq).returning(
            |zone, fmri| {
                assert_eq!(
                    zone.unwrap(),
                    propolis_zone_name(&test_propolis_uuid())
                );
                assert_eq!(fmri, "svc:/milestone/network:default");
                Ok(())
            },
        );

        let zone_create_address_ctx = MockZones::create_address_context();
        zone_create_address_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|zone, iface| {
                assert_eq!(zone, propolis_zone_name(&test_propolis_uuid()));
                assert_eq!(iface, interface_name(&control_vnic_name(0)));
                Ok("127.0.0.1/24".parse().unwrap())
            });

        let zone_run_propolis_ctx = MockZones::run_propolis_context();
        zone_run_propolis_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|zone, id, addr| {
                assert_eq!(zone, propolis_zone_name(&test_propolis_uuid()));
                assert_eq!(id, &test_propolis_uuid());
                assert_eq!(
                    addr,
                    &"127.0.0.1:12400".parse::<SocketAddr>().unwrap()
                );
                Ok(())
            });

        let wait_for_service_ctx =
            crate::illumos::svc::wait_for_service_context();
        wait_for_service_ctx.expect().times(1).in_sequence(&mut seq).returning(
            |zone, fmri| {
                let id = test_propolis_uuid();
                assert_eq!(zone.unwrap(), propolis_zone_name(&id));
                assert_eq!(
                    fmri,
                    format!("{}:{}", service_name(), instance_name(&id))
                );
                tokio::task::spawn(async {
                    fake_propolis_server().await.unwrap();
                });
                Ok(())
            },
        );

        // This invocation triggers all the aforementioned expectations.
        inst.start(ticket, None).await.unwrap();
    }

    // Returns a future which resolves when a state transition is reached.
    fn expect_state_transition(
        nexus_client: &mut MockNexusClient,
        seq: &mut mockall::Sequence,
        expected_state: InstanceState,
    ) -> impl core::future::Future<Output = ()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        nexus_client
            .expect_cpapi_instances_put()
            .times(1)
            .in_sequence(seq)
            .return_once(move |id, state| {
                assert_eq!(id, &test_uuid());
                assert_eq!(
                    InstanceState::from(&state.run_state),
                    expected_state
                );
                tx.send(()).unwrap();
                Ok(())
            });
        rx.map(|r| r.unwrap())
    }

    fn logger() -> Logger {
        dropshot::ConfigLogging::StderrTerminal {
            level: dropshot::ConfigLoggingLevel::Info,
        }
        .to_logger("test-logger")
        .unwrap()
    }

    fn new_initial_instance() -> InstanceHardware {
        InstanceHardware {
            runtime: InstanceRuntimeState {
                run_state: InstanceState::Creating,
                sled_uuid: Uuid::new_v4(),
                propolis_uuid: test_propolis_uuid(),
                dst_propolis_uuid: None,
                propolis_addr: None,
                migration_uuid: None,
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_mebibytes_u32(512),
                hostname: "myvm".to_string(),
                gen: Generation::new(),
                time_updated: Utc::now(),
            },
            nics: vec![],
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
    async fn start_then_stop() {
        let log = logger();
        let nic_id_allocator = IdAllocator::new();
        let mut nexus_client = MockNexusClient::default();

        // Set expectations about what will be seen (and when) by Nexus before
        // the test begins. We no longer have mutable access to "nexus_client"
        // when "Instance::new" is invoked, so we have to prepare early.
        let mut seq = mockall::Sequence::new();
        let create_fut = expect_state_transition(
            &mut nexus_client,
            &mut seq,
            InstanceState::Creating,
        );
        let run_fut = expect_state_transition(
            &mut nexus_client,
            &mut seq,
            InstanceState::Running,
        );
        let stop_fut = expect_state_transition(
            &mut nexus_client,
            &mut seq,
            InstanceState::Stopped,
        );

        let ticket = InstanceTicket::null(test_uuid());

        // Initialize and start the instance.
        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            nic_id_allocator,
            new_initial_instance(),
            None,
            Arc::new(nexus_client),
        )
        .unwrap();
        execute_instance_start(&inst, ticket).await;
        create_fut.await;

        // Start running the instance.
        inst.transition(InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Running,
            migration_params: None,
        })
        .await
        .unwrap();
        run_fut.await;

        // Stop the instance.
        // This terminates the zone to preserve per-sled resources.
        let zone_halt_and_remove_ctx = MockZones::halt_and_remove_context();
        zone_halt_and_remove_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Ok(()));
        let dladm_delete_vnic_ctx = Dladm::delete_vnic_context();
        dladm_delete_vnic_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|vnic| {
                assert_eq!(vnic, control_vnic_name(0));
                Ok(())
            });
        inst.transition(InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Stopped,
            migration_params: None,
        })
        .await
        .unwrap();
        stop_fut.await;
    }

    #[tokio::test]
    #[serial_test::serial]
    #[should_panic(
        expected = "Propolis client should be initialized before usage"
    )]
    async fn transition_before_start() {
        let log = logger();
        let nic_id_allocator = IdAllocator::new();
        let nexus_client = MockNexusClient::default();

        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            nic_id_allocator,
            new_initial_instance(),
            None,
            Arc::new(nexus_client),
        )
        .unwrap();

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
