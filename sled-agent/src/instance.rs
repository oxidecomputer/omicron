// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::{
    instance::{Action as InstanceAction, InstanceStates, PROPOLIS_PORT},
    vlan::VlanID,
};
use crate::illumos::running_zone::{InstalledZone, RunningZone};
use crate::illumos::svc::wait_for_service;
use crate::illumos::vnic::VnicAllocator;
use crate::illumos::zone::{AddressRequest, PROPOLIS_ZONE_PREFIX};
use crate::instance_manager::InstanceTicket;
use crate::nexus::NexusClient;
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

    #[error(transparent)]
    RunningZone(#[from] crate::illumos::running_zone::Error),
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
}

struct InstanceInner {
    id: Uuid,

    log: Logger,

    // Properties visible to Propolis
    properties: propolis_client::api::InstanceProperties,

    // NIC-related properties
    vnic_allocator: VnicAllocator,
    requested_nics: Vec<NetworkInterface>,
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
        let PropolisSetup { client, running_zone } = setup;

        // TODO: Store slot in NetworkInterface, make this more stable.
        let nics = self
            .requested_nics
            .iter()
            .enumerate()
            .map(|(i, _)| propolis_client::api::NetworkInterfaceRequest {
                name: running_zone.get_guest_vnics()[i].name().to_string(),
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

        self.running_state = Some(RunningState {
            client,
            ticket,
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
            vnic_allocator: VnicAllocator,
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

#[cfg_attr(test, allow(dead_code))]
impl Instance {
    /// Creates a new (not yet running) instance object.
    ///
    /// Arguments:
    /// * `log`: Logger for dumping debug information.
    /// * `id`: UUID of the instance to be created.
    /// * `vnic_allocator`: A unique (to the sled) ID generator to
    /// refer to a VNIC. (This exists because of a restriction on VNIC name
    /// lengths, otherwise the UUID would be used instead).
    /// * `initial`: State of the instance at initialization time.
    /// * `nexus_client`: Connection to Nexus, used for sending notifications.
    /// * `vlan`: An optional VLAN ID for tagging guest VNICs.
    // TODO: This arg list is getting a little long; can we clean this up?
    pub fn new(
        log: Logger,
        id: Uuid,
        vnic_allocator: VnicAllocator,
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
            vnic_allocator,
            requested_nics: initial.nics,
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
        // Instantiate all guest-requested VNICs.
        //
        // TODO: Ideally, we'd allocate VNICs directly within the Zone.
        // However, this seems to have been a SmartOS feature which
        // doesn't exist in illumos.
        //
        // https://github.com/illumos/ipd/blob/master/ipd/0003/README.md
        let physical_dl = Dladm::find_physical()?;
        let guest_nics = inner
            .requested_nics
            .clone()
            .into_iter()
            .map(|nic| {
                inner
                    .vnic_allocator
                    .new_guest(&physical_dl, Some(nic.mac), inner.vlan)
                    .map_err(|e| e.into())
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // Create a zone for the propolis instance, using the previously
        // configured VNICs.
        let zname = propolis_zone_name(inner.propolis_id());

        let installed_zone = InstalledZone::install(
            &inner.log,
            &inner.vnic_allocator,
            "propolis-server",
            Some(&inner.propolis_id().to_string()),
            /* dataset= */ &[],
            &[
                zone::Device { name: "/dev/vmm/*".to_string() },
                zone::Device { name: "/dev/vmmctl".to_string() },
                zone::Device { name: "/dev/viona".to_string() },
            ],
            guest_nics,
        )
        .await?;

        let running_zone = RunningZone::boot(installed_zone).await?;
        let network = running_zone.ensure_address(AddressRequest::Dhcp).await?;
        info!(inner.log, "Created address {} for zone: {}", network, zname);

        // Run Propolis in the Zone.
        let server_addr = SocketAddr::new(network.ip(), PROPOLIS_PORT);

        running_zone.run_cmd(&[
            crate::illumos::zone::SVCCFG,
            "import",
            "/var/svc/manifest/site/propolis-server/manifest.xml",
        ])?;

        running_zone.run_cmd(&[
            crate::illumos::zone::SVCCFG,
            "-s",
            "system/illumos/propolis-server",
            "setprop",
            &format!("config/server_addr={}", server_addr),
        ])?;

        running_zone.run_cmd(&[
            crate::illumos::zone::SVCCFG,
            "-s",
            "svc:/system/illumos/propolis-server",
            "add",
            &format!("vm-{}", inner.propolis_id()),
        ])?;

        running_zone.run_cmd(&[
            crate::illumos::zone::SVCADM,
            "enable",
            "-t",
            &format!(
                "svc:/system/illumos/propolis-server:vm-{}",
                inner.propolis_id()
            ),
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

        let client = Arc::new(PropolisClient::new(
            server_addr,
            inner.log.new(o!("component" => "propolis-client")),
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&inner.log, &client).await?;

        Ok(PropolisSetup { client, running_zone })
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
    use crate::mocks::MockNexusClient;
    use chrono::Utc;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState,
    };
    use omicron_common::api::internal::{
        nexus::InstanceRuntimeState, sled_agent::InstanceStateRequested,
    };
    static INST_UUID_STR: &str = "e398c5d5-5059-4e55-beac-3a1071083aaa";
    static PROPOLIS_UUID_STR: &str = "ed895b13-55d5-4e0b-88e9-3f4e74d0d936";

    fn test_uuid() -> Uuid {
        INST_UUID_STR.parse().unwrap()
    }

    fn test_propolis_uuid() -> Uuid {
        PROPOLIS_UUID_STR.parse().unwrap()
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
    #[should_panic(
        expected = "Propolis client should be initialized before usage"
    )]
    async fn transition_before_start() {
        let log = logger();
        let vnic_allocator = VnicAllocator::new("Test".to_string());
        let nexus_client = MockNexusClient::default();

        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            vnic_allocator,
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
