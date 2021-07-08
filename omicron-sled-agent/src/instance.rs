//! API for controlling a single instance.

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
use futures::lock::Mutex;
use omicron_common::dev::poll;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
#[cfg(not(test))]
use omicron_common::NexusClient;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceState};
use crate::instance_manager::InstanceTicket;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zone::Zones};
#[cfg(test)]
use crate::illumos::{dladm::MockDladm as Dladm, zone::MockZones as Zones};

use crate::illumos::svc::wait_for_service;
use crate::illumos::{dladm::VNIC_PREFIX, zone::ZONE_PREFIX};
use propolis_client::Client as PropolisClient;

// Issues read-only, idempotent HTTP requests at propolis until it responds with
// an acknowledgement. This provides a hacky mechanism to "wait until the HTTP
// server is serving requests".
//
// TODO: Plausibly we could use SMF to accomplish this goal in a less hacky way.
async fn wait_for_http_server(
    log: &Logger,
    client: &PropolisClient,
) -> Result<(), ApiError> {
    poll::wait_for_condition::<(), std::convert::Infallible, _, _>(
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
                    warn!(log, "waiting for http server, saw error: {}", value);
                    return Err(poll::CondCheckError::NotYet);
                }
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(10),
    )
    .await
    .map_err(|e| ApiError::InternalError {
        message: format!("Failed to wait for HTTP server: {}", e),
    })
}

fn service_name() -> &'static str {
    "svc:/system/illumos/propolis-server"
}

fn instance_name(id: &Uuid) -> String {
    format!("vm-{}", id.to_string())
}

fn fmri_name(id: &Uuid) -> String {
    format!("{}:{}", service_name(), instance_name(id))
}

fn zone_name(id: &Uuid) -> String {
    format!("{}{}", ZONE_PREFIX, id)
}

fn vnic_name(runtime_id: u64) -> String {
    format!("{}{}", VNIC_PREFIX, runtime_id)
}

fn interface_name(vnic_name: &str) -> String {
    format!("{}/omicron", vnic_name)
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

struct InstanceInner {
    log: Logger,
    runtime_id: u64,
    properties: propolis_client::api::InstanceProperties,
    state: InstanceState,
    nexus_client: Arc<NexusClient>,
    running_state: Option<RunningState>,
}

impl InstanceInner {
    fn id(&self) -> &Uuid {
        &self.properties.id
    }

    async fn observe_state(
        &mut self,
        state: propolis_client::api::InstanceState,
    ) -> Result<Reaction, ApiError> {
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
            .notify_instance_updated(&self.properties.id, self.state.current())
            .await?;

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
    ) -> Result<(), ApiError> {
        self.running_state
            .as_ref()
            .expect("Propolis client should be initialized before usage")
            .client
            .instance_state_put(self.properties.id, request)
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to set state of instance: {}", e),
            })
    }

    async fn ensure(&self) -> Result<(), ApiError> {
        let request = propolis_client::api::InstanceEnsureRequest {
            properties: self.properties.clone(),
        };
        self.running_state
            .as_ref()
            .expect("Propolis client should be initialized before usage")
            .client
            .instance_ensure(&request)
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to ensure instance: {}", e),
            })?;
        Ok(())
    }

    async fn take_action(
        &self,
        action: InstanceAction,
    ) -> Result<Reaction, ApiError> {
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
            runtime_id: u64,
            initial_runtime: ApiInstanceRuntimeState,
            nexus_client: Arc<NexusClient>,
        ) -> Result<Self, ApiError>;
        pub async fn start(&self, ticket: InstanceTicket) -> Result<(), ApiError>;
        pub async fn transition(
            &self,
            target: ApiInstanceRuntimeStateRequested,
        ) -> Result<ApiInstanceRuntimeState, ApiError>;
    }
    impl Clone for Instance {
        fn clone(&self) -> Self;
    }
}

impl Instance {
    /// Creates a new (not yet running) instance object.

    /// Arguments:
    /// * `log`: Logger for dumping debug information.
    /// * `id`: UUID of the instance to be created.
    /// * `runtime_id`: A unique (to the sled) numeric ID which may be used to
    /// refer to a VNIC. (This exists because of a restriction on VNIC name
    /// lengths, otherwise the UUID would be used instead).
    /// * `initial_runtime`: State of the instance at initialization time.
    /// * `nexus_client`: Connection to Nexus, used for sending notifications.
    pub fn new(
        log: Logger,
        id: Uuid,
        runtime_id: u64,
        initial_runtime: ApiInstanceRuntimeState,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, ApiError> {
        let instance = InstanceInner {
            log: log.new(o!("instance id" => id.to_string())),
            runtime_id,
            // NOTE: Mostly lies.
            properties: propolis_client::api::InstanceProperties {
                id,
                name: "Test instance".to_string(),
                description: "Test description".to_string(),
                image_id: Uuid::nil(),
                bootrom_id: Uuid::nil(),
                memory: 256,
                vcpus: 2,
            },
            state: InstanceState::new(initial_runtime),
            nexus_client,
            running_state: None,
        };

        let inner = Arc::new(Mutex::new(instance));

        Ok(Instance { inner })
    }

    /// Begins the execution of the instance's service (Propolis).
    pub async fn start(&self, ticket: InstanceTicket) -> Result<(), ApiError> {
        let mut inner = self.inner.lock().await;
        let log = &inner.log;

        // Create the VNIC which will be attached to the zone.
        let physical_dl = Dladm::find_physical()?;
        info!(log, "Saw physical DL: {}", physical_dl);

        // It would be preferable to use the UUID of the instance as a component
        // of the "per-Zone, control plane VNIC", but VNIC names are somewhat
        // restrictive. They must end with numerics, and they must be less than
        // 32 characters.
        //
        // Instead, we just use a per-agent incrementing number.
        let vnic_name = vnic_name(inner.runtime_id);
        Dladm::create_vnic(&physical_dl, &vnic_name)?;
        info!(log, "Created vnic: {}", vnic_name);

        // Create a zone for the propolis instance, using the previously
        // configured VNIC.
        let zname = zone_name(inner.id());
        Zones::configure_child_zone(&log, &zname, &vnic_name)?;
        info!(log, "Configured child zone: {}", zname);

        // Clone the zone from a base zone (faster than installing) and
        // boot it up.
        Zones::clone_from_base(&zname)?;
        info!(log, "Cloned child zone: {}", zname);
        Zones::boot(&zname)?;
        info!(log, "Booted zone: {}", zname);

        // Wait for the network services to come online, then create an address.
        wait_for_service(Some(&zname), "svc:/milestone/network:default")
            .await?;
        info!(log, "Network milestone ready for {}", zname);

        let ip = Zones::create_address(&zname, &interface_name(&vnic_name))?;
        info!(log, "Created address {} for zone: {}", ip, zname);

        // Run Propolis in the Zone.
        let port = 12400;
        let server_addr = SocketAddr::new(ip.addr(), port);
        Zones::run_propolis(&zname, inner.id(), &server_addr)?;
        info!(log, "Started propolis in zone: {}", zname);

        // This isn't strictly necessary - we wait for the HTTP server below -
        // but it helps distinguish "online in SMF" from "responding to HTTP
        // requests".
        let fmri = fmri_name(inner.id());
        wait_for_service(Some(&zname), &fmri).await?;

        let client = Arc::new(PropolisClient::new(
            server_addr,
            log.new(o!("component" => "propolis-client")),
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&log, &client).await?;

        inner.running_state =
            Some(RunningState { client, ticket, monitor_task: None });

        // Ensure the instance exists in the Propolis Server before we start
        // using it.
        inner.ensure().await?;

        // Monitor propolis for state changes in the background.
        let self_clone = self.clone();
        inner.running_state.as_mut().unwrap().monitor_task =
            Some(tokio::task::spawn(async move {
                let r = self_clone.monitor_state_task().await;
                let log = &self_clone.inner.lock().await.log;
                match r {
                    Err(e) => warn!(log, "State monitoring task failed: {}", e),
                    Ok(()) => info!(log, "State monitoring task complete"),
                }
            }));

        Ok(())
    }

    // Terminate the Propolis service.
    async fn stop(&self) -> Result<(), ApiError> {
        let mut inner = self.inner.lock().await;

        let zname = zone_name(inner.id());
        warn!(inner.log, "Halting and removing zone: {}", zname);
        Zones::halt_and_remove(&zname).unwrap();
        inner.running_state.as_mut().unwrap().ticket.terminate();

        Ok(())
    }

    // Monitors propolis until explicitly told to disconnect.
    //
    // Intended to be spawned in a tokio task within [`Instance::start`].
    async fn monitor_state_task(&self) -> Result<(), ApiError> {
        // Grab the UUID and Propolis Client before we start looping, so we
        // don't need to contend the lock to access them in steady state.
        //
        // They aren't modified after being initialized, so it's fine to grab
        // a copy.
        let (id, client) = {
            let inner = self.inner.lock().await;
            let id = *inner.id();
            let client = inner.running_state.as_ref().unwrap().client.clone();
            (id, client)
        };

        let mut gen = 0;
        loop {
            // State monitoring always returns the most recent state/gen pair
            // known to Propolis.
            let response = client
                .instance_state_monitor(id, gen)
                .await
                .map_err(|e| ApiError::InternalError {
                    message: format!("Failed to monitor propolis: {}", e),
                })?;
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
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        let mut inner = self.inner.lock().await;
        if let Some(action) =
            inner.state.request_transition(target.run_state)?
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
    use crate::illumos::{dladm::MockDladm, zone::MockZones};
    use crate::mocks::MockNexusClient;
    use chrono::Utc;
    use dropshot::{
        endpoint, ApiDescription, ConfigDropshot, ConfigLogging,
        ConfigLoggingLevel, HttpError, HttpResponseCreated, HttpResponseOk,
        HttpResponseUpdatedNoContent, HttpServer, HttpServerStarter, Path,
        RequestContext, TypedBody,
    };
    use futures::future::FutureExt;
    use omicron_common::model::{
        ApiGeneration, ApiInstanceRuntimeState, ApiInstanceState,
        ApiInstanceStateRequested,
    };
    use propolis_client::api;
    use tokio::sync::watch;

    static INST_UUID_STR: &str = "e398c5d5-5059-4e55-beac-3a1071083aaa";

    fn test_uuid() -> Uuid {
        INST_UUID_STR.parse().unwrap()
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
            return Ok(HttpResponseCreated(api::InstanceEnsureResponse {}));
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
            .returning(|| Ok("physical".to_string()));

        let dladm_create_vnic_ctx = MockDladm::create_vnic_context();
        dladm_create_vnic_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|phys, vnic| {
                assert_eq!(phys, "physical");
                assert_eq!(vnic, vnic_name(0));
                Ok(())
            });

        let zone_configure_child_ctx =
            MockZones::configure_child_zone_context();
        zone_configure_child_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, zone, vnic| {
                assert_eq!(zone, zone_name(&test_uuid()));
                assert_eq!(vnic, vnic_name(0));
                Ok(())
            });

        let zone_clone_from_base_ctx = MockZones::clone_from_base_context();
        zone_clone_from_base_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|zone| {
                assert_eq!(zone, zone_name(&test_uuid()));
                Ok(())
            });

        let zone_boot_ctx = MockZones::boot_context();
        zone_boot_ctx.expect().times(1).in_sequence(&mut seq).returning(
            |zone| {
                assert_eq!(zone, zone_name(&test_uuid()));
                Ok(())
            },
        );

        let wait_for_service_ctx =
            crate::illumos::svc::wait_for_service_context();
        wait_for_service_ctx.expect().times(1).in_sequence(&mut seq).returning(
            |zone, fmri| {
                assert_eq!(zone.unwrap(), zone_name(&test_uuid()));
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
                assert_eq!(zone, zone_name(&test_uuid()));
                assert_eq!(iface, interface_name(&vnic_name(0)));
                Ok("127.0.0.1/24".parse().unwrap())
            });

        let zone_run_propolis_ctx = MockZones::run_propolis_context();
        zone_run_propolis_ctx
            .expect()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|zone, id, addr| {
                assert_eq!(zone, zone_name(&test_uuid()));
                assert_eq!(id, &test_uuid());
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
                let id = test_uuid();
                assert_eq!(zone.unwrap(), zone_name(&id));
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
        inst.start(ticket).await.unwrap();
    }

    // Returns a future which resolves when a state transition is reached.
    fn expect_state_transition(
        nexus_client: &mut MockNexusClient,
        seq: &mut mockall::Sequence,
        expected_state: ApiInstanceState,
    ) -> impl core::future::Future<Output = ()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        nexus_client
            .expect_notify_instance_updated()
            .times(1)
            .in_sequence(seq)
            .return_once(move |id, state| {
                assert_eq!(id, &test_uuid());
                assert_eq!(state.run_state, expected_state);
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

    fn new_runtime_state() -> ApiInstanceRuntimeState {
        ApiInstanceRuntimeState {
            run_state: ApiInstanceState::Creating,
            sled_uuid: Uuid::new_v4(),
            gen: ApiGeneration::new(),
            time_updated: Utc::now(),
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
        let runtime_id = 0;
        let mut nexus_client = MockNexusClient::default();

        // Set expectations about what will be seen (and when) by Nexus before
        // the test begins. We no longer have mutable access to "nexus_client"
        // when "Instance::new" is invoked, so we have to prepare early.
        let mut seq = mockall::Sequence::new();
        let create_fut = expect_state_transition(
            &mut nexus_client,
            &mut seq,
            ApiInstanceState::Creating,
        );
        let run_fut = expect_state_transition(
            &mut nexus_client,
            &mut seq,
            ApiInstanceState::Running,
        );
        let stop_fut = expect_state_transition(
            &mut nexus_client,
            &mut seq,
            ApiInstanceState::Stopped,
        );

        let ticket = InstanceTicket::null(test_uuid());

        // Initialize and start the instance.
        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            runtime_id,
            new_runtime_state(),
            Arc::new(nexus_client),
        )
        .unwrap();
        execute_instance_start(&inst, ticket).await;
        create_fut.await;

        // Start running the instance.
        inst.transition(ApiInstanceRuntimeStateRequested {
            run_state: ApiInstanceStateRequested::Running,
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
            .returning(|_| Ok(()));
        inst.transition(ApiInstanceRuntimeStateRequested {
            run_state: ApiInstanceStateRequested::Stopped,
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
        let runtime_id = 0;
        let nexus_client = MockNexusClient::default();

        let inst = Instance::new(
            log.clone(),
            test_uuid(),
            runtime_id,
            new_runtime_state(),
            Arc::new(nexus_client),
        )
        .unwrap();

        // Trying to transition before the instance has been initialized will
        // result in a panic.
        inst.transition(ApiInstanceRuntimeStateRequested {
            run_state: ApiInstanceStateRequested::Running,
        })
        .await
        .unwrap();
    }
}
