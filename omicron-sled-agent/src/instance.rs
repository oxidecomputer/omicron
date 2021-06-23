//! Sled agent implementation

use futures::lock::Mutex;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::NexusClient;
use slog::Logger;
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use uuid::Uuid;

use crate::instance_manager::InstanceTicket;
use crate::common::instance::{Action as InstanceAction, InstanceState};

// TODO: Do a pass, possibly re-naming these things...
use crate::zone::{
    boot_zone, create_address, create_vnic, clone_zone_from_base,
    configure_child_zone, get_ip_address, find_physical_data_link,
    run_propolis,
};
use propolis_client::Client as PropolisClient;

// TODO(https://www.illumos.org/issues/13837): This is a hack;
// remove me when when fixed. Ideally, the ".synchronous()" argument
// to "svcadm enable" would wait for the service to be online, which
// would simplify all this stuff.
//
// Ideally, when "svccfg add" returns, these properties would be set,
// but unfortunately, they are not. This means that when we invoke
// "svcadm enable -s", it's possible for critical restarter
// properties to not exist when the command returns.
//
// We workaround this by querying for these properties in a loop.
async fn wait_for_service_to_come_online(
    log: &Logger,
    zone: Option<&str>,
    fmri: &str,
) -> Result<(), ApiError> {
    info!(log, "awaiting restarter/state online");
    let name = smf::PropertyName::new("restarter", "state").unwrap();
    let mut attempts = 0;
    loop {
        let mut p = smf::Properties::new();
        let properties = {
            if let Some(zone) = zone {
                p.zone(zone)
            } else {
                &mut p
            }
        };
        let result = properties.lookup().run(&name, &fmri);

        match result {
            Ok(value) => {
                if value.value()
                    == &smf::PropertyValue::Astring("online".to_string())
                {
                    return Ok(());
                }
                warn!(
                    log,
                    "restarter/state property exists for {}, but it is: {:?}",
                    fmri,
                    value
                );
            }
            Err(value) => {
                warn!(
                    log,
                    "No restarter/state property of {} - {}", fmri, value
                );
                if attempts > 50 {
                    return Err(ApiError::InternalError {
                        message: format!(
                            "Service never turned online: {}",
                            value
                        ),
                    });
                }
                sleep(Duration::from_millis(500)).await;
                attempts += 1;
            }
        }
    }
}

// Issues read-only, idempotent HTTP requests at propolis until it responds with
// an acknowledgemen. This provides a hacky mechanism to "wait until the HTTP
// server is serving requests".
//
// TODO: Plausibly we could use SMF to accomplish this goal in a less hacky way.
async fn wait_for_http_server(
    log: &Logger,
    client: &PropolisClient,
) -> Result<(), ApiError> {
    info!(log, "awaiting HTTP server");
    let mut attempts = 0;
    loop {
        // This request is nonsensical - we don't expect an instance to be using
        // the nil UUID - but getting a response that isn't a connection-based
        // error informs us the HTTP server is alive.
        match client.instance_get(Uuid::nil()).await {
            Ok(_) => return Ok(()),
            Err(value) => {
                if let propolis_client::Error::Status(_) = &value {
                    info!(
                        log,
                        "successfully waited for HTTP server initialization"
                    );
                    // This means the propolis server responded to our garbage
                    // request, instead of a connection error.
                    return Ok(());
                }
                warn!(log, "waiting for http server, saw error: {}", value);
                if attempts > 50 {
                    return Err(ApiError::InternalError {
                        message: format!(
                            "Http server never came online: {}",
                            value
                        ),
                    });
                }
                sleep(Duration::from_millis(50)).await;
                attempts += 1;
            }
        }
    }
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

// Action to be taken by the Sled Agent after monitoring Propolis for
// state changes.
enum Reaction {
    Continue,
    Terminate,
}

// State associated with a running instance.
struct RunningState {
    client: Arc<PropolisClient>,
    ticket: InstanceTicket,
    monitor_task: Option<JoinHandle<()>>,
}

// TODO: The things which are "Options" - is that just "running state"?
struct InstanceInner {
    log: Logger,
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
        info!(self.log, "Next recommended action: {:?}", action);

        // Notify Nexus of the state change.
        self.nexus_client
            .notify_instance_updated(&self.properties.id, self.state.current())
            .await?;

        if let Some(action) = action {
            // Take the next action, if any.
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
            .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to ensure instance: {}", e),
            }
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
            },
            InstanceAction::Stop => {
                propolis_client::api::InstanceStateRequested::Stop
            },
            InstanceAction::Reboot => {
                propolis_client::api::InstanceStateRequested::Reboot
            },
            InstanceAction::Destroy => {
                // Unlike the other actions, which update the Propolis state,
                // the "destroy" action indicates that the service should be
                // terminated.
                info!(self.log, "take_action: Taking the Destroy action");
                return Ok(Reaction::Terminate);
            },
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

impl Instance {
    /// Creates a new (not yet running) instance object.
    pub fn new(
        log: Logger,
        id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, ApiError> {
        let instance = InstanceInner {
            log: log.new(o!("instance id" => id.to_string())),
            // TODO: Mostly lies.
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
    pub async fn start(
        &self,
        ticket: InstanceTicket,
    ) -> Result<(), ApiError> {
        let mut inner = self.inner.lock().await;
        let log = &inner.log;
        // TODO: Launch in a Zone.

        // XXX Hacks below

        // TODO: NOT HERE! On setup, we should keep an eye out for straggler
        // resources, including:
        // - VNICs
        // - Zones
        // - VMMs
        // - Other???

        // Create the VNIC which will be attached to the zone.
        let physical_dl = find_physical_data_link()?;
        info!(log, "Saw physical DL: {}", physical_dl);
        let vnic_name = format!("vnic_prop5"); // XXX Gonna need patching
        create_vnic(&physical_dl, &vnic_name)?;
        info!(log, "Created vnic: {}", vnic_name);

        // Specify the IP address we'll use when communicating with the
        // device.
        let ip_net = get_ip_address(&physical_dl)?;
        info!(log, "Saw IP address: {}", ip_net);

        // TODO: Start by hardcoding it, allocate it properly later.
        // let ip = ip_net.hosts().next().unwrap();
        let ip = std::net::IpAddr::V4(
            std::net::Ipv4Addr::from_str("192.168.1.5").unwrap()
        );
        info!(log, "Using IP address {}", ip);

        // Create a zone for the propolis instance, using the previously
        // configured VNIC.
        let zone_name = format!("propolis-{}", inner.id());
        configure_child_zone(&log, &zone_name, &vnic_name)?;
        info!(log, "Configured child zone: {}", zone_name);

        // Clone the zone from a base zone (faster than installing) and
        // boot it up.
        clone_zone_from_base(&zone_name)?;
        info!(log, "Cloned child zone: {}", zone_name);
        boot_zone(&zone_name)?;
        info!(log, "Booted zone: {}", zone_name);

        // Wait for the network services to come online, then create an address.
        wait_for_service_to_come_online(&log, Some(&zone_name), "svc:/milestone/network:default").await?;
        info!(log, "Network milestone ready for {}", zone_name);

        let interface_name = format!("{}/omicron", vnic_name);
        create_address(&zone_name, &ip, &interface_name)?;
        info!(log, "Created address {} for zone: {}", ip, zone_name);

        // Run Propolis in the Zone.
        let port = 12400;
        let server_addr = SocketAddr::new(ip, port);
        run_propolis(&zone_name, inner.id(), &server_addr)?;
        info!(log, "Started propolis in zone: {}", zone_name);

        // XXX Hacks above

/*
        // Launch a new SMF service running Propolis.
        let manifest = "/opt/oxide/propolis-server/pkg/manifest.xml";

        // Import and enable the service as distinct steps.
        //
        // This allows the service to remain "transient", which avoids
        // it being auto-initialized by SMF across reboots.
        // Instead, the bootstrap agent remains responsible for verifying
        // and enabling the services on each access.
        info!(log, "Instance::new Importing {}", manifest);
        smf::Config::import().run(manifest).map_err(|e| {
            ApiError::InternalError {
                message: format!("Cannot import propolis SMF manifest: {}", e),
            }
        })?;

        info!(log, "Instance::new adding service: {}", service_name());
        smf::Config::add(service_name()).run(&instance_name(inner.id())).map_err(|e| {
            ApiError::InternalError {
                message: format!("Cannot create propolis SMF service: {}", e),
            }
        })?;

        let fmri = fmri_name(inner.id());
        info!(log, "Instance::new enabling FMRI: {}", fmri);
        // TODO(https://www.illumos.org/issues/13837): Ideally, this should call
        // ".synchronous()", but it doesn't, because of an SMF bug.
        smf::Adm::new()
            .enable()
            .temporary()
            .run(smf::AdmSelection::ByPattern(&[&fmri]))
            .map_err(|e| ApiError::InternalError {
                message: format!("Cannot enable propolis SMF service: {}", e),
            })?;
*/
        // XXX ???
//        wait_for_service_to_come_online(&log, Some(&zone_name), &fmri).await?;

        // TODO: IP ADDRESS??? Be less hardcoded?
        // let address = "127.0.0.1:12400";
        let client = Arc::new(PropolisClient::new(
            server_addr,
            log.clone(),
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&log, &client).await?;

        inner.running_state = Some(
            RunningState {
                client,
                ticket,
                monitor_task: None,
            }
        );

        // Ensure the instance exists in the Propolis Server before we start
        // using it.
        inner.ensure().await?;

        // Monitor propolis for state changes in the background.
        let self_clone = self.clone();
        inner.running_state.as_mut().unwrap().monitor_task =
            Some(tokio::task::spawn(async move {
                if let Err(e) = self_clone.monitor_state_task().await {
                    let log = &self_clone.inner.lock().await.log;
                    warn!(log, "State monitoring task failed: {}", e);
                }
            }));

        Ok(())
    }

    // Terminate the Propolis service.
    async fn stop(
        &self
    ) -> Result<(), ApiError> {
        let mut inner = self.inner.lock().await;
        let log = &inner.log;
        let fmri = fmri_name(inner.id());

        info!(log, "Instance::stop disabling service: {}", fmri);
        smf::Adm::new()
            .disable()
            .synchronous()
            .run(smf::AdmSelection::ByPattern(&[&fmri]))
            .map_err(|e| ApiError::InternalError {
                message: format!("Cannot disable propolis SMF service: {}", e),
            })?;

        info!(log, "Instance::stop deleting service: {}", fmri);
        smf::Config::delete()
            .run(fmri)
            .map_err(|e| ApiError::InternalError {
                message: format!("Cannot delete config for SMF service: {}", e),
            })?;

        info!(log, "Instance::stop removing self from instances map");
        inner.running_state.as_mut().unwrap().ticket.terminate();

        Ok(())
    }

    // Monitors propolis until explicitly told to disconnect.
    //
    // Intended to be spawned in a tokio task within [`Instance::start`].
    async fn monitor_state_task(
        &self,
    ) -> Result<(), ApiError> {
        // Grab the UUID and Propolis Client before we start looping, so we
        // don't need to contend the lock to access them in steady state.
        //
        // They aren't modified after being initialized, so it's fine to grab
        // a copy.
        let (id, client) = {
            let inner = self.inner.lock().await;
            let id = inner.id().clone();
            let client = inner.running_state.as_ref().unwrap().client.clone();
            (id, client)
        };

        let mut gen = 0;
        loop {
            // State monitor always returns the most recent state/gen pair
            // known to Propolis.
            let response = client
                .instance_state_monitor(id, gen)
                .await
                .map_err(|e| ApiError::InternalError {
                    message: format!(
                        "Failed to monitor propolis: {}",
                        e
                    ),
                })?;
           let reaction = self.inner
                .lock()
                .await
                .observe_state(response.state)
                .await?;

            match reaction {
                Reaction::Continue => {},
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
