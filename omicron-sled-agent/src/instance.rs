//! Sled agent implementation

use futures::lock::Mutex;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::NexusClient;
use slog::Logger;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceState};
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
    fmri: &str,
) -> Result<(), ApiError> {
    info!(log, "awaiting restarter/state online");
    let name = smf::PropertyName::new("restarter", "state").unwrap();
    let mut attempts = 0;
    loop {
        let result = smf::Properties::new().lookup().run(&name, &fmri);

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
                sleep(Duration::from_millis(1)).await;
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
                sleep(Duration::from_millis(1)).await;
                attempts += 1;
            }
        }
    }
}

struct InstanceInternal {
    log: Logger,
    properties: propolis_client::api::InstanceProperties,
    state: InstanceState,
    client: Arc<PropolisClient>,
    nexus_client: Arc<NexusClient>,
}

impl InstanceInternal {
    async fn observe_state(
        &mut self,
        state: propolis_client::api::InstanceState,
    ) -> Result<(), ApiError> {
        info!(self.log, "Observing new propolis state: {:?}", state);
        // Update the Sled Agent's internal state machine.
        let action = self.state.observe_transition(&state);
        info!(self.log, "Next recommended action: {:?}", action);

        // Notify Nexus of the state change.
        self.nexus_client
            .notify_instance_updated(&self.properties.id, self.state.current())
            .await?;

        // Take the next action, if any.
        if let Some(action) = action {
            self.take_action(action).await?;
        }
        Ok(())
    }

    async fn propolis_state_put(
        &self,
        request: propolis_client::api::InstanceStateRequested,
    ) -> Result<(), ApiError> {
        self.client
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
        self.client.instance_ensure(&request).await.map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to ensure instance: {}", e),
            }
        })?;
        Ok(())
    }

    pub async fn take_action(
        &self,
        action: InstanceAction,
    ) -> Result<(), ApiError> {
        info!(self.log, "Taking action: {:#?}", action);
        match action {
            InstanceAction::Run => {
                self.propolis_state_put(
                    propolis_client::api::InstanceStateRequested::Run,
                )
                .await?;
                info!(self.log, "Finished taking RUN action");
            }
            InstanceAction::Stop => {
                self.propolis_state_put(
                    propolis_client::api::InstanceStateRequested::Stop,
                )
                .await?;
                info!(self.log, "Finished taking STOP action");
            }
            InstanceAction::Reboot => {
                self.propolis_state_put(
                    propolis_client::api::InstanceStateRequested::Reboot,
                )
                .await?;
                info!(self.log, "Finished taking REBOOT action");
            }
            InstanceAction::Destroy => {
                self.propolis_state_put(
                    propolis_client::api::InstanceStateRequested::Stop,
                )
                .await?;
                info!(self.log, "Finished taking DESTROY (stop) action");
                // TODO: Need a way to clean up
            }
        }
        Ok(())
    }
}

/// Describes state for a single running Propolis server.
pub struct Instance {
    internal: Arc<Mutex<InstanceInternal>>,
    join_handle: JoinHandle<()>,
}

impl Instance {
    /// Creates a new instance object.
    pub async fn new(
        log: Logger,
        id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, ApiError> {
        // TODO: Launch in a Zone?

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

        let service = "svc:/system/illumos/propolis-server";
        let instance = format!("vm-{}", id.to_string());
        let fmri = format!("{}:{}", service, instance);
        info!(log, "Instance::new adding service: {}", service);
        smf::Config::add(service).run(&instance).map_err(|e| {
            ApiError::InternalError {
                message: format!("Cannot create propolis SMF service: {}", e),
            }
        })?;

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
        wait_for_service_to_come_online(&log, &fmri).await?;

        // TODO: IP ADDRESS??? Be less hardcoded?
        let address = "127.0.0.1:12400";
        let client = Arc::new(PropolisClient::new(
            address.parse().unwrap(),
            log.clone(),
        ));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&log, &client).await?;

        let instance = InstanceInternal {
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
            client: client.clone(),
            nexus_client,
        };

        // Ensure the instance exists in the Propolis Server before we start
        // using it.
        instance.ensure().await?;

        let internal = Arc::new(Mutex::new(instance));

        // Monitor propolis for state changes in the background.
        let internal_clone = internal.clone();
        let join_handle = tokio::task::spawn(async move {
            let mut gen = 0;
            loop {
                // State monitor always returns the most recent state/gen pair
                // known to Propolis.
                let response =
                    client.instance_state_monitor(id, gen).await.unwrap();
                internal_clone
                    .lock()
                    .await
                    .observe_state(response.state)
                    .await
                    .unwrap();

                // Update the generation number we're asking for, to ensure the
                // Propolis will only return more recent values.
                gen = response.gen + 1;
            }
        });

        Ok(Instance { internal, join_handle })
    }

    /// Transitions an instance object to a new state, taking any actions
    /// necessary to perform state transitions.
    ///
    /// Returns the new state after starting the transition.
    pub async fn transition(
        &self,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        let mut inner = self.internal.lock().await;
        if let Some(action) = inner.state.request_transition(&target)? {
            info!(
                &inner.log,
                "transition to {:?}; action: {:#?}", target, action
            );
            inner.take_action(action).await?;
        }
        Ok(inner.state.current().clone())
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}
