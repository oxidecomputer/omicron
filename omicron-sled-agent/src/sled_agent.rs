/// Sled agent implementation
use futures::lock::Mutex;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::NexusClient;
use slog::Logger;
use std::collections::BTreeMap;
use std::time::Duration;
use std::sync::Arc;
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
                    "restarter/state property exists for {}, but it is: {:#?}",
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

// TODO: Plausibly we could use SMF to accomplish this goal in a less hacky way.
//
// In the meantime, we throw read-only HTTP requests at propolis until it
// responds to us, even with an error.
async fn wait_for_http_server(
    log: &Logger,
    client: &PropolisClient,
) -> Result<(), ApiError> {
    info!(log, "awaiting HTTP server");
    let mut attempts = 0;
    loop {
        match client.instance_get(Uuid::nil()).await {
            Ok(_) => return Ok(()),
            Err(value) => {
                if let propolis_client::Error::Status(_) = &value {
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
    async fn observe_state(&mut self, state: propolis_client::api::InstanceState) -> Result<(), ApiError> {
        // Update the Sled Agent's internal state machine.
        let action = self.state.observe_transition(&state);

        // Notify Nexus of the state change.
        self.nexus_client.notify_instance_updated(
            &self.properties.id,
            self.state.current(),
        ).await?;

        // Take the next action, if any.
        if let Some(action) = action {
             self.take_action(action).await?;
        }
        Ok(())
    }

    async fn propolis_state_put(&self, request: propolis_client::api::InstanceStateRequested) -> Result<(), ApiError> {
        self.client
            .instance_state_put(self.properties.id, request)
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!(
                    "Failed to set state of instance: {}",
                    e
                ),
            })
    }

    async fn ensure(&self) -> Result<(), ApiError> {
        let request = propolis_client::api::InstanceEnsureRequest {
            properties: self.properties.clone(),
        };
        self.client.instance_ensure(&request).await.map_err(
            |e| ApiError::InternalError {
                message: format!(
                    "Failed to ensure instance: {}",
                    e
                ),
            },
        )?;
        Ok(())
    }

    async fn take_action(&self, action: InstanceAction) -> Result<(), ApiError> {
        info!(self.log, "Taking action: {:#?}", action);
        match action {
            InstanceAction::Run => {
                self.propolis_state_put(propolis_client::api::InstanceStateRequested::Run).await?;
                info!(self.log, "Finished taking RUN action");
            }
            InstanceAction::Stop => {
                self.propolis_state_put(propolis_client::api::InstanceStateRequested::Stop).await?;
                info!(self.log, "Finished taking STOP action");
            },
            InstanceAction::Reboot => {
                self.propolis_state_put(propolis_client::api::InstanceStateRequested::Reboot).await?;
                info!(self.log, "Finished taking REBOOT action");
            },
            InstanceAction::Destroy => todo!("DESTROY HAS NOT BEEN IMPLEMENTED YET"),
        }
        Ok(())
    }

}

// TODO: Could easily refactor this elsewhere...
struct Instance {
    internal: Arc<Mutex<InstanceInternal>>,
    join_handle: JoinHandle<()>,
}

impl Instance {
    async fn new(
        log: Logger,
        id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, ApiError> {
        // TODO: Probably needs more config?
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
        let client = Arc::new(PropolisClient::new(address.parse().unwrap(), log.clone()));

        // Although the instance is online, the HTTP server may not be running
        // yet. Wait for it to respond to requests, so users of the instance
        // don't need to worry about initialization races.
        wait_for_http_server(&log, &client).await?;

        let instance =
            InstanceInternal {
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
        // TODO: Store this handle; await it on drop (or cancel?)
        let internal_clone = internal.clone();
        let join_handle = tokio::task::spawn(async move {
            let mut gen = 0;
            loop {
                // State monitor always returns the most recent state/gen pair
                // known to Propolis.
                let response = client.instance_state_monitor(id, gen).await.unwrap();
                internal_clone.lock().await.observe_state(response.state).await.unwrap();

                // Update the generation number we're asking for, to ensure the
                // Propolis will only return more recent values.
                gen = response.gen + 1;
            }
        });

        Ok(Instance { internal, join_handle })
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}

pub struct SledAgent {
    log: Logger,
    nexus_client: Arc<NexusClient>,
    instances: Mutex<BTreeMap<Uuid, Instance>>,
}

impl SledAgent {
    pub fn new(
        id: &Uuid,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "created sled agent"; "id" => ?id);

        SledAgent {
            log,
            nexus_client,
            instances: Mutex::new(BTreeMap::new()),
        }
    }

    /// Idempotently ensures that the given API Instance (described by
    /// `api_instance`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn instance_ensure(
        self: &Arc<Self>,
        instance_id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        info!(&self.log, "instance_ensure {} -> {:#?}", instance_id, target);
        let mut instances = self.instances.lock().await;

        let mut instance = {
            if let Some(instance) = instances.get_mut(&instance_id) {
                // Instance already exists.
                info!(&self.log, "instance already exists");
                instance
            } else {
                // Instance does not exist - create it.
                info!(&self.log, "new instance");
                let instance_log =
                    self.log.new(o!("instance" => instance_id.to_string()));
                instances.insert(
                    instance_id,
                    Instance::new(instance_log, instance_id, initial_runtime, self.nexus_client.clone()).await?,
                );
                instances.get_mut(&instance_id).unwrap()
            }
        }.internal.lock().await;

        if let Some(action) = instance.state.request_transition(&target)? {
            info!(
                &self.log,
                "transition to {:#?}; action: {:#?}", target, action
            );
            instance.take_action(action).await?;
        }
        Ok(instance.state.current().clone())
    }

    /// Idempotently ensures that the given API Disk (described by `api_disk`)
    /// is attached (or not) as specified.
    pub async fn disk_ensure(
        self: &Arc<Self>,
        _disk_id: Uuid,
        _initial_state: ApiDiskRuntimeState,
        _target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        todo!("Disk attachment not yet implemented");
    }
}
