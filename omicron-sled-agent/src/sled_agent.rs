/// Sled agent implementation
use chrono::{DateTime, Utc};
use futures::lock::Mutex;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ProducerId;
use omicron_common::NexusClient;
use slog::Logger;
use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::{self, Arc};
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceState};
use oximeter::{
    collect,
    types::{Cumulative, Sample},
    Metric, Target,
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
fn wait_for_service_to_come_online(
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
                std::thread::sleep(std::time::Duration::from_millis(1));
                attempts += 1;
            }
        }
    }
}

// TODO: Could easily refactor this elsewhere...
struct Instance {
    id: Uuid,
    state: InstanceState,
    client: PropolisClient,
}

impl Instance {
    fn new(
        log: Logger,
        id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
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

        wait_for_service_to_come_online(&log, &fmri)?;

        // TODO: IP ADDRESS??? Be less hardcoded?
        let address = "127.0.0.1:12400";
        let client = PropolisClient::new(address.parse().unwrap(), log);

        Ok(Instance { id, state: InstanceState::new(initial_runtime), client })
    }
}

pub struct SledAgent {
    id: Uuid,
    log: Logger,
    nexus_client: Arc<NexusClient>,
    instances: Mutex<BTreeMap<Uuid, Instance>>,
    cpu_producer: CpuMonitor,
    metric_collector: collect::Collector,
}

impl SledAgent {
    pub fn new(
        id: &Uuid,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "created sled agent"; "id" => ?id);
        let cpu_producer = CpuMonitor::new();
        let uptime_tracker = UptimeTracker::new(
            OxideService { name: "sled-agent".into(), id: *id },
            ServiceUpTime { value: Cumulative::new(0.0) },
        );
        let metric_collector =
            collect::Collector::with_id(ProducerId { producer_id: *id });
        metric_collector.register_producer(Box::new(uptime_tracker)).unwrap();
        metric_collector
            .register_producer(Box::new(cpu_producer.clone()))
            .unwrap();

        SledAgent {
            id: *id,
            log,
            nexus_client,
            instances: Mutex::new(BTreeMap::new()),
            cpu_producer,
            metric_collector,
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

        let instance = {
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
                    Instance::new(instance_log, instance_id, initial_runtime)?,
                );
                instances.get_mut(&instance_id).unwrap()
            }
        };

        if let Some(action) = instance.state.request_transition(&target)? {
            info!(
                &self.log,
                "transition to {:#?}; action: {:#?}", target, action
            );

            // TODO: BACKOFF INSTEAD
            std::thread::sleep(core::time::Duration::from_secs(2));

            match action {
                InstanceAction::Run => {
                    // TODO: This is mostly lies right now.
                    let request = propolis_client::api::InstanceEnsureRequest {
                        properties: propolis_client::api::InstanceProperties {
                            generation_id: 0,
                            id: instance_id,
                            name: "Test instance".to_string(),
                            description: "Test description".to_string(),
                            image_id: instance_id,
                            bootrom_id: instance_id,
                            memory: 256,
                            vcpus: 2,
                        },
                    };

                    println!("Ensuring VM instance...");
                    instance.client.instance_ensure(&request).await.map_err(
                        |e| ApiError::InternalError {
                            message: format!(
                                "Failed to ensure instance: {}",
                                e
                            ),
                        },
                    )?;

                    println!("Setting VM state...");
                    let request =
                        propolis_client::api::InstanceStateRequested::Run;
                    instance
                        .client
                        .instance_state_put(instance_id, request)
                        .await
                        .map_err(|e| ApiError::InternalError {
                            message: format!(
                                "Failed to ensure instance: {}",
                                e
                            ),
                        })?;
                    println!("OK - RUN COMPLETE");

                    // Add metrics for this new VM
                    self.cpu_producer.add_instance(instance_id, 2);
                }
                InstanceAction::Stop => todo!(),
                InstanceAction::Reboot => todo!(),
                InstanceAction::Destroy => todo!(),
            }
        }
        Ok(instance.state.current().clone())
    }

    /// Idempotently ensures that the given API Disk (described by `api_disk`)
    /// is attached (or not) as specified.
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_state: ApiDiskRuntimeState,
        target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        todo!("Disk attachment not yet implemented");
    }

    /// Collect all available metrics from this agent
    pub async fn collect_metrics(
        self: &Arc<Self>,
        producer_id: ProducerId,
    ) -> Result<collect::ProducerResults, ApiError> {
        collect::collect(&self.metric_collector, producer_id)
            .await
            .map_err(|e| ApiError::internal_error(&e.to_string()))
    }
}

// Target identifying metrics from a single virtual machine
#[derive(Target, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VirtualMachine {
    pub id: Uuid,
}

// Metric with cumulative time a single vCPU is busy (in seconds)
#[derive(Clone, Metric)]
pub(crate) struct CpuBusy {
    pub cpu_id: i64,
    pub value: Cumulative<f64>,
}

#[derive(Clone)]
pub(crate) struct CpuMonitor {
    start_time: DateTime<Utc>,
    pub vms: Arc<sync::Mutex<BTreeMap<VirtualMachine, Vec<CpuBusy>>>>,
}

impl CpuMonitor {
    pub fn new() -> Self {
        Self {
            start_time: Utc::now(),
            vms: Arc::new(sync::Mutex::new(BTreeMap::new())),
        }
    }

    pub fn add_instance(&self, id: Uuid, n_cpus: usize) {
        let target = VirtualMachine { id };
        match self.vms.lock().unwrap().entry(target) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                let mut cpus = Vec::with_capacity(n_cpus);
                for cpu_id in 0..n_cpus {
                    cpus.push(CpuBusy {
                        cpu_id: cpu_id as _,
                        value: Cumulative::new(0.0),
                    });
                }
                entry.insert(cpus);
            }
        }
    }
}

impl oximeter::Producer for CpuMonitor {
    fn produce(
        &mut self,
    ) -> Result<Box<(dyn Iterator<Item = Sample> + 'static)>, oximeter::Error>
    {
        let mut vms = self.vms.lock().unwrap();
        // Je suis mendant
        let now = Utc::now();
        let accumulated_time: f64 =
            (self.start_time - now).num_nanoseconds().unwrap() as f64 / 1e9;
        let mut samples = Vec::with_capacity(vms.len());
        for (vm, cpus) in vms.iter_mut() {
            for cpu in cpus.iter_mut() {
                cpu.value += accumulated_time - cpu.value.value();
                samples.push(Sample::new(vm, cpu, Some(now)));
            }
        }
        Ok(Box::new(samples.into_iter()))
    }
}

#[derive(Target, Clone)]
pub(crate) struct OxideService {
    name: String,
    id: Uuid,
}

#[derive(Clone, Metric)]
pub(crate) struct ServiceUpTime {
    value: Cumulative<f64>,
}

#[derive(Clone)]
pub(crate) struct UptimeTracker {
    start_time: DateTime<Utc>,
    pub service: OxideService,
    pub uptime: ServiceUpTime,
}

impl UptimeTracker {
    pub fn new(service: OxideService, uptime: ServiceUpTime) -> Self {
        Self { start_time: Utc::now(), service, uptime }
    }
}

impl oximeter::Producer for UptimeTracker {
    fn produce(
        &mut self,
    ) -> Result<Box<(dyn Iterator<Item = Sample> + 'static)>, oximeter::Error>
    {
        let now = Utc::now();
        let diff =
            (now - self.start_time).num_nanoseconds().unwrap() as f64 / 1e9;
        self.uptime.value += diff - self.uptime.value.value();
        let sample = Sample::new(&self.service, &self.uptime, Some(now));
        Ok(Box::new(vec![sample].into_iter()))
    }
}
