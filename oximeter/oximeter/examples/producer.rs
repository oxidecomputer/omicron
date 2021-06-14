//! Integration test running a producer that exports a few basic metrics.
// Copyright 2021 Oxide Computer Company

use std::time::Duration;

use chrono::{DateTime, Utc};
use dropshot::{ConfigDropshot, ConfigLogging, ConfigLoggingLevel};
use omicron_common::model::ProducerEndpoint;
use oximeter::collect::{
    ProducerServer, ProducerServerConfig, RegistrationInfo,
};
use oximeter::{
    types::{Cumulative, Sample},
    Error, Metric, Producer, Target,
};
use uuid::Uuid;

/// Example target describing a virtual machine.
#[derive(Target)]
pub struct VirtualMachine {
    pub project_id: Uuid,
    pub instance_id: Uuid,
}

/// Example metric describing the cumulative time a vCPU is busy, by CPU ID.
#[derive(Metric)]
pub struct CpuBusy {
    pub cpu_id: i64,
    pub value: Cumulative<f64>,
}

/// A simple struct for tracking busy time of a set of vCPUs, relative to a start time.
pub struct CpuBusyProducer {
    start_time: DateTime<Utc>,
    vm: VirtualMachine,
    cpu: Vec<CpuBusy>,
}

impl CpuBusyProducer {
    /// Construct a producer to track a number of vCPUs.
    pub fn new(n_cpus: usize) -> Self {
        assert!(n_cpus > 0);
        Self {
            start_time: Utc::now(),
            vm: VirtualMachine {
                project_id: Uuid::new_v4(),
                instance_id: Uuid::new_v4(),
            },
            cpu: (0..n_cpus)
                .map(|i| CpuBusy {
                    cpu_id: i as _,
                    value: Cumulative::default(),
                })
                .collect(),
        }
    }
}

impl Producer for CpuBusyProducer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, Error> {
        let timestamp = Utc::now();
        let mut data = Vec::with_capacity(self.cpu.len());
        for cpu in self.cpu.iter_mut() {
            // Get total elapsed time, add the diff to the cumulative counter.
            //
            // This is a bit silly, since we have to artificially create a diff and then sum. This
            // is part of how we get type-safety in producing metrics, but it may need some work.
            let elapsed = (timestamp - self.start_time)
                .to_std()
                .map_err(|e| Error::ProductionError(e.to_string()))?
                .as_secs_f64();
            cpu.value += elapsed - cpu.value.value();
            data.push(Sample::new(&self.vm, cpu, None));
        }
        // Yield the available samples.
        Ok(Box::new(data.into_iter()))
    }
}

#[tokio::main]
async fn main() {
    let address = "[::1]:12225".parse().unwrap();
    let dropshot_config =
        ConfigDropshot { bind_address: address, request_body_max_bytes: 2048 };
    let logging_config =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug };
    let registration_info =
        RegistrationInfo::new("127.0.0.1:12221", "/metrics/producers");
    let server_info = ProducerEndpoint {
        id: Uuid::new_v4().into(),
        address,
        base_route: "/collect".to_string(),
        interval: Duration::from_secs(10),
    };
    let config = ProducerServerConfig {
        server_info,
        registration_info,
        dropshot_config,
        logging_config,
    };
    let server = ProducerServer::start(&config).await.unwrap();
    let producer = CpuBusyProducer::new(4);
    server.collector().register_producer(Box::new(producer)).unwrap();
    server.serve_forever().await.unwrap();
}
