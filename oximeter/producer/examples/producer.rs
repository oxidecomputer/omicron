// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration test running a producer that exports a few basic metrics.
// Copyright 2021 Oxide Computer Company

use chrono::{DateTime, Utc};
use dropshot::{ConfigDropshot, ConfigLogging, ConfigLoggingLevel};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use oximeter::{
    types::{Cumulative, Sample},
    Metric, MetricsError, Producer, Target,
};
use oximeter_producer::{Config, Server};
use std::time::Duration;
use uuid::Uuid;

/// Example target describing a virtual machine.
#[derive(Debug, Clone, Target)]
pub struct VirtualMachine {
    pub project_id: Uuid,
    pub instance_id: Uuid,
}

/// Example metric describing the cumulative time a vCPU is busy, by CPU ID.
#[derive(Debug, Clone, Metric)]
pub struct CpuBusy {
    pub cpu_id: i64,
    #[datum]
    pub busy: Cumulative<f64>,
}

/// A simple struct for tracking busy time of a set of vCPUs, relative to a start time.
#[derive(Debug, Clone)]
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
                    busy: Cumulative::default(),
                })
                .collect(),
        }
    }
}

impl Producer for CpuBusyProducer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        let timestamp = Utc::now();
        let mut data = Vec::with_capacity(self.cpu.len());
        for cpu in self.cpu.iter_mut() {
            // Get total elapsed time, add the diff to the cumulative counter.
            //
            // This is a bit silly, since we have to artificially create a diff and then sum. This
            // is part of how we get type-safety in producing metrics, but it may need some work.
            let elapsed = (timestamp - self.start_time)
                .to_std()
                .map_err(|e| MetricsError::DatumError(e.to_string()))?
                .as_secs_f64();
            let datum = cpu.datum_mut();
            *datum += elapsed - datum.value();
            data.push(Sample::new(&self.vm, cpu));
        }
        // Yield the available samples.
        Ok(Box::new(data.into_iter()))
    }
}

#[tokio::main]
async fn main() {
    let address = "[::1]:0".parse().unwrap();
    let dropshot_config = ConfigDropshot {
        bind_address: address,
        request_body_max_bytes: 2048,
        tls: None,
    };
    let logging_config =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug };
    let server_info = ProducerEndpoint {
        id: Uuid::new_v4(),
        address,
        base_route: "/collect".to_string(),
        interval: Duration::from_secs(10),
    };
    let config = Config {
        server_info,
        registration_address: "127.0.0.1:12221".parse().unwrap(),
        dropshot_config,
        logging_config,
    };
    let server = Server::start(&config).await.unwrap();
    let producer = CpuBusyProducer::new(4);
    server.registry().register_producer(producer).unwrap();
    server.serve_forever().await.unwrap();
}
