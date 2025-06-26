// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration test running a producer that exports a few basic metrics.

// Copyright 2023 Oxide Computer Company

use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::Metric;
use oximeter::MetricsError;
use oximeter::Producer;
use oximeter::Target;
use oximeter::types::Cumulative;
use oximeter::types::ProducerRegistry;
use oximeter::types::Sample;
use oximeter_producer::Config;
use oximeter_producer::LogConfig;
use oximeter_producer::Server;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// Run an example oximeter metric producer.
#[derive(Parser)]
struct Args {
    /// The address to use for the producer server.
    #[arg(long, default_value = "[::1]:0")]
    address: SocketAddr,

    /// The address of nexus at which to register.
    #[arg(long, default_value = "[::1]:12221")]
    nexus: SocketAddr,
}

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
            data.push(Sample::new(&self.vm, cpu)?);
        }
        // Yield the available samples.
        Ok(Box::new(data.into_iter()))
    }
}

#[expect(
    clippy::disallowed_macros,
    reason = "using tokio::main avoids an `oxide-tokio-rt` dependency for \
        examples"
)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let log = LogConfig::Config(ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Debug,
    });
    let registry = ProducerRegistry::new();
    let producer = CpuBusyProducer::new(4);
    registry.register_producer(producer).unwrap();
    let server_info = ProducerEndpoint {
        id: registry.producer_id(),
        kind: ProducerKind::Service,
        address: args.address,
        interval: Duration::from_secs(10),
    };
    let config = Config {
        server_info,
        registration_address: Some(args.nexus),
        default_request_body_max_bytes: 2048,
        log,
    };
    let server = Server::with_registry(registry, &config)
        .context("failed to create producer")?;
    server.serve_forever().await.context("server failed")
}
