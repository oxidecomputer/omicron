// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A load generator for testing oximeter.
//!
//! Runs an oximeter producer server that produces synthetic metrics, with
//! configurable label cardinality and churn. For simplicity, we model series
//! as a single target, with `label_cardinality` different metrics. To simulate
//! churn, we configure a `series_lifetime_seconds` argument, and expire series
//! after a random interval in 0..2*series_lifetime_seconds.

use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::Metric;
use oximeter::Target;
use oximeter::types::Cumulative;
use oximeter::types::ProducerRegistry;
use oximeter_producer::Config;
use oximeter_producer::LogConfig;
use oximeter_producer::Server;
use oximeter_types::MetricsError;
use oximeter_types::Producer;
use oximeter_types::Sample;
use rand::Rng;
use uuid::Uuid;

fn main() -> anyhow::Result<()> {
    oxide_tokio_rt::run(main_impl())
}

async fn main_impl() -> anyhow::Result<()> {
    let args = Cli::parse();
    let log = LogConfig::Config(ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Debug,
    });

    let registry = ProducerRegistry::new();

    let producer = LoadTestProducer::new(
        args.label_cardinality,
        args.series_lifetime_seconds,
    );
    registry
        .register_producer(producer)
        .context("failed to register producer")?;
    let server_info = ProducerEndpoint {
        id: registry.producer_id(),
        kind: ProducerKind::Service,
        address: args.address,
        interval: Duration::from_secs(args.interval_seconds),
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

#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    /// The address to use for the producer server.
    #[arg(long, default_value = "[::1]:0")]
    address: SocketAddr,

    /// The address of nexus at which to register.
    #[arg(long, default_value = "[::1]:12221")]
    nexus: SocketAddr,

    /// The cardinality (i.e. number of unique values) of the metric labels.
    #[arg(long, short, default_value_t = 100)]
    label_cardinality: u64,

    /// The mean lifetime of each metric series.
    #[arg(long, short, default_value_t = 300, value_parser = clap::value_parser!(u64).range(10..))]
    series_lifetime_seconds: u64,

    /// The poll interval to register with nexus.
    #[arg(long, short, default_value_t = 10)]
    interval_seconds: u64,
}

#[derive(Debug)]
struct LoadTestProducer {
    label_cardinality: u64,
    series_lifetime_seconds: u64,
    target: LoadTestTarget,
    series: Vec<LoadTestSeries>,
}

impl LoadTestProducer {
    fn new(label_cardinality: u64, series_lifetime_seconds: u64) -> Self {
        let target = LoadTestTarget::new_random();
        let series = vec![];
        Self { label_cardinality, series_lifetime_seconds, target, series }
    }

    // Ensure that the producer has fresh values, dropping and replacing expired series and incrementing datum values.
    fn refresh(&mut self) {
        let now = Instant::now();
        self.series.retain(|item| item.expiry >= now);
        self.series.extend(
            (0..(self.label_cardinality - self.series.len() as u64))
                .map(|_| self.build_series())
                .collect::<Vec<_>>(),
        );

        self.series.iter_mut().for_each(|series| {
            series.metric.metric_value += rand::rng().random_range(0..100);
        });
    }

    fn build_series(&self) -> LoadTestSeries {
        LoadTestSeries {
            metric: LoadTestMetric::new_random(),
            // Expire each series after a random interval in
            // 0..2*series_lifetime_seconds. This maintains the churn frequency
            // configured by the user without expiring all series at the same
            // time.
            expiry: Instant::now()
                + rand::rng().random_range(
                    Duration::ZERO
                        ..2 * Duration::from_secs(self.series_lifetime_seconds),
                ),
        }
    }
}

impl Producer for LoadTestProducer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        self.refresh();
        let samples = self
            .series
            .iter()
            .map(|series| Sample::new(&self.target, &series.metric))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Box::new(samples.into_iter()))
    }
}

#[derive(Debug)]
struct LoadTestSeries {
    metric: LoadTestMetric,
    expiry: Instant,
}

#[derive(Debug, Clone, Target)]
struct LoadTestTarget {
    target_id: Uuid,
    target_label: String,
}

impl LoadTestTarget {
    fn new_random() -> Self {
        Self { target_id: Uuid::new_v4(), target_label: random_string(16) }
    }
}

#[derive(Debug, Clone, Metric)]
struct LoadTestMetric {
    metric_id: Uuid,
    metric_label: String,
    #[datum]
    metric_value: Cumulative<u64>,
}

impl LoadTestMetric {
    fn new_random() -> Self {
        Self {
            metric_id: Uuid::new_v4(),
            metric_label: random_string(16),
            metric_value: Cumulative::new(rand::rng().random_range(0..100)),
        }
    }
}

fn random_string(length: usize) -> String {
    rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
