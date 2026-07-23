// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A load generator for testing oximeter.
//!
//! Runs an oximeter producer server that produces synthetic metrics, with
//! configurable series cardinality and churn. For simplicity, we model series
//! as a single target. The user configures the data type and available
//! cardinality of each field, as well as the overall desired series
//! cardinality. To simulate churn, we configure a `series_lifetime_seconds`
//! argument, and expire series after a random interval in
//! 0..2*series_lifetime_seconds.
//!
//! Note: we model both the cardinality of each field, and of the set of series
//! overall. Field cardinality defines the set of possible label values for the
//! simulation, and series cardinality is the number of series from that set
//! that we emit on any given `produce()`.

use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::DatumType;
use oximeter::FieldType;
use oximeter::FieldValue;
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
use rand::seq::IndexedRandom;
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

    let available_cardinality =
        args.field.iter().fold(1, |acc, field| acc * field.cardinality);
    if args.series_cardinality > available_cardinality {
        anyhow::bail!(
            "requested series cardinality {:?} is greater than available cardinality {:?}",
            args.series_cardinality,
            available_cardinality,
        );
    }

    let producer = LoadTestProducer::new(
        args.field,
        args.series_cardinality,
        args.series_lifetime_seconds,
        args.batches_per_poll,
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

    /// The fields to use, described as `<NAME>:<DATA_TYPE>:<CARDINALITY>`
    /// (component_kind:String:10, address:IpAddr:100, etc.).
    #[arg(long, short, required = true)]
    field: Vec<FieldRequest>,

    /// The number of series that should exist at a given moment.
    #[arg(long, default_value_t = 100)]
    series_cardinality: u64,

    /// The mean lifetime of each metric series.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(10..))]
    series_lifetime_seconds: u64,

    /// The number of values to emit for each series per poll. Used to simulate
    /// producers like mgs, which accumulate multiple samples per series
    /// between polls.
    #[arg(long, short, default_value_t = 1)]
    batches_per_poll: u64,

    /// The poll interval to register with nexus.
    #[arg(long, short, default_value_t = 10)]
    interval_seconds: u64,
}

#[derive(Clone, Debug)]
struct FieldRequest {
    name: String,
    field_type: FieldType,
    cardinality: u64,
}

impl FromStr for FieldRequest {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = value.splitn(3, ":").collect();
        let [name, raw_type, raw_cardinality] = parts.as_slice() else {
            return Err(format!("invalid field request {value:?}"));
        };

        let field_type = FieldType::from_str(raw_type)
            .map_err(|err| format!("invalid field request: {err}"))?;

        let cardinality = raw_cardinality
            .parse::<u64>()
            .map_err(|err| format!("invalid cardinality: {err}"))?;

        Ok(FieldRequest { name: name.to_string(), field_type, cardinality })
    }
}

#[derive(Debug)]
struct FieldSpec {
    field_type: FieldType,
    name: &'static str,
    pool: Vec<FieldValue>,
}

impl FieldSpec {
    fn new(req: &FieldRequest) -> Self {
        Self {
            name: req.name.clone().leak(),
            field_type: req.field_type,
            pool: build_pool(req),
        }
    }
}

// Build a pool of `FieldValue` with the requested data type and cardinality.
fn build_pool(req: &FieldRequest) -> Vec<FieldValue> {
    (0..req.cardinality)
        .map(|idx| match req.field_type {
            FieldType::String => {
                FieldValue::String(format!("field-value-{}", idx).into())
            }
            FieldType::Uuid => {
                FieldValue::Uuid(Uuid::from_u128(u128::from(idx)))
            }
            FieldType::IpAddr => {
                FieldValue::IpAddr(Ipv6Addr::from(u128::from(idx)).into())
            }
            FieldType::I64 => FieldValue::I64(idx as i64),
            FieldType::I32 => FieldValue::I32(idx as i32),
            FieldType::I16 => FieldValue::I16(idx as i16),
            FieldType::I8 => FieldValue::I8(idx as i8),
            FieldType::U64 => FieldValue::U64(idx),
            FieldType::U32 => FieldValue::U32(idx as u32),
            FieldType::U16 => FieldValue::U16(idx as u16),
            FieldType::U8 => FieldValue::U8(idx as u8),
            FieldType::Bool => FieldValue::Bool(idx > 0),
        })
        .collect::<Vec<_>>()
}

// Configuration and state for the test metrics producer. Responsible for
// generating samples according to the configured fields and churn.
#[derive(Debug)]
struct LoadTestProducer {
    metric_spec: Arc<MetricSpec>,
    series_cardinality: u64,
    series_lifetime_seconds: u64,
    batches_per_poll: u64,
    target: LoadTestTarget,
    series: Vec<LoadTestSeries>,
}

impl LoadTestProducer {
    fn new(
        field_reqs: Vec<FieldRequest>,
        series_cardinality: u64,
        series_lifetime_seconds: u64,
        batches_per_poll: u64,
    ) -> Self {
        let target = LoadTestTarget::new_random();
        let series = vec![];

        let field_specs = field_reqs
            .iter()
            .map(|req| FieldSpec::new(req))
            .collect::<Vec<_>>();
        let field_names =
            field_specs.iter().map(|spec| spec.name).collect::<Vec<_>>().leak();

        let metric_spec = Arc::new(MetricSpec {
            name: "load_test_metric",
            field_names,
            field_specs,
        });

        Self {
            series_lifetime_seconds,
            series_cardinality,
            batches_per_poll,
            target,
            series,
            metric_spec,
        }
    }

    // Ensure that the producer has fresh series, dropping and replacing
    // expired series to match the cardinality and expiry invariants.
    fn refresh(&mut self) {
        let now = Instant::now();
        self.series.retain(|item| item.expiry >= now);
        self.series.extend(
            (0..(self.series_cardinality - self.series.len() as u64))
                .map(|_| self.build_series())
                .collect::<Vec<_>>(),
        );
    }

    fn build_series(&self) -> LoadTestSeries {
        let mut rng = rand::rng();

        // For each field, choose a random value from its pool.
        let field_values = self
            .metric_spec
            .field_specs
            .iter()
            .map(|spec| {
                spec.pool
                    .choose(&mut rng)
                    .expect("choose random pool value")
                    .clone()
            })
            .collect::<Vec<_>>();

        LoadTestSeries {
            metric_spec: Arc::clone(&self.metric_spec),
            field_values,
            datum: Cumulative::new(rng.random_range(0..100)),
            // Expire each series after a random interval in
            // 0..2*series_lifetime_seconds. This maintains the churn frequency
            // configured by the user without expiring all series at the same
            // time.
            expiry: Instant::now()
                + rng.random_range(
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
        let mut samples = Vec::with_capacity(
            self.series.len() * self.batches_per_poll as usize,
        );
        for _ in 0..self.batches_per_poll {
            for series in self.series.iter_mut() {
                series.datum += rand::rng().random_range(0..100);
                samples.push(Sample::new(&self.target, series)?);
            }
        }
        Ok(Box::new(samples.into_iter()))
    }
}

#[derive(Debug)]
struct MetricSpec {
    name: &'static str,
    field_specs: Vec<FieldSpec>,
    field_names: &'static [&'static str],
}

// Represents a single series for the load test metric.
#[derive(Debug)]
struct LoadTestSeries {
    metric_spec: Arc<MetricSpec>,
    field_values: Vec<FieldValue>,
    datum: Cumulative<u64>,
    expiry: Instant,
}
// Our test metric has a variable number of fields of variable types, so we
// can't define it as a struct that derives `Metric`. Instead, we implement
// the `Metric` trait ourselves.
impl Metric for LoadTestSeries {
    type Datum = Cumulative<u64>;

    fn name(&self) -> &'static str {
        self.metric_spec.name
    }

    fn field_names(&self) -> &'static [&'static str] {
        self.metric_spec.field_names
    }

    fn field_types(&self) -> Vec<FieldType> {
        self.metric_spec
            .field_specs
            .iter()
            .map(|spec| spec.field_type)
            .collect()
    }

    fn field_values(&self) -> Vec<FieldValue> {
        self.field_values.clone()
    }

    fn datum_type(&self) -> oximeter::DatumType {
        DatumType::CumulativeU64
    }

    fn datum(&self) -> &Self::Datum {
        &self.datum
    }

    fn datum_mut(&mut self) -> &mut Self::Datum {
        &mut self.datum
    }

    fn measure(
        &self,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> oximeter::Measurement {
        oximeter::Measurement::new(
            timestamp,
            oximeter::Datum::from(&self.datum),
        )
    }

    fn start_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        Some(self.datum.start_time())
    }
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

fn random_string(length: usize) -> String {
    rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
