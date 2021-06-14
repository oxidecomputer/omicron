//! Implementation of the `oximeter` metric collection server.
// Copyright 2021 Oxide Computer Company

use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, ConfigLogging, HttpError,
    HttpResponseUpdatedNoContent, HttpServer, HttpServerStarter,
    RequestContext, TypedBody,
};
use omicron_common::backoff;
use omicron_common::model::{OximeterStartupInfo, ProducerEndpoint};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use slog::{debug, info, o, trace, warn, Logger};
use tokio::{sync::mpsc, task::JoinHandle, time::interval};
use uuid::Uuid;

use crate::{collect::ProducerResults, db, Error};

// Messages for controlling a collection task
#[allow(dead_code)]
enum CollectionMessage {
    // Explicit request that the task collect data from its producer
    Collect,
    // Request that the task update its interval on which it collects data
    Interval(Duration),
    // Request that the task exit
    Shutdown,
}

// Background task used to collect metrics from one producer on an interval.
//
// This function is started by the `OximeterAgent`, when a producer is registered. The task loops
// endlessly, and collects metrics from the assigned producer on a timeout. The assigned agent can
// also send a `CollectionMessage`, for example to update the collection interval. This is not
// currently used, but will likely be exposed via control plane interfaces in the future.
async fn collection_task(
    id: Uuid,
    log: Logger,
    producer: ProducerEndpoint,
    mut inbox: mpsc::Receiver<CollectionMessage>,
    outbox: mpsc::Sender<ProducerResults>,
) {
    let client = Client::new();
    let mut collection_timer = interval(producer.interval);
    collection_timer.tick().await; // completes immediately
    debug!(
        log,
        "starting oximeter collection task";
        "collector_id" => ?id,
        "interval" => ?producer.interval,
    );
    loop {
        tokio::select! {
            message = inbox.recv() => {
                match message {
                    None => {
                        debug!(
                            log,
                            "collection task inbox closed, shutting down";
                            "collector_id" => ?id
                        );
                    }
                    Some(CollectionMessage::Shutdown) => {
                        debug!(
                            log,
                            "collection task received shutdown request";
                            "collector_id" => ?id
                        );
                    },
                    Some(CollectionMessage::Collect) => {
                        debug!(
                            log,
                            "collection task received request to collect";
                            "collector_id" => ?id
                        );
                    },
                    Some(CollectionMessage::Interval(int)) => {
                        debug!(
                            log,
                            "collection task received request to update interval";
                            "collector_id" => ?id,
                            "interval" => ?int
                        );
                        collection_timer = interval(int);
                        collection_timer.tick().await; // completes immediately
                    }
                }
            }
            _ = collection_timer.tick() => {
                info!(
                    log,
                    "collecting from producer";
                    "collector_id" => ?id,
                    "producer_id" => ?producer.id,
                );
                let res = client.get(format!("http://{}{}", producer.address, producer.collection_route()))
                    .send()
                    .await;
                match res {
                    Ok(res) => {
                        match res.json::<ProducerResults>().await {
                            Ok(results) => {
                                debug!(
                                    log,
                                    "collected {} total results",
                                    results.len();
                                    "collector_id" => ?id,
                                    "producer_id" => ?producer.id,
                                );
                                outbox.send(results).await.unwrap();
                            },
                            Err(e) => {
                                warn!(
                                    log,
                                    "failed to collect results from producer: {}",
                                    e.to_string();
                                    "collector_id" => ?id,
                                    "producer_id" => ?producer.id,
                                );
                            }
                        }
                    },
                    Err(e) => {
                        warn!(
                            log,
                            "failed to send collection request to producer: {}",
                            e.to_string();
                            "collector_id" => ?id,
                            "producer_id" => ?producer.id,
                        );
                    }
                }
            }
        }
    }
}

// Struct representing a task for collecting metric data from a single producer
struct CollectionTask {
    // Channel used to send messages from the agent to the actual task. The task owns the other
    // side.
    pub inbox: mpsc::Sender<CollectionMessage>,
    // Handle to the actual tokio task running the collection loop.
    pub task: JoinHandle<()>,
}

// Aggregation point for all results, from all collection tasks.
async fn results_sink(
    log: Logger,
    client: db::Client,
    batch_size: usize,
    batch_interval: Duration,
    mut rx: mpsc::Receiver<ProducerResults>,
) {
    let mut timer = interval(batch_interval);
    timer.tick().await; // completes immediately
    let mut batch = Vec::with_capacity(batch_size);
    loop {
        let insert = tokio::select! {
            _ = timer.tick() => {
                if batch.is_empty() {
                    trace!(log, "batch interval expired, but no samples to insert");
                    false
                } else {
                    true
                }
            }
            results = rx.recv() => {
                match results {
                    Some(results) => {
                        let flattened_results = {
                            let mut flattened = Vec::with_capacity(results.len());
                            for inner_batch in results.into_iter() {
                                match inner_batch {
                                    Ok(samples) => flattened.extend(samples.into_iter()),
                                    Err(e) => {
                                        debug!(
                                            log,
                                            "received error (not samples) from a producer: {}",
                                            e.to_string()
                                        );
                                    }
                                }
                            }
                            flattened
                        };
                        batch.extend(flattened_results);
                        batch.len() >= batch_size
                    }
                    None => {
                        warn!(log, "result queue closed, exiting");
                        return;
                    }
                }
            }
        };

        if insert {
            debug!(log, "inserting {} samples into database", batch.len());
            match client.insert_samples(&batch).await {
                Ok(()) => trace!(log, "successfully inserted samples"),
                Err(e) => {
                    warn!(
                        log,
                        "failed to insert some results into metric DB: {}",
                        e.to_string()
                    );
                }
            }
            // TODO-correctness The `insert_samples` call above may fail. The method itself needs
            // better handling of partially-inserted results in that case, but we may need to retry
            // or otherwise handle an error here as well.
            batch.clear();
        }
    }
}

/// Configuration for interacting with the metric database.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct DbConfig {
    /// Address of the ClickHouse server
    pub address: SocketAddr,

    /// Batch size of samples at which to insert
    pub batch_size: usize,

    /// Interval on which to insert data into the database, regardless of the number of collected
    /// samples. Value is in seconds.
    pub batch_interval: u64,
}

/// The internal agent the oximeter server uses to collect metrics from producers.
struct OximeterAgent {
    /// The collector ID for this agent
    pub id: Uuid,
    log: Logger,
    // Handle to the TX-side of a channel for collecting results from the collection tasks
    result_sender: mpsc::Sender<ProducerResults>,
    // The actual tokio tasks running the collection on a timer.
    collection_tasks: Arc<Mutex<BTreeMap<Uuid, CollectionTask>>>,
}

impl OximeterAgent {
    /// Construct a new agent with the given ID and logger.
    pub async fn with_id(
        id: Uuid,
        db_config: DbConfig,
        log: &Logger,
    ) -> Result<Self, Error> {
        let (result_sender, result_receiver) = mpsc::channel(8);
        let log = log.new(o!("component" => "oximeter-agent"));
        let insertion_log = log.new(o!("component" => "results-sink"));
        let client_log = log.new(o!("component" => "clickhouse-client"));

        // Construct the ClickHouse client first, to propagate an error if needed.
        let client = db::Client::new(db_config.address, client_log).await?;

        // Spawn the task for aggregating and inserting all metrics
        tokio::spawn(async move {
            results_sink(
                insertion_log,
                client,
                db_config.batch_size,
                Duration::from_secs(db_config.batch_interval),
                result_receiver,
            )
            .await
        });
        Ok(Self {
            id,
            log,
            result_sender,
            collection_tasks: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Register a new producer with this oximeter instance.
    pub fn register_producer(
        &self,
        info: ProducerEndpoint,
    ) -> Result<(), Error> {
        let id = info.id;
        match self.collection_tasks.lock().unwrap().entry(id) {
            Entry::Vacant(value) => {
                info!(self.log, "registered new metric producer";
                      "producer_id" => id.to_string(),
                      "address" => info.address,
                );

                // Build channel to control the task and receive results.
                let (tx, rx) = mpsc::channel(4);
                let q = self.result_sender.clone();
                let id = self.id;
                let log = self.log.new(o!("component" => "collection-task"));
                let task = tokio::spawn(async move {
                    collection_task(id, log, info, rx, q).await;
                });
                value.insert(CollectionTask { inbox: tx, task });
                Ok(())
            }
            Entry::Occupied(_) => {
                warn!(
                    self.log,
                    "received request to register duplicate producer";
                   "producer_id" => id.to_string(),
                );
                Err(Error::OximeterServer(format!(
                    "producer with ID {} already exists",
                    id
                )))
            }
        }
    }
}

/// Configuration used to initialize an oximeter server
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// An unique ID for this oximeter server
    pub id: Uuid,

    /// The address used to connect to Nexus.
    pub nexus_address: SocketAddr,

    /// Configuration for working with ClickHouse
    pub db: DbConfig,

    /// The internal Dropshot HTTP server configuration
    pub dropshot: ConfigDropshot,

    /// Logging configuration
    pub log: ConfigLogging,
}

impl Config {
    /// Load configuration for an Oximeter server from a file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, Error> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .map_err(|e| Error::OximeterServer(e.to_string()))?;
        toml::from_str(&contents)
            .map_err(|e| Error::OximeterServer(e.to_string()))
    }
}

/// A server used to collect metrics from components in the control plane.
pub struct Oximeter {
    _agent: Arc<OximeterAgent>,
    server: HttpServer<Arc<OximeterAgent>>,
}

impl Oximeter {
    /// Create a new `Oximeter` with the given configuration.
    ///
    /// This starts an HTTP server used to communicate with other agents in Omicron, especially
    /// Nexus. It also registers itself as a new `oximeter` instance with Nexus.
    pub async fn new(config: &Config) -> Result<Self, Error> {
        let log = config
            .log
            .to_logger("oximeter")
            .map_err(|msg| Error::OximeterServer(msg.to_string()))?;
        info!(log, "starting oximeter server");

        // TODO-robustness Handle retries if the database is cannot be reached. This likely should
        // just retry forever, as the system is unusable until a connection is made.
        let agent =
            Arc::new(OximeterAgent::with_id(config.id, config.db, &log).await?);

        let dropshot_log = log.new(o!("component" => "dropshot"));
        let server = HttpServerStarter::new(
            &config.dropshot,
            oximeter_api(),
            Arc::clone(&agent),
            &dropshot_log,
        )
        .map_err(|msg| Error::OximeterServer(msg.to_string()))?
        .start();

        // Notify Nexus that this oximeter instance is available.
        let client = Client::new();
        let notify_nexus = || async {
            debug!(log, "contacting nexus");
            client
                .post(format!(
                    "http://{}/metrics/collectors",
                    config.nexus_address
                ))
                .json(&OximeterStartupInfo {
                    address: server.local_addr(),
                    collector_id: agent.id,
                })
                .send()
                .await
                .map_err(backoff::BackoffError::Transient)?
                .error_for_status()
                .map_err(backoff::BackoffError::Transient)
        };
        let log_notification_failure = |error, delay| {
            warn!(
                log,
                "failed to contact nexus, will retry in {:?}", delay;
                "error" => ?error
            );
        };
        backoff::retry_notify(
            backoff::internal_service_policy(),
            notify_nexus,
            log_notification_failure,
        )
        .await
        .expect("Expected an infinite retry loop contacting Nexus");

        info!(log, "oximeter registered with nexus"; "id" => ?agent.id);
        Ok(Self { _agent: agent, server })
    }

    /// Serve requests forever, consuming the server.
    pub async fn serve_forever(self) -> Result<(), Error> {
        self.server.await.map_err(Error::OximeterServer)
    }
}

// Build the HTTP API internal to the control plane
fn oximeter_api() -> ApiDescription<Arc<OximeterAgent>> {
    let mut api = ApiDescription::new();
    api.register(producers_post)
        .expect("Could not register producers_post API handler");
    api
}

// Handle a request from Nexus to register a new producer with this collector.
#[endpoint {
    method = POST,
    path = "/producers",
}]
async fn producers_post(
    request_context: Arc<RequestContext<Arc<OximeterAgent>>>,
    body: TypedBody<ProducerEndpoint>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let agent = request_context.context();
    let server_info = body.into_inner();
    agent
        .register_producer(server_info)
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
    Ok(HttpResponseUpdatedNoContent())
}
