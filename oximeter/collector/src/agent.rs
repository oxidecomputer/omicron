// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The oximeter agent handles collection tasks for each producer.

// Copyright 2023 Oxide Computer Company

use crate::self_stats;
use crate::DbConfig;
use crate::Error;
use crate::ProducerEndpoint;
use anyhow::anyhow;
use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use omicron_common::address::CLICKHOUSE_PORT;
use oximeter::types::ProducerResults;
use oximeter::types::ProducerResultsItem;
use oximeter_db::Client;
use oximeter_db::DbWrite;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::trace;
use slog::warn;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::interval;
use uuid::Uuid;

type CollectionToken = oneshot::Sender<()>;

// Messages for controlling a collection task
#[derive(Debug)]
enum CollectionMessage {
    // Explicit request that the task collect data from its producer
    //
    // Also sends a oneshot that is signalled once the task scrapes
    // data from the Producer, and places it in the Clickhouse server.
    Collect(CollectionToken),
    // Request that the task update its interval and the socket address on which it collects data
    // from its producer.
    Update(ProducerEndpoint),
    // Request that the task exit
    Shutdown,
    // Return the current statistics from a single task.
    #[cfg(test)]
    Statistics {
        reply_tx: oneshot::Sender<self_stats::CollectionTaskStats>,
    },
}

async fn perform_collection(
    log: &Logger,
    self_target: &mut self_stats::CollectionTaskStats,
    client: &reqwest::Client,
    producer: &ProducerEndpoint,
    outbox: &mpsc::Sender<(Option<CollectionToken>, ProducerResults)>,
    token: Option<CollectionToken>,
) {
    debug!(log, "collecting from producer");
    let res = client
        .get(format!(
            "http://{}{}",
            producer.address,
            producer.collection_route()
        ))
        .send()
        .await;
    match res {
        Ok(res) => {
            if res.status().is_success() {
                match res.json::<ProducerResults>().await {
                    Ok(results) => {
                        debug!(
                            log,
                            "collected results from producer";
                            "n_results" => results.len()
                        );
                        self_target.collections.datum.increment();
                        outbox.send((token, results)).await.unwrap();
                    }
                    Err(e) => {
                        warn!(
                            log,
                            "failed to collect results from producer";
                            "error" => ?e,
                        );
                        self_target
                            .failures_for_reason(
                                self_stats::FailureReason::Deserialization,
                            )
                            .datum
                            .increment()
                    }
                }
            } else {
                warn!(
                    log,
                    "failed to receive metric results from producer";
                    "status_code" => res.status().as_u16(),
                );
                self_target
                    .failures_for_reason(self_stats::FailureReason::Other(
                        res.status(),
                    ))
                    .datum
                    .increment()
            }
        }
        Err(e) => {
            error!(
                log,
                "failed to send collection request to producer";
                "error" => ?e
            );
            self_target
                .failures_for_reason(self_stats::FailureReason::Unreachable)
                .datum
                .increment()
        }
    }
}

// Background task used to collect metrics from one producer on an interval.
//
// This function is started by the `OximeterAgent`, when a producer is registered. The task loops
// endlessly, and collects metrics from the assigned producer on a timeout. The assigned agent can
// also send a `CollectionMessage`, for example to update the collection interval. This is not
// currently used, but will likely be exposed via control plane interfaces in the future.
async fn collection_task(
    log: Logger,
    collector: self_stats::OximeterCollector,
    mut producer: ProducerEndpoint,
    mut inbox: mpsc::Receiver<CollectionMessage>,
    outbox: mpsc::Sender<(Option<CollectionToken>, ProducerResults)>,
) {
    let client = reqwest::Client::new();
    let mut collection_timer = interval(producer.interval);
    collection_timer.tick().await; // completes immediately
    debug!(
        log,
        "starting oximeter collection task";
        "interval" => ?producer.interval,
    );

    // Set up the collection of self statistics about this collection task.
    let mut stats = self_stats::CollectionTaskStats::new(collector, &producer);
    let mut self_collection_timer = interval(self_stats::COLLECTION_INTERVAL);
    self_collection_timer.tick().await;

    loop {
        tokio::select! {
            message = inbox.recv() => {
                match message {
                    None => {
                        debug!(log, "collection task inbox closed, shutting down");
                        return;
                    }
                    Some(CollectionMessage::Shutdown) => {
                        debug!(log, "collection task received shutdown request");
                        return;
                    },
                    Some(CollectionMessage::Collect(token)) => {
                        debug!(log, "collection task received explicit request to collect");
                        perform_collection(&log, &mut stats, &client, &producer, &outbox, Some(token)).await;
                    },
                    Some(CollectionMessage::Update(new_info)) => {
                        producer = new_info;
                        debug!(
                            log,
                            "collection task received request to update its producer information";
                            "interval" => ?producer.interval,
                            "address" => producer.address,
                        );
                        collection_timer = interval(producer.interval);
                        collection_timer.tick().await; // completes immediately
                    }
                    #[cfg(test)]
                    Some(CollectionMessage::Statistics { reply_tx }) => {
                        debug!(
                            log,
                            "received request for current task statistics"
                        );
                        reply_tx.send(stats.clone()).expect("failed to send statistics");
                    }
                }
            }
            _ = self_collection_timer.tick() => {
                debug!(log, "reporting oximeter self-collection statistics");
                outbox.send((None, stats.sample())).await.unwrap();
            }
            _ = collection_timer.tick() => {
                perform_collection(&log, &mut stats, &client, &producer, &outbox, None).await;
            }
        }
    }
}

// Struct representing a task for collecting metric data from a single producer
#[derive(Debug)]
struct CollectionTask {
    // Channel used to send messages from the agent to the actual task. The task owns the other
    // side.
    pub inbox: mpsc::Sender<CollectionMessage>,
    // Handle to the actual tokio task running the collection loop.
    #[allow(dead_code)]
    pub task: JoinHandle<()>,
}

// A task run by `oximeter` in standalone mode, which simply prints results as
// they're received.
async fn results_printer(
    log: Logger,
    mut rx: mpsc::Receiver<(Option<CollectionToken>, ProducerResults)>,
) {
    loop {
        match rx.recv().await {
            Some((_, results)) => {
                for res in results.into_iter() {
                    match res {
                        ProducerResultsItem::Ok(samples) => {
                            for sample in samples.into_iter() {
                                info!(
                                    log,
                                    "";
                                    "sample" => ?sample,
                                );
                            }
                        }
                        ProducerResultsItem::Err(e) => {
                            error!(
                                log,
                                "received error from a producer";
                                "err" => ?e,
                            );
                        }
                    }
                }
            }
            None => {
                debug!(log, "result queue closed, exiting");
                return;
            }
        }
    }
}

// Aggregation point for all results, from all collection tasks.
async fn results_sink(
    log: Logger,
    client: Client,
    batch_size: usize,
    batch_interval: Duration,
    mut rx: mpsc::Receiver<(Option<CollectionToken>, ProducerResults)>,
) {
    let mut timer = interval(batch_interval);
    timer.tick().await; // completes immediately
    let mut batch = Vec::with_capacity(batch_size);
    loop {
        let mut collection_token = None;
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
                    Some((token, results)) => {
                        let flattened_results = {
                            let mut flattened = Vec::with_capacity(results.len());
                            for inner_batch in results.into_iter() {
                                match inner_batch {
                                    ProducerResultsItem::Ok(samples) => flattened.extend(samples.into_iter()),
                                    ProducerResultsItem::Err(e) => {
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

                        collection_token = token;
                        if collection_token.is_some() {
                            true
                        } else {
                            batch.len() >= batch_size
                        }
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
            //
            // See https://github.com/oxidecomputer/omicron/issues/740 for a
            // disucssion.
            batch.clear();
        }

        if let Some(token) = collection_token {
            let _ = token.send(());
        }
    }
}

/// The internal agent the oximeter server uses to collect metrics from producers.
#[derive(Debug)]
pub struct OximeterAgent {
    /// The collector ID for this agent
    pub id: Uuid,
    log: Logger,
    // Oximeter target used by this agent to produce metrics about itself.
    collection_target: self_stats::OximeterCollector,
    // Handle to the TX-side of a channel for collecting results from the collection tasks
    result_sender: mpsc::Sender<(Option<CollectionToken>, ProducerResults)>,
    // The actual tokio tasks running the collection on a timer.
    collection_tasks:
        Arc<Mutex<BTreeMap<Uuid, (ProducerEndpoint, CollectionTask)>>>,
}

impl OximeterAgent {
    /// Construct a new agent with the given ID and logger.
    pub async fn with_id(
        id: Uuid,
        address: SocketAddrV6,
        db_config: DbConfig,
        resolver: &Resolver,
        log: &Logger,
    ) -> Result<Self, Error> {
        let (result_sender, result_receiver) = mpsc::channel(8);
        let log = log.new(o!(
            "component" => "oximeter-agent",
            "collector_id" => id.to_string(),
        ));
        let insertion_log = log.new(o!("component" => "results-sink"));

        // Construct the ClickHouse client first, propagate an error if we can't reach the
        // database.
        let db_address = if let Some(address) = db_config.address {
            address
        } else {
            SocketAddr::new(
                resolver.lookup_ip(ServiceName::Clickhouse).await?,
                CLICKHOUSE_PORT,
            )
        };

        // Determine the version of the database.
        //
        // There are three cases
        //
        // - The database exists and is at the expected version. Continue in
        // this case.
        //
        // - The database exists and is at a lower-than-expected version. We
        // fail back to the caller here, which will retry indefinitely until the
        // DB has been updated.
        //
        // - The DB doesn't exist at all. This reports a version number of 0. We
        // need to create the DB here, at the latest version. This is used in
        // fresh installations and tests.
        let client = Client::new(db_address, &log);
        match client.check_db_is_at_expected_version().await {
            Ok(_) => {}
            Err(oximeter_db::Error::DatabaseVersionMismatch {
                found: 0,
                ..
            }) => {
                debug!(log, "oximeter database does not exist, creating");
                let replicated = client.is_oximeter_cluster().await?;
                client
                    .initialize_db_with_version(
                        replicated,
                        oximeter_db::OXIMETER_VERSION,
                    )
                    .await?;
            }
            Err(e) => return Err(Error::from(e)),
        }

        // Set up tracking of statistics about ourselves.
        let collection_target = self_stats::OximeterCollector {
            collector_id: id,
            collector_ip: (*address.ip()).into(),
            collector_port: address.port(),
        };

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
            collection_target,
            result_sender,
            collection_tasks: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Construct a new standalone `oximeter` collector.
    ///
    /// In this mode, `oximeter` can be used to test the collection of metrics
    /// from producers, without requiring all the normal machinery of the
    /// control plane. The collector is run as usual, but additionally starts a
    /// API server to stand-in for Nexus. The registrations of the producers and
    /// collectors occurs through the normal code path, but uses this mock Nexus
    /// instead of the real thing.
    pub async fn new_standalone(
        id: Uuid,
        address: SocketAddrV6,
        db_config: Option<DbConfig>,
        log: &Logger,
    ) -> Result<Self, Error> {
        let (result_sender, result_receiver) = mpsc::channel(8);
        let log = log.new(o!(
            "component" => "oximeter-standalone",
            "collector_id" => id.to_string(),
        ));

        // If we have configuration for ClickHouse, we'll spawn the results
        // sink task as usual. If not, we'll spawn a dummy task that simply
        // prints the results as they're received.
        let insertion_log = log.new(o!("component" => "results-sink"));
        if let Some(db_config) = db_config {
            let Some(address) = db_config.address else {
                return Err(Error::Standalone(anyhow!(
                    "Must provide explicit IP address in standalone mode"
                )));
            };
            let client = Client::new(address, &log);
            let replicated = client.is_oximeter_cluster().await?;
            if !replicated {
                client.init_single_node_db().await?;
            } else {
                client.init_replicated_db().await?;
            }

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
        } else {
            tokio::spawn(results_printer(insertion_log, result_receiver));
        }

        // Set up tracking of statistics about ourselves.
        let collection_target = self_stats::OximeterCollector {
            collector_id: id,
            collector_ip: (*address.ip()).into(),
            collector_port: address.port(),
        };
        Ok(Self {
            id,
            log,
            collection_target,
            result_sender,
            collection_tasks: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Register a new producer with this oximeter instance.
    pub async fn register_producer(
        &self,
        info: ProducerEndpoint,
    ) -> Result<(), Error> {
        let id = info.id;
        match self.collection_tasks.lock().await.entry(id) {
            Entry::Vacant(value) => {
                debug!(
                    self.log,
                    "registered new metric producer";
                    "producer_id" => id.to_string(),
                    "address" => info.address,
                );

                // Build channel to control the task and receive results.
                let (tx, rx) = mpsc::channel(4);
                let q = self.result_sender.clone();
                let log = self.log.new(o!("component" => "collection-task", "producer_id" => id.to_string()));
                let info_clone = info.clone();
                let target = self.collection_target;
                let task = tokio::spawn(async move {
                    collection_task(log, target, info_clone, rx, q).await;
                });
                value.insert((info, CollectionTask { inbox: tx, task }));
            }
            Entry::Occupied(mut value) => {
                debug!(
                    self.log,
                    "received request to register existing metric \
                    producer, updating collection information";
                   "producer_id" => id.to_string(),
                   "interval" => ?info.interval,
                   "address" => info.address,
                );
                value.get_mut().0 = info.clone();
                value
                    .get()
                    .1
                    .inbox
                    .send(CollectionMessage::Update(info))
                    .await
                    .unwrap();
            }
        }
        Ok(())
    }

    /// Forces a collection from all producers.
    ///
    /// Returns once all those values have been inserted into Clickhouse,
    /// or an error occurs trying to perform the collection.
    pub async fn force_collection(&self) {
        let mut collection_oneshots = vec![];
        let collection_tasks = self.collection_tasks.lock().await;
        for (_id, (_endpoint, task)) in collection_tasks.iter() {
            let (tx, rx) = oneshot::channel();
            // Scrape from each producer, into oximeter...
            task.inbox.send(CollectionMessage::Collect(tx)).await.unwrap();
            // ... and keep track of the token that indicates once the metric
            // has made it into Clickhouse.
            collection_oneshots.push(rx);
        }
        drop(collection_tasks);

        // Only return once all producers finish processing the token we
        // provided.
        //
        // NOTE: This can either mean that the collection completed
        // successfully, or an error occurred in the collection pathway.
        futures::future::join_all(collection_oneshots).await;
    }

    /// List existing producers.
    pub async fn list_producers(
        &self,
        start_id: Option<Uuid>,
        limit: usize,
    ) -> Vec<ProducerEndpoint> {
        let start = if let Some(id) = start_id {
            Bound::Excluded(id)
        } else {
            Bound::Unbounded
        };
        self.collection_tasks
            .lock()
            .await
            .range((start, Bound::Unbounded))
            .take(limit)
            .map(|(_id, (info, _t))| info.clone())
            .collect()
    }

    /// Delete a producer by ID, stopping its collection task.
    pub async fn delete_producer(&self, id: Uuid) -> Result<(), Error> {
        let (_info, task) = self
            .collection_tasks
            .lock()
            .await
            .remove(&id)
            .ok_or_else(|| Error::NoSuchProducer(id))?;
        debug!(
            self.log,
            "removed collection task from set";
            "producer_id" => %id,
        );
        match task.inbox.send(CollectionMessage::Shutdown).await {
            Ok(_) => debug!(
                self.log,
                "shut down collection task";
                "producer_id" => %id,
            ),
            Err(e) => error!(
                self.log,
                "failed to shut down collection task";
                "producer_id" => %id,
                "error" => ?e,
            ),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::CollectionMessage;
    use super::OximeterAgent;
    use super::ProducerEndpoint;
    use crate::self_stats::FailureReason;
    use hyper::service::make_service_fn;
    use hyper::service::service_fn;
    use hyper::Body;
    use hyper::Request;
    use hyper::Response;
    use hyper::Server;
    use hyper::StatusCode;
    use omicron_common::api::internal::nexus::ProducerKind;
    use omicron_test_utils::dev::test_setup_log;
    use std::convert::Infallible;
    use std::net::Ipv6Addr;
    use std::net::SocketAddr;
    use std::net::SocketAddrV6;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::Instant;
    use uuid::Uuid;

    // Test that we count successful collections from a target correctly.
    #[tokio::test]
    async fn test_self_stat_collection_count() {
        let logctx = test_setup_log("test_self_stat_collection_count");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            None,
            log,
        )
        .await
        .unwrap();

        // And a dummy server that will always report empty statistics. There
        // will be no actual data here, but the sample counter will increment.
        let addr =
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0));
        async fn handler(
            _: Request<Body>,
        ) -> Result<Response<Body>, Infallible> {
            Ok(Response::new(Body::from("[]")))
        }
        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handler))
        });
        let server = Server::bind(&addr).serve(make_svc);
        let address = server.local_addr();
        let _task = tokio::task::spawn(server);

        // Register the dummy producer.
        let interval = Duration::from_secs(1);
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: Some(ProducerKind::Service),
            address,
            base_route: String::from("/"),
            interval,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register dummy producer");

        // Step time until there has been exactly `N_COLLECTIONS` collections.
        tokio::time::pause();
        let now = Instant::now();
        const N_COLLECTIONS: usize = 5;
        let wait_for = interval * N_COLLECTIONS as u32 + interval / 2;
        while now.elapsed() < wait_for {
            tokio::time::advance(interval / 10).await;
        }

        // Request the statistics from the task itself.
        let (reply_tx, rx) = oneshot::channel();
        collector
            .collection_tasks
            .lock()
            .await
            .values()
            .next()
            .unwrap()
            .1
            .inbox
            .send(CollectionMessage::Statistics { reply_tx })
            .await
            .expect("failed to request statistics from task");
        let stats = rx.await.expect("failed to receive statistics from task");
        assert_eq!(stats.collections.datum.value(), N_COLLECTIONS as u64);
        assert!(stats.failed_collections.is_empty());
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_self_stat_unreachable_counter() {
        let logctx = test_setup_log("test_self_stat_unreachable_counter");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            None,
            log,
        )
        .await
        .unwrap();

        // Register a bogus producer, which is equivalent to a producer that is
        // unreachable.
        let interval = Duration::from_secs(1);
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: Some(ProducerKind::Service),
            address: SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                0,
                0,
                0,
            )),
            base_route: String::from("/"),
            interval,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register bogus producer");

        // Step time until there has been exactly `N_COLLECTIONS` collections.
        tokio::time::pause();
        let now = Instant::now();
        const N_COLLECTIONS: usize = 5;
        let wait_for = interval * N_COLLECTIONS as u32 + interval / 2;
        while now.elapsed() < wait_for {
            tokio::time::advance(interval / 10).await;
        }

        // Request the statistics from the task itself.
        let (reply_tx, rx) = oneshot::channel();
        collector
            .collection_tasks
            .lock()
            .await
            .values()
            .next()
            .unwrap()
            .1
            .inbox
            .send(CollectionMessage::Statistics { reply_tx })
            .await
            .expect("failed to request statistics from task");
        let stats = rx.await.expect("failed to receive statistics from task");
        assert_eq!(stats.collections.datum.value(), 0);
        assert_eq!(
            stats
                .failed_collections
                .get(&FailureReason::Unreachable)
                .unwrap()
                .datum
                .value(),
            N_COLLECTIONS as u64
        );
        assert_eq!(stats.failed_collections.len(), 1);
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_self_stat_error_counter() {
        let logctx = test_setup_log("test_self_stat_error_counter");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            None,
            log,
        )
        .await
        .unwrap();

        // And a dummy server that will always fail with a 500.
        let addr =
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0));
        async fn handler(
            _: Request<Body>,
        ) -> Result<Response<Body>, Infallible> {
            let mut res = Response::new(Body::from("im ded"));
            *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(res)
        }
        let make_svc = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handler))
        });
        let server = Server::bind(&addr).serve(make_svc);
        let address = server.local_addr();
        let _task = tokio::task::spawn(server);

        // Register the rather flaky producer.
        let interval = Duration::from_secs(1);
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: Some(ProducerKind::Service),
            address,
            base_route: String::from("/"),
            interval,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register flaky producer");

        // Step time until there has been exactly `N_COLLECTIONS` collections.
        tokio::time::pause();
        let now = Instant::now();
        const N_COLLECTIONS: usize = 5;
        let wait_for = interval * N_COLLECTIONS as u32 + interval / 2;
        while now.elapsed() < wait_for {
            tokio::time::advance(interval / 10).await;
        }

        // Request the statistics from the task itself.
        let (reply_tx, rx) = oneshot::channel();
        collector
            .collection_tasks
            .lock()
            .await
            .values()
            .next()
            .unwrap()
            .1
            .inbox
            .send(CollectionMessage::Statistics { reply_tx })
            .await
            .expect("failed to request statistics from task");
        let stats = rx.await.expect("failed to receive statistics from task");
        assert_eq!(stats.collections.datum.value(), 0);
        assert_eq!(
            stats
                .failed_collections
                .get(&FailureReason::Other(StatusCode::INTERNAL_SERVER_ERROR))
                .unwrap()
                .datum
                .value(),
            N_COLLECTIONS as u64
        );
        assert_eq!(stats.failed_collections.len(), 1);
        logctx.cleanup_successful();
    }
}
