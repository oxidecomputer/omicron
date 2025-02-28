// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The oximeter agent handles collection tasks for each producer.

// Copyright 2024 Oxide Computer Company

use crate::collection_task::CollectionTaskHandle;
use crate::collection_task::CollectionTaskOutput;
use crate::collection_task::ForcedCollectionError;
use crate::results_sink;
use crate::self_stats;
use crate::DbConfig;
use crate::Error;
use crate::ProducerEndpoint;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use futures::TryStreamExt;
use nexus_client::types::IdSortMode;
use nexus_client::Client as NexusClient;
use omicron_common::backoff;
use omicron_common::backoff::BackoffError;
use oximeter_api::ProducerDetails;
use oximeter_db::Client;
use oximeter_db::DbWrite;
use qorb::claim::Handle;
use qorb::pool::Pool;
use qorb::resolver::BoxedResolver;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::trace;
use slog::warn;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use uuid::Uuid;

/// The internal agent the oximeter server uses to collect metrics from producers.
#[derive(Clone, Debug)]
pub struct OximeterAgent {
    /// The collector ID for this agent
    pub id: Uuid,
    log: Logger,
    // Oximeter target used by this agent to produce metrics about itself.
    collection_target: self_stats::OximeterCollector,
    // Handle to the TX-side of a channel for collecting results from the collection tasks
    result_sender: mpsc::Sender<CollectionTaskOutput>,
    // Handle to each Tokio task collection from a single producer.
    collection_tasks: Arc<Mutex<BTreeMap<Uuid, CollectionTaskHandle>>>,
    // The interval on which we refresh our list of producers from Nexus
    refresh_interval: Duration,
    // Handle to the task used to periodically refresh the list of producers.
    refresh_task: Arc<StdMutex<Option<tokio::task::JoinHandle<()>>>>,
    /// The last time we've refreshed our list of producers from Nexus.
    pub last_refresh_time: Arc<StdMutex<Option<DateTime<Utc>>>>,
}

impl OximeterAgent {
    /// Construct a new agent with the given ID and logger.
    pub async fn with_id(
        id: Uuid,
        address: SocketAddrV6,
        refresh_interval: Duration,
        db_config: DbConfig,
        native_resolver: BoxedResolver,
        log: &Logger,
        replicated: bool,
    ) -> Result<Self, Error> {
        let (result_sender, result_receiver) = mpsc::channel(8);
        let log = log.new(o!(
            "component" => "oximeter-agent",
            "collector_id" => id.to_string(),
            "collector_ip" => address.ip().to_string(),
        ));
        let insertion_log = log.new(o!("component" => "results-sink"));

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
        let client = Client::new_with_resolver(native_resolver, &log);
        match client.check_db_is_at_expected_version().await {
            Ok(_) => {}
            Err(oximeter_db::Error::DatabaseVersionMismatch {
                found: 0,
                ..
            }) => {
                debug!(log, "oximeter database does not exist, creating");
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
            crate::results_sink::database_inserter(
                insertion_log,
                client,
                db_config.batch_size,
                Duration::from_secs(db_config.batch_interval),
                result_receiver,
            )
            .await
        });

        let self_ = Self {
            id,
            log,
            collection_target,
            result_sender,
            collection_tasks: Arc::new(Mutex::new(BTreeMap::new())),
            refresh_interval,
            refresh_task: Arc::new(StdMutex::new(None)),
            last_refresh_time: Arc::new(StdMutex::new(None)),
        };

        Ok(self_)
    }

    /// Ensure the background task that polls Nexus periodically for our list of
    /// assigned producers is running.
    pub(crate) fn ensure_producer_refresh_task(
        &self,
        nexus_pool: Pool<NexusClient>,
    ) {
        let mut task = self.refresh_task.lock().unwrap();
        if task.is_none() {
            let refresh_task = tokio::spawn(refresh_producer_list_task(
                self.clone(),
                nexus_pool,
            ));
            *task = Some(refresh_task);
        }
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
        refresh_interval: Duration,
        db_config: Option<DbConfig>,
        log: &Logger,
    ) -> Result<Self, Error> {
        let (result_sender, result_receiver) = mpsc::channel(8);
        let log = log.new(o!(
            "component" => "oximeter-standalone",
            "collector_id" => id.to_string(),
            "collector_ip" => address.ip().to_string(),
        ));

        // If we have configuration for ClickHouse, we'll spawn the results
        // sink task as usual. If not, we'll spawn a dummy task that simply
        // prints the results as they're received.
        let insertion_log = log.new(o!("component" => "results-sink"));
        if let Some(db_config) = db_config {
            // Take the explicit native TCP address if provided, or the HTTP
            // IP address and use the default TCP port. One of these has to be
            // provided.
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
                results_sink::database_inserter(
                    insertion_log,
                    client,
                    db_config.batch_size,
                    Duration::from_secs(db_config.batch_interval),
                    result_receiver,
                )
                .await
            });
        } else {
            tokio::spawn(results_sink::logger(insertion_log, result_receiver));
        }

        // Set up tracking of statistics about ourselves.
        let collection_target = self_stats::OximeterCollector {
            collector_id: id,
            collector_ip: (*address.ip()).into(),
            collector_port: address.port(),
        };

        // We don't spawn the task to periodically refresh producers when run
        // in standalone mode. We can just pretend we registered once, and
        // that's it.
        let last_refresh_time = Arc::new(StdMutex::new(Some(Utc::now())));

        Ok(Self {
            id,
            log,
            collection_target,
            result_sender,
            collection_tasks: Arc::new(Mutex::new(BTreeMap::new())),
            refresh_interval,
            refresh_task: Arc::new(StdMutex::new(None)),
            last_refresh_time,
        })
    }

    /// Fetch details about a producer, if it exists.
    pub async fn producer_details(
        &self,
        id: Uuid,
    ) -> Result<ProducerDetails, Error> {
        let tasks = self.collection_tasks.lock().await;
        let Some(task) = tasks.get(&id) else {
            return Err(Error::NoSuchProducer { id });
        };
        task.details().await
    }

    /// Register a new producer with this oximeter instance.
    pub async fn register_producer(
        &self,
        info: ProducerEndpoint,
    ) -> Result<(), Error> {
        let mut tasks = self.collection_tasks.lock().await;
        self.register_producer_locked(&mut tasks, info).await;
        Ok(())
    }

    // Internal implementation that registers a producer, assuming the lock on
    // the map is held.
    async fn register_producer_locked(
        &self,
        tasks: &mut MutexGuard<'_, BTreeMap<Uuid, CollectionTaskHandle>>,
        info: ProducerEndpoint,
    ) {
        let id = info.id;
        match tasks.entry(id) {
            Entry::Vacant(value) => {
                debug!(
                    self.log,
                    "registered new metric producer";
                    "producer_id" => id.to_string(),
                    "address" => info.address,
                );
                let handle = CollectionTaskHandle::new(
                    &self.log,
                    self.collection_target,
                    info,
                    self.result_sender.clone(),
                )
                .await;
                value.insert(handle);
            }
            Entry::Occupied(mut value) => {
                // Only update the endpoint information if it's actually
                // different, to avoid indefinitely delaying the collection
                // timer from expiring.
                if value.get().producer == info {
                    trace!(
                        self.log,
                        "ignoring request to update existing metric \
                        producer, since the endpoint information is \
                        the same as the existing";
                        "producer_id" => %id,
                    );
                } else {
                    debug!(
                        self.log,
                        "received request to register existing metric \
                        producer, updating collection information";
                        "producer_id" => id.to_string(),
                        "interval" => ?info.interval,
                        "address" => info.address,
                    );
                    value.get_mut().update(info).await;
                }
            }
        }
    }

    /// Forces a collection from all producers.
    ///
    /// Returns once all those values have been inserted into ClickHouse,
    /// or an error occurs trying to perform the collection.
    ///
    /// NOTE: This collection is best effort, as the name implies. It's possible
    /// that we lose track of requests internally, in cases where there are
    /// many concurrent calls. Callers should strive to avoid this, since it
    /// rarely makes sense to do that.
    pub async fn try_force_collection(
        &self,
    ) -> Result<(), ForcedCollectionError> {
        let mut collection_oneshots = vec![];
        let collection_tasks = self.collection_tasks.lock().await;
        for (_id, task) in collection_tasks.iter() {
            // Scrape from each producer, into oximeter...
            let rx = task.collect();
            // ... and keep track of the token that indicates once the metric
            // has made it into ClickHouse.
            collection_oneshots.push(rx);
        }
        drop(collection_tasks);

        // Only return once all producers finish processing the token we
        // provided.
        //
        // NOTE: This can either mean that the collection completed
        // successfully, or an error occurred in the collection pathway.
        //
        // We use `join_all` to ensure that all futures are run, rather than
        // bailing on the first error. We extract the first error we received,
        // or map an actual `RecvError` to `Closed`, since it does really mean
        // the other side hung up without sending.
        let results = futures::future::join_all(collection_oneshots).await;
        for result in results.into_iter() {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(ForcedCollectionError::Closed),
            }
        }
        Ok(())
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
            .map(|(_id, task)| task.producer)
            .collect()
    }

    /// Delete a producer by ID, stopping its collection task.
    pub async fn delete_producer(&self, id: Uuid) -> Result<(), Error> {
        let mut tasks = self.collection_tasks.lock().await;
        self.delete_producer_locked(&mut tasks, id).await
    }

    // Internal implementation that deletes a producer, assuming the lock on
    // the map is held.
    async fn delete_producer_locked(
        &self,
        tasks: &mut MutexGuard<'_, BTreeMap<Uuid, CollectionTaskHandle>>,
        id: Uuid,
    ) -> Result<(), Error> {
        let Some(task) = tasks.remove(&id) else {
            // We have no such producer, so good news, we've removed it!
            return Ok(());
        };
        debug!(
            self.log,
            "removed collection task from set";
            "producer_id" => %id,
        );
        task.shutdown().await;
        Ok(())
    }

    // Ensure that exactly the set of producers is registered with `self`.
    //
    // Errors logged, but not returned, and an attempt to register all producers
    // is made, even if an error is encountered part-way through.
    //
    // This returns the number of pruned tasks.
    async fn ensure_producers(
        &self,
        expected_producers: BTreeMap<Uuid, ProducerEndpoint>,
    ) -> usize {
        let mut tasks = self.collection_tasks.lock().await;

        // First prune unwanted collection tasks.
        //
        // This is set of all producers that we currently have, which are not in
        // the new list from Nexus.
        let ids_to_prune: Vec<_> = tasks
            .keys()
            .filter(|id| !expected_producers.contains_key(id))
            .copied()
            .collect();
        let n_pruned = ids_to_prune.len();
        for id in ids_to_prune.into_iter() {
            // This method only returns an error if the provided ID does not
            // exist in the current tasks. That is impossible, because we hold
            // the lock, and we've just computed this as the set that _is_ in
            // the map, and not in the new set from Nexus.
            self.delete_producer_locked(&mut tasks, id).await.unwrap();
        }

        // And then ensure everything in the list.
        //
        // This will insert new tasks, and update any that we already know
        // about.
        for info in expected_producers.into_values() {
            self.register_producer_locked(&mut tasks, info).await;
        }
        n_pruned
    }
}

// A task which periodically updates our list of producers from Nexus.
async fn refresh_producer_list_task(
    agent: OximeterAgent,
    nexus_pool: Pool<NexusClient>,
) {
    // Setup our refresh timer.
    //
    // If we miss a tick, we'll skip until the next multiple of the interval
    // from the start time. This is a good compromise between taking a bunch of
    // quick updates (burst) and shifting our interval (delay).
    let mut interval = tokio::time::interval(agent.refresh_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(agent.log, "starting refresh list task");
    loop {
        interval.tick().await;
        info!(agent.log, "refreshing list of producers from Nexus");
        refresh_producer_list_once(&agent, &nexus_pool).await;
    }
}

// Run a single "producer refresh from Nexus" operation (which may require
// multiple requests to Nexus to fetch multiple pages of producers).
async fn refresh_producer_list_once(
    agent: &OximeterAgent,
    nexus_pool: &Pool<NexusClient>,
) {
    let client = claim_nexus_with_backoff(&agent.log, &nexus_pool).await;
    let mut stream = client.cpapi_assigned_producers_list_stream(
        &agent.id,
        // This is a _total_ limit, not a page size, so `None` means "get
        // all entries".
        None,
        Some(IdSortMode::IdAscending),
    );
    let mut expected_producers = BTreeMap::new();
    loop {
        match stream.try_next().await {
            Err(e) => {
                // TODO-robustness: Some errors here may not be "fatal", in
                // the sense that we can continue to process the list we
                // receive from Nexus. It's not clear which ones though,
                // since most of these are either impossible (pre-hook
                // errors) or indicate we've made a serious programming
                // error or updated to incompatible versions of Nexus /
                // Oximeter. One that we might be able to handle is a
                // communication error, say failing to fetch the last page
                // when we've already fetched the first few. But for now,
                // we'll simply keep the list we have and try again on the
                // next refresh.
                //
                // For now, let's just avoid doing anything at all here, on
                // the theory that if we hit this, we should just continue
                // collecting from the last known-good set of producers.
                error!(
                    agent.log,
                    "error fetching next assigned producer, \
                    abandoning this refresh attempt";
                    InlineErrorChain::new(&e),
                );
                return;
            }
            Ok(Some(p)) => {
                let endpoint = match ProducerEndpoint::try_from(p) {
                    Ok(ep) => ep,
                    Err(e) => {
                        error!(
                            agent.log,
                            "failed to convert producer description \
                            from Nexus, skipping producer";
                            // No `InlineErrorChain` here: `e` is a string
                            "error" => e,
                        );
                        continue;
                    }
                };
                let old = expected_producers.insert(endpoint.id, endpoint);
                if let Some(ProducerEndpoint { id, .. }) = old {
                    error!(
                        agent.log,
                        "Nexus appears to have sent duplicate producer info";
                        "producer_id" => %id,
                    );
                }
            }
            Ok(None) => break,
        }
    }
    let n_current_tasks = expected_producers.len();
    let n_pruned_tasks = agent.ensure_producers(expected_producers).await;
    *agent.last_refresh_time.lock().unwrap() = Some(Utc::now());
    info!(
        agent.log,
        "refreshed list of producers from Nexus";
        "n_pruned_tasks" => n_pruned_tasks,
        "n_current_tasks" => n_current_tasks,
    );
}

async fn claim_nexus_with_backoff(
    log: &Logger,
    nexus_pool: &Pool<NexusClient>,
) -> Handle<NexusClient> {
    let log_failure = |error, delay| {
        warn!(
            log,
            "failed to lookup Nexus IP, will retry";
            "delay" => ?delay,
            // No `InlineErrorChain` here: `error` is a string
            "error" => error,
        );
    };
    let do_lookup = || async {
        nexus_pool
            .claim()
            .await
            .map_err(|e| BackoffError::transient(e.to_string()))
    };
    backoff::retry_notify(
        backoff::retry_policy_internal_service(),
        do_lookup,
        log_failure,
    )
    .await
    .expect("Expected infinite retry loop resolving Nexus address")
}

#[cfg(test)]
mod tests {
    use super::OximeterAgent;
    use super::ProducerEndpoint;
    use crate::self_stats::FailureReason;
    use chrono::Utc;
    use dropshot::HttpError;
    use dropshot::HttpResponseOk;
    use dropshot::Path;
    use dropshot::RequestContext;
    use dropshot::ServerBuilder;
    use omicron_common::api::internal::nexus::ProducerKind;
    use omicron_test_utils::dev::test_setup_log;
    use oximeter::types::ProducerResults;
    use std::net::Ipv6Addr;
    use std::net::SocketAddr;
    use std::net::SocketAddrV6;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;
    use uuid::Uuid;

    // Interval on which oximeter collects from producers in these tests.
    const COLLECTION_INTERVAL: Duration = Duration::from_secs(1);

    // Interval in calls to `tokio::time::advance`. This must be sufficiently
    // small relative to `COLLECTION_INTERVAL` to ensure all ticks of internal
    // timers complete as expected.
    const TICK_INTERVAL: Duration = Duration::from_millis(10);

    // Total number of collection attempts, and the expected number of
    // collections which fail in the "unreachability" test below.
    const N_COLLECTIONS: u64 = 5;

    // Period these tests wait using `tokio::time::advance()` before checking
    // their test conditions.
    const TEST_WAIT_PERIOD: Duration = Duration::from_millis(
        COLLECTION_INTERVAL.as_millis() as u64 * N_COLLECTIONS,
    );

    #[derive(
        Clone,
        Copy,
        Debug,
        schemars::JsonSchema,
        serde::Deserialize,
        serde::Serialize,
    )]
    struct IdPath {
        id: Uuid,
    }

    /// Simplified API for a producer, implemented for tests below.
    #[dropshot::api_description]
    trait ProducerApi {
        type Context;

        #[endpoint {
            method = GET,
            path = "/{id}",
        }]
        async fn collect(
            request_context: RequestContext<Self::Context>,
            path: Path<IdPath>,
        ) -> Result<HttpResponseOk<ProducerResults>, HttpError>;
    }

    /// A producer that always responds successfully with no samples.
    struct EmptyProducer;

    impl ProducerApi for EmptyProducer {
        type Context = Arc<AtomicUsize>;

        async fn collect(
            request_context: RequestContext<Self::Context>,
            _: Path<IdPath>,
        ) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
            request_context.context().fetch_add(1, Ordering::SeqCst);
            Ok(HttpResponseOk(vec![]))
        }
    }

    /// A producer that always responds with a 500.
    struct DedProducer;

    impl ProducerApi for DedProducer {
        type Context = Arc<AtomicUsize>;

        async fn collect(
            request_context: RequestContext<Self::Context>,
            _: Path<IdPath>,
        ) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
            request_context.context().fetch_add(1, Ordering::SeqCst);
            Err(HttpError::for_internal_error(String::from("i'm ded")))
        }
    }

    // Test that we count successful collections from a target correctly.
    #[tokio::test]
    async fn test_self_stat_collection_count() {
        let logctx = test_setup_log("test_self_stat_collection_count");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            crate::default_refresh_interval(),
            None,
            log,
        )
        .await
        .unwrap();

        // Spawn the mock server that always reports empty statistics.
        let collection_count = Arc::new(AtomicUsize::new(0));
        let server = ServerBuilder::new(
            producer_api_mod::api_description::<EmptyProducer>().unwrap(),
            collection_count.clone(),
            log.new(slog::o!("component" => "dropshot")),
        )
        .config(Default::default())
        .start()
        .expect("failed to spawn empty dropshot server");

        // Register the dummy producer.
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: server.local_addr(),
            interval: COLLECTION_INTERVAL,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register dummy producer");

        // Step time for a few collections.
        //
        // Due to scheduling variations, we don't verify the number of
        // collections we expect based on time, but we instead check that every
        // collection that _has_ occurred bumps the counter.
        tokio::time::pause();
        let now = Instant::now();
        while now.elapsed() < TEST_WAIT_PERIOD {
            tokio::time::advance(TICK_INTERVAL).await;
        }

        // Request the statistics from the task itself.
        let stats = collector
            .collection_tasks
            .lock()
            .await
            .values()
            .next()
            .unwrap()
            .statistics()
            .await;
        let count = stats.collections.datum.value() as usize;

        assert!(count != 0);
        let server_count = collection_count.load(Ordering::SeqCst);
        assert!(
            count == server_count || count + 1 == server_count,
            "number of collections reported by the collection \
            task ({count}) differs from the number reported by the empty \
            producer server itself ({server_count})"
        );
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
            crate::default_refresh_interval(),
            None,
            log,
        )
        .await
        .unwrap();

        // Register a bogus producer, which is equivalent to a producer that is
        // unreachable.
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                0,
                0,
                0,
            )),
            interval: COLLECTION_INTERVAL,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register bogus producer");

        // Step time until there has been exactly `N_COLLECTIONS` collections.
        tokio::time::pause();
        let now = Instant::now();
        while now.elapsed() < TEST_WAIT_PERIOD {
            tokio::time::advance(TICK_INTERVAL).await;
        }

        // Request the statistics from the task itself.
        let stats = collector
            .collection_tasks
            .lock()
            .await
            .values()
            .next()
            .unwrap()
            .statistics()
            .await;
        assert_eq!(stats.collections.datum.value(), 0);
        assert_eq!(
            stats
                .failed_collections
                .get(&FailureReason::Unreachable)
                .unwrap()
                .datum
                .value(),
            N_COLLECTIONS,
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
            crate::default_refresh_interval(),
            None,
            log,
        )
        .await
        .unwrap();

        // Spawn the mock server that always responds with a server error
        let collection_count = Arc::new(AtomicUsize::new(0));
        let server = ServerBuilder::new(
            producer_api_mod::api_description::<DedProducer>().unwrap(),
            collection_count.clone(),
            log.new(slog::o!("component" => "dropshot")),
        )
        .config(Default::default())
        .start()
        .expect("failed to spawn empty dropshot server");

        // Register the rather flaky producer.
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: server.local_addr(),
            interval: COLLECTION_INTERVAL,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register flaky producer");

        // Step time for a few collections.
        //
        // Due to scheduling variations, we don't verify the number of
        // collections we expect based on time, but we instead check that every
        // collection that _has_ occurred bumps the counter.
        tokio::time::pause();
        let now = Instant::now();
        while now.elapsed() < TEST_WAIT_PERIOD {
            tokio::time::advance(TICK_INTERVAL).await;
        }

        // Request the statistics from the task itself.
        let stats = collector
            .collection_tasks
            .lock()
            .await
            .values()
            .next()
            .unwrap()
            .statistics()
            .await;
        let count = stats
            .failed_collections
            .get(&FailureReason::Other(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            ))
            .unwrap()
            .datum
            .value() as usize;

        assert_eq!(stats.collections.datum.value(), 0);
        assert!(count != 0);

        // The server may have handled a request that we've not yet recorded on
        // our collection task side, so we allow the server count to be greater
        // than our own. But since the collection task is single-threaded, it
        // cannot ever be more than _one_ greater than our count, since we
        // should increment that counter before making another request to the
        // server.
        let server_count = collection_count.load(Ordering::SeqCst);
        assert!(
            count == server_count || count + 1 == server_count,
            "number of collections reported by the collection \
            task ({count}) differs from the number reported by the always-ded \
            producer server itself ({server_count})"
        );
        assert_eq!(
            stats.failed_collections.len(),
            1,
            "unexpected failed_collections content: {:?}",
            stats.failed_collections,
        );
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_delete_nonexistent_producer_succeeds() {
        let logctx =
            test_setup_log("test_delete_nonexistent_producer_succeeds");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            crate::default_refresh_interval(),
            None,
            log,
        )
        .await
        .unwrap();
        assert!(
            collector.delete_producer(Uuid::new_v4()).await.is_ok(),
            "Deleting a non-existent producer should be OK"
        );
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn verify_producer_details() {
        let logctx = test_setup_log("verify_producer_details");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            crate::default_refresh_interval(),
            None,
            log,
        )
        .await
        .unwrap();

        // Spawn the mock server that always reports empty statistics.
        let collection_count = Arc::new(AtomicUsize::new(0));
        let server = ServerBuilder::new(
            producer_api_mod::api_description::<EmptyProducer>().unwrap(),
            collection_count.clone(),
            log.new(slog::o!("component" => "dropshot")),
        )
        .config(Default::default())
        .start()
        .expect("failed to spawn empty dropshot server");

        // Register the dummy producer.
        let endpoint = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: server.local_addr(),
            interval: COLLECTION_INTERVAL,
        };
        let id = endpoint.id;
        let before = Utc::now();
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register dummy producer");

        // We don't manipulate time manually here, since this is pretty short
        // and we want to assert things about the actual timing in the test
        // below.
        let is_ready = || async {
            // We need to check if the server has had a collection request, and
            // also if we've processed it on our task side. If we don't wait for
            // the second bit, updating our collection details in the task races
            // with the rest of this test that checks those details.
            if collection_count.load(Ordering::SeqCst) < 1 {
                return false;
            }
            collector
                .producer_details(id)
                .await
                .expect("Should be able to get producer details")
                .n_collections
                > 0
        };
        while !is_ready().await {
            tokio::time::sleep(TICK_INTERVAL).await;
        }

        // Get details about the producer.
        let count = collection_count.load(Ordering::SeqCst) as u64;
        let details = collector
            .producer_details(id)
            .await
            .expect("Should be able to get producer details");
        println!("{details:#?}");
        assert_eq!(details.id, id);
        assert!(details.registered > before);
        assert!(details.updated > before);
        assert_eq!(details.registered, details.updated);
        assert!(
            details.n_collections == count
                || details.n_collections == count - 1
        );
        assert_eq!(details.n_failures, 0);
        let success =
            details.last_success.expect("Should have a successful collection");
        assert!(success.time_queued > Duration::ZERO);
        assert!(success.time_collecting > Duration::ZERO);
        assert!(success.n_samples == 0);
        assert!(details.last_failure.is_none());
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_updated_producer_is_still_collected_from() {
        let logctx =
            test_setup_log("test_updated_producer_is_still_collected_from");
        let log = &logctx.log;

        // Spawn an oximeter collector ...
        let collector = OximeterAgent::new_standalone(
            Uuid::new_v4(),
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            crate::default_refresh_interval(),
            None,
            log,
        )
        .await
        .unwrap();

        // Spawn the mock server that always reports empty statistics.
        let collection_count = Arc::new(AtomicUsize::new(0));
        let server = ServerBuilder::new(
            producer_api_mod::api_description::<EmptyProducer>().unwrap(),
            collection_count.clone(),
            log.new(slog::o!("component" => "dropshot")),
        )
        .config(Default::default())
        .start()
        .expect("failed to spawn empty dropshot server");

        // Register the dummy producer.
        let id = Uuid::new_v4();
        let endpoint = ProducerEndpoint {
            id,
            kind: ProducerKind::Service,
            address: server.local_addr(),
            interval: COLLECTION_INTERVAL,
        };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register dummy producer");

        let details = collector.producer_details(id).await.unwrap();
        println!("{details:#?}");

        // Ensure we get some collections from it.
        tokio::time::pause();
        while collection_count.load(Ordering::SeqCst) < 1 {
            tokio::time::advance(TICK_INTERVAL).await;
        }

        // Now, drop and recreate the server, and register with the same ID at a
        // different address.
        let collection_count = Arc::new(AtomicUsize::new(0));
        let server = ServerBuilder::new(
            producer_api_mod::api_description::<EmptyProducer>().unwrap(),
            collection_count.clone(),
            log.new(slog::o!("component" => "dropshot")),
        )
        .config(Default::default())
        .start()
        .expect("failed to spawn empty dropshot server");

        // Register the dummy producer.
        let endpoint =
            ProducerEndpoint { address: server.local_addr(), ..endpoint };
        collector
            .register_producer(endpoint)
            .await
            .expect("failed to register dummy producer a second time");

        // We should just have one producer.
        assert_eq!(
            collector.collection_tasks.lock().await.len(),
            1,
            "Should only have one producer, it was updated and has the \
            same UUID",
        );

        // We should eventually collect from it again.
        let now = Instant::now();
        while now.elapsed() < TEST_WAIT_PERIOD {
            tokio::time::advance(TICK_INTERVAL).await;
        }
        let details = collector.producer_details(id).await.unwrap();
        println!("{details:#?}");
        assert_eq!(details.id, id);
        assert_eq!(details.address, server.local_addr());
        assert!(details.n_collections > 0);
        assert!(collection_count.load(Ordering::SeqCst) > 0);
        logctx.cleanup_successful();
    }
}
