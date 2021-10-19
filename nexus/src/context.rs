/*!
 * Shared state used by API request handlers
 */
use super::db;
use super::Nexus;

use oximeter::types::ProducerRegistry;
use oximeter_instruments::http::{HttpService, LatencyTracker};
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Shared state available to all API request handlers
 */
pub struct ServerContext {
    /** reference to the underlying nexus */
    pub nexus: Arc<Nexus>,
    /** debug log */
    pub log: Logger,
    /** internal API request latency tracker */
    pub internal_latencies: LatencyTracker,
    /** external API request latency tracker */
    pub external_latencies: LatencyTracker,
    /** registry of metric producers */
    pub producer_registry: ProducerRegistry,
}

impl ServerContext {
    /**
     * Create a new context with the given rack id and log.  This creates the
     * underlying nexus as well.
     */
    pub fn new(
        rack_id: &Uuid,
        log: Logger,
        pool: db::Pool,
        nexus_id: &Uuid,
    ) -> Arc<ServerContext> {
        let create_tracker = |name: &str| {
            let target = HttpService { name: name.to_string(), id: *nexus_id };
            const START_LATENCY_DECADE: i8 = -6;
            const END_LATENCY_DECADE: i8 = 3;
            LatencyTracker::with_latency_decades(
                target,
                START_LATENCY_DECADE,
                END_LATENCY_DECADE,
            )
            .unwrap()
        };
        let internal_latencies = create_tracker("nexus-internal");
        let external_latencies = create_tracker("nexus-external");
        let producer_registry = ProducerRegistry::with_id(*nexus_id);
        producer_registry
            .register_producer(internal_latencies.clone())
            .unwrap();
        producer_registry
            .register_producer(external_latencies.clone())
            .unwrap();

        Arc::new(ServerContext {
            nexus: Nexus::new_with_id(
                rack_id,
                log.new(o!("component" => "nexus")),
                pool,
                nexus_id,
            ),
            log,
            internal_latencies,
            external_latencies,
            producer_registry,
        })
    }
}
