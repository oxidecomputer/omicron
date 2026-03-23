// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of a standalone fake Nexus, simply for registering producers
//! and collectors with one another.

// Copyright 2024 Oxide Computer Company

use crate::Error;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::HttpError;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::HttpServer;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use dropshot::TypedBody;
use dropshot::endpoint;
use nexus_types::internal_api::params::OximeterInfo;
use omicron_common::FileKv;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerRegistrationResponse;
use rand::seq::IteratorRandom;
use slog::Drain;
use slog::Level;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

// An assignment of a producer to an oximeter collector.
#[derive(Debug)]
struct ProducerAssignment {
    producer: ProducerEndpoint,
    collector_id: Uuid,
}

#[derive(Debug)]
struct Inner {
    // Map of producers by ID to their information and assigned oximeter
    // collector.
    producers: HashMap<Uuid, ProducerAssignment>,
    // Map of available oximeter collectors.
    collectors: HashMap<Uuid, OximeterInfo>,
}

impl Inner {
    fn random_collector(&self) -> Option<(Uuid, OximeterInfo)> {
        self.collectors
            .iter()
            .choose(&mut rand::rng())
            .map(|(id, info)| (*id, *info))
    }
}

// The period on which producers must renew their lease.
//
// This is different from the one we actually use in Nexus (and shorter). That's
// fine, since this is really a testing interface more than anything.
const PRODUCER_RENEWAL_INTERVAL: Duration = Duration::from_secs(60);

const fn default_producer_response() -> ProducerRegistrationResponse {
    ProducerRegistrationResponse { lease_duration: PRODUCER_RENEWAL_INTERVAL }
}

// A stripped-down Nexus server, with only the APIs for registering metric
// producers and collectors.
#[derive(Debug)]
pub struct StandaloneNexus {
    pub log: Logger,
    inner: Mutex<Inner>,
}

impl StandaloneNexus {
    fn new(log: Logger) -> Self {
        Self {
            log,
            inner: Mutex::new(Inner {
                producers: HashMap::new(),
                collectors: HashMap::new(),
            }),
        }
    }

    /// Register an oximeter producer, returning the lease period.
    async fn register_producer(
        &self,
        info: &ProducerEndpoint,
    ) -> Result<ProducerRegistrationResponse, HttpError> {
        let mut inner = self.inner.lock().await;
        let assignment = match inner.producers.get_mut(&info.id) {
            None => {
                // There is no record for this producer.
                //
                // Select a random collector, and assign it to the producer.
                // We'll return the assignment from this match block.
                let Some((collector_id, _collector_info)) =
                    inner.random_collector()
                else {
                    return Err(HttpError::for_unavail(
                        None,
                        String::from("No collectors available"),
                    ));
                };
                let assignment =
                    ProducerAssignment { producer: *info, collector_id };
                assignment
            }
            Some(existing_assignment) => {
                // We have a record, first check if it matches the assignment we
                // have.
                if &existing_assignment.producer == info {
                    return Ok(default_producer_response());
                }

                // This appears to be a re-registration, e.g., the producer
                // changed its IP address. The collector will learn of this when
                // it next fetches its list.
                let collector_id = existing_assignment.collector_id;
                ProducerAssignment { producer: *info, collector_id }
            }
        };
        inner.producers.insert(info.id, assignment);
        Ok(default_producer_response())
    }

    async fn register_collector(
        &self,
        info: OximeterInfo,
    ) -> Result<(), HttpError> {
        // No-op if this is being re-registered. It will fetch its list of
        // producers again if needed.
        self.inner.lock().await.collectors.insert(info.collector_id, info);
        Ok(())
    }
}

// Build the HTTP API of the fake Nexus for registration.
pub fn standalone_nexus_api() -> ApiDescription<Arc<StandaloneNexus>> {
    let mut api = ApiDescription::new();
    api.register(cpapi_producers_post)
        .expect("Could not register cpapi_producers_post API handler");
    api.register(cpapi_collectors_post)
        .expect("Could not register cpapi_collectors_post API handler");
    api
}

/// Accept a registration from a new metric producer
#[endpoint {
     method = POST,
     path = "/metrics/producers",
 }]
async fn cpapi_producers_post(
    request_context: RequestContext<Arc<StandaloneNexus>>,
    producer_info: TypedBody<ProducerEndpoint>,
) -> Result<HttpResponseCreated<ProducerRegistrationResponse>, HttpError> {
    let context = request_context.context();
    let producer_info = producer_info.into_inner();
    context
        .register_producer(&producer_info)
        .await
        .map(HttpResponseCreated)
        .map_err(|e| HttpError::for_internal_error(e.to_string()))
}

/// Accept a notification of a new oximeter collection server.
#[endpoint {
     method = POST,
     path = "/metrics/collectors",
 }]
async fn cpapi_collectors_post(
    request_context: RequestContext<Arc<StandaloneNexus>>,
    oximeter_info: TypedBody<OximeterInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let context = request_context.context();
    let oximeter_info = oximeter_info.into_inner();
    context
        .register_collector(oximeter_info)
        .await
        .map(|_| HttpResponseUpdatedNoContent())
        .map_err(|e| HttpError::for_internal_error(e.to_string()))
}

/// A standalone Nexus server, with APIs only for registering metric collectors
/// and producers.
pub struct Server {
    server: HttpServer<Arc<StandaloneNexus>>,
}

impl Server {
    /// Create a new server listening on the provided address.
    pub fn new(address: SocketAddr, log_level: Level) -> Result<Self, Error> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let drain = slog::LevelFilter::new(drain, log_level).fuse();
        let (drain, registration) = slog_dtrace::with_drain(drain);
        let log = slog::Logger::root(drain.fuse(), o!(FileKv));
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            return Err(Error::Server(msg));
        } else {
            debug!(log, "registered DTrace probes");
        }

        let nexus = Arc::new(StandaloneNexus::new(
            log.new(slog::o!("component" => "nexus-standalone")),
        ));
        let server = ServerBuilder::new(
            standalone_nexus_api(),
            Arc::clone(&nexus),
            log.clone(),
        )
        .config(ConfigDropshot { bind_address: address, ..Default::default() })
        .start()
        .map_err(|e| Error::Server(e.to_string()))?;
        info!(
            log,
            "created standalone nexus server for metric collections";
            "address" => %address,
        );
        Ok(Self { server })
    }

    pub fn log(&self) -> &Logger {
        &self.server.app_private().log
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }
}
