//! Implementation of the `oximeter` metric collection server.
// Copyright 2021 Oxide Computer Company

use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, ConfigLogging, HttpError,
    HttpResponseUpdatedNoContent, HttpServer, HttpServerStarter,
    RequestContext, TypedBody,
};
use omicron_common::backoff;
use omicron_common::model::{ProducerId, ProducerServerInfo};
use reqwest::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{debug, info, o, warn, Logger};
use uuid::Uuid;

use crate::Error;

/// The internal agent the oximeter server uses to collect metrics from producers.
struct OximeterAgent {
    pub id: Uuid,
    _client: Client,
    log: Logger,
    producers: Arc<Mutex<BTreeMap<ProducerId, ProducerServerInfo>>>,
}

impl OximeterAgent {
    /// Construct a new agent, using the `log` as its root logger.
    ///
    /// This will internally create a new random UUID as the agent's ID.
    pub fn new(log: &Logger) -> Self {
        let log = log.new(o!("component" => "oximeter-agent"));
        Self::with_id(Uuid::new_v4(), log)
    }

    /// Construct a new agent with the given ID and logger.
    pub fn with_id(id: Uuid, log: Logger) -> Self {
        Self {
            id,
            _client: Client::new(),
            log,
            producers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Register a new producer with this oximeter instance.
    pub fn register_producer(
        &self,
        info: ProducerServerInfo,
    ) -> Result<(), Error> {
        info!(self.log, "registering new metric producer";
              "producer_id" => info.producer_id().to_string(),
              "address" => info.address(),
        );
        let id = info.producer_id();
        match self.producers.lock().unwrap().entry(id) {
            Entry::Vacant(value) => {
                value.insert(info);
                debug!(self.log, "inserted new producer"; "producer_id" => id.to_string());
                Ok(())
            }
            Entry::Occupied(_) => {
                debug!(
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
pub struct Config {
    /// The address used to connect to Nexus.
    pub nexus_address: SocketAddr,

    /// The internal Dropshot HTTP server configuration
    pub dropshot: ConfigDropshot,

    /// Logging configuration
    pub log: ConfigLogging,
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterStartupInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
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
        let agent = Arc::new(OximeterAgent::new(&log));

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
    body: TypedBody<ProducerServerInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let agent = request_context.context();
    let server_info = body.into_inner();
    agent
        .register_producer(server_info)
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
    Ok(HttpResponseUpdatedNoContent())
}
