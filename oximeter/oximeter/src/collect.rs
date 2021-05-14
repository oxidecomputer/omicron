//! Collect metric data in an application and serve it to clients.

// Copyright 2021 Oxide Computer Company

use std::boxed::Box;
use std::collections::BTreeSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};

use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, ConfigLogging, HttpError,
    HttpResponseOk, HttpServer, HttpServerStarter, Path, RequestContext,
};
use reqwest::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{debug, info, o};
use uuid::Uuid;

use crate::types;
use crate::{Error, Producer};

/// Information describing how a [`MetricServer`] registers itself for collection.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct RegistrationInfo {
    address: SocketAddr,
    registration_route: String,
}

impl RegistrationInfo {
    /// Construct `RegistrationInfo`, to register at the given address and route.
    pub fn new<T>(address: T, route: &str) -> Self
    where
        T: ToSocketAddrs,
    {
        Self {
            address: address.to_socket_addrs().unwrap().next().unwrap(),
            registration_route: route.to_string(),
        }
    }

    /// Return the address of the server to be registered with.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Return the route at which the registration request will be sent.
    pub fn registration_route(&self) -> &str {
        &self.registration_route
    }
}

/// Idenitifier for a producer.
#[derive(Debug, Clone, Copy, PartialEq, JsonSchema, Serialize, Deserialize)]
pub struct ProducerId {
    pub producer_id: Uuid,
}

impl ProducerId {
    /// Construct a new producer ID.
    pub fn new() -> Self {
        Self { producer_id: Uuid::new_v4() }
    }
}

impl std::fmt::Display for ProducerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.producer_id.to_string())
    }
}

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct MetricServerInfo {
    producer_id: ProducerId,
    address: SocketAddr,
    collection_route: String,
}

impl MetricServerInfo {
    /// Generate info for a metric server listening on the given address and route.
    ///
    /// This will generate a new, random [`ProducerId`] for the server. The `base_route` should be
    /// a route stem, to which the producer ID will be appended.
    ///
    /// Example
    /// -------
    /// ```rust
    /// use oximeter::collect::MetricServerInfo;
    ///
    /// let info = MetricServerInfo::new("127.0.0.1:4444", "/collect");
    /// assert_eq!(info.collection_route(), format!("/collect/{}", info.producer_id());
    /// ```
    pub fn new<T>(address: T, base_route: &str) -> Self
    where
        T: ToSocketAddrs,
    {
        Self::with_id(ProducerId::new(), address, base_route)
    }

    /// Generate info for a metric server, listening on the given address and route, with a known
    /// ID.
    pub fn with_id<T>(
        producer_id: ProducerId,
        address: T,
        base_route: &str,
    ) -> Self
    where
        T: ToSocketAddrs,
    {
        Self {
            producer_id,
            address: address.to_socket_addrs().unwrap().next().unwrap(),
            collection_route: format!(
                "{}/{}",
                base_route, producer_id.producer_id
            ),
        }
    }

    /// Return the producer ID for this server.
    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    /// Return the address on which this server listens.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Return the route that can be used to request metric data.
    pub fn collection_route(&self) -> &str {
        &self.collection_route
    }
}

type ProducerList = Vec<Box<dyn Producer>>;
pub type ProducerResults = Vec<Result<BTreeSet<types::Sample>, Error>>;

/// A central collection point for metrics within an application.
#[derive(Clone)]
pub struct Collector {
    producers: Arc<Mutex<ProducerList>>,
    producer_id: ProducerId,
}

impl Collector {
    /// Construct a new `Collector`.
    pub fn new() -> Self {
        Self::with_id(ProducerId::new())
    }

    /// Construct a new `Collector` with the given producer ID.
    pub fn with_id(producer_id: ProducerId) -> Self {
        Self { producers: Arc::new(Mutex::new(vec![])), producer_id }
    }

    /// Register a new [`Producer`] object with the collector.
    pub fn register_producer(
        &self,
        producer: Box<dyn Producer>,
    ) -> Result<(), Error> {
        Ok(self.producers.lock().unwrap().push(producer))
    }

    /// Collect available samples from all registered producers.
    ///
    /// This method returns a vector of results, one from each producer. If the producer generates
    /// an error, that's propagated here. Successfully produced samples are returned in a set,
    /// ordered by the [`types::Sample::cmp`] method.
    pub fn collect(&self) -> ProducerResults {
        let mut producers = self.producers.lock().unwrap();
        let mut results = Vec::with_capacity(producers.len());
        for producer in producers.iter_mut() {
            results.push(producer.produce().map(|samples| samples.collect()));
        }
        results
    }

    /// Return the producer ID associated with this collector.
    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }
}

unsafe impl Sync for Collector {}
unsafe impl Send for Collector {}

/// Information used to configure a [`MetricServer`]
#[derive(Debug, Clone)]
pub struct MetricServerConfig {
    pub server_info: MetricServerInfo,
    pub registration_info: RegistrationInfo,
    pub dropshot_config: ConfigDropshot,
    pub logging_config: ConfigLogging,
}

/// A Dropshot server used to expose metrics to be collected over the network.
pub struct MetricServer {
    collector: Collector,
    server: HttpServer<Collector>,
}

impl MetricServer {
    /// Start a new metric server, registering it with the chosen endpoint, and listening for
    /// requests on the associated address and route.
    pub async fn start(config: &MetricServerConfig) -> Result<Self, Error> {
        let log = config
            .logging_config
            .to_logger("metric-server")
            .map_err(|msg| Error::MetricServer(msg.to_string()))?;
        let collector = Collector::with_id(config.server_info.producer_id);
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let server = HttpServerStarter::new(
            &config.dropshot_config,
            metric_server_api(),
            collector.clone(),
            &dropshot_log,
        )
        .map_err(|msg| Error::MetricServer(msg.to_string()))?
        .start();

        debug!(log, "registering metric server as a producer");
        register(
            &Client::new(),
            &config.registration_info,
            &config.server_info,
        )
        .await?;
        info!(
            log,
            "starting oximeter metric server";
            "route" => config.server_info.collection_route(),
            "producer_id" => collector.producer_id().to_string(),
            "address" => config.server_info.address(),
        );
        Ok(Self { collector, server })
    }

    /// Serve requests for metrics.
    pub async fn serve_forever(self) -> Result<(), Error> {
        Ok(self
            .server
            .await
            .map_err(|msg| Error::MetricServer(msg.to_string()))?)
    }

    /// Return the [`Collector`] managed by this server.
    ///
    /// The collector is thread-safe and clonable, so the returned reference can be used throughout
    /// an application to register [`Producer`]s. The samples generated by the registered producers
    /// will be included in response to a requst on the collection endpoint.
    pub fn collector(&self) -> &Collector {
        &self.collector
    }
}

// Register API endpoints of the `MetricServer`.
fn metric_server_api() -> ApiDescription<Collector> {
    let mut api = ApiDescription::new();
    api.register(collect_endpoint)
        .expect("Failed to register handler for collect_endpoint");
    api
}

// Implementation of the actual collection routine used by the `MetricServer`.
#[endpoint {
    method = GET,
    path = "/collect/{producer_id}",
}]
async fn collect_endpoint(
    request_context: Arc<RequestContext<Collector>>,
    path_params: Path<ProducerId>,
) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
    let collector = request_context.context();
    let producer_id = path_params.into_inner();
    collect(collector, producer_id).await
}

/// Register a metric server to be polled for metric data.
///
/// This function is used to provide consumers the flexibility to define their own Dropshot
/// servers, rather than using the `MetricServer` provided by this crate (which starts a _new_
/// server).
pub async fn register(
    client: &Client,
    registration_info: &RegistrationInfo,
    server_info: &MetricServerInfo,
) -> Result<(), Error> {
    client
        .post(format!(
            "http://{}{}",
            registration_info.address, registration_info.registration_route
        ))
        .json(server_info)
        .send()
        .await
        .map_err(|msg| Error::MetricServer(msg.to_string()))?
        .error_for_status()
        .map_err(|msg| Error::MetricServer(msg.to_string()))?;
    Ok(())
}

/// Handle a request to pull available metric data from a [`Collector`].
pub async fn collect(
    collector: &Collector,
    producer_id: ProducerId,
) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
    if producer_id == collector.producer_id() {
        Ok(HttpResponseOk(collector.collect()))
    } else {
        Err(HttpError::for_not_found(
            None,
            format!(
                "Producer ID {} is not valid, expected {}",
                producer_id,
                collector.producer_id()
            ),
        ))
    }
}
