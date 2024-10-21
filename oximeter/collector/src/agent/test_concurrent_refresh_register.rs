// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpServer;
use dropshot::HttpServerStarter;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanParams as _;
use omicron_common::api::internal::nexus::ProducerKind;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::test_setup_log;
use qorb::backend::Backend;
use qorb::backend::Connector;
use qorb::policy::Policy;
use qorb::resolvers::single_host::SingleHostResolver;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

#[tokio::test]
async fn test_register_during_refresh() {
    let logctx = test_setup_log("test_register_during_refresh");
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

    // It should have no registered producers.
    {
        let producers = collector.producers.lock().await;
        assert!(producers.tasks.is_empty());
        assert_eq!(producers.generation, 0);
    }

    // Create three dummy producers. We'll return the first two from the
    // "refresh from Nexus" path, and concurrently register the third.
    let dummy_producer_servers = (0..3)
        .map(|_| {
            let server = httpmock::MockServer::start();
            server.mock(|when, then| {
                when.any_request();
                then.status(reqwest::StatusCode::OK).body("[]");
            });
            server
        })
        .collect::<Vec<_>>();
    let endpoints = dummy_producer_servers
        .iter()
        .map(|server| ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: *server.address(),
            interval: Duration::from_secs(3600),
        })
        .collect::<Vec<_>>();

    // Start our mock Nexus server. It will only respond from the producer list
    // endpoint when we send it producers over this channel.
    let (producers_to_list_tx, producers_to_list_rx) = mpsc::channel(1);
    let mock_nexus = start_mock_nexus(producers_to_list_rx).await;
    let nexus_pool = Pool::new(
        Box::new(SingleHostResolver::new(mock_nexus.local_addr())),
        Arc::new(NexusClientConnector { log: log.clone() }),
        Policy::default(),
    );

    // Start the refresh task (pointed to our mock nexus).
    collector.ensure_producer_refresh_task(nexus_pool);

    // Wait until our mock nexus has received the request for its first page of
    // producers.
    wait_for_condition(
        || async {
            match mock_nexus
                .app_private()
                .requests_received
                .load(Ordering::SeqCst)
            {
                0 => Err(CondCheckError::NotYet),
                1 => Ok(()),
                n => Err(CondCheckError::Failed(format!(
                    "unexpected request count: {n}"
                ))),
            }
        },
        &Duration::from_millis(100),
        &Duration::from_secs(30),
    )
    .await
    .expect("waited until 1 request received");

    // We know `collector` has started its refresh operation, but it's blocked
    // because our mock Nexus won't respond until we tell it to (which we'll do
    // below). In the meantime, explicitly register one producer.
    collector
        .register_producer(endpoints[0].clone())
        .await
        .expect("registered producer");
    {
        let producers = collector.producers.lock().await;
        assert_eq!(producers.tasks.len(), 1);
        assert!(producers.tasks.contains_key(&endpoints[0].id));
        assert_eq!(producers.generation, 1);
    }

    // Send two pages to the collector's refresh operation via our channel to
    // our mock Nexus; this will finish the refresh operation, but we explicitly
    // omit endpoint[0].
    producers_to_list_tx
        .send(endpoints[1..].to_vec())
        .await
        .expect("send page to mock nexus");
    producers_to_list_tx.send(vec![]).await.expect("send page to mock nexus");

    // Wait until the collector's generation has been bumped, signaling its
    // completion of the refresh.
    wait_for_condition(
        || async {
            match collector.collection_generation().await {
                1 => Err(CondCheckError::NotYet),
                2 => Ok(()),
                n => Err(CondCheckError::Failed(format!(
                    "unexpected generation: {n}"
                ))),
            }
        },
        &Duration::from_millis(100),
        &Duration::from_secs(3),
    )
    .await
    .expect("waited until refresh complete");

    // The refresh operation should have bumped our generation and added the two
    // new producers, but _not_ pruned the producer that was added while it was
    // running.
    {
        let producers = collector.producers.lock().await;
        assert_eq!(producers.generation, 2);
        assert_eq!(producers.tasks.len(), 3);
        for (i, endpoint) in endpoints.iter().enumerate() {
            assert!(
                producers.tasks.contains_key(&endpoint.id),
                "missing producer {i}"
            );
        }
    }

    logctx.cleanup_successful();
}

struct NexusClientConnector {
    log: Logger,
}

#[async_trait::async_trait]
impl Connector for NexusClientConnector {
    type Connection = NexusClient;

    async fn connect(
        &self,
        backend: &Backend,
    ) -> Result<Self::Connection, qorb::backend::Error> {
        let baseurl = format!("http://{}", backend.address);
        Ok(NexusClient::new(&baseurl, self.log.clone()))
    }
}

async fn start_mock_nexus(
    producers_to_return_rx: Receiver<Vec<ProducerEndpoint>>,
) -> HttpServer<Arc<MockNexusContext>> {
    let config_dropshot = ConfigDropshot {
        bind_address: "[::1]:0".parse().unwrap(),
        ..Default::default()
    };

    let config_logging =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info };
    let log = config_logging
        .to_logger("example-basic")
        .map_err(|error| format!("failed to create logger: {}", error))
        .expect("created logger");

    let mut api = ApiDescription::new();
    api.register(cpapi_assigned_producers_list).unwrap();

    let api_context = MockNexusContext {
        requests_received: AtomicU64::new(0),
        producers_to_return_rx: Mutex::new(producers_to_return_rx),
    };

    let server = HttpServerStarter::new(
        &config_dropshot,
        api,
        Arc::new(api_context),
        &log,
    )
    .map_err(|error| format!("failed to create server: {}", error))
    .expect("started mock nexus dropshot server")
    .start();

    server
}

struct MockNexusContext {
    requests_received: AtomicU64,
    producers_to_return_rx: Mutex<Receiver<Vec<ProducerEndpoint>>>,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
struct CollectorIdPathParams {
    collector_id: Uuid,
}

#[endpoint {
    method = GET,
    path = "/metrics/collectors/{collector_id}/producers",
}]
async fn cpapi_assigned_producers_list(
    context: RequestContext<Arc<MockNexusContext>>,
    _path_params: Path<CollectorIdPathParams>,
    query: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<ProducerEndpoint>>, HttpError> {
    let context = context.context();
    let query = query.into_inner();

    context.requests_received.fetch_add(1, Ordering::SeqCst);

    let producers = tokio::time::timeout(
        Duration::from_secs(30),
        context.producers_to_return_rx.lock().await.recv(),
    )
    .await
    .expect("received producers from test before timeout")
    .expect("producer channel from test was not closed");

    let page = ScanById::results_page(
        &query,
        producers,
        &|_, producer: &ProducerEndpoint| producer.id,
    )
    .expect("constructed results page");
    eprintln!("constructed results page {page:?}");

    Ok(HttpResponseOk(page))
}
