// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating service configuration to all sleds

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use serde_json::json;
use sled_agent_client::types::ServiceEnsureBody;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::watch;

type SledServices = HashMap<SocketAddr, ServiceEnsureBody>;

/// Background task that propagates service configuration to sleds
pub struct ServicePropagator {
    // Input: What are the services to be propagated?
    rx_config: watch::Receiver<Option<SledServices>>,

    max_concurrent_server_updates: usize,
}

impl ServicePropagator {
    pub fn new(
        rx_config: watch::Receiver<Option<SledServices>>,
        max_concurrent_server_updates: usize,
    ) -> ServicePropagator {
        ServicePropagator { rx_config, max_concurrent_server_updates }
    }
}

impl BackgroundTask for ServicePropagator {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            // Read the service list from the other background task that
            // assemble it.  Clone it because borrowing takes a read lock that
            // would block these tasks from updating the value.  We don't want
            // to do that while we go off (potentially for a while) attempting
            // to update the servers.
            let service_config = self.rx_config.borrow().clone();

            let service_config = match service_config {
                Some(s) => s,
                None => {
                    warn!(
                        &opctx.log,
                        "Service propagation: skipped";
                        "reason" => "no known services"
                    );
                    return json!({ "error": "no known services" });
                }
            };

            // Propagate the config to all of the sled agent servers.
            let log = &opctx.log;
            let result = services_propagate(
                &log,
                service_config,
                self.max_concurrent_server_updates,
            )
            .await;

            // Report results.
            if result.iter().any(|r| r.is_err()) {
                warn!(&log, "Sled agent service propagation: failed");
            } else {
                info!(&log, "Sled agent service propagation: done");
            }

            serde_json::to_value(
                result
                    .into_iter()
                    .map(|r| r.map_err(|e| format!("{:#}", e)))
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|error| {
                json!({
                    "error":
                        format!("failed to serialize final value: {:#}", error)
                })
            })
        }
        .boxed()
    }
}

async fn services_propagate(
    log: &slog::Logger,
    service_config: SledServices,
    max_concurrent_server_updates: usize,
) -> Vec<anyhow::Result<()>> {
    stream::iter(service_config.into_iter())
        .map(|(sled_id, services)| async move {
            services_propagate_one_sled(log, sled_id, services).await
        })
        .buffered(max_concurrent_server_updates)
        .collect()
        .await
}

async fn services_propagate_one_sled(
    log: &slog::Logger,
    sled_addr: SocketAddr,
    services: ServiceEnsureBody,
) -> anyhow::Result<()> {
    let url = format!("http://{}", sled_addr);
    let log = log.new(o!("sled_server_url" => url.clone()));
    let client = sled_agent_client::Client::new(&url, log.clone());

    let result = client.services_put(&services).await.with_context(|| {
        format!("failed to propagate services to sled {}", sled_addr,)
    });

    match result {
        Err(error) => {
            warn!(log, "{:#}", error);
            Err(error)
        }
        Ok(_) => {
            info!(
                log,
                "Sled Services Propagated";
                "address" => sled_addr,
                "generation" => services.generation.to_string(),
            );
            Ok(())
        }
    }
}

/*
#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::dns_propagation::ServicePropagator;
    use crate::app::background::dns_servers::DnsServersList;
    use dns_service_client::types::DnsConfigParams;
    use httptest::matchers::request;
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use serde_json::json;
    use tokio::sync::watch;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (config_tx, config_rx) = watch::channel(None);
        let (servers_tx, servers_rx) = watch::channel(None);
        let mut task = ServicePropagator::new(config_rx, servers_rx, 3);

        let dns_config = DnsConfigParams {
            generation: 1,
            time_created: chrono::Utc::now(),
            zones: vec![],
        };
        let dns_servers = DnsServersList { addresses: vec![] };

        // With no config or servers, we should fail with an appropriate
        // message.
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({ "error": "no config nor servers" }));
        config_tx.send(Some(dns_config.clone())).unwrap();
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({ "error": "no servers" }));
        config_tx.send(None).unwrap();
        servers_tx.send(Some(dns_servers)).unwrap();
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({ "error": "no config" }));

        // With a config and no servers, the operation should be a noop.
        config_tx.send(Some(dns_config)).unwrap();
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!([]));

        // Now, create some fake servers ready to respond successfully to a
        // propagation attempt.
        let mut s1 = httptest::Server::run();
        let mut s2 = httptest::Server::run();

        servers_tx
            .send(Some(DnsServersList {
                addresses: [&s1, &s2].iter().map(|s| s.addr()).collect(),
            }))
            .unwrap();

        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(request::method_path("PUT", "/config"))
                    .respond_with(status_code(204)),
            );
        }

        // Define a type we can use to pick stuff out of error objects.
        type ServerResult = Vec<Result<(), String>>;

        // Do it!
        let value = task.activate(&opctx).await;
        // Check the results.
        let result: ServerResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_ok());
        assert!(result[1].is_ok());
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Do it all again.  The task doesn't keep track of what servers have
        // what versions so both servers should see the same request
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(request::method_path("PUT", "/config"))
                    .respond_with(status_code(204)),
            );
        }

        let value = task.activate(&opctx).await;
        let result: ServerResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_ok());
        assert!(result[1].is_ok());
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Take another lap, but this time, have one server fail the request and
        // try again.
        s1.expect(
            Expectation::matching(request::method_path("PUT", "/config"))
                .respond_with(status_code(204)),
        );
        s2.expect(
            Expectation::matching(request::method_path("PUT", "/config"))
                .respond_with(status_code(500)),
        );

        let value = task.activate(&opctx).await;
        println!("{:?}", value);
        let result: ServerResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_ok());
        assert!(result[1].is_err());
        assert!(result[1].as_ref().unwrap_err().starts_with(&format!(
            "failed to propagate DNS generation 1 to server {}",
            s2.addr()
        )));
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Take one more lap.  Both servers should get the request again.  This
        // time we'll have both succeed again.
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(request::method_path("PUT", "/config"))
                    .respond_with(status_code(204)),
            );
        }

        let value = task.activate(&opctx).await;
        let result: ServerResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_ok());
        assert!(result[1].is_ok());
        s1.verify_and_clear();
        s2.verify_and_clear();
    }
}
*/
