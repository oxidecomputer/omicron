// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating DNS configuration to all DNS servers

use super::dns_servers::DnsServersList;
use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use internal_dns_types::config::DnsConfigParams;
use nexus_db_queries::context::OpContext;
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use tokio::sync::watch;

/// Background task that propagates DNS configuration to DNS servers
pub struct DnsPropagator {
    rx_config: watch::Receiver<Option<DnsConfigParams>>,
    rx_servers: watch::Receiver<Option<DnsServersList>>,
    max_concurrent_server_updates: usize,
}

impl DnsPropagator {
    pub fn new(
        rx_config: watch::Receiver<Option<DnsConfigParams>>,
        rx_servers: watch::Receiver<Option<DnsServersList>>,
        max_concurrent_server_updates: usize,
    ) -> DnsPropagator {
        DnsPropagator { rx_config, rx_servers, max_concurrent_server_updates }
    }
}

impl BackgroundTask for DnsPropagator {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // Read the DNS configuration and server list from the other
            // background tasks that assemble these.  Clone them because
            // borrowing takes a read lock that would block these tasks from
            // updating the value.  We don't want to do that while we go off
            // (potentially for a while) attempting to update the servers.
            let (dns_config, dns_servers) = {
                (
                    self.rx_config.borrow().clone(),
                    self.rx_servers.borrow().clone(),
                )
            };

            // Bail out early if we don't have both a valid configuration and a
            // list of servers.
            let (dns_config, dns_servers) = match (dns_config, dns_servers) {
                (Some(d), Some(s)) => (d, s),
                (None, None) => {
                    warn!(
                        &opctx.log,
                        "DNS propagation: skipped";
                        "reason" => "no config nor servers"
                    );
                    return json!({ "error": "no config nor servers" });
                }
                (None, Some(_)) => {
                    warn!(
                        &opctx.log,
                        "DNS propagation: skipped";
                        "reason" => "no config"
                    );
                    return json!({ "error": "no config" });
                }
                (Some(_), None) => {
                    warn!(
                        &opctx.log,
                        "DNS propagation: skipped";
                        "reason" => "no servers"
                    );
                    return json!({ "error": "no servers" });
                }
            };

            // Set up a logger for this activation that includes metadata about
            // the current generation and servers.
            let log = opctx.log.new(o!(
                "generation" => u64::from(dns_config.generation),
                "servers" => format!("{:?}", dns_servers),
            ));

            // Propagate the config to all of the DNS servers.
            let result = dns_propagate(
                &log,
                &dns_config,
                &dns_servers,
                self.max_concurrent_server_updates,
            )
            .await;

            // Report results.
            if result.iter().any(|(_, r)| r.is_err()) {
                warn!(&log, "DNS propagation: failed");
            } else {
                info!(&log, "DNS propagation: done");
            }

            let server_results = serde_json::to_value(
                result
                    .into_iter()
                    .map(|(e, r)| (e, r.map_err(|e| format!("{:#}", e))))
                    .collect::<BTreeMap<_, _>>(),
            )
            .unwrap_or_else(|error| {
                json!({
                    "error":
                        format!("failed to serialize final value: {:#}", error)
                })
            });

            json!({
                "generation": dns_config.generation,
                "server_results": server_results,
            })
        }
        .boxed()
    }
}

async fn dns_propagate(
    log: &slog::Logger,
    dns_config: &DnsConfigParams,
    servers: &DnsServersList,
    max_concurrent_server_updates: usize,
) -> BTreeMap<String, anyhow::Result<()>> {
    stream::iter(servers.addresses.clone())
        .map(|server_addr| async move {
            let result = dns_propagate_one(log, dns_config, &server_addr).await;
            (server_addr.to_string(), result)
        })
        .buffered(max_concurrent_server_updates)
        .collect()
        .await
}

async fn dns_propagate_one(
    log: &slog::Logger,
    dns_config: &DnsConfigParams,
    server_addr: &SocketAddr,
) -> anyhow::Result<()> {
    let url = format!("http://{}", server_addr);
    let log = log.new(o!("dns_server_url" => url.clone()));
    let client = dns_service_client::Client::new(&url, log.clone());

    let result = client.dns_config_put(dns_config).await.with_context(|| {
        format!(
            "failed to propagate DNS generation {} to server {}",
            dns_config.generation, server_addr,
        )
    });

    match result {
        Err(error) => {
            warn!(log, "{:#}", error);
            Err(error)
        }
        Ok(_) => {
            info!(
                log,
                "DNS server now at generation {}", dns_config.generation
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::DnsPropagator;
    use crate::app::background::BackgroundTask;
    use crate::app::background::tasks::dns_servers::DnsServersList;
    use httptest::Expectation;
    use httptest::matchers::request;
    use httptest::responders::status_code;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::params::DnsConfigParams;
    use omicron_common::api::external::Generation;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tokio::sync::watch;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (config_tx, config_rx) = watch::channel(None);
        let (servers_tx, servers_rx) = watch::channel(None);
        let mut task = DnsPropagator::new(config_rx, servers_rx, 3);

        let dns_config = DnsConfigParams {
            generation: Generation::from_u32(1),
            serial: 1,
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

        // Define a type we can use to pick stuff out of error objects.
        #[derive(Deserialize)]
        struct ServerResult {
            server_results: BTreeMap<String, Result<(), String>>,
            generation: u32,
        }

        // With a config and no servers, the operation should be a noop.
        config_tx.send(Some(dns_config)).unwrap();
        let value = task.activate(&opctx).await;
        let result: ServerResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.generation, 1);
        assert!(result.server_results.is_empty());

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

        // Do it!
        let value = task.activate(&opctx).await;
        // Check the results.
        let result: ServerResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.server_results.len(), 2);
        assert!(result.server_results.values().all(|r| r.is_ok()));
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
        assert_eq!(result.server_results.len(), 2);
        assert!(result.server_results.values().all(|r| r.is_ok()));
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
        assert_eq!(result.server_results.len(), 2);
        assert!(result.server_results.values().any(|r| r.is_ok()));
        let message = result
            .server_results
            .values()
            .find(|r| r.is_err())
            .cloned()
            .unwrap()
            .unwrap_err();
        assert!(message.starts_with(&format!(
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
        assert_eq!(result.server_results.len(), 2);
        assert!(result.server_results.values().all(|r| r.is_ok()));
        s1.verify_and_clear();
        s2.verify_and_clear();
    }
}
