// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating DNS configuration to all DNS servers

use super::common::BackgroundTask;
use super::dns_servers::DnsServersList;
use anyhow::Context;
use dns_service_client::types::DnsConfigParams;
use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use serde_json::json;
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
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
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
                "generation" => dns_config.generation,
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
            if result.iter().any(|r| r.is_err()) {
                warn!(&log, "DNS propagation: failed");
            } else {
                info!(&log, "DNS propagation: done");
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

async fn dns_propagate(
    log: &slog::Logger,
    dns_config: &DnsConfigParams,
    servers: &DnsServersList,
    max_concurrent_server_updates: usize,
) -> Vec<anyhow::Result<()>> {
    stream::iter(&servers.addresses)
        .map(|server_addr| async move {
            dns_propagate_one(log, dns_config, server_addr).await
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
