// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating DNS configuration to all DNS servers

use super::common::BackgroundTask;
use super::dns_servers::DnsServersList;
use dns_service_client::types::DnsConfigParams;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use tokio::sync::watch;

/// Background task that propagates DNS configuration to DNS servers
pub struct DnsPropagator {
    rx_config: watch::Receiver<Option<DnsConfigParams>>,
    rx_servers: watch::Receiver<Option<DnsServersList>>,
}

impl DnsPropagator {
    pub fn new(
        rx_config: watch::Receiver<Option<DnsConfigParams>>,
        rx_servers: watch::Receiver<Option<DnsServersList>>,
    ) -> DnsPropagator {
        DnsPropagator { rx_config, rx_servers }
    }
}

impl BackgroundTask for DnsPropagator {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, ()>
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
                    return;
                }
                (None, Some(_)) => {
                    warn!(
                        &opctx.log,
                        "DNS propagation: skipped";
                        "reason" => "no config"
                    );
                    return;
                }
                (Some(_), None) => {
                    warn!(
                        &opctx.log,
                        "DNS propagation: skipped";
                        "reason" => "no servers"
                    );
                    return;
                }
            };

            // Set up a logger for this activation that includes metadata about
            // the current generation and servers.
            let log = opctx.log.new(o!(
                "generation" => dns_config.generation,
                "servers" => format!("{:?}", dns_servers),
            ));

            // Propate the config to all of the DNS servers.
            match dns_propagate(opctx, &log, &dns_config, &dns_servers).await {
                Ok(_) => {
                    info!(&log, "DNS propagation: done");
                    // XXX-dap track this somewhere for visibility
                }
                Err(error) => {
                    info!(
                        &log,
                        "DNS propagation: failed";
                        "error" => format!("{:#}", error)
                    );
                }
            };
        }
        .boxed()
    }
}

async fn dns_propagate(
    opctx: &OpContext,
    log: &slog::Logger,
    dns_config: &DnsConfigParams,
    servers: &DnsServersList,
) -> anyhow::Result<()> {
    todo!();
}
