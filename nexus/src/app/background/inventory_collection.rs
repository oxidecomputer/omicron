// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reading inventory for the rack

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use internal_dns::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_types::inventory::Collection;
use omicron_common::address::MGS_PORT;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that reads inventory for the rack
pub struct InventoryCollector {
    resolver: internal_dns::resolver::Resolver,
    creator: String,
    tx: watch::Sender<Option<Collection>>,
    rx: watch::Receiver<Option<Collection>>,
}

impl InventoryCollector {
    pub fn new(
        resolver: internal_dns::resolver::Resolver,
        creator: &str,
    ) -> InventoryCollector {
        let (tx, rx) = watch::channel(None);
        InventoryCollector { resolver, creator: creator.to_owned(), tx, rx }
    }

    /// Exposes the latest inventory collection
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// configuration or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<Collection>> {
        self.rx.clone()
    }
}

impl BackgroundTask for InventoryCollector {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            match do_collect(&self.resolver, &self.creator, &opctx.log)
                .await
                .context("failed to collect inventory")
            {
                Err(error) => {
                    let message = format!("{:#}", error);
                    warn!(opctx.log, "inventory collection failed";
                        "error" => message.clone());
                    json!({ "error": message })
                }
                Ok(collection) => {
                    debug!(opctx.log, "inventory collection complete";
                        "collection_id" => collection.id.to_string(),
                        "time_started" => collection.time_started.to_string(),
                    );
                    let result = json!({
                        "collection_id": collection.id.to_string(),
                        "time_started": collection.time_started.to_string(),
                        "time_done": collection.time_done.to_string()
                    });
                    self.tx.send_replace(Some(collection));
                    result
                }
            }
        }
        .boxed()
    }
}

async fn do_collect(
    resolver: &internal_dns::resolver::Resolver,
    creator: &str,
    log: &slog::Logger,
) -> Result<Collection, anyhow::Error> {
    // Find MGS clients.
    // XXX-dap separate background task?
    // XXX-dap definitely not a great way to do this.
    let switch_zone_ips = resolver
        .lookup_all_ipv6(ServiceName::Dendrite)
        .await
        .context("looking up switch zone addresses")?;

    // XXX-dap doubly bad (hardcoding port)
    let mgs_clients = switch_zone_ips
        .into_iter()
        .map(|ip| {
            let sockaddr = std::net::SocketAddr::V6(
                std::net::SocketAddrV6::new(ip, MGS_PORT, 0, 0),
            );
            let url = format!("http://{}", sockaddr);
            let log = log.new(o!("gateway_url" => url.clone()));
            Arc::new(gateway_client::Client::new(&url, log))
        })
        .collect::<Vec<_>>();

    // Run a collection.
    let inventory = nexus_inventory::Collector::new(
        creator,
        "activation", // TODO-dap useless
        &mgs_clients,
    );
    inventory.enumerate().await.context("collecting inventory")
}
