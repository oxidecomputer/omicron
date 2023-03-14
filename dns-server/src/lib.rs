// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Dropshot-configurable DNS server
///
/// This crate provides a standalone program that runs a UDP-based DNS server
/// along with a Dropshot server for configuring the records served over DNS.
/// The following RFDs describe the overall design of this server and how it's
/// used:
///
///   RFD 248 Omicron service discovery: server side
///   RFD 357 External DNS in the MVP
///   RFD 367 DNS propagation in Omicron
///
/// Here are the highlights:
///
/// (1) This component is used for both internal and external DNS in Omicron
///     (the control plane for the Oxide system).  These are deployed
///     separately.  A given instance is either an internal DNS server or an
///     external one, not both.
///
/// (2) For the internal DNS use case, it's absolutely essential that the DNS
///     servers have no external dependencies.  That's why we persistently store
///     a copy of the DNS data.  After a cold start of the whole Oxide system,
///     the DNS servers must be able to come up and serve DNS so that the rest of
///     the control plane components and locate each other.  The internal DNS use
///     case is expected to be a fairly small amount of data, updated fairly
///     infrequently, and not particularly latency-sensitive.
///
/// (3) External DNS is required for availability of user-facing services like
///     the web console and API for the Oxide system.   Eventually, these will
///     also provide names for user resources like Instances.  As a result,
///     there could be a fair bit of data and it may be updated fairly
///     frequently.
///
/// This crate provides three main pieces for running the DNS server program:
///
/// 1. Persistent [`storage::Store`] of DNS data
/// 2. A [`dns_server::Server`], that serves data from a `storage::Store` out
///    over the DNS protocol
/// 3. A Dropshot server that serves HTTP endpoints for reading and modifying
///    the persistent DNS data

pub mod dns_server;
pub mod dns_types;
pub mod http_server;
pub mod storage;

use anyhow::{anyhow, Context};
use slog::o;

/// Starts both the HTTP and DNS servers over a given store.
pub async fn start_servers(
    log: slog::Logger,
    store: storage::Store,
    dns_server_config: &dns_server::Config,
    dropshot_config: &dropshot::ConfigDropshot,
) -> Result<
    (dns_server::ServerHandle, dropshot::HttpServer<http_server::Context>),
    anyhow::Error,
> {
    let dns_server = {
        dns_server::Server::start(
            log.new(o!("component" => "dns")),
            store.clone(),
            dns_server_config,
        )
        .await
        .context("starting DNS server")?
    };

    let dropshot_server = {
        let http_api = http_server::api();
        let http_api_context = http_server::Context::new(store);

        dropshot::HttpServerStarter::new(
            dropshot_config,
            http_api,
            http_api_context,
            &log.new(o!("component" => "http")),
        )
        .map_err(|error| anyhow!("setting up HTTP server: {:#}", error))?
        .start()
    };

    Ok((dns_server, dropshot_server))
}
