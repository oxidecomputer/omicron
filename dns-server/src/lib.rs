// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// There are three pieces to our running DNS server:
///
/// 1. Persistent [`storage::Store`] of DNS data
/// 2. A [`dns_server::Server`], that serves the data in a `storage::Client` out
///    over the DNS protocol
/// 3. A Dropshot server that serves HTTP endpoints for reading and modifying
///    the persistent DNS data.

use anyhow::anyhow;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

pub mod dns_server;
pub mod dns_types;
pub mod http_server;
pub mod storage;

// XXX-dap where should this go?  depends on what uses it besides the CLI?
#[derive(Deserialize, Debug)]
pub struct Config {
    pub log: dropshot::ConfigLogging,
    pub dropshot: dropshot::ConfigDropshot,
    pub storage: storage::Config,
}

// XXX-dap weird that this config isn't just the dropshot config.  The reason is
// that the storage client that we create here also looks at the
// nmax_messages...but weirdly, it *doesn't* look at the storage section.
/// Starts just the Dropshot server
pub async fn start_dropshot_server(
    log: slog::Logger,
    store: storage::Store,
    config: &dropshot::Config,
) -> Result<dropshot::HttpServer<http_server::Context>, anyhow::Error> {
    let api = http_server::api();
    let api_context = http_server::Context::new(store);

    Ok(dropshot::HttpServerStarter::new(
        &config.dropshot,
        api,
        api_context,
        &log,
    )
    .map_err(|e| anyhow!("{}", e))?
    .start())
}
