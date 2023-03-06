// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

mod dns_server;
mod dns_types;
pub mod http_server; // XXX-dap remove pub?
mod storage;

// XXX-dap where should this go?  depends on what uses it besides the CLI?
#[derive(Deserialize, Debug)]
pub struct Config {
    pub log: dropshot::ConfigLogging,
    pub dropshot: dropshot::ConfigDropshot,
    pub data: storage::Config,
}

// XXX-dap still something weird about the fact that we have separate start()
// and pub start_dropshot_server().  Should there be one object that combines
// both?
pub async fn start(
    log: slog::Logger,
    config: Config,
    dns_address: SocketAddr,
) -> anyhow::Result<(
    dns_server::Server,
    dropshot::HttpServer<http_server::Context>,
)> {
    let db = Arc::new(sled::open(&config.data.storage_path)?);

    let dns_server = {
        let db = db.clone();
        let log = log.clone();
        let dns_config =
            dns_server::Config { bind_address: dns_address.to_string() };
        dns_server::run(log, db, dns_config).await?
    };

    let dropshot_server = start_dropshot_server(config, log, db).await?;

    Ok((dns_server, dropshot_server))
}

// XXX-dap weird that this config isn't just the dropshot config.  The reason is
// that the storage client that we create here also looks at the
// nmax_messages...but weirdly, it *doesn't* look at the storage section.
pub async fn start_dropshot_server(
    config: Config,
    log: slog::Logger,
    db: Arc<sled::Db>,
) -> Result<dropshot::HttpServer<http_server::Context>, anyhow::Error> {
    let data_client = storage::Client::new(
        log.new(slog::o!("component" => "DataClient")),
        db,
    );

    let api = http_server::api();
    let api_context = http_server::Context::new(data_client);

    Ok(dropshot::HttpServerStarter::new(
        &config.dropshot,
        api,
        api_context,
        &log,
    )
    .map_err(|e| anyhow!("{}", e))?
    .start())
}
