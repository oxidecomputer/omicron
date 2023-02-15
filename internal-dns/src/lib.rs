// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![allow(clippy::type_complexity)]
#![allow(clippy::ptr_arg)]

use anyhow::anyhow;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

pub mod dns_data;
pub mod dns_server;
pub mod dropshot_server;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub log: dropshot::ConfigLogging,
    pub dropshot: dropshot::ConfigDropshot,
    pub data: dns_data::Config,
}

pub async fn start(
    log: slog::Logger,
    config: Config,
    zone: String,
    dns_address: SocketAddr,
) -> anyhow::Result<(
    dns_server::Server,
    dropshot::HttpServer<Arc<dropshot_server::Context>>,
)> {
    let db = Arc::new(sled::open(&config.data.storage_path)?);

    let dns_server = {
        let db = db.clone();
        let log = log.clone();
        let dns_config = dns_server::Config {
            bind_address: dns_address.to_string(),
            zone: zone.to_string(),
        };
        dns_server::run(log, db, dns_config).await?
    };

    let dropshot_server = start_dropshot_server(config, log, db).await?;

    Ok((dns_server, dropshot_server))
}

pub async fn start_dropshot_server(
    config: Config,
    log: slog::Logger,
    db: Arc<sled::Db>,
) -> Result<dropshot::HttpServer<Arc<dropshot_server::Context>>, anyhow::Error>
{
    let data_client = dns_data::Client::new(
        log.new(slog::o!("component" => "DataClient")),
        &config.data,
        db,
    );

    let api = dropshot_server::api();
    let api_context = Arc::new(dropshot_server::Context::new(data_client));

    Ok(dropshot::HttpServerStarter::new(
        &config.dropshot,
        api,
        api_context,
        &log,
    )
    .map_err(|e| anyhow!("{}", e))?
    .start())
}
