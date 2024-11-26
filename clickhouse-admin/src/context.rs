// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use oximeter_db::Client as OximeterClient;
use slog::Logger;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ServerContext {
    clickward: Clickward,
    clickhouse_cli: ClickhouseCli,
    _log: Logger,
}

impl ServerContext {
    pub fn new(
        clickward: Clickward,
        clickhouse_cli: ClickhouseCli,
        _log: Logger,
    ) -> Self {
        Self { clickward, clickhouse_cli, _log }
    }

    pub fn clickward(&self) -> &Clickward {
        &self.clickward
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }
}

pub struct SingleServerContext {
    clickhouse_cli: ClickhouseCli,
    oximeter_client: OximeterClient,
    initialization_lock: Arc<Mutex<()>>,
}

impl SingleServerContext {
    pub fn new(clickhouse_cli: ClickhouseCli) -> Self {
        let ip = clickhouse_cli.listen_address.ip();
        let address = SocketAddrV6::new(*ip, CLICKHOUSE_TCP_PORT, 0, 0);
        let log = clickhouse_cli
            .log
            .as_ref()
            .expect("should have configured logging via CLI or config");
        let oximeter_client = OximeterClient::new(address.into(), log);
        Self {
            clickhouse_cli,
            oximeter_client,
            initialization_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn oximeter_client(&self) -> &OximeterClient {
        &self.oximeter_client
    }

    pub fn initialization_lock(&self) -> Arc<Mutex<()>> {
        self.initialization_lock.clone()
    }
}
