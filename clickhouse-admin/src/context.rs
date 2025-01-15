// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_common::api::external::Generation;
use oximeter_db::Client as OximeterClient;
use slog::Logger;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct KeeperServerContext {
    clickward: Clickward,
    clickhouse_cli: ClickhouseCli,
    log: Logger,
    generation: Option<Generation>,
}

impl KeeperServerContext {
    pub fn new(clickhouse_cli: ClickhouseCli) -> Self {
        let log = clickhouse_cli
            .log
            .new(slog::o!("component" => "KeeperServerContext"));
        let clickward = Clickward::new();
        // TODO: Read configuration file to retrieve generation
        // number if it already exists.
        let generation = None;
        Self { clickward, clickhouse_cli, log, generation }
    }

    pub fn clickward(&self) -> &Clickward {
        &self.clickward
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn generation(&self) -> Option<Generation> {
        self.generation
    }

    // TODO: actually make this work
    pub fn update_generation(mut self, new_generation: Generation) {
        self.generation = Some(new_generation)
    }
}

pub struct ServerContext {
    clickhouse_cli: ClickhouseCli,
    clickward: Clickward,
    oximeter_client: OximeterClient,
    initialization_lock: Arc<Mutex<()>>,
    log: Logger,
    generation: Option<Generation>,
}

impl ServerContext {
    pub fn new(clickhouse_cli: ClickhouseCli) -> Self {
        let ip = clickhouse_cli.listen_address.ip();
        let address = SocketAddrV6::new(*ip, CLICKHOUSE_TCP_PORT, 0, 0);
        let oximeter_client =
            OximeterClient::new(address.into(), &clickhouse_cli.log);
        let clickward = Clickward::new();
        let log =
            clickhouse_cli.log.new(slog::o!("component" => "ServerContext"));
        // TODO: Read configuration file to retrieve generation
        // number if it already exists.
        let generation = None;
        Self {
            clickhouse_cli,
            clickward,
            oximeter_client,
            initialization_lock: Arc::new(Mutex::new(())),
            log,
            generation,
        }
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn clickward(&self) -> &Clickward {
        &self.clickward
    }

    pub fn oximeter_client(&self) -> &OximeterClient {
        &self.oximeter_client
    }

    pub fn initialization_lock(&self) -> Arc<Mutex<()>> {
        self.initialization_lock.clone()
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn generation(&self) -> Option<Generation> {
        self.generation
    }

    // TODO: actually make this work
    pub fn update_generation(mut self, new_generation: Generation) {
        self.generation = Some(new_generation)
    }
}
