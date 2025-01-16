// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};
use camino::Utf8PathBuf;
use clickhouse_admin_types::{
    CLICKHOUSE_SERVER_CONFIG_DIR, CLICKHOUSE_SERVER_CONFIG_FILE,
};
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_common::api::external::Generation;
use oximeter_db::Client as OximeterClient;
use slog::Logger;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddrV6;
use std::str::FromStr;
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
        // TODO: Check that error is file doesn't exist so we can set as None
        let _directory = Utf8PathBuf::from_str(CLICKHOUSE_SERVER_CONFIG_DIR);

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

fn read_generation_from_file(path: Utf8PathBuf) -> Option<Generation> {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    // We know the generation number is on the top of the file so we only
    // need the first line.
    let first_line = reader.lines().next().unwrap().unwrap();
    let gen = first_line
        .rsplit(':')
        .next()
        .unwrap()
        .split_terminator(" -->")
        .next()
        .unwrap();
    let gen_int: u64 = gen.parse().unwrap();

    Some(Generation::try_from(gen_int).unwrap())
}

#[cfg(test)]
mod tests {
    use super::read_generation_from_file;
    use camino::Utf8PathBuf;
    use clickhouse_admin_types::CLICKHOUSE_SERVER_CONFIG_FILE;
    use omicron_common::api::external::Generation;
    use std::str::FromStr;

    #[test]
    fn test_read_generation_from_file() {
        let dir = Utf8PathBuf::from_str("types/testutils")
            .unwrap()
            .join(CLICKHOUSE_SERVER_CONFIG_FILE);
        let generation = read_generation_from_file(dir).unwrap();

        assert_eq!(Generation::from(1), generation);
    }
}
