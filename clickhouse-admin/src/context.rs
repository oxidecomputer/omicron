// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};

use anyhow::{anyhow, bail, Result};
use camino::Utf8PathBuf;
use clickhouse_admin_types::{
    CLICKHOUSE_KEEPER_CONFIG_DIR, CLICKHOUSE_KEEPER_CONFIG_FILE,
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
    pub fn new(
        log: &Logger,
        binary_path: Utf8PathBuf,
        listen_address: SocketAddrV6,
    ) -> Result<Self> {
        let clickhouse_cli =
            ClickhouseCli::new(binary_path, listen_address, log);
        let log = log.new(slog::o!("component" => "KeeperServerContext"));
        let clickward = Clickward::new();
        let config_path = Utf8PathBuf::from_str(CLICKHOUSE_KEEPER_CONFIG_DIR)
            .unwrap()
            .join(CLICKHOUSE_KEEPER_CONFIG_FILE);
        let generation = read_generation_from_file(config_path)?;
        Ok(Self { clickward, clickhouse_cli, log, generation })
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
    pub fn new(
        log: &Logger,
        binary_path: Utf8PathBuf,
        listen_address: SocketAddrV6,
    ) -> Result<Self> {
        let clickhouse_cli =
            ClickhouseCli::new(binary_path, listen_address, log);

        let ip = listen_address.ip();
        let address = SocketAddrV6::new(*ip, CLICKHOUSE_TCP_PORT, 0, 0);
        let oximeter_client = OximeterClient::new(address.into(), log);
        let clickward = Clickward::new();
        let log = log.new(slog::o!("component" => "ServerContext"));

        let config_path = Utf8PathBuf::from_str(CLICKHOUSE_SERVER_CONFIG_DIR)
            .unwrap()
            .join(CLICKHOUSE_SERVER_CONFIG_FILE);
        let generation = read_generation_from_file(config_path)?;

        Ok(Self {
            clickhouse_cli,
            clickward,
            oximeter_client,
            initialization_lock: Arc::new(Mutex::new(())),
            log,
            generation,
        })
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

pub struct SingleServerContext {
    clickhouse_cli: ClickhouseCli,
    oximeter_client: OximeterClient,
    initialization_lock: Arc<Mutex<()>>,
    log: Logger,
}

impl SingleServerContext {
    pub fn new(
        log: &Logger,
        binary_path: Utf8PathBuf,
        listen_address: SocketAddrV6,
    ) -> Self {
        let clickhouse_cli =
            ClickhouseCli::new(binary_path, listen_address, log);

        let ip = listen_address.ip();
        let address = SocketAddrV6::new(*ip, CLICKHOUSE_TCP_PORT, 0, 0);
        let oximeter_client = OximeterClient::new(address.into(), log);
        let log =
            clickhouse_cli.log.new(slog::o!("component" => "ServerContext"));

        Self {
            clickhouse_cli,
            oximeter_client,
            initialization_lock: Arc::new(Mutex::new(())),
            log,
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

    pub fn log(&self) -> &Logger {
        &self.log
    }
}

fn read_generation_from_file(path: Utf8PathBuf) -> Result<Option<Generation>> {
    // When the configuration file does not exist yet, this means it's a new server.
    // It won't have a running clickhouse server and no generation number yet.
    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    // We know the generation number is on the top of the file so we only
    // need the first line.
    let first_line = match reader.lines().next() {
        Some(g) => g?,
        // When the clickhouse configuration file exists but has no contents,
        // it means something went wrong when creating the file earlier.
        // We should return because something is definitely broken.
        None => bail!(
            "clickhouse configuration file exists at {}, but is empty",
            path
        ),
    };

    let line_parts: Vec<&str> = first_line.rsplit(':').collect();
    if line_parts.len() != 2 {
        bail!("first line of configuration file is malformed: {}", first_line);
    }

    // It's safe to unwrap since we already know `line_parts` contains two items.
    let line_end_part: Vec<&str> =
        line_parts.first().unwrap().split_terminator(" -->").collect();
    if line_end_part.len() != 1 {
        bail!("first line of configuration file is malformed: {}", first_line);
    }

    // It's safe to unwrap since we already know `line_end_part` contains an item.
    let gen_u64: u64 = line_end_part.first().unwrap().parse().map_err(|e| {
        anyhow!(
            concat!(
                "first line of configuration file is malformed: {}; ",
                "error = {}",
            ),
            first_line,
            e
        )
    })?;

    let gen = Generation::try_from(gen_u64)?;

    Ok(Some(gen))
}

#[cfg(test)]
mod tests {
    use super::read_generation_from_file;
    use camino::Utf8PathBuf;
    use clickhouse_admin_types::CLICKHOUSE_SERVER_CONFIG_FILE;
    use omicron_common::api::external::Generation;
    use std::str::FromStr;

    #[test]
    fn test_read_generation_from_file_success() {
        let dir = Utf8PathBuf::from_str("types/testutils")
            .unwrap()
            .join(CLICKHOUSE_SERVER_CONFIG_FILE);
        let generation = read_generation_from_file(dir).unwrap().unwrap();

        assert_eq!(Generation::from(1), generation);
    }

    #[test]
    fn test_read_generation_from_file_none() {
        let dir = Utf8PathBuf::from_str("types/testutils")
            .unwrap()
            .join("i-dont-exist.xml");
        let generation = read_generation_from_file(dir).unwrap();

        assert_eq!(None, generation);
    }

    #[test]
    fn test_read_generation_from_file_malformed_1() {
        let dir = Utf8PathBuf::from_str("types/testutils")
            .unwrap()
            .join("malformed_1.xml");
        let result = read_generation_from_file(dir);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "first line of configuration file is malformed: <clickhouse>"
        );
    }

    #[test]
    fn test_read_generation_from_file_malformed_2() {
        let dir = Utf8PathBuf::from_str("types/testutils")
            .unwrap()
            .join("malformed_2.xml");
        let result = read_generation_from_file(dir);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "first line of configuration file is malformed: <!-- generation:bob -->; error = invalid digit found in string"
        );
    }

    #[test]
    fn test_read_generation_from_file_malformed_3() {
        let dir = Utf8PathBuf::from_str("types/testutils")
            .unwrap()
            .join("malformed_3.xml");
        let result = read_generation_from_file(dir);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
           "first line of configuration file is malformed: <!-- generation:2 --> -->"
        );
    }
}
