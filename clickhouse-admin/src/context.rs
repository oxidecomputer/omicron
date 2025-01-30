// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};

use anyhow::{anyhow, bail, Result};
use camino::Utf8PathBuf;
use clickhouse_admin_types::{
    ReplicaConfig, ServerConfigurableSettings, CLICKHOUSE_KEEPER_CONFIG_DIR,
    CLICKHOUSE_KEEPER_CONFIG_FILE, CLICKHOUSE_SERVER_CONFIG_DIR,
    CLICKHOUSE_SERVER_CONFIG_FILE,
};
use dropshot::HttpError;
use flume::{Receiver, Sender};
use http::StatusCode;
use illumos_utils::svcadm::Svcadm;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_common::api::external::Generation;
use oximeter_db::Client as OximeterClient;
use oximeter_db::OXIMETER_VERSION;
use slog::Logger;
use slog::{error, info};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddrV6;
use std::str::FromStr;
use tokio::sync::{oneshot, watch};

pub struct KeeperServerContext {
    clickward: Clickward,
    clickhouse_cli: ClickhouseCli,
    log: Logger,
    pub generation_tx: watch::Sender<Option<Generation>>,
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

        // If there is already a configuration file with a generation number we'll
        // use that. Otherwise, we set the generation number to None.
        let gen = read_generation_from_file(config_path)?;
        let (generation_tx, _rx) = watch::channel(gen);

        Ok(Self { clickward, clickhouse_cli, log, generation_tx })
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

    pub fn generation_tx(&self) -> watch::Sender<Option<Generation>> {
        self.generation_tx.clone()
    }
}

pub struct ServerContext {
    clickhouse_cli: ClickhouseCli,
    clickward: Clickward,
    clickhouse_address: SocketAddrV6,
    log: Logger,
    pub tx: Sender<ClickhouseAdminServerRequest>,
    pub generation_tx: watch::Sender<Option<Generation>>,
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
        let clickhouse_address =
            SocketAddrV6::new(*ip, CLICKHOUSE_TCP_PORT, 0, 0);
        let clickward = Clickward::new();
        let log = log.new(slog::o!("component" => "ServerContext"));

        let config_path = Utf8PathBuf::from_str(CLICKHOUSE_SERVER_CONFIG_DIR)
            .unwrap()
            .join(CLICKHOUSE_SERVER_CONFIG_FILE);

        // If there is already a configuration file with a generation number we'll
        // use that. Otherwise, we set the generation number to None.
        let gen = read_generation_from_file(config_path)?;
        let (generation_tx, _rx) = watch::channel(gen);

        // We only want to handle one in flight request at a time. Reconfigurator execution will retry
        // again later anyway. We use a flume bounded channel with a size of 0 to act as a rendezvous channel.
        let (inner_tx, inner_rx) = flume::bounded(0);
        tokio::spawn(long_running_ch_admin_server_task(inner_rx));

        Ok(Self {
            clickhouse_cli,
            clickward,
            clickhouse_address,
            log,
            tx: inner_tx,
            generation_tx,
        })
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn clickward(&self) -> Clickward {
        self.clickward
    }

    pub fn clickhouse_address(&self) -> SocketAddrV6 {
        self.clickhouse_address
    }

    pub fn log(&self) -> Logger {
        self.log.clone()
    }

    pub fn generation_tx(&self) -> watch::Sender<Option<Generation>> {
        self.generation_tx.clone()
    }
}

pub enum ClickhouseAdminServerRequest {
    /// Generates configuration files for a replicated cluster
    GenerateConfig {
        generation_tx: watch::Sender<Option<Generation>>,
        clickward: Clickward,
        log: Logger,
        replica_settings: ServerConfigurableSettings,
        response: oneshot::Sender<Result<ReplicaConfig, HttpError>>,
    },
    /// Initiliases the oximeter database on either a single node
    /// ClickHouse installation or a replicated cluster.
    DbInit {
        clickhouse_address: SocketAddrV6,
        log: Logger,
        replicated: bool,
        response: oneshot::Sender<Result<(), HttpError>>,
    },
}

async fn long_running_ch_admin_server_task(
    incoming: Receiver<ClickhouseAdminServerRequest>,
) {
    while let Ok(request) = incoming.recv_async().await {
        match request {
            ClickhouseAdminServerRequest::GenerateConfig {
                generation_tx,
                clickward,
                log,
                replica_settings,
                response,
            } => {
                let result = generate_config_and_enable_svc(
                    generation_tx,
                    clickward,
                    replica_settings,
                );
                if let Err(e) = response.send(result) {
                    error!(
                        &log,
                        "failed to send value from configuration generation to channel: {e:?}"
                    );
                };
            }
            ClickhouseAdminServerRequest::DbInit {
                clickhouse_address,
                log,
                replicated,
                response,
            } => {
                let result =
                    init_db(clickhouse_address, log.clone(), replicated).await;
                if let Err(e) = response.send(result) {
                    error!(
                        log,
                        "failed to send value from database initialization to channel: {e:?}"
                    );
                };
            }
        }
    }
}

pub fn generate_config_and_enable_svc(
    generation_tx: watch::Sender<Option<Generation>>,
    clickward: Clickward,
    replica_settings: ServerConfigurableSettings,
) -> Result<ReplicaConfig, HttpError> {
    let incoming_generation = replica_settings.generation();
    let generation_rx = generation_tx.subscribe();
    let current_generation = *generation_rx.borrow();

    // If the incoming generation number is lower, then we have a problem.
    // We should return an error instead of silently skipping the configuration
    // file generation.
    if let Some(current) = current_generation {
        if current > incoming_generation {
            return Err(HttpError::for_client_error(
                Some(String::from("Conflict")),
                StatusCode::CONFLICT,
                format!(
                    "current generation '{}' is greater than incoming generation '{}'",
                    current,
                    incoming_generation,
                )
            ));
        }
    };

    let output = clickward.generate_server_config(replica_settings)?;

    // We want to update the generation number only if the config file has been
    // generated successfully.
    generation_tx.send(Some(incoming_generation)).map_err(|e| {
        HttpError::for_internal_error(format!("failure to send request: {e}"))
    })?;

    // Once we have generated the client we can safely enable the clickhouse_server service
    let fmri = "svc:/oxide/clickhouse_server:default".to_string();
    Svcadm::enable_service(fmri)?;

    Ok(output)
}

pub async fn init_db(
    clickhouse_address: SocketAddrV6,
    log: Logger,
    replicated: bool,
) -> Result<(), HttpError> {
    // We initialise the Oximeter client here instead of keeping it as part of the context as
    // this client is not cloneable, copyable by design.
    let client = OximeterClient::new(clickhouse_address.into(), &log);

    // Initialize the database only if it was not previously initialized.
    // TODO: Migrate schema to newer version without wiping data.
    let version = client.read_latest_version().await.map_err(|e| {
        HttpError::for_internal_error(format!(
            "can't read ClickHouse version: {e}",
        ))
    })?;
    if version == 0 {
        info!(
            log,
            "initializing oximeter database to version {OXIMETER_VERSION}"
        );
        client
            .initialize_db_with_version(replicated, OXIMETER_VERSION)
            .await
            .map_err(|e| {
            HttpError::for_internal_error(format!(
                "can't initialize oximeter database \
                     to version {OXIMETER_VERSION}: {e}",
            ))
        })?;
    } else {
        info!(
            log,
            "skipping initialization of oximeter database at version {version}"
        );
    }

    Ok(())
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
