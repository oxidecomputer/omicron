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
use http::StatusCode;
use illumos_utils::svcadm::Svcadm;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_common::api::external::Generation;
use oximeter_db::Client as OximeterClient;
use oximeter_db::OXIMETER_VERSION;
use slog::info;
use slog::Logger;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddrV6;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, oneshot};
use tokio::task::JoinHandle;

pub struct KeeperServerContext {
    clickward: Clickward,
    clickhouse_cli: ClickhouseCli,
    log: Logger,
    pub generation: std::sync::Mutex<Option<Generation>>,
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
        let generation = std::sync::Mutex::new(gen);
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
        *self.generation.lock().unwrap()
    }
}

pub struct ServerContext {
    clickhouse_cli: ClickhouseCli,
    clickward: Clickward,
    oximeter_client: OximeterClient,
    log: Logger,
    pub generation: std::sync::Mutex<Option<Generation>>,
    pub task_handle: JoinHandle<()>,
    pub tx: mpsc::Sender<ClickhouseAdminServerRequest>,
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

        // If there is already a configuration file with a generation number we'll
        // use that. Otherwise, we set the generation number to None.
        let gen = read_generation_from_file(config_path)?;
        let generation = std::sync::Mutex::new(gen);

        // TODO: change the buffer size. Use that flume bounded channel thing?
        let (inner_tx, inner_rx) = mpsc::channel(10);
        let task_handle = tokio::spawn(long_running_ch_server_task(inner_rx));

        Ok(Self {
            clickhouse_cli,
            clickward,
            oximeter_client,
            log,
            generation,
            task_handle,
            tx: inner_tx,
        })
    }

    pub fn generate_config_and_enable_svc(
        &self,
        replica_settings: ServerConfigurableSettings,
    ) -> Result<ReplicaConfig, HttpError> {
        let mut current_generation = self.generation.lock().unwrap();
        let incoming_generation = replica_settings.generation();

        // If the incoming generation number is lower, then we have a problem.
        // We should return an error instead of silently skipping the configuration
        // file generation.
        if let Some(current) = *current_generation {
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

        let output =
            self.clickward().generate_server_config(replica_settings)?;

        // We want to update the generation number only if the config file has been
        // generated successfully.
        *current_generation = Some(incoming_generation);

        // Once we have generated the client we can safely enable the clickhouse_server service
        let fmri = "svc:/oxide/clickhouse_server:default".to_string();
        Svcadm::enable_service(fmri)?;

        Ok(output)
    }

    pub async fn init_db(&self) -> Result<(), HttpError> {
        let log = self.log();
        // Initialize the database only if it was not previously initialized.
        // TODO: Migrate schema to newer version without wiping data.
        let client = self.oximeter_client();
        let version = client.read_latest_version().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "can't read ClickHouse version: {e}",
            ))
        })?;
        if version == 0 {
            info!(
                log,
                "initializing replicated ClickHouse cluster to version {OXIMETER_VERSION}"
            );
            let replicated = true;
            self.oximeter_client()
                .initialize_db_with_version(replicated, OXIMETER_VERSION)
                .await
                .map_err(|e| {
                    HttpError::for_internal_error(format!(
                        "can't initialize replicated ClickHouse cluster \
                         to version {OXIMETER_VERSION}: {e}",
                    ))
                })?;
        } else {
            info!(
                log,
                "skipping initialization of replicated ClickHouse cluster at version {version}"
            );
        }

        Ok(())
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

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn generation(&self) -> Option<Generation> {
        *self.generation.lock().unwrap()
    }
}

pub enum ClickhouseAdminServerRequest {
    GenerateConfig {
        ctx: Arc<ServerContext>,
        replica_settings: ServerConfigurableSettings,
        response: oneshot::Sender<Result<ReplicaConfig, HttpError>>,
    },
    DbInit {
        ctx: Arc<ServerContext>,
        response: oneshot::Sender<Result<(), HttpError>>,
    },
}

async fn long_running_ch_server_task(
    mut incoming: Receiver<ClickhouseAdminServerRequest>,
) {
    while let Some(request) = incoming.recv().await {
        match request {
            ClickhouseAdminServerRequest::GenerateConfig {
                ctx,
                replica_settings,
                response,
            } => {
                let result =
                    ctx.generate_config_and_enable_svc(replica_settings);
                response.send(result).expect("failed to send value from configuration generation to channel");
            }
            ClickhouseAdminServerRequest::DbInit { ctx, response } => {
                let result = ctx.init_db().await;
                response.send(result).expect("failed to send value from database initialization to channel");
            }
        }
    }
}

pub struct SingleServerContext {
    clickhouse_cli: ClickhouseCli,
    oximeter_client: OximeterClient,
    log: Logger,
    pub task_handle: JoinHandle<()>,
    pub tx: mpsc::Sender<ClickhouseAdminSingleServerRequest>,
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

        // TODO: change the buffer size. Use that flume bounded channel thing?
        let (inner_tx, inner_rx) = mpsc::channel(10);
        let task_handle =
            tokio::spawn(long_running_ch_single_server_task(inner_rx));

        Self { clickhouse_cli, oximeter_client, log, task_handle, tx: inner_tx }
    }

    pub async fn init_db(&self) -> Result<(), HttpError> {
        let log = self.log();

        // Initialize the database only if it was not previously initialized.
        // TODO: Migrate schema to newer version without wiping data.
        let client = self.oximeter_client();
        let version = client.read_latest_version().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "can't read ClickHouse version: {e}",
            ))
        })?;
        if version == 0 {
            info!(
                log,
                "initializing single-node ClickHouse to version {OXIMETER_VERSION}"
            );
            let replicated = false;
            self.oximeter_client()
                .initialize_db_with_version(replicated, OXIMETER_VERSION)
                .await
                .map_err(|e| {
                    HttpError::for_internal_error(format!(
                        "can't initialize single-node ClickHouse \
                         to version {OXIMETER_VERSION}: {e}",
                    ))
                })?;
        } else {
            info!(
                log,
                "skipping initialization of single-node ClickHouse at version {version}"
            );
        }

        Ok(())
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn oximeter_client(&self) -> &OximeterClient {
        &self.oximeter_client
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }
}

pub enum ClickhouseAdminSingleServerRequest {
    DbInit {
        ctx: Arc<SingleServerContext>,
        response: oneshot::Sender<Result<(), HttpError>>,
    },
}

async fn long_running_ch_single_server_task(
    mut incoming: Receiver<ClickhouseAdminSingleServerRequest>,
) {
    while let Some(request) = incoming.recv().await {
        match request {
            ClickhouseAdminSingleServerRequest::DbInit { ctx, response } => {
                let result = ctx.init_db().await;
                response.send(result).expect("failed to send value from database initialization to channel");
            }
        }
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
