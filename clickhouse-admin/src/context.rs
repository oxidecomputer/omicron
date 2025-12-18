// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};

use anyhow::{Context, Result, anyhow, bail};
use camino::Utf8PathBuf;
use clickhouse_admin_types::{
    CLICKHOUSE_KEEPER_CONFIG_DIR, CLICKHOUSE_KEEPER_CONFIG_FILE,
    CLICKHOUSE_SERVER_CONFIG_DIR, CLICKHOUSE_SERVER_CONFIG_FILE,
    GenerateConfigResult, KeeperConfigurableSettings,
    ServerConfigurableSettings,
};
use dropshot::{ClientErrorStatusCode, HttpError};
use flume::{Receiver, Sender, TrySendError};
use illumos_utils::svcadm::Svcadm;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_common::api::external::Generation;
use oximeter_db::Client as OximeterClient;
use oximeter_db::OXIMETER_VERSION;
use slog::Logger;
use slog::{error, info};
use slog_error_chain::InlineErrorChain;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddrV6;
use tokio::sync::{oneshot, watch};

pub struct KeeperServerContext {
    clickward: Clickward,
    clickhouse_cli: ClickhouseCli,
    log: Logger,
    generate_config_tx: Sender<GenerateConfigRequest>,
    generation_rx: watch::Receiver<Option<Generation>>,
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
        let config_path = Utf8PathBuf::from(CLICKHOUSE_KEEPER_CONFIG_DIR)
            .join(CLICKHOUSE_KEEPER_CONFIG_FILE);

        // If there is already a configuration file with a generation number we'll
        // use that. Otherwise, we set the generation number to None.
        let generation = read_generation_from_file(config_path)?;
        let (generation_tx, generation_rx) = watch::channel(generation);

        // We only want to handle one in flight request at a time. Reconfigurator execution will retry
        // again later anyway. We use flume bounded channels with a size of 0 to act as a rendezvous channel.
        let (generate_config_tx, generate_config_rx) = flume::bounded(0);
        tokio::spawn(long_running_generate_config_task(
            generate_config_rx,
            generation_tx,
        ));

        Ok(Self {
            clickward,
            clickhouse_cli,
            log,
            generate_config_tx,
            generation_rx,
        })
    }

    pub async fn generate_config_and_enable_svc(
        &self,
        keeper_settings: KeeperConfigurableSettings,
    ) -> Result<GenerateConfigResult, HttpError> {
        let clickward = self.clickward();
        let log = self.log();
        let node_settings = NodeSettings::Keeper {
            settings: keeper_settings,
            fmri: "svc:/oxide/clickhouse_keeper:default".to_string(),
        };

        generate_config_request(
            &self.generate_config_tx,
            node_settings,
            clickward,
            log,
        )
        .await
    }

    pub fn clickward(&self) -> Clickward {
        self.clickward
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn log(&self) -> Logger {
        self.log.clone()
    }

    pub fn generation(&self) -> Option<Generation> {
        *self.generation_rx.borrow()
    }
}

pub struct ServerContext {
    clickhouse_cli: ClickhouseCli,
    clickward: Clickward,
    clickhouse_address: SocketAddrV6,
    log: Logger,
    generate_config_tx: Sender<GenerateConfigRequest>,
    generation_rx: watch::Receiver<Option<Generation>>,
    db_init_tx: Sender<DbInitRequest>,
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

        let config_path = Utf8PathBuf::from(CLICKHOUSE_SERVER_CONFIG_DIR)
            .join(CLICKHOUSE_SERVER_CONFIG_FILE);

        // If there is already a configuration file with a generation number we'll
        // use that. Otherwise, we set the generation number to None.
        let generation = read_generation_from_file(config_path)?;
        let (generation_tx, generation_rx) = watch::channel(generation);

        // We only want to handle one in flight request at a time. Reconfigurator execution will retry
        // again later anyway. We use flume bounded channels with a size of 0 to act as a rendezvous channel.
        let (generate_config_tx, generate_config_rx) = flume::bounded(0);
        tokio::spawn(long_running_generate_config_task(
            generate_config_rx,
            generation_tx,
        ));

        let (db_init_tx, db_init_rx) = flume::bounded(0);
        tokio::spawn(long_running_db_init_task(db_init_rx));

        Ok(Self {
            clickhouse_cli,
            clickward,
            clickhouse_address,
            log,
            generate_config_tx,
            generation_rx,
            db_init_tx,
        })
    }

    pub async fn generate_config_and_enable_svc(
        &self,
        replica_settings: ServerConfigurableSettings,
    ) -> Result<GenerateConfigResult, HttpError> {
        let clickward = self.clickward();
        let log = self.log();
        let node_settings = NodeSettings::Replica {
            settings: replica_settings,
            fmri: "svc:/oxide/clickhouse_server:default".to_string(),
        };
        generate_config_request(
            &self.generate_config_tx,
            node_settings,
            clickward,
            log,
        )
        .await
    }

    pub async fn init_db(&self, replicated: bool) -> Result<(), HttpError> {
        let log = self.log();
        let clickhouse_address = self.clickhouse_address();

        let (response_tx, response_rx) = oneshot::channel();
        self.db_init_tx
            .try_send(DbInitRequest::DbInit {
                clickhouse_address,
                log,
                replicated,
                response: response_tx,
            })
            .map_err(|e| match e {
                TrySendError::Full(_) => HttpError::for_unavail(
                    None,
                    "channel full: another config request is still running"
                        .to_string(),
                ),
                TrySendError::Disconnected(_) => HttpError::for_internal_error(
                    "long-running generate-config task died".to_string(),
                ),
            })?;
        response_rx.await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "failure to receive response; long-running task died before it could respond: {e}"
            ))
        })?
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

    pub fn generation(&self) -> Option<Generation> {
        *self.generation_rx.borrow()
    }
}

pub enum DbInitRequest {
    /// Initiliases the oximeter database on either a single node
    /// ClickHouse installation or a replicated cluster.
    DbInit {
        clickhouse_address: SocketAddrV6,
        log: Logger,
        replicated: bool,
        response: oneshot::Sender<Result<(), HttpError>>,
    },
}

async fn long_running_db_init_task(incoming: Receiver<DbInitRequest>) {
    while let Ok(request) = incoming.recv_async().await {
        match request {
            DbInitRequest::DbInit {
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

pub enum GenerateConfigRequest {
    /// Generates a configuration file for a server or keeper node
    GenerateConfig {
        clickward: Clickward,
        log: Logger,
        node_settings: NodeSettings,
        response: oneshot::Sender<Result<GenerateConfigResult, HttpError>>,
    },
}

async fn long_running_generate_config_task(
    incoming: Receiver<GenerateConfigRequest>,
    generation_tx: watch::Sender<Option<Generation>>,
) {
    while let Ok(request) = incoming.recv_async().await {
        match request {
            GenerateConfigRequest::GenerateConfig {
                clickward,
                log,
                node_settings,
                response,
            } => {
                let result = generate_config_and_enable_svc(
                    generation_tx.clone(),
                    clickward,
                    node_settings,
                )
                .await;
                if let Err(e) = response.send(result) {
                    error!(
                        &log,
                        "failed to send value from configuration generation to channel: {e:?}"
                    );
                };
            }
        }
    }
}

#[derive(Debug)]
pub enum NodeSettings {
    Replica { settings: ServerConfigurableSettings, fmri: String },
    Keeper { settings: KeeperConfigurableSettings, fmri: String },
}

impl NodeSettings {
    fn fmri(self) -> String {
        match self {
            NodeSettings::Replica { fmri, .. } => fmri,
            NodeSettings::Keeper { fmri, .. } => fmri,
        }
    }

    fn generation(&self) -> Generation {
        match self {
            NodeSettings::Replica { settings, .. } => settings.generation(),
            NodeSettings::Keeper { settings, .. } => settings.generation(),
        }
    }
}

async fn generate_config_and_enable_svc(
    generation_tx: watch::Sender<Option<Generation>>,
    clickward: Clickward,
    node_settings: NodeSettings,
) -> Result<GenerateConfigResult, HttpError> {
    let incoming_generation = node_settings.generation();
    let current_generation = *generation_tx.borrow();

    // If the incoming generation number is lower, then we have a problem.
    // We should return an error instead of silently skipping the configuration
    // file generation.
    if let Some(current) = current_generation {
        if current > incoming_generation {
            return Err(HttpError::for_client_error(
                Some(String::from("Conflict")),
                ClientErrorStatusCode::CONFLICT,
                format!(
                    "current generation '{}' is greater than incoming generation '{}'",
                    current, incoming_generation,
                ),
            ));
        }
    };

    let output = match &node_settings {
        NodeSettings::Replica { settings, .. } => {
            GenerateConfigResult::Replica(
                clickward.generate_server_config(settings)?,
            )
        }
        NodeSettings::Keeper { settings, .. } => GenerateConfigResult::Keeper(
            clickward.generate_keeper_config(settings)?,
        ),
    };

    // We want to update the generation number only if the config file has been
    // generated successfully.
    generation_tx.send_replace(Some(incoming_generation));

    // Once we have generated the client we can safely enable the clickhouse_server service
    Svcadm::enable_service(node_settings.fmri()).await?;

    Ok(output)
}

async fn generate_config_request(
    generate_config_tx: &Sender<GenerateConfigRequest>,
    node_settings: NodeSettings,
    clickward: Clickward,
    log: Logger,
) -> Result<GenerateConfigResult, HttpError> {
    let (response_tx, response_rx) = oneshot::channel();
    generate_config_tx
        .try_send(GenerateConfigRequest::GenerateConfig {
            clickward,
            log,
            node_settings,
            response: response_tx,
        })
        .map_err(|e| match e {
            TrySendError::Full(_) => HttpError::for_unavail(
                None,
                "channel full: another config request is still running"
                    .to_string(),
            ),
            TrySendError::Disconnected(_) => HttpError::for_internal_error(
                "long-running generate-config task died".to_string(),
            ),
        })?;
    response_rx.await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "failure to receive response; long-running task died before it could respond: {e}"
            ))
        })?
}

async fn init_db(
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
            "can't read ClickHouse version: {}",
            InlineErrorChain::new(&e),
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
                     to version {OXIMETER_VERSION}: {}",
                InlineErrorChain::new(&e),
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

    let file =
        File::open(&path).with_context(|| format!("failed to open {path}"))?;
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
        bail!(
            "first line of configuration file '{}' is malformed: {}",
            path,
            first_line
        );
    }

    // It's safe to unwrap since we already know `line_parts` contains two items.
    let line_end_part: Vec<&str> =
        line_parts.first().unwrap().split_terminator(" -->").collect();
    if line_end_part.len() != 1 {
        bail!(
            "first line of configuration file '{}' is malformed: {}",
            path,
            first_line
        );
    }

    // It's safe to unwrap since we already know `line_end_part` contains an item.
    let gen_u64: u64 = line_end_part.first().unwrap().parse().map_err(|e| {
        anyhow!(
            concat!(
                "first line of configuration file '{}' is malformed: {}; ",
                "error = {}",
            ),
            path,
            first_line,
            e
        )
    })?;

    let generation = Generation::try_from(gen_u64)?;

    Ok(Some(generation))
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
            "first line of configuration file 'types/testutils/malformed_1.xml' is malformed: <clickhouse>"
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
            "first line of configuration file 'types/testutils/malformed_2.xml' is malformed: <!-- generation:bob -->; error = invalid digit found in string"
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
            "first line of configuration file 'types/testutils/malformed_3.xml' is malformed: <!-- generation:2 --> -->"
        );
    }
}
