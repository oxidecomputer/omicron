// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};

use anyhow::{Context, Result, anyhow, bail};
use camino::Utf8PathBuf;
use chrono::Utc;
use clickhouse_admin_types::config::GenerateConfigResult;
use clickhouse_admin_types::keeper::KeeperConfigurableSettings;
use clickhouse_admin_types::retention::{
    DatabaseRetentionPolicy, RetentionPolicyRequest,
};
use clickhouse_admin_types::server::ServerConfigurableSettings;
use clickhouse_admin_types::usage::{
    DatabaseUsage, DatabaseUsageError, DatabaseUsageResult,
};
use clickhouse_admin_types::{
    CLICKHOUSE_KEEPER_CONFIG_DIR, CLICKHOUSE_KEEPER_CONFIG_FILE,
    CLICKHOUSE_SERVER_CONFIG_DIR, CLICKHOUSE_SERVER_CONFIG_FILE,
};
use dropshot::{ClientErrorStatusCode, HttpError};
use flume::{Receiver, Sender, TrySendError};
use illumos_utils::svcadm::Svcadm;
use omicron_common::api::external::Generation;
use oximeter_db::Client as OximeterDbClient;
use oximeter_db::OXIMETER_VERSION;
use slog::{Logger, debug, warn};
use slog::{error, info};
use slog_error_chain::InlineErrorChain;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::MissedTickBehavior;

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
    _replicated: bool,
    generate_config_tx: Sender<GenerateConfigRequest>,
    generation_rx: watch::Receiver<Option<Generation>>,
    db_init_tx: Sender<DbInitRequest>,
    retention_tx: Sender<RetentionRequest>,
    usage_rx: watch::Receiver<DatabaseUsageResult>,
}

impl ServerContext {
    pub fn new_replicated(
        log: &Logger,
        binary_path: Utf8PathBuf,
        listen_address: SocketAddrV6,
    ) -> Result<Self> {
        Self::new(log, binary_path, listen_address, true)
    }

    pub fn new_single_node(
        log: &Logger,
        binary_path: Utf8PathBuf,
        listen_address: SocketAddrV6,
    ) -> Result<Self> {
        Self::new(log, binary_path, listen_address, false)
    }

    fn new(
        log: &Logger,
        binary_path: Utf8PathBuf,
        clickhouse_address: SocketAddrV6,
        replicated: bool,
    ) -> Result<Self> {
        let clickhouse_cli =
            ClickhouseCli::new(binary_path, clickhouse_address, log);
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
        let (retention_tx, retention_rx) = flume::bounded(0);
        tokio::spawn(long_running_retention_task(
            retention_rx,
            clickhouse_address,
            replicated,
            log.new(slog::o!("component" => "retention-task")),
        ));
        let (usage_tx, usage_rx) =
            watch::channel(DatabaseUsageResult::default());
        tokio::spawn(long_running_usage_task(
            usage_tx,
            clickhouse_address,
            log.new(slog::o!("component" => "usage-task")),
        ));

        Ok(Self {
            clickhouse_cli,
            clickward,
            clickhouse_address,
            log,
            _replicated: replicated,
            generate_config_tx,
            generation_rx,
            db_init_tx,
            retention_tx,
            usage_rx,
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

    /// Update the retention policy of the oximeter database tables.
    pub async fn set_retention_policy(
        &self,
        policy: RetentionPolicyRequest,
    ) -> Result<(), HttpError> {
        let (tx, rx) = oneshot::channel();
        self.retention_tx
            .try_send(RetentionRequest::Set { policy, tx })
            .map_err(|_| {
                HttpError::for_unavail(
                    None,
                    "Retention policy request channel is full".to_string(),
                )
            })?;
        rx.await
            .map_err(|_| {
                HttpError::for_internal_error(
                    "Retention policy reply channel closed".to_string(),
                )
            })
            .flatten()
    }

    /// Request the retention policy from the database.
    pub async fn retention_policy(
        &self,
    ) -> Result<DatabaseRetentionPolicy, HttpError> {
        let (tx, rx) = oneshot::channel();
        self.retention_tx.try_send(RetentionRequest::Get { tx }).map_err(
            |_| {
                HttpError::for_unavail(
                    None,
                    "Retention policy request channel is full".to_string(),
                )
            },
        )?;
        rx.await
            .map_err(|_| {
                HttpError::for_internal_error(
                    "Retention policy reply channel closed".to_string(),
                )
            })
            .flatten()
    }

    /// Return the usage of the oximeter database.
    pub fn database_usage(&self) -> DatabaseUsageResult {
        self.usage_rx.borrow().clone()
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
    let client = OximeterDbClient::new(clickhouse_address.into(), &log);

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

#[derive(Debug)]
enum RetentionRequest {
    Set {
        policy: RetentionPolicyRequest,
        tx: oneshot::Sender<Result<(), HttpError>>,
    },
    Get {
        tx: oneshot::Sender<Result<DatabaseRetentionPolicy, HttpError>>,
    },
}

async fn long_running_retention_task(
    rx: Receiver<RetentionRequest>,
    clickhouse_address: SocketAddrV6,
    replicated: bool,
    log: Logger,
) {
    debug!(log, "task starting, creating Oximeter client");
    let client = OximeterDbClient::new(clickhouse_address.into(), &log);
    debug!(log, "created client, starting recv loop");
    while let Ok(request) = rx.recv_async().await {
        debug!(
            log,
            "received request for retention policy";
            "request" => ?&request,
        );
        match request {
            RetentionRequest::Set { policy, tx } => {
                set_retention_policy(&log, &client, policy, replicated, tx)
                    .await;
            }
            RetentionRequest::Get { tx } => {
                get_retention_policy(&log, &client, tx).await;
            }
        }
    }
    error!(log, "retention policy request channel closed, exiting");
}

async fn set_retention_policy(
    log: &Logger,
    client: &OximeterDbClient,
    policy: RetentionPolicyRequest,
    replicated: bool,
    tx: oneshot::Sender<Result<(), HttpError>>,
) {
    debug!(log, "setting database retention policy"; "policy" => ?&policy);
    let result = match client.set_retention_policy(policy, replicated).await {
        Ok(_) => {
            debug!(log, "set retention policy");
            Ok(())
        }
        Err(e) => {
            let new_err = InlineErrorChain::new(&e);
            error!(
                log,
                "failed to set retention policy";
                "error" => &new_err,
            );
            let http_err = match &e {
                oximeter_db::Error::DatabaseUnavailable(_)
                | oximeter_db::Error::Connection(_)
                | oximeter_db::Error::DatabaseVersionMismatch { .. } => {
                    HttpError::for_unavail(None, new_err.to_string())
                }
                _ => HttpError::for_internal_error(new_err.to_string()),
            };
            Err(http_err)
        }
    };
    if tx.send(result).is_ok() {
        debug!(log, "sent retention policy on reply channel");
    } else {
        error!(log, "failed to send retention policy on reply channel");
    }
}

async fn get_retention_policy(
    log: &Logger,
    client: &OximeterDbClient,
    tx: oneshot::Sender<Result<DatabaseRetentionPolicy, HttpError>>,
) {
    debug!(log, "fetching retention policy from database");
    let result = match client.retention_policy().await {
        Ok(Some(policy)) => {
            debug!(log, "fetched retention policy"; "policy" => ?&policy);
            Ok(policy)
        }
        Ok(None) => {
            warn!(
                log,
                "database is not yet populated, retention policy \
                is unavailable"
            );
            Err(HttpError::for_unavail(
                None,
                "Database is not yet populated".to_string(),
            ))
        }
        Err(e) => {
            let new_err = InlineErrorChain::new(&e);
            error!(
                log,
                "failed to set retention policy";
                "error" => &new_err,
            );
            let http_err = match &e {
                oximeter_db::Error::DatabaseUnavailable(_)
                | oximeter_db::Error::Connection(_)
                | oximeter_db::Error::DatabaseVersionMismatch { .. } => {
                    HttpError::for_unavail(None, new_err.to_string())
                }
                _ => HttpError::for_internal_error(new_err.to_string()),
            };
            Err(http_err)
        }
    };
    if tx.send(result).is_ok() {
        debug!(log, "sent retention policy on reply channel");
    } else {
        error!(log, "failed to send retention policy on reply channel");
    }
}

// Interval on which we attempt to update the database usage.
//
// This is a WAG, but the computations we do today to report table usage are
// pretty inexpensive.
//
// NOTE: We explicitly use a smaller value during testing. This is to avoid
// manipulating time with Tokio's test utils. That cannot work, because we run
// every query to ClickHouse with a timeout, and that causes the tokio timers to
// "auto-advance" to the end of that timeout when we pause in a test.
#[cfg(not(test))]
const USAGE_UPDATE_INTERVAL: Duration = Duration::from_mins(2);
#[cfg(test)]
const USAGE_UPDATE_INTERVAL: Duration = Duration::from_millis(250);

async fn long_running_usage_task(
    tx: watch::Sender<DatabaseUsageResult>,
    clickhouse_address: SocketAddrV6,
    log: Logger,
) {
    let mut timer = tokio::time::interval(USAGE_UPDATE_INTERVAL);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let client = OximeterDbClient::new(clickhouse_address.into(), &log);
    loop {
        timer.tick().await;
        let database_result = compute_database_table_usage(&client, &log).await;
        tx.send_modify(|current| match database_result {
            Ok(usage) => current.last_success = Some(usage),
            Err(error) => {
                current.last_error =
                    Some(DatabaseUsageError { timestamp: Utc::now(), error });
            }
        })
    }
}

async fn compute_database_table_usage(
    client: &OximeterDbClient,
    log: &Logger,
) -> Result<DatabaseUsage, String> {
    debug!(log, "starting database table usage computation");
    match client.database_table_usage().await {
        Ok(usage) => {
            debug!(log, "finished database table usage computation");
            Ok(usage)
        }
        Err(e) => {
            let e = InlineErrorChain::new(&e);
            error!(
                log,
                "failed to compute database table usage";
                "error" => &e,
            );
            Err(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::read_generation_from_file;
    use crate::context::ServerContext;
    use crate::context::USAGE_UPDATE_INTERVAL;
    use camino::Utf8PathBuf;
    use chrono::NaiveDate;
    use chrono::TimeZone;
    use chrono_tz::Tz;
    use clickhouse_admin_types::CLICKHOUSE_SERVER_CONFIG_FILE;
    use clickhouse_admin_types::retention::Days;
    use clickhouse_admin_types::retention::RetentionPolicyRequest;
    use dropshot::ErrorStatusCode;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use oximeter_db::native::block::Block;
    use oximeter_db::native::block::Column;
    use oximeter_db::native::block::Precision;
    use oximeter_db::native::block::ValueArray;
    use slog::info;
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

    #[tokio::test]
    async fn test_retention_policy() {
        let logctx = dev::test_setup_log("test_retention_policy");
        let log = &logctx.log;
        let mut clickhouse =
            dev::clickhouse::ClickHouseDeployment::new_single_node(&logctx)
                .await
                .expect("failed to start ClickHouse");
        info!(log, "started clickhouse"; "address" => %clickhouse.native_address());
        let context = ServerContext::new_single_node(
            log,
            "bogus-path".into(),
            clickhouse.native_address(),
        )
        .expect("failed to create server context");

        // We need to wait until there's a receiver on the other end of the
        // retention request queue. So attempt this until we _don't_ get the
        // "request queue is full" message.
        dev::poll::wait_for_condition(
            || async {
                match context.retention_policy().await {
                    Err(e) if e.internal_message.contains("is full") => {
                        Err(dev::poll::CondCheckError::<()>::NotYet {
                            status: None,
                        })
                    }
                    Err(_) | Ok(_) => Ok(()),
                }
            },
            &std::time::Duration::from_millis(10),
            &std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

        let err = context.retention_policy().await.expect_err(
            "retention policy should fail before the DB is initialized",
        );
        assert!(matches!(
            err.status_code,
            ErrorStatusCode::SERVICE_UNAVAILABLE
        ));

        context.init_db(false).await.expect("failed to initialize database");

        let policy = context
            .retention_policy()
            .await
            .expect("failed to get retention policy");
        assert!(policy.tables.iter().all(|pol| { u8::from(pol.days) == 30 }));

        // Set everything to 3, and ensure we can read it back.
        let days = Days::new(3).unwrap();
        let new = RetentionPolicyRequest { days };
        context
            .set_retention_policy(new)
            .await
            .expect("failed to set new retention policy");
        let policy = context
            .retention_policy()
            .await
            .expect("failed to get retention policy");
        assert!(!policy.tables.is_empty());
        assert!(policy.tables.iter().all(|pol| pol.days == days));

        clickhouse.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_usage() {
        let logctx = dev::test_setup_log("test_database_usage");
        let log = &logctx.log;
        let mut clickhouse =
            dev::clickhouse::ClickHouseDeployment::new_single_node(&logctx)
                .await
                .expect("failed to start ClickHouse");
        info!(log, "started clickhouse"; "address" => %clickhouse.native_address());
        let context = ServerContext::new_single_node(
            log,
            "bogus-path".into(),
            clickhouse.native_address(),
        )
        .expect("failed to create server context");

        let usage = context.database_usage();
        assert!(usage.last_success.is_none());
        assert!(usage.last_error.is_none());

        // We need to wait until there's a receiver on the other end of the
        // database populator queue.
        dev::poll::wait_for_condition(
            || async {
                match context.init_db(false).await {
                    Err(e) if e.internal_message.contains("channel full") => {
                        Err(dev::poll::CondCheckError::<()>::NotYet {
                            status: None,
                        })
                    }
                    Err(_) | Ok(_) => Ok(()),
                }
            },
            &std::time::Duration::from_millis(10),
            &std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();

        let usage = context.database_usage();
        println!("{usage:#?}");
        assert!(usage.last_success.is_some());

        // Wait until we actually do compute the usage again.
        //
        // From `grep -c "CREATE TABLE" oximeter/db/schema/single-node/db-init.sql`.
        const N_EXPECTED_TABLES: usize = 39;
        let usage = dev::poll::wait_for_condition(
            || async {
                let usage = context.database_usage();
                match &usage.last_success {
                    Some(success) => {
                        if success.tables.len() < N_EXPECTED_TABLES {
                            Err(dev::poll::CondCheckError::<()>::NotYet {
                                status: None,
                            })
                        } else {
                            Ok(usage)
                        }
                    }
                    None => Err(dev::poll::CondCheckError::<()>::NotYet {
                        status: None,
                    }),
                }
            },
            &std::time::Duration::from_millis(50),
            &(2 * USAGE_UPDATE_INTERVAL),
        )
        .await
        .unwrap();
        println!("{usage:#?}");
        let tables = &usage
            .last_success
            .as_ref()
            .expect("Should have computed something")
            .tables;
        assert!(
            tables.contains_key(&String::from("oximeter.measurements_u64"))
        );
        assert!(
            tables.contains_key(&String::from("oximeter.measurements_f64"))
        );

        // Insert some rows in the version table, so we can see it updated.
        let version_table = String::from("oximeter.version");
        let version_usage =
            tables.get(&version_table).expect("Should have this table");
        let naive_dt = NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(12, 12, 12)
            .unwrap();
        let timestamp = Tz::UTC.from_local_datetime(&naive_dt).unwrap();
        oximeter_db::native::Connection::new(
            clickhouse.native_address().into(),
        )
        .await
        .expect("Should be able to make client")
        .insert(
            uuid::Uuid::new_v4(),
            "INSERT INTO oximeter.version FORMAT NATIVE",
            Block {
                name: String::new(),
                info: Default::default(),
                columns: [
                    (
                        "value".to_string(),
                        Column::from(ValueArray::UInt64(vec![1, 2, 3])),
                    ),
                    (
                        "timestamp".to_string(),
                        Column::from(ValueArray::DateTime64 {
                            precision: Precision::new(9).unwrap(),
                            tz: Tz::UTC,
                            values: vec![timestamp; 3],
                        }),
                    ),
                ]
                .into(),
            },
        )
        .await
        .expect("Should be able to insert data");

        // Check again, waiting until we get more than the previous usage.
        let usage = dev::poll::wait_for_condition(
            || async {
                let usage = context.database_usage();
                match &usage.last_success {
                    Some(success) => {
                        let Some(usage) = success.tables.get(&version_table)
                        else {
                            return Err(
                                dev::poll::CondCheckError::<()>::NotYet {
                                    status: None,
                                },
                            );
                        };
                        if usage.n_rows > version_usage.n_rows {
                            Ok(usage.clone())
                        } else {
                            Err(dev::poll::CondCheckError::<()>::NotYet {
                                status: None,
                            })
                        }
                    }
                    None => Err(dev::poll::CondCheckError::<()>::NotYet {
                        status: None,
                    }),
                }
            },
            &std::time::Duration::from_millis(50),
            &(2 * USAGE_UPDATE_INTERVAL),
        )
        .await
        .expect("New rows didn't show up on time");
        assert_eq!(usage.n_rows, 4);

        // Kill the database, and wait for another collection. This one should
        // fail.
        clickhouse.cleanup().await.unwrap();
        let usage = dev::poll::wait_for_condition(
            || async {
                let usage = context.database_usage();
                match &usage.last_error {
                    Some(_) => Ok(usage),
                    None => Err(dev::poll::CondCheckError::<()>::NotYet {
                        status: None,
                    }),
                }
            },
            &std::time::Duration::from_millis(100),
            &(2 * USAGE_UPDATE_INTERVAL),
        )
        .await
        .unwrap();
        println!("{usage:#?}");
        assert!(
            usage.last_success.is_some(),
            "Should still have last successful result"
        );
        let Some(err) = usage.last_error.as_ref() else {
            panic!("expected an error to have occurred, but found None");
        };
        let is_network_err = |msg: &str| -> bool {
            msg.starts_with("Failed to check out")
                || msg.starts_with("Native protocol error")
        };
        assert!(is_network_err(&err.error), "Expected a network error error");

        logctx.cleanup_successful();
    }
}
