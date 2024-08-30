// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clickhouse_admin_api::{KeeperSettings, ServerSettings};
use clickhouse_admin_types::config::{KeeperConfig, ReplicaConfig};
use clickhouse_admin_types::{
    ClickhouseKeeperConfig, ClickhouseServerConfig, KeeperId, ServerId,
};
use dropshot::HttpError;
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use std::str::FromStr;

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum ClickwardError {
    #[error("clickward failure")]
    Failure {
        #[source]
        err: anyhow::Error,
    },
}

impl From<ClickwardError> for HttpError {
    fn from(err: ClickwardError) -> Self {
        match err {
            ClickwardError::Failure { .. } => {
                let message = InlineErrorChain::new(&err).to_string();
                HttpError {
                    status_code: http::StatusCode::INTERNAL_SERVER_ERROR,
                    error_code: Some(String::from("Internal")),
                    external_message: message.clone(),
                    internal_message: message,
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Clickward {}

impl Clickward {
    pub fn new() -> Self {
        Self {}
    }

    pub fn generate_server_config(
        &self,
        settings: ServerSettings,
    ) -> Result<ReplicaConfig, ClickwardError> {
        let config = ClickhouseServerConfig::new(
            // We can safely call unwrap here as this method is infallible
            Utf8PathBuf::from_str(&settings.config_dir).unwrap(),
            ServerId(settings.node_id),
            Utf8PathBuf::from_str(&settings.datastore_path).unwrap(),
            settings.listen_addr,
            settings.keepers,
            settings.remote_servers,
        );

        let replica_config = config
            .generate_xml_file()
            .map_err(|e| ClickwardError::Failure { err: e })?;

        Ok(replica_config)
    }

    pub fn generate_keeper_config(
        &self,
        settings: KeeperSettings,
    ) -> Result<KeeperConfig, ClickwardError> {
        let config = ClickhouseKeeperConfig::new(
            Utf8PathBuf::from_str(&settings.config_dir).unwrap(),
            KeeperId(settings.node_id),
            settings.keepers,
            Utf8PathBuf::from_str(&settings.datastore_path).unwrap(),
            settings.listen_addr,
        );

        let keeper_config = config
            .generate_xml_file()
            .map_err(|e| ClickwardError::Failure { err: e })?;

        Ok(keeper_config)
    }
}
