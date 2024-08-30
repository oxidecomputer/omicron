// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clickhouse_admin_api::{
    ClickhouseAddress, ServerConfigGenerateResponse, ServerSettings,
};
use clickhouse_admin_types::config::{KeeperNodeConfig, ServerNodeConfig};
use clickhouse_admin_types::{ClickhouseServerConfig, ServerId};
use dropshot::HttpError;
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use std::io;
use std::net::SocketAddrV6;
use std::str::FromStr;

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum ClickwardError {
    #[error("clickward failure")]
    Failure {
        #[source]
        err: io::Error,
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
pub struct Clickward {
    // TODO: Remove address?
    clickhouse_address: SocketAddrV6,
}

impl Clickward {
    pub fn new(clickhouse_address: SocketAddrV6) -> Self {
        Self { clickhouse_address }
    }

    // TODO: Remove this endpoint?
    pub fn clickhouse_address(
        &self,
    ) -> Result<ClickhouseAddress, ClickwardError> {
        Ok(ClickhouseAddress { clickhouse_address: self.clickhouse_address })
    }

    pub fn generate_server_config(
        &self,
        settings: ServerSettings,
    ) -> Result<ServerConfigGenerateResponse, ClickwardError> {
        // TODO: This should be part of the request body
        //   let keepers = vec![
        //       KeeperNodeConfig::new("ff::01".to_string()),
        //       KeeperNodeConfig::new("127.0.0.1".to_string()),
        //       KeeperNodeConfig::new("we.dont.want.brackets.com".to_string()),
        //   ];

        // let servers = vec![
        //     ServerNodeConfig::new("ff::08".to_string()),
        //     ServerNodeConfig::new("ff::09".to_string()),
        // ];

        let keepers = settings
            .keepers
            .iter()
            .map(|host| KeeperNodeConfig::new(host.clone()))
            .collect();

        let remote_servers = settings
            .remote_servers
            .iter()
            .map(|host| ServerNodeConfig::new(format!("{host:?}")))
            .collect();

        let config = ClickhouseServerConfig::new(
            Utf8PathBuf::from_str("./").unwrap(),
            ServerId(settings.node_id),
            Utf8PathBuf::from_str("./").unwrap(),
            settings.listen_addr,
            keepers,
            remote_servers,
        );

        config.generate_xml_file().unwrap();

        Ok(ServerConfigGenerateResponse::success())
    }
}
