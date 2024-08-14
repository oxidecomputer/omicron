// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_api::ClickhouseAddress;
use dropshot::HttpError;
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use std::io;
use std::net::SocketAddrV6;

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
    clickhouse_address: SocketAddrV6,
}

impl Clickward {
    pub fn new(clickhouse_address: SocketAddrV6) -> Self {
        Self { clickhouse_address }
    }

    pub fn clickhouse_address(&self) -> Result<ClickhouseAddress, ClickwardError> {
        Ok(ClickhouseAddress { clickhouse_address: self.clickhouse_address })
    }
}
