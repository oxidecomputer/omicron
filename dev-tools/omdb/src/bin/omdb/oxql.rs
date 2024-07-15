// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb OxQL shell for interactive queries on metrics/timeseries.

// Copyright 2024 Oxide Computer

use crate::helpers::CONNECTION_OPTIONS_HEADING;
use crate::Omdb;
use anyhow::Context;
use clap::Args;
use oximeter_db::{
    self,
    shells::oxql::{self, ShellOptions},
};
use slog::Logger;
use std::net::SocketAddr;
use url::Url;

/// Command-line arguments for the OxQL shell.
#[derive(Debug, Args)]
pub struct OxqlArgs {
    /// URL of the ClickHouse server to connect to.
    #[arg(
        long,
        env = "OMDB_CLICKHOUSE_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    clickhouse_url: Option<String>,

    /// Print summaries of each SQL query run against the database.
    #[clap(long = "summaries")]
    print_summaries: bool,

    /// Print the total elapsed query duration.
    #[clap(long = "elapsed")]
    print_elapsed: bool,
}

impl OxqlArgs {
    /// Run the OxQL shell via the `omdb oxql` subcommand.
    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        let addr = self.addr(omdb, log).await?;

        let opts = ShellOptions {
            print_summaries: self.print_summaries,
            print_elapsed: self.print_elapsed,
        };

        oxql::shell(
            addr.ip(),
            addr.port(),
            log.new(slog::o!("component" => "clickhouse-client")),
            opts,
        )
        .await
    }

    /// Resolve the ClickHouse URL to a socket address.
    async fn addr(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<SocketAddr> {
        match &self.clickhouse_url {
            Some(cli_or_env_url) => Url::parse(&cli_or_env_url)
                .context(
                    "failed parsing URL from command-line or environment variable",
                )?
                .socket_addrs(|| None)
                .context("failed resolving socket addresses")?
                .into_iter()
                .next()
                .context("failed resolving socket addresses"),
            None => {
                eprintln!(
                    "note: ClickHouse URL not specified. Will pick one from DNS."
                );

                Ok(SocketAddr::V6(
                    omdb.dns_lookup_one(
                        log.clone(),
                        internal_dns::ServiceName::Clickhouse,
                    )
                    .await
                    .context("failed looking up ClickHouse internal DNS entry")?,
                ))
            }
        }
    }
}
