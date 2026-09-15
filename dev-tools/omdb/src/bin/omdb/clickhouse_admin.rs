// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that clickhouse admin servers

use crate::Omdb;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use anyhow::Context;
use chrono::SecondsFormat;
use clap::Args;
use clap::Subcommand;
use clickhouse_admin_single_client::Client;
use clickhouse_admin_single_client::types;
use clickhouse_admin_single_client::types::DatabaseUsageResult;
use internal_dns_types::names::ServiceName;
use slog::Logger;
use tabled::Table;
use tabled::Tabled;

/// Arguments for the clickhouse-admin subcommand.
#[derive(Debug, Args)]
pub struct ClickHouseAdminArgs {
    /// URL of the ClickHouse admin server to query
    #[arg(
        long,
        env = "OMDB_CLICKHOUSE_ADMIN_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    clickhouse_admin_url: Option<String>,

    /// Run this against the replicated ClickHouse admin server.
    ///
    /// The default is to run this against the single-node. This option
    /// conflicts with specifying the admin URL manually.
    #[arg(long, conflicts_with = "clickhouse_admin_url")]
    use_replicated: bool,

    #[command(subcommand)]
    command: ClickHouseAdminCommands,
}

#[derive(Debug, Args)]
struct RetentionPolicyRequest {
    /// The number of days to retain telemetry data.
    #[arg(long)]
    days: std::num::NonZeroU8,
}

/// Subcommands that query ClickHouse admin server state
#[derive(Debug, Subcommand)]
enum ClickHouseAdminCommands {
    /// Fetch the current database retention policy
    RetentionPolicy,
    /// Set the current database retention policy
    SetRetentionPolicy(RetentionPolicyRequest),
    /// Fetch the current database table usage.
    DatabaseUsage,
}

impl ClickHouseAdminArgs {
    async fn client(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> Result<Client, anyhow::Error> {
        let (maybe_replicated, srvname) = if self.use_replicated {
            ("replicated ", ServiceName::ClickhouseAdminServer)
        } else {
            ("", ServiceName::ClickhouseAdminSingleServer)
        };
        let server_url = match &self.clickhouse_admin_url {
            Some(cli_or_env_url) => cli_or_env_url.clone(),
            None => {
                eprintln!(
                    "note: {}clickhouse-admin URL not specified.  Will pick one from DNS.",
                    maybe_replicated,
                );
                let addr = omdb.dns_lookup_one(log.clone(), srvname).await?;
                format!("http://{}", addr)
            }
        };
        eprintln!(
            "note: using {}clickhouse-admin URL {}",
            maybe_replicated, server_url
        );

        let client = Client::new(
            &server_url,
            log.new(slog::o!("component" => "clickhouse-admin-client")),
        );
        Ok(client)
    }

    /// Run the command.
    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        let client = self.client(omdb, log).await?;
        match &self.command {
            ClickHouseAdminCommands::RetentionPolicy => {
                self.retention_policy(&client).await
            }
            ClickHouseAdminCommands::SetRetentionPolicy(retention) => {
                let _token = omdb.check_allow_destructive()?;
                self.set_retention_policy(&client, retention).await
            }
            ClickHouseAdminCommands::DatabaseUsage => {
                self.database_usage(&client).await
            }
        }
    }

    async fn retention_policy(&self, client: &Client) -> anyhow::Result<()> {
        let types::DatabaseRetentionPolicy { tables } = client
            .retention_policy()
            .await
            .context("fetching retention policy")?
            .into_inner();
        let rows = tables
            .into_iter()
            .map(|t| RetentionPolicy {
                table_name: t.table,
                retention: t.days.as_human_str(),
            })
            .collect::<Vec<_>>();
        let table = Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!();
        println!("{table}");
        Ok(())
    }

    async fn database_usage(&self, client: &Client) -> anyhow::Result<()> {
        let DatabaseUsageResult { last_error, last_success } = client
            .database_usage()
            .await
            .context("fetching database usage")?
            .into_inner();
        println!();
        println!("Last usage");
        if let Some(u) = last_success {
            let mut rows = Vec::with_capacity(u.tables.len());
            for table in u.tables {
                let row = TableUsage {
                    table_name: table.name,
                    n_bytes: format_bytes(table.n_bytes),
                    n_rows: table.n_rows,
                };
                rows.push(row);
            }
            let table = Table::new(rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();

            println!(
                "   Started: {}",
                u.started_at.to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            println!(
                " Completed: {}",
                u.completed_at.to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            println!();
            println!("{table}");
        } else {
            println!(" Never computed");
        }

        println!();
        println!("Last error");
        if let Some(e) = last_error {
            println!(
                "  Timestamp: {}",
                e.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            println!("    Message: {}", e.error);
        } else {
            println!("  None");
        }
        Ok(())
    }

    async fn set_retention_policy(
        &self,
        client: &Client,
        retention: &RetentionPolicyRequest,
    ) -> anyhow::Result<()> {
        let days = types::Days(retention.days);
        client
            .set_retention_policy(&types::RetentionPolicyRequest { days })
            .await
            .context("setting retention policy")
            .map(|_| ())
    }
}

fn format_bytes(n_bytes: u64) -> String {
    const PREFIXES: &[&str] = &["Ki", "Mi", "Gi", "Ti"];
    if n_bytes < 1024 {
        return format!("{n_bytes} B");
    }
    let mut n_bytes = (n_bytes as f64) / 1024.0;
    for prefix in PREFIXES.iter() {
        if n_bytes < 1024.0 {
            return format!("{n_bytes:.2} {prefix}B");
        }
        n_bytes /= 1024.0;
    }
    format!("{n_bytes:.2} PiB")
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct TableUsage {
    table_name: String,
    n_bytes: String,
    n_rows: u64,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct RetentionPolicy {
    table_name: String,
    retention: String,
}

#[cfg(test)]
#[test]
fn test_format_bytes() {
    assert_eq!(format_bytes(1), "1 B");
    assert_eq!(format_bytes(1023), "1023 B");
    assert_eq!(format_bytes(1_024), "1.00 KiB");
    assert_eq!(format_bytes((1024 * 99) + 100), "99.10 KiB");
    assert_eq!(format_bytes(1024 * 100), "100.00 KiB");
    assert_eq!(format_bytes(1024 * 1000 - 10), "999.99 KiB");
    assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
}
