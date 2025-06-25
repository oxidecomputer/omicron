// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;
use clickana::Clickana;
use std::net::SocketAddr;

const CLICKANA_LOG_FILE: &str = "/tmp/clickana.log";

fn main() -> Result<()> {
    let args = Cli::parse();

    let terminal = ratatui::init();
    let result = oxide_tokio_rt::run(async move {
        Clickana::new(
            args.clickhouse_addr,
            args.log_path,
            args.sampling_interval,
            args.time_range,
            args.refresh_interval,
        )
        .run(terminal)
        .await
    });
    ratatui::restore();
    result
}

#[derive(Debug, Parser)]
struct Cli {
    /// Path to the log file
    #[arg(
        long,
        short,
        env = "CLICKANA_LOG_PATH",
        default_value = CLICKANA_LOG_FILE,
    )]
    log_path: Utf8PathBuf,

    /// Address where a clickhouse admin server is listening on
    #[arg(long, short = 'a')]
    clickhouse_addr: SocketAddr,

    /// The interval to collect monitoring data in seconds
    #[arg(long, short, default_value_t = 60)]
    sampling_interval: u64,

    /// Range of time to collect monitoring data in seconds
    #[arg(long, short, default_value_t = 3600)]
    time_range: u64,

    /// The interval at which the dashboards will refresh
    #[arg(long, short, default_value_t = 60)]
    refresh_interval: u64,
}
