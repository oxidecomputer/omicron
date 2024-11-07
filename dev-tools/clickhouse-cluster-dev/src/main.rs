// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use dropshot::{test_util::LogContext, ConfigLogging, ConfigLoggingLevel};

#[tokio::main]
async fn main() -> Result<()> {
    let logctx = LogContext::new(
        "clickhouse-cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    slog::info!(
        logctx.log,
        "Setting up a ClickHouse cluster"
    );

    Ok(())
}
