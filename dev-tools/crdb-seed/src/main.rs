// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use dropshot::{ConfigLogging, ConfigLoggingLevel, test_util::LogContext};
use omicron_test_utils::dev::CRDB_SEED_TAR_ENV;
use omicron_test_utils::dev::seed::{
    ensure_seed_tarball_exists, should_invalidate_seed,
};
use std::io::Write;

#[expect(
    clippy::disallowed_macros,
    reason = "this is a tiny binary that doesn't do very much,`tokio::main` \
        is fine."
)]
#[tokio::main]
async fn main() -> Result<()> {
    // TODO: dropshot is v heavyweight for this, we should be able to pull in a
    // smaller binary
    let logctx = LogContext::new(
        "crdb_seeding",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );
    let (seed_tar, status) =
        ensure_seed_tarball_exists(&logctx.log, should_invalidate_seed())
            .await?;
    status.log(&logctx.log, &seed_tar);

    if let Ok(env_path) = std::env::var("NEXTEST_ENV") {
        let mut file = std::fs::File::create(&env_path)
            .context("failed to open NEXTEST_ENV file")?;
        writeln!(file, "{CRDB_SEED_TAR_ENV}={seed_tar}")
            .context("failed to write to NEXTEST_ENV file")?;
    } else {
        slog::warn!(
            logctx.log,
            "NEXTEST_ENV not set (is this script running under nextest?)"
        );
    }

    Ok(())
}
