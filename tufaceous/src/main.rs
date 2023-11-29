// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use clap::Parser;
use slog::Drain;
use tufaceous::Args;

#[tokio::main]
async fn main() -> Result<()> {
    let log = setup_log();
    let args = Args::parse();
    args.exec(&log).await
}

fn setup_log() -> slog::Logger {
    let stderr_drain = stderr_env_drain("RUST_LOG");
    let drain = slog_async::Async::new(stderr_drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

fn stderr_env_drain(env_var: &str) -> impl Drain<Ok = (), Err = slog::Never> {
    let stderr_decorator = slog_term::TermDecorator::new().build();
    let stderr_drain =
        slog_term::FullFormat::new(stderr_decorator).build().fuse();
    let mut builder = slog_envlogger::LogBuilder::new(stderr_drain);
    if let Ok(s) = std::env::var(env_var) {
        builder = builder.parse(&s);
    } else {
        // Log at the info level by default.
        builder = builder.filter(None, slog::FilterLevel::Info);
    }
    builder.build()
}
