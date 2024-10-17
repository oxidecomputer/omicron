// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::Parser;
#[derive(Clone, Debug, Parser)]
struct Args {
    /// The log level.
    #[arg(long, default_value_t = slog::Level::Info, value_parser = parse_log_level)]
    log_level: slog::Level,

    #[clap(flatten)]
    ingester: ereport_test_utils::IngesterConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { log_level, ingester } = Args::parse();
    let log = {
        use omicron_common::FileKv;
        use slog::Drain;
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let drain = slog::LevelFilter::new(drain, log_level).fuse();
        slog::Logger::root(drain.fuse(), slog::o!(FileKv))
    };
    ingester.run(log).await
}

fn parse_log_level(s: &str) -> Result<slog::Level, String> {
    s.parse().map_err(|_| format!("invalid log level {s:?}"))
}
