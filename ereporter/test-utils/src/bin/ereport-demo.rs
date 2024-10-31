// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use clap::Parser;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use uuid::Uuid;

/// A demo binary that generates a single ereport and then waits for it to be collected.
#[derive(Clone, Parser)]
struct Args {
    #[arg(long, short)]
    uuid: Uuid,

    /// The address for the mock Nexus server used to register.
    #[arg(
        long,
        default_value_t = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0),
    )]
    server_address: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { uuid, server_address } = Args::parse();
    let log = {
        use omicron_common::FileKv;
        use slog::Drain;
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        // let drain = slog::LevelFilter::new(drain, log_level).fuse();
        slog::Logger::root(drain.fuse(), slog::o!(FileKv))
    };
    let registry = ereporter::ReporterRegistry::new(
        ereporter::registry::LogConfig::Logger(log.clone()),
        128,
    )?;
    let reporter = registry.register_reporter(uuid);
    let _server = registry.start_server(ereporter::server::Config {
        server_address,
        request_body_max_bytes: 1024 * 8,
    })?;

    let mut report = reporter
        .report("list.suspect")
        .await
        .map_err(|_| anyhow::anyhow!("couldnt get ye report"))?;
    // https://www.youtube.com/watch?v=TSOtPQ-K3Ds
    report.fact("de.version", "0");
    report.fact("de.mod-name", "hal-9000-diagnosis");
    report.fact("de.mod-version", "1.0");
    report.fact("de.authority.product-id", "HAL 9000-series computer");
    report.fact("de.authority.server-id", "HAL 9000");
    report.fact("fault-list.0.unit", "AE-35");
    report.fact("fault-list.0.class", "defect.discovery-one.ae-32-fault");
    report.fact("fault-list.0.certainty", "0x64");
    report.fact("fault-list.0.100-failure-in", "72h");
    report.fact(
        "fault-list.0.nosub_class",
        "ereport.discovery-one.communications-dish.ae-35-unit",
    );
    report.submit();

    tokio::signal::ctrl_c().await?;
    Ok(())
}
