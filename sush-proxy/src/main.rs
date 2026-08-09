// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable for the sush switch zone proxy.

use std::net::{SocketAddr, SocketAddrV6};

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Parser;
use slog::{Drain, o};
use sush_proxy::{Config, Tls, run};

#[derive(Debug, Parser)]
#[clap(name = "sush-proxy", about = "sush switch zone proxy")]
struct Args {
    /// The address to listen on for sush clients
    #[clap(long)]
    address: SocketAddr,

    /// The address (expected to be on localhost) for MGS
    #[clap(long)]
    mgs_address: SocketAddrV6,

    /// How to authenticate to clients
    #[clap(long, value_enum)]
    tls: TlsArg,

    /// The TLS private key (PEM), for `--tls platform`
    #[clap(long, required_if_eq("tls", "platform"))]
    priv_key: Option<Utf8PathBuf>,

    /// The TLS certificate chain (PEM), for `--tls platform`
    #[clap(long, required_if_eq("tls", "platform"))]
    cert_chain: Option<Utf8PathBuf>,
}

/// How the proxy authenticates itself to clients.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum TlsArg {
    /// The sled's platform identity, from a local key and chain
    Platform,
    /// None. For development images only
    Insecure,
}

fn main() -> anyhow::Result<()> {
    oxide_tokio_rt::run(run_proxy())
}

async fn run_proxy() -> anyhow::Result<()> {
    let Args { address, mgs_address, tls, priv_key, cert_chain } =
        Args::parse();
    let tls = match tls {
        TlsArg::Platform => Tls::Platform {
            priv_key: priv_key.unwrap(),
            cert_chain: cert_chain.unwrap(),
        },
        TlsArg::Insecure => Tls::Insecure,
    };
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!("component" => "sush-proxy"));
    let (drain, registration) = slog_dtrace::with_drain(log);
    let log = slog::Logger::root(drain.fuse(), o!());
    if let slog_dtrace::ProbeRegistration::Failed(err) = registration {
        anyhow::bail!("failed to register DTrace probes: {err}");
    }
    run(&log, Config { address, mgs_address, tls })
        .await
        .context("running the proxy")
}
