// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Main entry point to run an `oximeter` server in the control plane.

// Copyright 2023 Oxide Computer Company

use anyhow::{Context, anyhow};
use clap::Parser;
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
use oximeter_collector::Config;
use oximeter_collector::Oximeter;
use oximeter_collector::OximeterArguments;
use oximeter_collector::StandaloneNexus;
use oximeter_collector::standalone_nexus_api;
use slog::Level;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::path::PathBuf;
use uuid::Uuid;

pub fn run_standalone_openapi() -> Result<(), String> {
    standalone_nexus_api()
        .openapi("Oxide Nexus API", semver::Version::new(0, 0, 1))
        .description("API for interacting with Nexus")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

/// Run an oximeter metric collection server in the Oxide Control Plane.
#[derive(Parser)]
#[clap(name = "oximeter", about = "See README.adoc for more information")]
enum Args {
    /// Start an Oximeter server
    Run {
        /// Path to TOML file with configuration for the server
        #[clap(name = "CONFIG_FILE", action)]
        config_file: PathBuf,

        /// The UUID for this instance of the `oximeter` collector.
        #[clap(short, long, action)]
        id: Uuid,

        /// The socket address at which `oximeter`'s HTTP server runs.
        #[clap(short, long, action)]
        address: SocketAddrV6,
    },

    /// Run `oximeter` in standalone mode for development.
    ///
    /// In this mode, `oximeter` can be used to test the collection of metrics
    /// from producers, without requiring all the normal machinery of the
    /// control plane. The collector is run as usual, but additionally starts a
    /// API server to stand-in for Nexus. The registrations of the producers and
    /// collectors occurs through the normal code path, but uses this mock Nexus
    /// instead of the real thing.
    Standalone {
        /// The ID for the collector.
        ///
        /// Default is to generate a new, random UUID.
        #[arg(long, default_value_t = Uuid::new_v4())]
        id: Uuid,

        /// Address at which `oximeter` itself listens.
        ///
        /// This address can be used to register new producers, after the
        /// program has already started.
        #[arg(
            long,
            default_value_t = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 12223, 0, 0)
        )]
        address: SocketAddrV6,

        /// The address for the mock Nexus server used to register.
        ///
        /// This program starts a mock version of Nexus, which is used only to
        /// register the producers and collectors. This allows them to operate
        /// as they usually would, registering each other with Nexus so that an
        /// assignment between them can be made.
        #[arg(
            long,
            default_value_t = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 12221, 0, 0)
        )]
        nexus: SocketAddrV6,

        /// The address for ClickHouse.
        ///
        /// If not provided, `oximeter` will not attempt to insert records into
        /// the database at all. In this mode, the program will print the
        /// collected samples, instead of inserting them into the database.
        #[arg(long)]
        clickhouse: Option<SocketAddr>,

        /// The log-level.
        #[arg(long, default_value_t = Level::Info, value_parser = parse_log_level)]
        log_level: Level,
    },

    /// Print the fake Nexus's standalone API.
    StandaloneOpenapi,
}

fn parse_log_level(s: &str) -> Result<Level, String> {
    s.parse().map_err(|_| "Invalid log level".to_string())
}

fn main() {
    if let Err(cmd_error) = oxide_tokio_rt::run(do_run()) {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();
    match args {
        Args::Run { config_file, id, address } => {
            let config = Config::from_file(config_file).unwrap();
            let args = OximeterArguments { id, address };
            Oximeter::new(&config, &args)
                .await
                .unwrap()
                .serve_forever()
                .await
                .context("Failed to create oximeter")
                .map_err(CmdError::Failure)
        }
        Args::Standalone { id, address, nexus, clickhouse, log_level } => {
            // Start the standalone Nexus server, for registration of both the
            // collector and producers.
            let nexus_server = StandaloneNexus::new(nexus.into(), log_level)
                .context("Failed to create nexus")
                .map_err(CmdError::Failure)?;
            let args = OximeterArguments { id, address };
            Oximeter::new_standalone(
                nexus_server.log(),
                &args,
                nexus_server.local_addr(),
                clickhouse,
            )
            .await
            .unwrap()
            .serve_forever()
            .await
            .context("Failed to create standalone oximeter")
            .map_err(CmdError::Failure)
        }
        Args::StandaloneOpenapi => run_standalone_openapi()
            .map_err(|err| CmdError::Failure(anyhow!(err))),
    }
}
