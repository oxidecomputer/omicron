// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::DISCOVERY_MULTICAST_ADDR;
use slog::info;
use slog::o;
use slog::Drain;
use slog::Level;
use slog::Logger;
use std::net::SocketAddrV6;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UdpSocket;

mod hubris_archive;
mod usart;

use self::hubris_archive::HubrisArchive;

/// Command line program that can send MGS messages to a single SP.
#[derive(Parser, Debug)]
struct Args {
    #[clap(
        short,
        long,
        default_value = "info",
        value_parser = level_from_str,
        help = "Log level for MGS client",
    )]
    log_level: Level,

    /// Address to bind to locally.
    ///
    /// May need an interface specification (e.g., `[::%2]:0`), depending
    /// on the host OS and network setup between the host and SP.
    #[clap(long, default_value = "[::]:0")]
    local_addr: SocketAddrV6,

    /// Listening port for the `mgmt-gateway` task on the SP.
    #[clap(long, short, default_value = "11111")]
    discovery_port: u16,

    /// Maximum number of attempts to make when sending requests to the SP.
    #[clap(long, default_value = "5")]
    max_attempts: usize,

    /// Timeout (in milliseconds) for each attempt.
    #[clap(long, default_value = "2000")]
    per_attempt_timeout_millis: u64,

    #[clap(subcommand)]
    command: Command,
}

fn level_from_str(s: &str) -> Result<Level> {
    if let Ok(level) = s.parse() {
        Ok(level)
    } else {
        bail!(format!("Invalid log level: {}", s))
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Discover a connected SP.
    Discover,

    /// Send a command to a connected SP.
    Sp {
        /// Address of the SP.
        ///
        /// If not provided, we attempt to discover an SP via the normal
        /// discovery mechanism (the same as the `discover` subcommand).
        #[clap(long)]
        addr: Option<SocketAddrV6>,

        #[clap(subcommand)]
        command: SpCommand,
    },
}

#[derive(Subcommand, Debug)]
enum SpCommand {
    /// Ask SP for its current state.
    State,

    /// Attach to the SP's USART.
    UsartAttach {
        /// Put the local terminal in raw mode.
        #[clap(long)]
        raw: bool,

        /// Amount of time to buffer input from stdin before forwarding to SP.
        #[clap(long, default_value = "500")]
        stdin_buffer_time_millis: u64,
    },

    /// Detach any other attached USART connection.
    UsartDetach,

    /// Upload a new image to the SP and have it swap banks (requires reset)
    Update { hubris_archive: PathBuf },

    /// Instruct the SP to reset.
    Reset,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(args.log_level)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!("component" => "faux-mgs"));

    let socket = UdpSocket::bind(args.local_addr).await.with_context(|| {
        format!("failed to bind UDP socket to {}", args.local_addr)
    })?;

    let per_attempt_timeout =
        Duration::from_millis(args.per_attempt_timeout_millis);

    let mut discovery_addr =
        SocketAddrV6::new(DISCOVERY_MULTICAST_ADDR, args.discovery_port, 0, 0);
    let command = match args.command {
        Command::Discover => {
            info!(
                log, "attempting SP discovery";
                "discovery_addr" => %discovery_addr,
            );
            None
        }
        Command::Sp { addr, command } => {
            if let Some(addr) = addr {
                discovery_addr = addr;
            }
            Some(command)
        }
    };

    let sp = SingleSp::new(
        socket,
        discovery_addr,
        args.max_attempts,
        per_attempt_timeout,
        log.clone(),
    );

    match command {
        None => {
            // "None" command indicates only discovery was requested; loop until
            // discovery completes, then log the result.
            let mut addr_watch = sp.sp_addr_watch().clone();
            loop {
                let current = *addr_watch.borrow();
                match current {
                    Some((addr, port)) => {
                        info!(
                            log, "SP discovered";
                            "addr" => %addr,
                            "port" => ?port,
                        );
                        break;
                    }
                    None => {
                        addr_watch.changed().await.unwrap();
                    }
                }
            }
        }
        Some(SpCommand::State) => {
            info!(log, "{:?}", sp.state().await?);
        }
        Some(SpCommand::UsartAttach { raw, stdin_buffer_time_millis }) => {
            usart::run(
                sp,
                raw,
                Duration::from_millis(stdin_buffer_time_millis),
                log,
            )
            .await?;
        }
        Some(SpCommand::UsartDetach) => {
            sp.serial_console_detach().await?;
            info!(log, "SP serial console detached");
        }
        Some(SpCommand::Update { hubris_archive }) => {
            let mut archive = HubrisArchive::open(&hubris_archive)?;
            let data = archive.final_bin()?;
            sp.update(data).await.with_context(|| {
                format!("updating to {} failed", hubris_archive.display())
            })?;
        }
        Some(SpCommand::Reset) => {
            sp.reset_prepare().await?;
            info!(log, "SP is prepared to reset");
            sp.reset_trigger().await?;
            info!(log, "SP reset complete");
        }
    }

    Ok(())
}
