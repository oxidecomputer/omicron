// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use gateway_messages::PowerState;
use gateway_messages::SpComponent;
use gateway_messages::UpdateId;
use gateway_messages::UpdateStatus;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::DISCOVERY_MULTICAST_ADDR;
use slog::info;
use slog::o;
use slog::Drain;
use slog::Level;
use slog::Logger;
use std::fs;
use std::net::SocketAddrV6;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UdpSocket;
use uuid::Uuid;

mod usart;

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

    /// Upload a new image to the SP or one of its components.
    ///
    /// To update the SP itself:
    ///
    /// 1. Use the component name "sp"
    /// 2. Specify slot 0 (the SP only has a single updateable slot: its
    ///    alternate bank).
    /// 3. Pass the path to a hubris archive as `image`.
    Update { component: String, slot: u16, image: PathBuf },

    /// Get the status of an update to the specified component.
    UpdateStatus { component: String },

    /// Abort an in-progress update.
    UpdateAbort {
        /// Component with an update-in-progress to be aborted. Omit to abort
        /// updates to the SP itself.
        component: String,
        /// ID of the update to abort.
        update_id: Uuid,
    },

    /// Get or set the power state.
    PowerState {
        /// If present, instruct the SP to set this power state. If not present,
        /// get the current power state instead.
        #[clap(value_parser = power_state_from_str)]
        new_power_state: Option<PowerState>,
    },

    /// Instruct the SP to reset.
    Reset,
}

fn power_state_from_str(s: &str) -> Result<PowerState> {
    match s {
        "a0" | "A0" => Ok(PowerState::A0),
        "a1" | "A1" => Ok(PowerState::A1),
        "a2" | "A2" => Ok(PowerState::A2),
        _ => Err(anyhow!("Invalid power state: {s}")),
    }
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
        Some(SpCommand::Update { component, slot, image }) => {
            let sp_component = SpComponent::try_from(component.as_str())
                .map_err(|_| {
                    anyhow!("invalid component name: {}", component)
                })?;
            let data = fs::read(&image).with_context(|| {
                format!("failed to read {}", image.display())
            })?;
            update(&log, &sp, sp_component, slot, data).await.with_context(
                || {
                    format!(
                        "updating {} slot {} to {} failed",
                        component,
                        slot,
                        image.display()
                    )
                },
            )?;
        }
        Some(SpCommand::UpdateStatus { component }) => {
            let sp_component = SpComponent::try_from(component.as_str())
                .map_err(|_| anyhow!("invalid component name: {component}"))?;
            let status =
                sp.update_status(sp_component).await.with_context(|| {
                    format!(
                        "failed to get update status to component {component}"
                    )
                })?;
            match status {
                UpdateStatus::Preparing(sub_status) => {
                    let id = Uuid::from(sub_status.id);
                    if let Some(progress) = sub_status.progress {
                        info!(
                            log, "update still preparing (progress: {}/{})",
                            progress.current, progress.total;
                            "id" => %id,
                        );
                    } else {
                        info!(
                            log, "update still preparing (no progress available)";
                            "id" => %id,
                        );
                    }
                }
                UpdateStatus::SpUpdateAuxFlashChckScan {
                    id,
                    found_match,
                    ..
                } => {
                    let id = Uuid::from(id);
                    info!(
                        log, "aux flash scan complete";
                        "id" => %id,
                        "found_match" => found_match,
                    );
                }
                UpdateStatus::InProgress(sub_status) => {
                    let id = Uuid::from(sub_status.id);
                    info!(
                        log, "update in progress";
                        "id" => %id,
                        "bytes_received" => sub_status.bytes_received,
                        "total_size" => sub_status.total_size,
                    );
                }
                UpdateStatus::Complete(id) => {
                    let id = Uuid::from(id);
                    info!(log, "update complete"; "id" => %id);
                }
                UpdateStatus::Aborted(id) => {
                    let id = Uuid::from(id);
                    info!(log, "update aborted"; "id" => %id);
                }
                UpdateStatus::Failed { id, code } => {
                    let id = Uuid::from(id);
                    info!(log, "update failed"; "id" => %id, "code" => code);
                }
                UpdateStatus::None => {
                    info!(log, "no update status available");
                }
            }
        }
        Some(SpCommand::UpdateAbort { component, update_id }) => {
            let sp_component = SpComponent::try_from(component.as_str())
                .map_err(|_| anyhow!("invalid component name: {component}"))?;
            sp.update_abort(sp_component, update_id).await.with_context(
                || format!("aborting update to {} failed", component),
            )?;
        }
        Some(SpCommand::PowerState { new_power_state }) => {
            if let Some(state) = new_power_state {
                sp.set_power_state(state).await.with_context(|| {
                    format!("failed to set power state to {state:?}")
                })?;
                info!(log, "successfully set SP power state to {state:?}");
            } else {
                let state = sp
                    .power_state()
                    .await
                    .context("failed to get power state")?;
                info!(log, "SP power state = {state:?}");
            }
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

async fn update(
    log: &Logger,
    sp: &SingleSp,
    component: SpComponent,
    slot: u16,
    data: Vec<u8>,
) -> Result<()> {
    let update_id = Uuid::new_v4();
    info!(log, "generated update ID"; "id" => %update_id);
    sp.start_update(component, update_id, slot, data)
        .await
        .context("failed to start update")?;

    let sp_update_id = UpdateId::from(update_id);
    loop {
        let status = sp
            .update_status(component)
            .await
            .context("failed to get update status")?;
        match status {
            UpdateStatus::None => {
                bail!("no update status returned by SP (did it reset?)");
            }
            UpdateStatus::Preparing(sub_status) => {
                if sub_status.id != sp_update_id {
                    bail!("different update preparing ({:?})", sub_status.id);
                }
                if let Some(progress) = sub_status.progress {
                    info!(
                        log,
                        "update preparing: {}/{}",
                        progress.current,
                        progress.total,
                    );
                } else {
                    info!(log, "update preparing (no progress available)");
                }
            }
            UpdateStatus::SpUpdateAuxFlashChckScan {
                id,
                found_match,
                total_size,
            } => {
                if id != sp_update_id {
                    bail!("different update in progress ({:?})", id);
                }
                info!(
                    log, "aux flash scan complete";
                    "found_match" => found_match,
                    "total_size" => total_size,
                );
            }
            UpdateStatus::InProgress(sub_status) => {
                if sub_status.id != sp_update_id {
                    bail!("different update in progress ({:?})", sub_status.id);
                }
                info!(
                    log, "update in progress";
                    "bytes_received" => sub_status.bytes_received,
                    "total_size" => sub_status.total_size,
                );
            }
            UpdateStatus::Complete(id) => {
                if id != sp_update_id {
                    bail!("different update complete ({id:?})");
                }
                return Ok(());
            }
            UpdateStatus::Aborted(id) => {
                if id != sp_update_id {
                    bail!("different update aborted ({id:?})");
                }
                bail!("update aborted");
            }
            UpdateStatus::Failed { id, code } => {
                if id != sp_update_id {
                    bail!("different update failed ({id:?}, code {code})");
                }
                bail!("update failed (code {code})");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
