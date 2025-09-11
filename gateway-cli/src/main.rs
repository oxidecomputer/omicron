// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use clap::Parser;
use clap::Subcommand;
use gateway_client::Client;
use gateway_client::types::HostStartupOptions;
use gateway_client::types::IgnitionCommand;
use gateway_client::types::InstallinatorImageId;
use gateway_client::types::PowerState;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpUpdateStatus;
use gateway_client::types::UpdateAbortBody;
use omicron_uuid_kinds::MupdateUuid;
use serde::Serialize;
use slog::Drain;
use slog::Level;
use slog::Logger;
use slog::o;
use std::fs;
use std::io;
use std::net::SocketAddrV6;
use std::path::PathBuf;
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

mod picocom_map;
mod usart;

// MGS's serial console APIs expect a component name, but in practice we only
// have the one serial console associated with the CPU, so we don't require
// users of this CLI to specify it.
const SERIAL_CONSOLE_COMPONENT: &str = "sp3-host-cpu";

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

    #[clap(short, long, help = "Address of MGS server")]
    server: SocketAddrV6,

    #[clap(long, help = "Pretty JSON output")]
    pretty: bool,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Get state of one SP
    State {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
    },

    /// Get ignition state of one (or all) SPs
    // `faux-mgs` also has `IngitionLinkEvents` and `ClearIgnitionLinkEvents`.
    // Endpoints for these are not currently exposed by MGS, but I believe these
    // are a "manual-debugging-only" kind of message, so that's okay?
    Ignition {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: Option<SpIdentifier>,
    },

    /// Send an ignition command
    IgnitionCommand {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Command (power-on, power-off, power-reset)
        #[clap(value_parser = ignition_command_from_str, action)]
        command: IgnitionCommand,
    },

    /// Get or set the active slot of a component
    ComponentActiveSlot {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component (e.g., host-boot-flash)
        component: String,
        /// If provided, set the active slot
        set_slot: Option<u16>,
        /// If setting the slot, should that choice be made persistent?
        #[clap(requires = "set_slot")]
        persist: bool,
    },

    /// Get or set startup options on an SP.
    StartupOptions {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Set startup options to the bitwise-OR of all further options
        #[clap(long)]
        set: bool,
        #[clap(long, requires = "set")]
        phase2_recovery: bool,
        #[clap(long, requires = "set")]
        kbm: bool,
        #[clap(long, requires = "set")]
        bootrd: bool,
        #[clap(long, requires = "set")]
        prom: bool,
        #[clap(long, requires = "set")]
        kmdb: bool,
        #[clap(long, requires = "set")]
        kmdb_boot: bool,
        #[clap(long, requires = "set")]
        boot_ramdisk: bool,
        #[clap(long, requires = "set")]
        boot_net: bool,
        #[clap(long, requires = "set")]
        startup_verbose: bool,
    },

    /// Set the installinator image ID IPCC key/value.
    SetInstallinatorImageId {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Clear any previously-set ID.
        #[clap(long)]
        clear: bool,
        /// ID to assign to this update for progress/completion messages.
        #[clap(
            long,
            conflicts_with = "clear",
            required_unless_present = "clear"
        )]
        update_id: Option<MupdateUuid>,
        /// Hash of the host OS image.
        #[clap(
            long,
            conflicts_with = "clear",
            required_unless_present = "clear"
        )]
        host_phase_2: Option<ArtifactHash>,
        /// Hash of the control plane image.
        #[clap(
            long,
            conflicts_with = "clear",
            required_unless_present = "clear"
        )]
        control_plane: Option<ArtifactHash>,
    },

    /// Ask SP for its inventory.
    Inventory {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
    },

    /// Ask SP for details of a component.
    ComponentDetails {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component name, from this SP's inventory of components
        component: String,
    },

    /// Ask SP to clear the state (e.g., reset counters) on a component.
    ComponentClearStatus {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component name, from this SP's inventory of components
        component: String,
    },

    /// Attach to the SP's USART.
    UsartAttach {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,

        /// Put the local terminal in raw mode.
        #[clap(
            long = "no-raw",
            help = "do not put terminal in raw mode",
            action = clap::ArgAction::SetFalse,
        )]
        raw: bool,

        /// Amount of time to buffer input from stdin before forwarding to SP.
        #[clap(long, default_value = "500")]
        stdin_buffer_time_millis: u64,

        /// Specifies the input character map (i.e., special characters to be
        /// replaced when reading from the serial port). See picocom's manpage.
        #[clap(long)]
        imap: Option<String>,

        /// Specifies the output character map (i.e., special characters to be
        /// replaced when writing to the serial port). See picocom's manpage.
        #[clap(long)]
        omap: Option<String>,

        /// Record all input read from the serial port to this logfile (before
        /// any remapping).
        #[clap(long)]
        uart_logfile: Option<PathBuf>,
    },

    /// Force-detach any attached USART connection
    UsartDetach {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
    },

    /// Upload a host phase 2 recovery image to be served on request
    UploadRecoveryHostPhase2 { path: PathBuf },

    /// Upload a new image to the SP or one of its components.
    ///
    /// To update the SP itself:
    ///
    /// 1. Use the component name "sp"
    /// 2. Specify slot 0 (the SP only has a single updateable slot: its
    ///    alternate bank).
    /// 3. Pass the path to a hubris archive as `image`.
    Update {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component name, from this SP's inventory of components
        component: String,
        /// Slot number to apply the update
        slot: u16,
        /// Path to the image
        image: PathBuf,
    },

    /// Get the status of an update to the specified component.
    UpdateStatus {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component name, from this SP's inventory of components
        component: String,
    },

    /// Abort an in-progress update.
    UpdateAbort {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component with an update-in-progress to be aborted
        component: String,
        /// ID of the update to abort (from `update-status`)
        update_id: Uuid,
    },

    /// Get or set the power state.
    PowerState {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// If present, instruct the SP to set this power state. If not present,
        /// get the current power state instead.
        #[clap(value_parser = power_state_from_str)]
        new_power_state: Option<PowerState>,
    },

    /// Instruct the SP to reset.
    Reset {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
    },

    /// Instruct the SP to reset a component.
    ResetComponent {
        /// Target SP (e.g., 'sled/7', 'switch/1', 'power/0')
        #[clap(value_parser = sp_identifier_from_str, action)]
        sp: SpIdentifier,
        /// Component to reset
        component: String,
    },

    /// Get the ID for the switch this MGS instance is connected to.
    LocalSwitchId {},
}

fn level_from_str(s: &str) -> Result<Level> {
    if let Ok(level) = s.parse() {
        Ok(level)
    } else {
        bail!(format!("Invalid log level: {}", s))
    }
}

fn sp_identifier_from_str(s: &str) -> Result<SpIdentifier> {
    const BAD_FORMAT: &str = concat!(
        "SP identifier must be of the form TYPE/SLOT; ",
        "e.g., `sled/7`, `switch/1`, `power/0`"
    );
    let mut parts = s.split('/');
    let type_ = parts.next().context(BAD_FORMAT)?;
    let slot = parts.next().context(BAD_FORMAT)?;
    if parts.next().is_some() {
        bail!(BAD_FORMAT);
    }
    Ok(SpIdentifier {
        type_: type_.parse().map_err(|s| {
            anyhow!("failed to parse {type_} as an SpType: {s}")
        })?,
        slot: slot.parse().map_err(|err| {
            anyhow!("failed to parse slot {slot} as a u32: {err}")
        })?,
    })
}

fn ignition_command_from_str(s: &str) -> Result<IgnitionCommand> {
    match s {
        "power-on" => Ok(IgnitionCommand::PowerOn),
        "power-off" => Ok(IgnitionCommand::PowerOff),
        "power-reset" => Ok(IgnitionCommand::PowerReset),
        _ => Err(anyhow!("Invalid ignition command: {s}")),
    }
}

fn power_state_from_str(s: &str) -> Result<PowerState> {
    match s {
        "a0" | "A0" => Ok(PowerState::A0),
        "a1" | "A1" => Ok(PowerState::A1),
        "a2" | "A2" => Ok(PowerState::A2),
        _ => Err(anyhow!("Invalid power state: {s}")),
    }
}

struct Dumper {
    pretty: bool,
}

impl Dumper {
    fn dump<T: Serialize>(&self, value: &T) -> Result<()> {
        let stdout = io::stdout().lock();
        if self.pretty {
            serde_json::to_writer_pretty(stdout, value)?;
        } else {
            serde_json::to_writer(stdout, value)?;
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    oxide_tokio_rt::run(main_impl())
}

async fn main_impl() -> Result<()> {
    let args = Args::parse();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(args.log_level)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!("component" => "gateway-client"));

    // Workaround lack of support for scoped IPv6 addresses
    // in URLs by adding an override to resolve the given domain
    // (mgs.localhorse) to the desired address, scope-id and all.
    // Note the port must still be passed via the URL.
    let reqwest_client = reqwest::Client::builder()
        .resolve_to_addrs("mgs.localhorse", &[args.server.into()])
        .build()?;
    let client = Client::new_with_client(
        &format!("http://mgs.localhorse:{}", args.server.port()),
        reqwest_client,
        log.clone(),
    );

    let dumper = Dumper { pretty: args.pretty };

    match args.command {
        Command::State { sp } => {
            let info = client.sp_get(sp.type_, sp.slot).await?.into_inner();
            dumper.dump(&info)?;
        }
        Command::Ignition { sp } => {
            if let Some(sp) = sp {
                let info =
                    client.ignition_get(sp.type_, sp.slot).await?.into_inner();
                dumper.dump(&info)?;
            } else {
                let info = client.ignition_list().await?.into_inner();
                dumper.dump(&info)?;
            }
        }
        Command::IgnitionCommand { sp, command } => {
            client.ignition_command(sp.type_, sp.slot, command).await?;
        }
        Command::ComponentActiveSlot { sp, component, set_slot, persist } => {
            if let Some(slot) = set_slot {
                client
                    .sp_component_active_slot_set(
                        sp.type_,
                        sp.slot,
                        &component,
                        persist,
                        &SpComponentFirmwareSlot { slot },
                    )
                    .await?;
            } else {
                let info = client
                    .sp_component_active_slot_get(sp.type_, sp.slot, &component)
                    .await?
                    .into_inner();
                dumper.dump(&info)?;
            }
        }
        Command::StartupOptions {
            sp,
            set,
            phase2_recovery,
            kbm,
            bootrd,
            prom,
            kmdb,
            kmdb_boot,
            boot_ramdisk,
            boot_net,
            startup_verbose,
        } => {
            if set {
                let options = HostStartupOptions {
                    phase2_recovery_mode: phase2_recovery,
                    kbm,
                    bootrd,
                    prom,
                    kmdb,
                    kmdb_boot,
                    boot_ramdisk,
                    boot_net,
                    verbose: startup_verbose,
                };
                client
                    .sp_startup_options_set(sp.type_, sp.slot, &options)
                    .await?;
            } else {
                let info = client
                    .sp_startup_options_get(sp.type_, sp.slot)
                    .await?
                    .into_inner();
                dumper.dump(&info)?;
            }
        }
        Command::SetInstallinatorImageId {
            sp,
            clear,
            update_id,
            host_phase_2,
            control_plane,
        } => {
            if clear {
                client
                    .sp_installinator_image_id_delete(sp.type_, sp.slot)
                    .await?;
            } else {
                // clap guarantees these are not `None` when `clear` is false.
                let update_id = update_id.unwrap();
                let host_phase_2 = host_phase_2.unwrap().to_string();
                let control_plane = control_plane.unwrap().to_string();
                client
                    .sp_installinator_image_id_set(
                        sp.type_,
                        sp.slot,
                        &InstallinatorImageId {
                            update_id,
                            host_phase_2,
                            control_plane,
                        },
                    )
                    .await?;
            }
        }
        Command::Inventory { sp } => {
            let info =
                client.sp_component_list(sp.type_, sp.slot).await?.into_inner();
            dumper.dump(&info)?;
        }
        Command::ComponentDetails { sp, component } => {
            let info = client
                .sp_component_get(sp.type_, sp.slot, &component)
                .await?
                .into_inner();
            dumper.dump(&info)?;
        }
        Command::ComponentClearStatus { sp, component } => {
            client
                .sp_component_clear_status(sp.type_, sp.slot, &component)
                .await?;
        }
        Command::UsartAttach {
            sp,
            raw,
            stdin_buffer_time_millis,
            imap,
            omap,
            uart_logfile,
        } => {
            let upgraded = client
                .sp_component_serial_console_attach(
                    sp.type_,
                    sp.slot,
                    SERIAL_CONSOLE_COMPONENT,
                )
                .await
                .map_err(|err| anyhow!("{err}"))?;

            let ws = WebSocketStream::from_raw_socket(
                upgraded.into_inner(),
                Role::Client,
                None,
            )
            .await;
            usart::run(
                ws,
                raw,
                Duration::from_millis(stdin_buffer_time_millis),
                imap,
                omap,
                uart_logfile,
            )
            .await?;
        }
        Command::UsartDetach { sp } => {
            client
                .sp_component_serial_console_detach(
                    sp.type_,
                    sp.slot,
                    SERIAL_CONSOLE_COMPONENT,
                )
                .await?;
        }
        Command::UploadRecoveryHostPhase2 { path } => {
            let image_stream =
                tokio::fs::File::open(&path).await.with_context(|| {
                    format!("failed to open {}", path.display())
                })?;
            let info = client
                .recovery_host_phase2_upload(image_stream)
                .await?
                .into_inner();
            dumper.dump(&info)?;
        }
        Command::Update { sp, component, slot, image } => {
            let image = fs::read(&image).with_context(|| {
                format!("failed to read {}", image.display())
            })?;
            update(&client, &dumper, sp, &component, slot, image).await?;
        }
        Command::UpdateStatus { sp, component } => {
            let info = client
                .sp_component_update_status(sp.type_, sp.slot, &component)
                .await?
                .into_inner();
            dumper.dump(&info)?;
        }
        Command::UpdateAbort { sp, component, update_id } => {
            let body = UpdateAbortBody { id: update_id };
            client
                .sp_component_update_abort(sp.type_, sp.slot, &component, &body)
                .await?;
        }
        Command::PowerState { sp, new_power_state } => {
            if let Some(power_state) = new_power_state {
                client
                    .sp_power_state_set(sp.type_, sp.slot, power_state)
                    .await?;
            } else {
                let info = client
                    .sp_power_state_get(sp.type_, sp.slot)
                    .await?
                    .into_inner();
                dumper.dump(&info)?;
            }
        }
        Command::Reset { sp } => {
            let component =
                gateway_messages::SpComponent::SP_ITSELF.const_as_str();
            client.sp_component_reset(sp.type_, sp.slot, component).await?;
        }
        Command::ResetComponent { sp, component } => {
            client.sp_component_reset(sp.type_, sp.slot, &component).await?;
        }
        Command::LocalSwitchId {} => {
            let sp_id = client.sp_local_switch_id().await?.into_inner();
            dumper.dump(&sp_id)?;
        }
    }

    Ok(())
}

async fn update(
    client: &Client,
    dumper: &Dumper,
    sp: SpIdentifier,
    component: &str,
    slot: u16,
    image: Vec<u8>,
) -> Result<()> {
    let update_id = Uuid::new_v4();
    println!("generated update ID {update_id}");

    client
        .sp_component_update(
            sp.type_, sp.slot, component, slot, &update_id, image,
        )
        .await
        .context("failed to start update")?;

    loop {
        let status = client
            .sp_component_update_status(sp.type_, sp.slot, component)
            .await
            .context("failed to get update status")?
            .into_inner();
        dumper.dump(&status)?;
        match status {
            SpUpdateStatus::None => {
                bail!("no update status returned by SP (did it reset?)");
            }
            SpUpdateStatus::Preparing { id, .. } => {
                if id != update_id {
                    bail!("different update preparing ({:?})", id);
                }
            }
            SpUpdateStatus::InProgress { id, .. } => {
                if id != update_id {
                    bail!("different update in progress ({:?})", id);
                }
            }
            SpUpdateStatus::Complete { id } => {
                if id != update_id {
                    bail!("different update complete ({id:?})");
                }
                return Ok(());
            }
            SpUpdateStatus::Aborted { id } => {
                if id != update_id {
                    bail!("different update aborted ({id:?})");
                }
                bail!("update aborted");
            }
            SpUpdateStatus::Failed { id, code } => {
                if id != update_id {
                    bail!("different update failed ({id:?}, code {code})");
                }
                bail!("update failed (code {code})");
            }
            SpUpdateStatus::RotError { id, message } => {
                if id != update_id {
                    bail!(
                        "different update failed ({id:?}, rot error {message})"
                    );
                }
                bail!("update failed (rot error {message})");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
