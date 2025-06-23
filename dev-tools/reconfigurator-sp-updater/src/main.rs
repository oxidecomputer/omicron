// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interactively manage SP updates from the command line

use anyhow::Context;
use anyhow::anyhow;
use clap::Args;
use clap::ColorChoice;
use clap::Parser;
use clap::Subcommand;
use futures::StreamExt;
use gateway_client::types::SpIgnition;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
use internal_dns_types::names::ServiceName;
use nexus_mgs_updates::ArtifactCache;
use nexus_mgs_updates::MgsUpdateDriver;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::internal_api::views::MgsUpdateDriverStatus;
use nexus_types::inventory::BaseboardId;
use omicron_repl_utils::run_repl_on_stdin;
use qorb::resolver::Resolver;
use qorb::resolvers::fixed::FixedResolver;
use slog::{info, o, warn};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;

fn main() -> Result<(), anyhow::Error> {
    let args = ReconfiguratorSpUpdater::parse();

    if let Err(error) = omicron_runtime::run(args.exec()) {
        eprintln!("error: {:#}", error);
        std::process::exit(1);
    }

    Ok(())
}

/// Execute blueprints from the command line
#[derive(Debug, Parser)]
struct ReconfiguratorSpUpdater {
    /// log level filter
    #[arg(
        env,
        long,
        value_parser = parse_dropshot_log_level,
        default_value = "info",
    )]
    log_level: dropshot::ConfigLoggingLevel,

    /// an internal DNS server in this deployment
    // This default value is currently appropriate for all deployed systems.
    // That relies on two assumptions:
    //
    // 1. The internal DNS servers' underlay addresses are at a fixed location
    //    from the base of the AZ subnet.  This is unlikely to change, since the
    //    DNS servers must be discoverable with virtually no other information.
    // 2. The AZ subnet used for all deployments today is fixed.
    //
    // For simulated systems (e.g., `cargo xtask omicron-dev run-all`), or if
    // these assumptions change in the future, we may need to adjust this.
    #[arg(long, default_value = "[fd00:1122:3344:3::1]:53")]
    dns_server: SocketAddr,

    /// HOST:PORT for a TUF repo depot server
    repo_depot_addr: SocketAddr,

    /// Color output
    #[arg(long, value_enum, default_value_t)]
    color: ColorChoice,
}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}

impl ReconfiguratorSpUpdater {
    async fn exec(self) -> Result<(), anyhow::Error> {
        let log = dropshot::ConfigLogging::StderrTerminal {
            level: self.log_level.clone(),
        }
        .to_logger("reconfigurator-sp-updater")
        .context("failed to create logger")?;

        info!(&log, "setting up resolver");
        let qorb_resolver =
            internal_dns_resolver::QorbResolver::new(vec![self.dns_server]);

        let mut mgs_resolver =
            qorb_resolver.for_service(ServiceName::ManagementGatewayService);
        let mut mgs_rx = mgs_resolver.monitor();

        // Fetch an initial inventory from MGS.  We'll use this to allow users
        // to specify just a serial number and have us look up the rest of the
        // information that we need.
        info!(&log, "resolve MGS in DNS");
        let mgs_url = {
            let mgs_backends = mgs_rx
                .wait_for(|all_backends| !all_backends.is_empty())
                .await
                .context("waiting to resolve MGS in DNS")?;
            let mgs_backend = mgs_backends
                .values()
                .next()
                .expect("we just waited for this condition");
            format!("http://{}", mgs_backend.address)
        };

        let mgs_client = gateway_client::Client::new(
            &mgs_url,
            log.new(o!("mgs_url" => mgs_url.clone())),
        );
        let inventory = Inventory::load(&log, mgs_client)
            .await
            .context("loading inventory")?;
        info!(&log, "loaded inventory from MGS");

        let mut repo_depot_resolver =
            FixedResolver::new([self.repo_depot_addr]);
        let artifact_cache = Arc::new(ArtifactCache::new(
            log.clone(),
            repo_depot_resolver.monitor(),
        ));

        let (requests_tx, requests_rx) =
            watch::channel(PendingMgsUpdates::new());

        let driver = MgsUpdateDriver::new(
            log.clone(),
            artifact_cache,
            requests_rx,
            mgs_rx,
            Duration::from_secs(20),
        );
        let status_rx = driver.status_rx();
        let driver_task = tokio::spawn(async move { driver.run().await });

        let mut updater_state =
            UpdaterState { requests_tx, status_rx, inventory };

        run_repl_on_stdin(&mut |cmd: TopLevelArgs| {
            process_cmd(&mut updater_state, cmd)
        })?;

        info!(&log, "waiting for qorb to shut down");
        mgs_resolver.terminate().await;
        repo_depot_resolver.terminate().await;
        info!(&log, "waiting for driver task to stop");
        drop(updater_state);
        driver_task.await.context("waiting for driver task")?;

        Ok(())
    }
}

struct UpdaterState {
    requests_tx: watch::Sender<PendingMgsUpdates>,
    status_rx: watch::Receiver<MgsUpdateDriverStatus>,
    inventory: Inventory,
}

struct Inventory {
    sps_by_serial: BTreeMap<String, SpInfo>,
}

impl Inventory {
    fn info_for_serial(&self, serial: &str) -> anyhow::Result<&SpInfo> {
        self.sps_by_serial.get(serial).ok_or_else(|| {
            anyhow!("did not find serial number in inventory: {:?}", serial)
        })
    }
}

struct SpInfo {
    baseboard_id: Arc<BaseboardId>,
    sp_type: SpType,
    sp_slot_id: u32,
}

impl Inventory {
    pub async fn load(
        log: &slog::Logger,
        mgs_client: gateway_client::Client,
    ) -> anyhow::Result<Inventory> {
        let sp_list_ignition = mgs_client
            .ignition_list()
            .await
            .context("listing ignition")?
            .into_inner();

        let c = &mgs_client;
        let sp_infos = futures::stream::iter(
            sp_list_ignition.iter().filter_map(|ignition| {
                if matches!(ignition.details, SpIgnition::Yes { .. }) {
                    Some(ignition.id)
                } else {
                    None
                }
            }),
        )
        .then(async move |sp_id| {
            c.sp_get(sp_id.type_, sp_id.slot)
                .await
                .with_context(|| format!("fetching info about SP {:?}", sp_id))
                .map(|s| (sp_id, s))
        })
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok((sp_id, v)) => Some((sp_id, v.into_inner())),
            Err(error) => {
                warn!(
                    log,
                    "error getting SP state";
                    "error" => #?error,
                );
                None
            }
        })
        .collect::<Vec<_>>();

        let sps_by_serial = sp_infos
            .into_iter()
            .map(|(sp_id, sp_state)| {
                let baseboard_id = Arc::new(BaseboardId {
                    serial_number: sp_state.serial_number,
                    part_number: sp_state.model,
                });
                let serial_number = baseboard_id.serial_number.clone();
                let sp_info = SpInfo {
                    baseboard_id,
                    sp_type: sp_id.type_,
                    sp_slot_id: sp_id.slot,
                };
                (serial_number, sp_info)
            })
            .collect();

        Ok(Inventory { sps_by_serial })
    }
}

/// Processes one "line" of user input.
fn process_cmd(
    updater_state: &mut UpdaterState,
    cmd: TopLevelArgs,
) -> anyhow::Result<Option<String>> {
    let TopLevelArgs { command } = cmd;
    match command {
        Commands::Config => cmd_config(updater_state),
        Commands::Status => cmd_status(updater_state),
        Commands::Set(args) => cmd_set(updater_state, args),
        Commands::Delete(args) => cmd_delete(updater_state, args),
    }
}

// clap configuration for the REPL commands

/// reconfigurator-sp-updater: interactively manage SP updates
#[derive(Debug, Parser)]
struct TopLevelArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Show configured updates
    Config,
    /// Show status of recent and in-progress updates
    Status,
    /// Configure an update
    Set(SetArgs),
    /// Delete a configured update
    Delete(DeleteArgs),
}

fn cmd_config(
    updater_state: &mut UpdaterState,
) -> anyhow::Result<Option<String>> {
    let configured = updater_state.requests_tx.borrow();

    let mut s = String::new();
    writeln!(&mut s, "configured updates ({}):", configured.len())?;
    for update in &*configured {
        let baseboard_id = &update.baseboard_id;
        writeln!(
            &mut s,
            "    part {} serial {} (type {:?} slot {}):",
            baseboard_id.part_number,
            baseboard_id.serial_number,
            update.sp_type,
            update.slot_id,
        )?;
        writeln!(&mut s, "        artifact hash: {}", update.artifact_hash)?;
        writeln!(
            &mut s,
            "        user-provided artifact version: {}",
            update.artifact_version,
        )?;
        match &update.details {
            PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            } => {
                writeln!(
                    &mut s,
                    "        preconditions: active slot {:?}, inactive slot {:?}",
                    expected_active_version, expected_inactive_version,
                )?;
            }
            PendingMgsUpdateDetails::Rot {
                expected_active_slot,
                expected_inactive_version,
                expected_persistent_boot_preference,
                expected_pending_persistent_boot_preference,
                expected_transient_boot_preference,
            } => {
                writeln!(
                    &mut s,
                    "        preconditions: expected active slot {:?}
                                            expected active version {:?}
                                            expected inactive version {:?}
                                            expected persistent_boot_preference {:?}
                                            expected pending_persistent_boot_preference {:?}
                                            expected transient_boot_preference {:?}",
                    expected_active_slot.slot(), expected_active_slot.version(),
                    expected_inactive_version, expected_persistent_boot_preference,
                    expected_pending_persistent_boot_preference,
                    expected_transient_boot_preference,
                )?;
            }
            PendingMgsUpdateDetails::RotBootloader {
                expected_stage0_version,
                expected_stage0_next_version,
            } => {
                writeln!(
                    &mut s,
                    "        preconditions: stage 0 {:?}, stage 0 next {:?}",
                    expected_stage0_version, expected_stage0_next_version,
                )?;
            }
        }

        writeln!(&mut s)?;
    }

    Ok(Some(s))
}

fn cmd_status(
    updater_state: &mut UpdaterState,
) -> anyhow::Result<Option<String>> {
    let status = updater_state.status_rx.borrow();
    Ok(Some(status.detailed_display().to_string()))
}

#[derive(Debug, Args)]
struct SetArgs {
    /// serial number to update
    serial: String,
    /// artifact hash id
    artifact_hash: ArtifactHash,
    /// version
    version: String,
    /// component to update
    #[command(subcommand)]
    component: Component,
}

#[derive(Clone, Debug, Subcommand)]
enum Component {
    Sp {
        /// expected version of the active slot
        #[arg(long, short = 'a')]
        expected_active_version: ArtifactVersion,
        /// expected version of the inactive slot
        #[arg(long, short = 'i')]
        expected_inactive_version: ExpectedVersion,
    },
    Rot {
        /// whether we expect the "A" or "B" slot to be active
        #[arg(long, short = 's')]
        expected_active_slot: RotSlot,
        /// expected version of the "A" slot
        #[arg(long, short = 'a')]
        expected_slot_a_version: ExpectedVersion,
        /// expected version of the "B" slot
        #[arg(long, short = 'b')]
        expected_slot_b_version: ExpectedVersion,
        /// the expected persistent boot preference written into the current
        /// authoritative CFPA page (ping or pong).
        /// Will default to the value of expected_active_version when not set
        #[arg(long, short = 'p')]
        expected_persistent_boot_preference: Option<RotSlot>,
        /// the expected persistent boot preference written into the CFPA scratch
        /// page that will become the persistent boot preference in the authoritative
        /// CFPA page upon reboot, unless CFPA update of the authoritative page fails
        /// for some reason
        #[arg(long, short = 'x')]
        expected_pending_persistent_boot_preference: Option<RotSlot>,
        // this field is not in use yet.
        //
        /// override persistent preference selection for a single boot
        #[arg(long, short = 't')]
        expected_transient_boot_preference: Option<RotSlot>,
    },
    RotBootloader {
        /// expected version of stage0 (active slot)
        #[arg(long, short = 'a')]
        expected_stage0_version: ArtifactVersion,
        /// expected version of stage0 next (inactive slot)
        #[arg(long, short = 'i')]
        expected_stage0_next_version: ExpectedVersion,
    },
}

fn cmd_set(
    updater_state: &mut UpdaterState,
    args: SetArgs,
) -> anyhow::Result<Option<String>> {
    let serial = &args.serial;
    let info = updater_state.inventory.info_for_serial(serial)?;
    let request = PendingMgsUpdate {
        baseboard_id: info.baseboard_id.clone(),
        sp_type: info.sp_type,
        slot_id: info.sp_slot_id,
        details: match args.component {
            Component::Sp {
                expected_active_version,
                expected_inactive_version,
            } => PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            },
            Component::Rot {
                expected_active_slot,
                expected_slot_a_version,
                expected_slot_b_version,
                expected_persistent_boot_preference,
                expected_pending_persistent_boot_preference,
                expected_transient_boot_preference,
            } => {
                let (active_version, expected_inactive_version) =
                    match expected_active_slot {
                        RotSlot::A => {
                            (expected_slot_a_version, expected_slot_b_version)
                        }
                        RotSlot::B => {
                            (expected_slot_b_version, expected_slot_a_version)
                        }
                    };

                let expected_active_version = match active_version {
                    ExpectedVersion::Version(v) => v,
                    ExpectedVersion::NoValidVersion => {
                        return Err(anyhow!(
                            "the expected active slot version must have a valid version"
                        ));
                    }
                };

                PendingMgsUpdateDetails::Rot {
                    expected_active_slot: ExpectedActiveRotSlot {
                        slot: expected_active_slot,
                        version: expected_active_version,
                    },
                    expected_inactive_version,
                    expected_persistent_boot_preference:
                        expected_persistent_boot_preference
                            .unwrap_or(expected_active_slot),
                    expected_pending_persistent_boot_preference,
                    expected_transient_boot_preference,
                }
            }
            Component::RotBootloader {
                expected_stage0_version,
                expected_stage0_next_version,
            } => PendingMgsUpdateDetails::RotBootloader {
                expected_stage0_version,
                expected_stage0_next_version,
            },
        },
        artifact_hash: args.artifact_hash,
        artifact_version: ArtifactVersion::new(args.version)
            .context("parsing artifact version")?,
    };

    updater_state.requests_tx.send_modify(|requests| {
        requests.insert(request);
    });

    Ok(Some(format!("updated configuration for {serial}")))
}

#[derive(Debug, Args)]
struct DeleteArgs {
    /// serial number of SP with update to delete
    serial: String,
}

fn cmd_delete(
    updater_state: &mut UpdaterState,
    args: DeleteArgs,
) -> anyhow::Result<Option<String>> {
    let serial = &args.serial;
    let baseboard_id =
        &updater_state.inventory.info_for_serial(serial)?.baseboard_id;
    let changed = updater_state
        .requests_tx
        .send_if_modified(|requests| requests.remove(&baseboard_id).is_some());
    if changed {
        Ok(Some(format!("deleted configured update for serial {serial}")))
    } else {
        Err(anyhow!("no update was configured for serial {serial}"))
    }
}
