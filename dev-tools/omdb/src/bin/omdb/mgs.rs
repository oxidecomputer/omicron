// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Prototype code for collecting information from systems in the rack

use crate::Omdb;
use crate::db::option_impl_display;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use futures::StreamExt;
use gateway_client::types::PowerState;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpComponentInfo;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use gateway_client::types::SpIgnitionInfo;
use gateway_client::types::SpIgnitionSystemType;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
use internal_dns_types::names::ServiceName;
use tabled::Tabled;

mod dashboard;
mod sensors;

use dashboard::DashboardArgs;
use sensors::SensorsArgs;

/// Arguments to the "omdb mgs" subcommand
#[derive(Debug, Args)]
pub struct MgsArgs {
    /// URL of an MGS instance to query
    #[clap(
        long,
        env = "OMDB_MGS_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    mgs_url: Option<String>,

    #[command(subcommand)]
    command: MgsCommands,
}

#[derive(Debug, Subcommand)]
enum MgsCommands {
    /// Dashboard of SPs
    Dashboard(DashboardArgs),

    /// Show information about devices and components visible to MGS
    Inventory(InventoryArgs),

    /// Show information about sensors, as gleaned by MGS
    Sensors(SensorsArgs),
}

#[derive(Debug, Args)]
struct InventoryArgs {}

impl MgsArgs {
    async fn mgs_client(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<gateway_client::Client, anyhow::Error> {
        let mgs_url = match &self.mgs_url {
            Some(cli_or_env_url) => cli_or_env_url.clone(),
            None => {
                eprintln!(
                    "note: MGS URL not specified.  Will pick one from DNS."
                );
                let addr = omdb
                    .dns_lookup_one(
                        log.clone(),
                        ServiceName::ManagementGatewayService,
                    )
                    .await?;
                format!("http://{}", addr)
            }
        };
        eprintln!("note: using MGS URL {}", &mgs_url);
        Ok(gateway_client::Client::new(&mgs_url, log.clone()))
    }

    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        match &self.command {
            MgsCommands::Dashboard(args) => {
                dashboard::cmd_mgs_dashboard(omdb, log, self, args).await
            }
            MgsCommands::Inventory(args) => {
                let mgs_client = self.mgs_client(omdb, log).await?;
                cmd_mgs_inventory(&mgs_client, args).await
            }
            MgsCommands::Sensors(args) => {
                sensors::cmd_mgs_sensors(omdb, log, self, args).await
            }
        }
    }
}

/// Runs `omdb mgs inventory`
///
/// Shows devices and components that are visible to an MGS instance.
async fn cmd_mgs_inventory(
    mgs_client: &gateway_client::Client,
    _args: &InventoryArgs,
) -> Result<(), anyhow::Error> {
    // Report all the SP identifiers that MGS is configured to talk to.
    println!("ALL CONFIGURED SPs\n");
    let mut sp_ids = mgs_client
        .sp_all_ids()
        .await
        .context("listing SP identifiers")?
        .into_inner();
    sp_ids.sort();
    show_sp_ids(&sp_ids)?;
    println!("");

    // Report which SPs are visible via Ignition.
    println!("SPs FOUND THROUGH IGNITION\n");
    let mut sp_list_ignition = mgs_client
        .ignition_list()
        .await
        .context("listing ignition")?
        .into_inner();
    sp_list_ignition.sort_by(|a, b| a.id.cmp(&b.id));
    show_sps_from_ignition(&sp_list_ignition)?;
    println!("");

    // Print basic state about each SP that's visible to ignition.
    println!("SERVICE PROCESSOR STATES\n");
    let mgs_client = std::sync::Arc::new(mgs_client);
    let c = &mgs_client;
    let mut sp_infos =
        futures::stream::iter(sp_list_ignition.iter().filter_map(|ignition| {
            if matches!(ignition.details, SpIgnition::Yes { .. }) {
                Some(ignition.id)
            } else {
                None
            }
        }))
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
                eprintln!("error: {:?}", error);
                None
            }
        })
        .collect::<Vec<_>>();
    sp_infos.sort();
    show_sp_states(&sp_infos)?;
    println!("");

    // Print detailed information about each SP that we've found so far.
    for (sp_id, sp_state) in &sp_infos {
        show_sp_details(&mgs_client, sp_id, sp_state).await?;
    }

    Ok(())
}

fn sp_type_to_str(s: &SpType) -> &'static str {
    match s {
        SpType::Sled => "Sled",
        SpType::Power => "Power",
        SpType::Switch => "Switch",
    }
}

fn sp_to_string(s: &SpIdentifier) -> String {
    format!("{} {}", sp_type_to_str(&s.type_), s.slot)
}

fn show_sp_ids(sp_ids: &[SpIdentifier]) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpIdRow {
        #[tabled(rename = "TYPE")]
        type_: &'static str,
        slot: u32,
    }

    impl From<&SpIdentifier> for SpIdRow {
        fn from(id: &SpIdentifier) -> Self {
            SpIdRow { type_: sp_type_to_str(&id.type_), slot: id.slot }
        }
    }

    let table_rows = sp_ids.iter().map(SpIdRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", textwrap::indent(&table.to_string(), "    "));
    Ok(())
}

fn show_sps_from_ignition(
    sp_list_ignition: &[SpIgnitionInfo],
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct IgnitionRow {
        #[tabled(rename = "TYPE")]
        type_: &'static str,
        slot: u32,
        system_type: String,
    }

    impl From<&SpIgnitionInfo> for IgnitionRow {
        fn from(value: &SpIgnitionInfo) -> Self {
            IgnitionRow {
                type_: sp_type_to_str(&value.id.type_),
                slot: value.id.slot,
                system_type: match value.details {
                    SpIgnition::No => "-".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Gimlet,
                        ..
                    } => "Gimlet".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Sidecar,
                        ..
                    } => "Sidecar".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Psc, ..
                    } => "PSC".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Unknown(v),
                        ..
                    } => format!("unknown: type {}", v),
                },
            }
        }
    }

    let table_rows = sp_list_ignition.iter().map(IgnitionRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", textwrap::indent(&table.to_string(), "    "));
    Ok(())
}

fn show_sp_states(
    sp_states: &[(SpIdentifier, SpState)],
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpStateRow<'a> {
        #[tabled(rename = "TYPE")]
        type_: &'static str,
        slot: u32,
        model: String,
        serial: String,
        rev: u32,
        hubris: &'a str,
        pwr: &'static str,
        rot_active: String,
    }

    impl<'a> From<&'a (SpIdentifier, SpState)> for SpStateRow<'a> {
        fn from((id, v): &'a (SpIdentifier, SpState)) -> Self {
            SpStateRow {
                type_: sp_type_to_str(&id.type_),
                slot: id.slot,
                model: v.model.clone(),
                serial: v.serial_number.clone(),
                rev: v.revision,
                hubris: &v.hubris_archive_id,
                pwr: match v.power_state {
                    PowerState::A0 => "A0",
                    PowerState::A1 => "A1",
                    PowerState::A2 => "A2",
                },
                rot_active: match &v.rot {
                    RotState::CommunicationFailed { message } => {
                        format!("error: {}", message)
                    }
                    RotState::V2 { active: RotSlot::A, .. } => {
                        "slot A".to_string()
                    }
                    RotState::V2 { active: RotSlot::B, .. } => {
                        "slot B".to_string()
                    }
                    RotState::V3 { active: RotSlot::A, .. } => {
                        "slot A".to_string()
                    }
                    RotState::V3 { active: RotSlot::B, .. } => {
                        "slot B".to_string()
                    }
                },
            }
        }
    }

    let table_rows = sp_states.iter().map(SpStateRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", textwrap::indent(&table.to_string(), "    "));
    Ok(())
}

const COMPONENTS_WITH_CABOOSES: &'static [&'static str] = &["sp", "rot"];

async fn show_sp_details(
    mgs_client: &gateway_client::Client,
    sp_id: &SpIdentifier,
    sp_state: &SpState,
) -> Result<(), anyhow::Error> {
    println!(
        "SP DETAILS: type {:?} slot {}\n",
        sp_type_to_str(&sp_id.type_),
        sp_id.slot
    );

    println!("    ROOT OF TRUST\n");
    match &sp_state.rot {
        RotState::CommunicationFailed { message } => {
            println!("        error: {}", message);
        }
        RotState::V2 {
            active,
            pending_persistent_boot_preference,
            persistent_boot_preference,
            slot_a_sha3_256_digest,
            slot_b_sha3_256_digest,
            transient_boot_preference,
        } => {
            #[derive(Tabled)]
            #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
            struct Row {
                name: &'static str,
                value: String,
            }

            let rows = vec![
                Row {
                    name: "active slot",
                    value: format!("slot {:?}", active),
                },
                Row {
                    name: "persistent boot preference",
                    value: format!("slot {:?}", persistent_boot_preference),
                },
                Row {
                    name: "pending persistent boot preference",
                    value: pending_persistent_boot_preference
                        .map(|s| format!("slot {:?}", s))
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "transient boot preference",
                    value: transient_boot_preference
                        .map(|s| format!("slot {:?}", s))
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "slot A SHA3 256 digest",
                    value: slot_a_sha3_256_digest
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "slot B SHA3 256 digest",
                    value: slot_b_sha3_256_digest
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                },
            ];

            let table = tabled::Table::new(rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();
            println!("{}", textwrap::indent(&table.to_string(), "        "));
            println!("");
        }
        RotState::V3 {
            active,
            pending_persistent_boot_preference,
            persistent_boot_preference,
            slot_a_fwid,
            slot_b_fwid,
            transient_boot_preference,
            stage0_fwid,
            stage0next_fwid,
            slot_a_error,
            slot_b_error,
            stage0_error,
            stage0next_error,
        } => {
            #[derive(Tabled)]
            #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
            struct Row {
                name: &'static str,
                value: String,
            }

            let rows = vec![
                Row {
                    name: "active slot",
                    value: format!("slot {:?}", active),
                },
                Row {
                    name: "persistent boot preference",
                    value: format!("slot {:?}", persistent_boot_preference),
                },
                Row {
                    name: "pending persistent boot preference",
                    value: pending_persistent_boot_preference
                        .map(|s| format!("slot {:?}", s))
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "transient boot preference",
                    value: transient_boot_preference
                        .map(|s| format!("slot {:?}", s))
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row { name: "slot A FWID", value: slot_a_fwid.clone() },
                Row { name: "slot B FWID", value: slot_b_fwid.clone() },
                Row { name: "Stage0 FWID", value: stage0_fwid.clone() },
                Row { name: "Stage0Next FWID", value: stage0next_fwid.clone() },
                Row {
                    name: "Slot A status",
                    value: (*slot_a_error)
                        .map(|x| format!("error: {:?}", x))
                        .unwrap_or_else(|| "VALID".to_string()),
                },
                Row {
                    name: "Slot B status",
                    value: (*slot_b_error)
                        .map(|x| format!("error: {:?}", x))
                        .unwrap_or_else(|| "VALID".to_string()),
                },
                Row {
                    name: "Stage0 status",
                    value: (*stage0_error)
                        .map(|x| format!("error: {:?}", x))
                        .unwrap_or_else(|| "VALID".to_string()),
                },
                Row {
                    name: "stage0next status",
                    value: (*stage0next_error)
                        .map(|x| format!("error: {:?}", x))
                        .unwrap_or_else(|| "VALID".to_string()),
                },
            ];

            let table = tabled::Table::new(rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();
            println!("{}", textwrap::indent(&table.to_string(), "        "));
            println!("");
        }
    }

    let component_list = mgs_client
        .sp_component_list(sp_id.type_, sp_id.slot)
        .await
        .with_context(|| format!("fetching components for SP {:?}", sp_id));
    let list = match component_list {
        Ok(l) => l.into_inner(),
        Err(e) => {
            eprintln!("error: {:#}", e);
            return Ok(());
        }
    };

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpComponentRow<'a> {
        name: &'a str,
        description: &'a str,
        device: &'a str,
        presence: String,
        serial: String,
    }

    impl<'a> From<&'a SpComponentInfo> for SpComponentRow<'a> {
        fn from(v: &'a SpComponentInfo) -> Self {
            SpComponentRow {
                name: &v.component,
                description: &v.description,
                device: &v.device,
                presence: format!("{:?}", v.presence),
                serial: format!("{:?}", v.serial_number),
            }
        }
    }

    if list.components.is_empty() {
        println!("    COMPONENTS: none found\n");
        return Ok(());
    }

    let table_rows = list.components.iter().map(SpComponentRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("    COMPONENTS\n");
    println!("{}", textwrap::indent(&table.to_string(), "        "));
    println!("");

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct CabooseRow {
        component: String,
        board: String,
        git_commit: String,
        name: String,
        version: String,
        #[tabled(display_with = "option_impl_display")]
        sign: Option<String>,
    }

    impl<'a> From<(&'a SpIdentifier, &'a SpComponentInfo, SpComponentCaboose)>
        for CabooseRow
    {
        fn from(
            (_sp_id, component, caboose): (
                &'a SpIdentifier,
                &'a SpComponentInfo,
                SpComponentCaboose,
            ),
        ) -> Self {
            CabooseRow {
                component: component.component.clone(),
                board: caboose.board,
                git_commit: caboose.git_commit,
                name: caboose.name,
                version: caboose.version,
                sign: caboose.sign,
            }
        }
    }

    let mut cabooses = Vec::new();
    for c in &list.components {
        if !COMPONENTS_WITH_CABOOSES.contains(&c.component.as_str()) {
            continue;
        }

        for i in 0..1 {
            let r = mgs_client
                .sp_component_caboose_get(
                    sp_id.type_,
                    sp_id.slot,
                    &c.component,
                    i,
                )
                .await
                .with_context(|| {
                    format!(
                        "get caboose for sp type {:?} sp slot {} \
                        component {:?} slot {}",
                        sp_id.type_, sp_id.slot, &c.component, i
                    )
                });
            match r {
                Ok(v) => {
                    cabooses.push(CabooseRow::from((sp_id, c, v.into_inner())))
                }
                Err(error) => {
                    eprintln!("warn: {:#}", error);
                }
            }
        }
    }

    if cabooses.is_empty() {
        println!("    CABOOSES: none found\n");
        return Ok(());
    }

    let table = tabled::Table::new(cabooses)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("    COMPONENT CABOOSES\n");
    println!("{}", textwrap::indent(&table.to_string(), "        "));
    println!("");

    Ok(())
}
