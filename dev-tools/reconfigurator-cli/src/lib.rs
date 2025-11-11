// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! developer REPL for driving blueprint planning

use anyhow::{Context, anyhow, bail, ensure};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clap::{ArgAction, ValueEnum};
use clap::{Args, Parser, Subcommand};
use daft::Diffable;
use gateway_types::rot::RotSlot;
use iddqd::IdOrdMap;
use indent_write::fmt::IndentWriter;
use internal_dns_types::diff::DnsDiff;
use itertools::Itertools;
pub use log_capture::LogCapture;
use nexus_inventory::CollectionBuilder;
use nexus_reconfigurator_blippy::Blippy;
use nexus_reconfigurator_blippy::BlippyReportSortKey;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::blueprint_editor::ExternalNetworkingAllocator;
use nexus_reconfigurator_planning::example::{
    ExampleSystemBuilder, extract_tuf_repo_description, tuf_assemble,
};
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::system::{
    RotStateOverrides, SledBuilder, SledInventoryVisibility, SystemDescription,
};
use nexus_reconfigurator_simulation::{BlueprintId, CollectionId, SimState};
use nexus_reconfigurator_simulation::{SimStateBuilder, SimTufRepoSource};
use nexus_reconfigurator_simulation::{SimTufRepoDescription, Simulator};
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::execution;
use nexus_types::deployment::execution::blueprint_external_dns_config;
use nexus_types::deployment::execution::blueprint_internal_dns_config;
use nexus_types::deployment::{Blueprint, UnstableReconfiguratorState};
use nexus_types::deployment::{BlueprintArtifactVersion, PendingMgsUpdate};
use nexus_types::deployment::{
    BlueprintHostPhase2DesiredContents, PlannerConfig,
};
use nexus_types::deployment::{BlueprintSource, SledFilter};
use nexus_types::deployment::{BlueprintZoneDisposition, ExpectedVersion};
use nexus_types::deployment::{
    BlueprintZoneImageSource, PendingMgsUpdateDetails,
};
use nexus_types::deployment::{OmicronZoneNic, TargetReleaseDescription};
use nexus_types::deployment::{PendingMgsUpdateSpDetails, PlanningInput};
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use nexus_types::inventory::CollectionDisplayCliFilter;
use omicron_common::address::REPO_DEPOT_PORT;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Name;
use omicron_common::disk::M2Slot;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_common::update::OmicronZoneManifestSource;
use omicron_repl_utils::run_repl_from_file;
use omicron_repl_utils::run_repl_on_stdin;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::ReconfiguratorSimUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::VnicUuid;
use omicron_uuid_kinds::{BlueprintUuid, MupdateOverrideUuid};
use omicron_uuid_kinds::{CollectionUuid, MupdateUuid};
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::{self, Write};
use std::io::IsTerminal;
use std::net::IpAddr;
use std::num::ParseIntError;
use std::str::FromStr;
use swrite::{SWrite, swrite, swriteln};
use tabled::Tabled;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::ArtifactVersionError;
use tufaceous_lib::assemble::ArtifactManifest;

mod log_capture;

/// REPL state
#[derive(Debug)]
struct ReconfiguratorSim {
    // The simulator currently being used.
    sim: Simulator,
    // The current state.
    current: ReconfiguratorSimUuid,
    // The current system state
    log: slog::Logger,
}

impl ReconfiguratorSim {
    fn new(log: slog::Logger, seed: Option<String>) -> Self {
        Self {
            sim: Simulator::new(&log, seed),
            current: Simulator::ROOT_ID,
            log,
        }
    }

    fn current_state(&self) -> &SimState {
        self.sim
            .get_state(self.current)
            .expect("current state should always exist")
    }

    fn commit_and_bump(&mut self, description: String, state: SimStateBuilder) {
        let new_id = state.commit(description, &mut self.sim);
        self.current = new_id;
    }

    fn planning_input(
        &self,
        parent_blueprint: &Blueprint,
    ) -> anyhow::Result<PlanningInput> {
        let state = self.current_state();
        let mut builder = state
            .system()
            .description()
            .to_planning_input_builder()
            .context("generating planning input builder")?;

        // The internal and external DNS numbers that go here are supposed to be
        // the _current_ internal and external DNS generations at the point
        // when planning happened.  This is racy (these generations can change
        // immediately after they're fetched from the database) but correctness
        // only requires that the values here be *no newer* than the real
        // values so it's okay if the real values get changed.
        //
        // The problem is we have no real system here to fetch these values
        // from.  What should the value be?
        //
        // - If we assume that the parent blueprint here was successfully
        //   executed immediately before generating this plan, then the values
        //   here should come from the generation number produced by executing
        //   the parent blueprint.
        //
        // - If the parent blueprint was never executed, or execution is still
        //   in progress, or if other blueprints have been executed in the
        //   meantime that changed DNS, then the values here could be different
        //   (older if the blueprint was never executed or is currently
        //   executing and newer if other blueprints have changed DNS in the
        //   meantime).
        //
        // But in this CLI, there's no execution at all.  As a result, there's
        // no way to really choose between these -- and it doesn't really
        // matter, either.  We'll just pick the parent blueprint's.
        builder.set_internal_dns_version(parent_blueprint.internal_dns_version);
        builder.set_external_dns_version(parent_blueprint.external_dns_version);

        // Handle zone networking setup first
        for (_, zone) in parent_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        {
            if let Some((external_ip, nic)) =
                zone.zone_type.external_networking()
            {
                builder
                    .add_omicron_zone_external_ip(zone.id, external_ip)
                    .context("adding omicron zone external IP")?;

                // TODO-completeness: This needs to support dual-stack zones.
                // See https://github.com/oxidecomputer/omicron/issues/9288 and
                // related issues.
                let maybe_ip = if matches!(external_ip.ip(), IpAddr::V4(_)) {
                    nic.ip_config.ipv4_addr().copied().map(IpAddr::V4)
                } else {
                    nic.ip_config.ipv6_addr().copied().map(IpAddr::V6)
                };
                let ip = maybe_ip.with_context(|| {
                    format!(
                        "Omicron zone has an external and private IP \
                        configurations of different IP versions. \
                        zone_id={} zone_kind={} \
                        external_ip={} private_ip_config={:?}",
                        zone.id,
                        zone.zone_type.kind().report_str(),
                        external_ip.ip(),
                        nic.ip_config,
                    )
                })?;
                let nic = OmicronZoneNic {
                    // TODO-cleanup use `TypedUuid` everywhere
                    id: VnicUuid::from_untyped_uuid(nic.id),
                    mac: nic.mac,
                    ip,
                    slot: nic.slot,
                    primary: nic.primary,
                };
                builder
                    .add_omicron_zone_nic(zone.id, nic)
                    .context("adding omicron zone NIC")?;
            }
        }

        // Determine active and not-yet nexus zones (use explicit overrides if available)
        let active_nexus_zones = if let Some(explicit) =
            state.config().explicit_active_nexus_zones()
        {
            explicit.clone()
        } else {
            // Infer from generation
            let active_nexus_gen =
                state.config().active_nexus_zone_generation();
            let mut active_nexus_zones = BTreeSet::new();
            for (_, zone, nexus) in parent_blueprint
                .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
            {
                if nexus.nexus_generation == active_nexus_gen {
                    active_nexus_zones.insert(zone.id);
                }
            }
            active_nexus_zones
        };

        let not_yet_nexus_zones = if let Some(explicit) =
            state.config().explicit_not_yet_nexus_zones()
        {
            explicit.clone()
        } else {
            // Infer from generation
            let active_nexus_gen =
                state.config().active_nexus_zone_generation();
            let mut not_yet_nexus_zones = BTreeSet::new();
            for (_, zone) in parent_blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            {
                match &zone.zone_type {
                    nexus_types::deployment::BlueprintZoneType::Nexus(
                        nexus,
                    ) => {
                        if nexus.nexus_generation > active_nexus_gen {
                            not_yet_nexus_zones.insert(zone.id);
                        }
                    }
                    _ => (),
                }
            }
            not_yet_nexus_zones
        };

        if active_nexus_zones.is_empty() {
            let active_nexus_gen =
                state.config().active_nexus_zone_generation();
            bail!(
                "no Nexus zones found at current active generation \
                 ({active_nexus_gen})"
            );
        }

        builder.set_active_nexus_zones(active_nexus_zones);
        builder.set_not_yet_nexus_zones(not_yet_nexus_zones);

        Ok(builder.build())
    }
}

/// interactive REPL for exploring the planner
#[derive(Parser, Debug)]
pub struct CmdReconfiguratorSim {
    input_file: Option<Utf8PathBuf>,

    /// The RNG seed to initialize the simulator with.
    #[clap(long)]
    seed: Option<String>,
}

impl CmdReconfiguratorSim {
    /// Execute the command.
    pub fn exec(self) -> anyhow::Result<()> {
        let (log_capture, log) =
            LogCapture::new(std::io::stdout().is_terminal());

        let seed_provided = self.seed.is_some();
        let mut sim = ReconfiguratorSim::new(log, self.seed);
        if seed_provided {
            println!("using provided RNG seed: {}", sim.sim.initial_seed());
        } else {
            println!("generated RNG seed: {}", sim.sim.initial_seed());
        }

        if let Some(input_file) = &self.input_file {
            run_repl_from_file(input_file, &mut |cmd: TopLevelArgs| {
                process_command(&mut sim, cmd, &log_capture)
            })
        } else {
            run_repl_on_stdin(&mut |cmd: TopLevelArgs| {
                process_command(&mut sim, cmd, &log_capture)
            })
        }
    }
}

/// Processes one "line" of user input.
fn process_command(
    sim: &mut ReconfiguratorSim,
    cmd: TopLevelArgs,
    logs: &LogCapture,
) -> anyhow::Result<Option<String>> {
    let TopLevelArgs { command } = cmd;
    let cmd_result = match command {
        Commands::SledList => cmd_sled_list(sim),
        Commands::SledAdd(args) => cmd_sled_add(sim, args),
        Commands::SledRemove(args) => cmd_sled_remove(sim, args),
        Commands::SledShow(args) => cmd_sled_show(sim, args),
        Commands::SledSet(args) => cmd_sled_set(sim, args),
        Commands::SledUpdateInstallDataset(args) => {
            cmd_sled_update_install_dataset(sim, args)
        }
        Commands::SledUpdateRot(args) => cmd_sled_update_rot(sim, args),
        Commands::SledUpdateSp(args) => cmd_sled_update_sp(sim, args),
        Commands::SledUpdateHostPhase1(args) => {
            cmd_sled_update_host_phase_1(sim, args)
        }
        Commands::SledUpdateHostPhase2(args) => {
            cmd_sled_update_host_phase_2(sim, args)
        }
        Commands::SledUpdateRotBootloader(args) => {
            cmd_sled_update_rot_bootloader(sim, args)
        }
        Commands::SiloList => cmd_silo_list(sim),
        Commands::SiloAdd(args) => cmd_silo_add(sim, args),
        Commands::SiloRemove(args) => cmd_silo_remove(sim, args),
        Commands::InventoryList => cmd_inventory_list(sim),
        Commands::InventoryGenerate => cmd_inventory_generate(sim),
        Commands::InventoryShow(args) => cmd_inventory_show(sim, args),
        Commands::BlueprintList => cmd_blueprint_list(sim),
        Commands::BlueprintBlippy(args) => cmd_blueprint_blippy(sim, args),
        Commands::BlueprintEdit(args) => cmd_blueprint_edit(sim, args),
        Commands::BlueprintPlan(args) => cmd_blueprint_plan(sim, args),
        Commands::BlueprintShow(args) => cmd_blueprint_show(sim, args),
        Commands::BlueprintDiff(args) => cmd_blueprint_diff(sim, args),
        Commands::BlueprintDiffDns(args) => cmd_blueprint_diff_dns(sim, args),
        Commands::BlueprintHistory(args) => cmd_blueprint_history(sim, args),
        Commands::BlueprintSave(args) => cmd_blueprint_save(sim, args),
        Commands::Show => cmd_show(sim),
        Commands::Set(args) => cmd_set(sim, args),
        Commands::TufAssemble(args) => cmd_tuf_assemble(sim, args),
        Commands::Load(args) => cmd_load(sim, args),
        Commands::LoadExample(args) => cmd_load_example(sim, args),
        Commands::FileContents(args) => cmd_file_contents(args),
        Commands::Save(args) => cmd_save(sim, args),
        Commands::Wipe(args) => cmd_wipe(sim, args),
    };

    for line in logs.take_log_lines() {
        println!("{line}");
    }

    cmd_result
}

// clap configuration for the REPL commands

/// reconfigurator-sim: simulate blueprint planning and execution
#[derive(Debug, Parser)]
struct TopLevelArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// list sleds
    SledList,
    /// add a new sled
    SledAdd(SledAddArgs),
    /// remove a sled from the system
    SledRemove(SledRemoveArgs),
    /// show details about one sled
    SledShow(SledArgs),
    /// set a value on a sled
    SledSet(SledSetArgs),
    /// update the install dataset on a sled, simulating a mupdate
    SledUpdateInstallDataset(SledUpdateInstallDatasetArgs),
    /// simulate updating the sled's RoT versions
    SledUpdateRot(SledUpdateRotArgs),
    /// simulate updating the sled's RoT Bootloader versions
    SledUpdateRotBootloader(SledUpdateRotBootloaderArgs),
    /// simulate updating the sled's SP versions
    SledUpdateSp(SledUpdateSpArgs),
    /// simulate updating the sled's host OS phase 1 artifacts
    SledUpdateHostPhase1(SledUpdateHostPhase1Args),
    /// simulate updating the sled's host OS phase 2 artifacts
    SledUpdateHostPhase2(SledUpdateHostPhase2Args),

    /// list silos
    SiloList,
    /// add a silo
    SiloAdd(SiloAddRemoveArgs),
    /// remove a silo
    SiloRemove(SiloAddRemoveArgs),

    /// list all inventory collections
    InventoryList,
    /// generates an inventory collection from the configured sleds
    InventoryGenerate,
    /// show details about an inventory collection
    InventoryShow(InventoryShowArgs),

    /// list all blueprints
    BlueprintList,
    /// run blippy on a blueprint
    BlueprintBlippy(BlueprintArgs),
    /// run planner to generate a new blueprint
    BlueprintPlan(BlueprintPlanArgs),
    /// edit contents of a blueprint directly
    BlueprintEdit(BlueprintEditArgs),
    /// show details about a blueprint
    BlueprintShow(BlueprintArgs),
    /// show differences between two blueprints
    BlueprintDiff(BlueprintDiffArgs),
    /// show differences between a blueprint and a particular DNS version
    BlueprintDiffDns(BlueprintDiffDnsArgs),
    /// print a summary of the history of blueprints
    ///
    /// This is similar to `omdb reconfigurator history` in a live system, but
    /// it walks blueprints directly via their parent blueprint id rather than
    /// walking the `bp_target` table.
    BlueprintHistory(BlueprintHistoryArgs),
    /// write one blueprint to a file
    BlueprintSave(BlueprintSaveArgs),

    /// show system properties
    Show,
    /// set system properties
    #[command(subcommand)]
    Set(SetArgs),

    /// use tufaceous to generate a repo from a manifest
    TufAssemble(TufAssembleArgs),

    /// save state to a file
    Save(SaveArgs),
    /// load state from a file
    Load(LoadArgs),
    /// generate and load an example system
    LoadExample(LoadExampleArgs),
    /// show information about what's in a saved file
    FileContents(FileContentsArgs),
    /// reset the state of the REPL
    Wipe(WipeArgs),
}

#[derive(Debug, Args)]
struct SledAddArgs {
    /// id of the new sled
    sled_id: Option<SledUuid>,

    /// number of disks or pools
    #[clap(short = 'd', long, visible_alias = "npools", default_value_t = SledBuilder::DEFAULT_NPOOLS)]
    ndisks: u8,

    /// The policy for the sled.
    #[clap(long, value_enum, default_value_t = SledPolicyOpt::InService)]
    policy: SledPolicyOpt,
}

#[derive(Debug, Args)]
struct SledArgs {
    /// id of the sled
    sled_id: SledOpt,

    /// Filter to match sled ID against
    #[clap(short = 'F', long, value_enum, default_value_t = SledFilter::Commissioned)]
    filter: SledFilter,
}

#[derive(Debug, Args)]
struct SledSetArgs {
    /// id of the sled
    sled_id: SledOpt,

    /// the command to set on the sled
    #[clap(subcommand)]
    command: SledSetCommand,
}

#[derive(Debug, Subcommand)]
enum SledSetCommand {
    /// set the policy for this sled
    Policy(SledSetPolicyArgs),
    /// set the Omicron config for this sled from a blueprint
    OmicronConfig(SledSetOmicronConfigArgs),
    #[clap(flatten)]
    Visibility(SledSetVisibilityCommand),
    /// set the mupdate override for this sled
    MupdateOverride(SledSetMupdateOverrideArgs),
}

#[derive(Debug, Args)]
struct SledSetPolicyArgs {
    /// the policy to set
    #[clap(value_enum)]
    policy: SledPolicyOpt,
}

#[derive(Debug, Args)]
struct SledSetOmicronConfigArgs {
    /// the blueprint to derive the Omicron config from
    blueprint: BlueprintIdOpt,
}

#[derive(Debug, Subcommand)]
enum SledSetVisibilityCommand {
    /// mark a sled hidden from inventory
    InventoryHidden,
    /// mark a sled visible in inventory
    InventoryVisible,
}

impl SledSetVisibilityCommand {
    fn to_visibility(&self) -> SledInventoryVisibility {
        match self {
            SledSetVisibilityCommand::InventoryHidden => {
                SledInventoryVisibility::Hidden
            }
            SledSetVisibilityCommand::InventoryVisible => {
                SledInventoryVisibility::Visible
            }
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SledPolicyOpt {
    InService,
    NonProvisionable,
    Expunged,
}

impl fmt::Display for SledPolicyOpt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledPolicyOpt::InService => write!(f, "in-service"),
            SledPolicyOpt::NonProvisionable => write!(f, "non-provisionable"),
            SledPolicyOpt::Expunged => write!(f, "expunged"),
        }
    }
}

impl From<SledPolicyOpt> for SledPolicy {
    fn from(value: SledPolicyOpt) -> Self {
        match value {
            SledPolicyOpt::InService => SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            SledPolicyOpt::NonProvisionable => SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            SledPolicyOpt::Expunged => SledPolicy::Expunged,
        }
    }
}

#[derive(Debug, Args)]
struct SledUpdateInstallDatasetArgs {
    /// id of the sled
    sled_id: SledOpt,

    #[clap(flatten)]
    source: SledMupdateSource,
}

#[derive(Debug, Args)]
// This makes it so that only one source can be specified.
struct SledMupdateSource {
    #[clap(flatten)]
    valid: SledMupdateValidSource,

    /// set the mupdate source to Installinator with the given ID
    #[clap(long, requires = "sled-mupdate-valid-source")]
    mupdate_id: Option<MupdateUuid>,

    /// simulate an error reading the zone manifest
    #[clap(long, conflicts_with = "sled-mupdate-valid-source")]
    with_manifest_error: bool,

    /// simulate an error validating zones by this artifact ID name
    ///
    /// This uses the `artifact_id_name` representation of a zone kind.
    #[clap(
        long,
        value_name = "ARTIFACT_ID_NAME",
        requires = "sled-mupdate-valid-source"
    )]
    with_zone_error: Vec<String>,
}

#[derive(Debug, Args)]
#[group(id = "sled-mupdate-valid-source", multiple = false)]
struct SledMupdateValidSource {
    /// the TUF repo.zip to simulate the mupdate from
    #[clap(long)]
    from_repo: Option<Utf8PathBuf>,

    /// simulate a mupdate to the target release
    #[clap(long)]
    to_target_release: bool,
}

#[derive(Debug, Args)]
struct SledUpdateRotBootloaderArgs {
    /// id of the sled
    sled_id: SledOpt,

    /// sets the version reported for the RoT bootloader stage0 (active) slot
    #[clap(long, required_unless_present_any = &["stage0_next"])]
    stage0: Option<ArtifactVersion>,

    /// sets the version reported for the RoT bootloader stage0_next (inactive)
    /// slot
    #[clap(long, required_unless_present_any = &["stage0"])]
    stage0_next: Option<ExpectedVersion>,
}

#[derive(Debug, Args)]
struct SledUpdateSpArgs {
    /// id of the sled
    sled_id: SledOpt,

    /// sets the version reported for the SP active slot
    #[clap(long, required_unless_present_any = &["inactive"])]
    active: Option<ArtifactVersion>,

    /// sets the version reported for the SP inactive slot
    #[clap(long, required_unless_present_any = &["active"])]
    inactive: Option<ExpectedVersion>,
}

#[derive(Debug, Args)]
struct SledUpdateRotArgs {
    /// id of the sled
    sled_id: SledOpt,

    /// sets the version reported for the RoT slot a
    #[clap(long, required_unless_present_any = &["slot_b"])]
    slot_a: Option<ExpectedVersion>,

    /// sets the version reported for the RoT slot b
    #[clap(long, required_unless_present_any = &["slot_a"])]
    slot_b: Option<ExpectedVersion>,

    /// sets whether we expect the "A" or "B" slot to be active
    #[clap(long)]
    active_slot: Option<RotSlot>,

    /// sets the persistent boot preference written into the current
    /// authoritative CFPA page (ping or pong).
    #[clap(long)]
    persistent_boot_preference: Option<RotSlot>,

    /// sets the pending persistent boot preference written into the CFPA
    /// scratch page that will become the persistent boot preference in the
    /// authoritative CFPA page upon reboot, unless CFPA update of the
    /// authoritative page fails for some reason
    #[clap(long, num_args(0..=1))]
    pending_persistent_boot_preference: Option<Option<RotSlot>>,

    /// sets the transient boot preference, which overrides persistent
    /// preference selection for a single boot (unimplemented)
    #[clap(long, num_args(0..=1),)]
    transient_boot_preference: Option<Option<RotSlot>>,
}

#[derive(Debug, Args)]
struct SledUpdateHostPhase1Args {
    /// id of the sled
    sled_id: SledOpt,

    /// sets which phase 1 slot is active
    #[clap(long, value_parser = parse_m2_slot)]
    active: Option<M2Slot>,

    /// sets the artifact hash reported for host OS phase 1 slot A
    #[clap(long)]
    slot_a: Option<ArtifactHash>,

    /// sets the artifact hash reported for host OS phase 1 slot B
    #[clap(long)]
    slot_b: Option<ArtifactHash>,
}

#[derive(Debug, Args)]
struct SledUpdateHostPhase2Args {
    /// id of the sled
    sled_id: SledOpt,

    /// sets which phase 2 slot is the boot disk
    #[clap(long, value_parser = parse_m2_slot)]
    boot_disk: Option<M2Slot>,

    /// sets the artifact hash reported for host OS phase 2 slot A
    #[clap(long)]
    slot_a: Option<ArtifactHash>,

    /// sets the artifact hash reported for host OS phase 2 slot B
    #[clap(long)]
    slot_b: Option<ArtifactHash>,
}

#[derive(Debug, Args)]
struct SledSetMupdateOverrideArgs {
    #[clap(flatten)]
    source: SledMupdateOverrideSource,
}

#[derive(Debug, Args)]
#[group(id = "sled-mupdate-override-source", required = true, multiple = false)]
struct SledMupdateOverrideSource {
    /// the new value of the mupdate override, or "unset"
    mupdate_override_id: Option<MupdateOverrideUuidOpt>,

    /// simulate an error reading the mupdate override
    #[clap(long, conflicts_with = "mupdate_override_id")]
    with_error: bool,
}

#[derive(Debug, Args)]
struct SledRemoveArgs {
    /// id of the sled
    sled_id: SledOpt,
}

#[derive(Debug, Args)]
struct SiloAddRemoveArgs {
    /// name of the silo
    silo_name: Name,
}

#[derive(Debug, Args)]
struct InventoryShowArgs {
    /// id of the inventory collection to show or "latest"
    collection_id: CollectionIdOpt,

    /// show long strings in their entirety
    #[clap(long)]
    show_long_strings: bool,

    #[clap(subcommand)]
    filter: Option<CollectionDisplayCliFilter>,
}

#[derive(Debug, Args)]
struct BlueprintPlanArgs {
    /// id of the blueprint on which this one will be based, "latest", or
    /// "target"
    parent_blueprint_id: BlueprintIdOpt,
    /// id of the inventory collection to use in planning or "latest"
    ///
    /// Must be provided unless there is only one collection in the loaded
    /// state.
    collection_id: Option<CollectionIdOpt>,
}

#[derive(Debug, Args)]
struct BlueprintEditArgs {
    /// id of the blueprint to edit, "latest", or "target"
    blueprint_id: BlueprintIdOpt,
    /// "creator" field for the new blueprint
    #[arg(long)]
    creator: Option<String>,
    /// "comment" field for the new blueprint
    #[arg(long)]
    comment: Option<String>,
    #[command(subcommand)]
    edit_command: BlueprintEditCommands,
}

#[derive(Debug, Subcommand)]
enum BlueprintEditCommands {
    /// add a Nexus instance to a particular sled
    AddNexus {
        /// sled on which to deploy the new instance
        sled_id: SledOpt,

        /// generation of the new Nexus instance
        nexus_generation: Generation,

        /// image source for the new zone
        ///
        /// The image source is required if the planning input of the system
        /// being edited has a TUF repo; otherwise, it will default to the
        /// install dataset.
        #[clap(subcommand)]
        image_source: Option<ImageSourceArgs>,
    },
    /// add a CockroachDB instance to a particular sled
    AddCockroach {
        /// sled on which to deploy the new instance
        sled_id: SledOpt,
        /// image source for the new zone
        ///
        /// The image source is required if the planning input of the system
        /// being edited has a TUF repo; otherwise, it will default to the
        /// install dataset.
        #[clap(subcommand)]
        image_source: Option<ImageSourceArgs>,
    },
    /// set the image source for a zone
    SetZoneImage {
        /// id of zone whose image to set
        zone_id: OmicronZoneUuid,
        #[command(subcommand)]
        image_source: ImageSourceArgs,
    },
    /// set the desired host phase 2 image for an internal disk slot
    SetHostPhase2 {
        /// sled to set the field on
        sled_id: SledOpt,
        /// internal disk slot
        #[clap(value_parser = parse_m2_slot)]
        slot: M2Slot,
        #[command(subcommand)]
        phase_2_source: HostPhase2SourceArgs,
    },
    /// set the remove_mupdate_override field for a sled
    SetRemoveMupdateOverride {
        /// sled to set the field on
        sled_id: SledOpt,

        /// the UUID to set the field to, or "unset"
        value: MupdateOverrideUuidOpt,
    },
    /// set the minimum generation for which target releases are accepted
    ///
    /// At the moment, this just sets the field to the given value. In the
    /// future, we'll likely want to set this based on the current target
    /// release generation.
    #[clap(visible_alias = "set-target-release-min-gen")]
    SetTargetReleaseMinimumGeneration {
        /// the minimum target release generation
        generation: Generation,
    },
    /// expunge a zone
    ExpungeZones { zone_ids: Vec<OmicronZoneUuid> },
    /// mark an expunged zone ready for cleanup
    MarkForCleanup { zone_id: OmicronZoneUuid },
    /// configure an SP update
    SetSpUpdate {
        /// serial number to update
        serial: String,
        /// artifact hash id
        artifact_hash: ArtifactHash,
        /// version
        version: String,
        /// component to update
        #[command(subcommand)]
        component: SpUpdateComponent,
    },
    /// delete a configured SP update
    DeleteSpUpdate {
        /// baseboard serial number whose update to delete
        serial: String,
    },
    /// debug commands that bypass normal checks
    ///
    /// These commands mutate the blueprint directly, bypassing higher-level
    /// planner checks. They're meant for getting into weird states to test
    /// against.
    Debug {
        #[command(subcommand)]
        command: BlueprintEditDebugCommands,
    },
    /// bumps the blueprint's Nexus generation
    ///
    /// This initiates a handoff from the current generation of Nexus zones to
    /// the next generation of Nexus zones.
    BumpNexusGeneration,
}

#[derive(Debug, Subcommand)]
enum BlueprintEditDebugCommands {
    /// remove a sled from the blueprint
    ///
    /// This bypasses expungement and decommissioning checks, and simply drops
    /// the sled from the blueprint.
    RemoveSled {
        /// the sled to remove
        sled: SledOpt,
    },

    /// Bump a sled's generation number, even if nothing else about the sled has
    /// changed.
    ForceSledGenerationBump {
        /// the sled to bump the sled-agent generation number of
        sled: SledOpt,
    },
}

/// Identifies a sled in a system.
#[derive(Clone, Debug)]
enum SledOpt {
    /// Identifies a sled by its UUID.
    Uuid(SledUuid),
    /// Identifies a sled by its serial number.
    Serial(String),
}

impl SledOpt {
    /// Resolves this sled option into a sled UUID.
    fn to_sled_id(
        &self,
        description: &SystemDescription,
    ) -> anyhow::Result<SledUuid> {
        match self {
            SledOpt::Uuid(uuid) => Ok(*uuid),
            SledOpt::Serial(serial) => description.serial_to_sled_id(&serial),
        }
    }
}

impl FromStr for SledOpt {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // If the sled looks like a UUID, parse it as that.
        if let Ok(uuid) = s.parse::<SledUuid>() {
            return Ok(SledOpt::Uuid(uuid));
        }

        // We treat anything that doesn't parse as a UUID as a serial number.
        //
        // Can we do something more intelligent here, like looking for a
        // particular prefix? In principle, yes, but in reality there are
        // several different sources of serial numbers:
        //
        // * simulated sleds ("serial0", "serial1", ...)
        // * real sleds ("BRM42220014")
        // * a4x2 ("g0", "g1", ...)
        // * single-sled dev deployments
        //
        // and possibly more. We could exhaustively enumerate all of them, but
        // it's easier to assume that if it doesn't look like a UUID, it's a
        // serial number.
        Ok(Self::Serial(s.to_owned()))
    }
}

#[derive(Clone, Debug)]
enum BlueprintIdOpt {
    /// use the target blueprint
    Target,
    /// use the latest blueprint sorted by time created
    Latest,
    /// use a specific blueprint
    Id(BlueprintUuid),
}

impl FromStr for BlueprintIdOpt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(BlueprintIdOpt::Latest),
            // These values match the ones supported in omdb.
            "current-target" | "current" | "target" => {
                Ok(BlueprintIdOpt::Target)
            }
            _ => Ok(BlueprintIdOpt::Id(s.parse()?)),
        }
    }
}

impl fmt::Display for BlueprintIdOpt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlueprintIdOpt::Target => f.write_str("target"),
            BlueprintIdOpt::Latest => f.write_str("latest"),
            BlueprintIdOpt::Id(id) => id.fmt(f),
        }
    }
}

impl From<BlueprintIdOpt> for BlueprintId {
    fn from(value: BlueprintIdOpt) -> Self {
        match value {
            BlueprintIdOpt::Latest => BlueprintId::Latest,
            BlueprintIdOpt::Target => BlueprintId::Target,
            BlueprintIdOpt::Id(id) => BlueprintId::Id(id),
        }
    }
}

#[derive(Clone, Debug)]
enum CollectionIdOpt {
    /// use the latest collection sorted by time created
    Latest,
    /// use a specific collection
    Id(CollectionUuid),
}

impl FromStr for CollectionIdOpt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(CollectionIdOpt::Latest),
            _ => Ok(CollectionIdOpt::Id(s.parse()?)),
        }
    }
}

impl From<CollectionIdOpt> for CollectionId {
    fn from(value: CollectionIdOpt) -> Self {
        match value {
            CollectionIdOpt::Latest => CollectionId::Latest,
            CollectionIdOpt::Id(id) => CollectionId::Id(id),
        }
    }
}

/// Clap field for an optional mupdate override UUID.
///
/// This structure is similar to `Option`, but is specified separately to:
///
/// 1. Disable clap's magic around `Option`.
/// 2. Provide a custom parser.
///
/// There are other ways to do both 1 and 2 (e.g. specify the type as
/// `std::option::Option`), but when combined they're uglier than this.
#[derive(Clone, Copy, Debug)]
enum MupdateOverrideUuidOpt {
    Unset,
    Set(MupdateOverrideUuid),
}

impl FromStr for MupdateOverrideUuidOpt {
    type Err = newtype_uuid::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "unset" || s == "none" {
            Ok(MupdateOverrideUuidOpt::Unset)
        } else {
            Ok(MupdateOverrideUuidOpt::Set(s.parse::<MupdateOverrideUuid>()?))
        }
    }
}

impl From<MupdateOverrideUuidOpt> for Option<MupdateOverrideUuid> {
    fn from(value: MupdateOverrideUuidOpt) -> Self {
        match value {
            MupdateOverrideUuidOpt::Unset => None,
            MupdateOverrideUuidOpt::Set(uuid) => Some(uuid),
        }
    }
}

/// Clap field for an optional generation.
///
/// This structure is similar to `Option`, but is specified separately to:
///
/// 1. Disable clap's magic around `Option`.
/// 2. Provide a custom parser.
///
/// There are other ways to do both 1 and 2 (e.g. specify the type as
/// `std::option::Option`), but when combined they're uglier than this.
#[derive(Clone, Copy, Debug)]
enum GenerationOpt {
    Unset,
    Set(Generation),
}

impl FromStr for GenerationOpt {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "unset" || s == "none" {
            Ok(Self::Unset)
        } else {
            Ok(Self::Set(s.parse::<Generation>()?))
        }
    }
}

impl From<GenerationOpt> for Option<Generation> {
    fn from(value: GenerationOpt) -> Self {
        match value {
            GenerationOpt::Unset => None,
            GenerationOpt::Set(generation) => Some(generation),
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum SpUpdateComponent {
    /// update the SP itself
    Sp {
        expected_active_version: ArtifactVersion,
        expected_inactive_version: ExpectedVersion,
    },
}

#[derive(Debug, Subcommand)]
enum ImageSourceArgs {
    /// the zone image comes from the `install` dataset
    InstallDataset,
    /// the zone image comes from a specific TUF repo artifact
    Artifact {
        #[clap(value_parser = parse_blueprint_artifact_version)]
        version: BlueprintArtifactVersion,
        hash: ArtifactHash,
    },
}

/// Adding a new zone to a blueprint needs to choose an image source for that
/// zone. Subcommands that add a zone take an optional [`ImageSourceArgs`]
/// parameter. In the (common in test) case where the planning input has no TUF
/// repo at all, the new and old TUF repo policy are identical (i.e., "use the
/// install dataset"), and therefore we have only one logical choice for the
/// image source for any new zone (the install dataset). If a TUF repo _is_
/// involved, we have two choices: use the artifact from the newest TUF repo, or
/// use the artifact from the previous TUF repo policy (which might itself be
/// another TUF repo, or might be the install dataset).
fn image_source_unwrap_or(
    image_source: Option<ImageSourceArgs>,
    planning_input: &PlanningInput,
    zone_kind: ZoneKind,
) -> anyhow::Result<BlueprintZoneImageSource> {
    if let Some(image_source) = image_source {
        Ok(image_source.into())
    } else if planning_input.tuf_repo() == planning_input.old_repo() {
        planning_input
            .tuf_repo()
            .description()
            .zone_image_source(zone_kind)
            .context("could not determine image source")
    } else {
        let mut options = vec!["`install-dataset`".to_string()];
        for (name, repo) in [
            ("previous", planning_input.old_repo()),
            ("current", planning_input.tuf_repo()),
        ] {
            match repo.description().zone_image_source(zone_kind) {
                // Install dataset is already covered, and if either TUF repo is
                // missing an artifact of this kind, it's not an option.
                Ok(BlueprintZoneImageSource::InstallDataset) | Err(_) => (),
                Ok(BlueprintZoneImageSource::Artifact { version, hash }) => {
                    let version = match version {
                        BlueprintArtifactVersion::Available { version } => {
                            version.to_string()
                        }
                        BlueprintArtifactVersion::Unknown => {
                            "unknown".to_string()
                        }
                    };
                    options.push(format!(
                        "`artifact {version} {hash}` (from {name} TUF repo)"
                    ));
                }
            }
        }
        bail!(
            "must specify image source for new zone; options: {}",
            options.join(", ")
        )
    }
}

impl From<ImageSourceArgs> for BlueprintZoneImageSource {
    fn from(value: ImageSourceArgs) -> Self {
        match value {
            ImageSourceArgs::InstallDataset => {
                BlueprintZoneImageSource::InstallDataset
            }
            ImageSourceArgs::Artifact { version, hash } => {
                BlueprintZoneImageSource::Artifact { version, hash }
            }
        }
    }
}

#[derive(Debug, Subcommand)]
enum HostPhase2SourceArgs {
    /// keep the current phase 2 contents
    CurrentContents,
    /// the host phase 2 comes from a specific TUF repo artifact
    Artifact {
        #[clap(value_parser = parse_blueprint_artifact_version)]
        version: BlueprintArtifactVersion,
        hash: ArtifactHash,
    },
}

impl From<HostPhase2SourceArgs> for BlueprintHostPhase2DesiredContents {
    fn from(value: HostPhase2SourceArgs) -> Self {
        match value {
            HostPhase2SourceArgs::CurrentContents => Self::CurrentContents,
            HostPhase2SourceArgs::Artifact { version, hash } => {
                Self::Artifact { version, hash }
            }
        }
    }
}

fn parse_blueprint_artifact_version(
    version: &str,
) -> Result<BlueprintArtifactVersion, ArtifactVersionError> {
    // Treat the literal string "unknown" as an unknown version.
    if version == "unknown" {
        return Ok(BlueprintArtifactVersion::Unknown);
    }

    Ok(BlueprintArtifactVersion::Available {
        version: version.parse::<ArtifactVersion>()?,
    })
}

fn parse_m2_slot(slot: &str) -> anyhow::Result<M2Slot> {
    match slot {
        "A" | "a" | "0" => Ok(M2Slot::A),
        "B" | "b" | "1" => Ok(M2Slot::B),
        _ => bail!("invalid slot `{slot}` (expected `A` or `B`)"),
    }
}

#[derive(Debug, Args)]
struct BlueprintArgs {
    /// id of the blueprint, "latest", or "target"
    blueprint_id: BlueprintIdOpt,
}

#[derive(Debug, Args)]
struct BlueprintDiffDnsArgs {
    /// DNS group (internal or external)
    dns_group: CliDnsGroup,
    /// DNS version to diff against
    dns_version: u32,
    /// id of the blueprint, "latest", or "target"
    blueprint_id: BlueprintIdOpt,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliDnsGroup {
    Internal,
    External,
}

#[derive(Debug, Args)]
struct BlueprintHistoryArgs {
    /// how many blueprints worth of history to report
    #[clap(long, default_value_t = 128)]
    limit: usize,

    /// also attempt to diff each blueprint
    #[clap(long, default_value_t = false)]
    diff: bool,

    /// id of the blueprint to start history from
    #[clap(default_value_t = BlueprintIdOpt::Target)]
    blueprint_id: BlueprintIdOpt,
}

#[derive(Debug, Args)]
struct BlueprintSaveArgs {
    /// id of the blueprint, "latest", or "target"
    blueprint_id: BlueprintIdOpt,
    /// output file
    filename: Utf8PathBuf,
}

#[derive(Debug, Args)]
struct BlueprintDiffArgs {
    /// id of the first blueprint, "latest", or "target"
    blueprint1_id: BlueprintIdOpt,
    /// id of the second blueprint, "latest", or "target", or None to mean "the
    /// parent of blueprint1"
    blueprint2_id: Option<BlueprintIdOpt>,
}

#[derive(Debug, Subcommand)]
enum SetArgs {
    /// RNG seed for future commands
    Seed { seed: String },
    /// target number of Nexus instances (for planning)
    NumNexus { num_nexus: u16 },
    /// specify the generation of Nexus zones that are considered active when
    /// running the blueprint planner
    ActiveNexusGen { gen: Generation },
    /// Control the set of Nexus zones seen as input to the planner
    NexusZones {
        #[clap(long, conflicts_with = "active")]
        active_inferred: bool,
        #[clap(long, num_args = 0.., required_unless_present = "active_inferred")]
        active: Vec<OmicronZoneUuid>,
        #[clap(long, conflicts_with = "not_yet")]
        not_yet_inferred: bool,
        #[clap(long, num_args = 0.., required_unless_present = "not_yet_inferred")]
        not_yet: Vec<OmicronZoneUuid>,
    },
    /// system's external DNS zone name (suffix)
    ExternalDnsZoneName { zone_name: String },
    /// system target release
    TargetRelease {
        /// TUF repo containing release artifacts
        filename: Utf8PathBuf,
    },
    /// planner config
    PlannerConfig(SetPlannerConfigArgs),
    /// timestamp for ignoring impossible MGS updates
    IgnoreImpossibleMgsUpdatesSince {
        since: SetIgnoreImpossibleMgsUpdatesSinceArgs,
    },
}

#[derive(Debug, Clone)]
struct SetIgnoreImpossibleMgsUpdatesSinceArgs(DateTime<Utc>);

impl FromStr for SetIgnoreImpossibleMgsUpdatesSinceArgs {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("now") {
            return Ok(Self(Utc::now()));
        }
        if let Ok(datetime) = humantime::parse_rfc3339(s) {
            return Ok(Self(datetime.into()));
        }
        bail!("invalid timestamp: expected `now` or an RFC3339 timestamp")
    }
}

#[derive(Debug, Args)]
struct SetPlannerConfigArgs {
    #[clap(flatten)]
    planner_config: PlannerConfigOpts,
}

// Define the config fields separately so we can use `group(required = true,
// multiple = true).`
#[derive(Debug, Clone, Args)]
#[group(required = true, multiple = true)]
pub struct PlannerConfigOpts {
    #[clap(long, action = ArgAction::Set)]
    add_zones_with_mupdate_override: Option<bool>,
}

impl PlannerConfigOpts {
    fn update_if_modified(
        &self,
        current: &PlannerConfig,
    ) -> Option<PlannerConfig> {
        let new = PlannerConfig {
            add_zones_with_mupdate_override: self
                .add_zones_with_mupdate_override
                .unwrap_or(current.add_zones_with_mupdate_override),
        };
        (new != *current).then_some(new)
    }
}

#[derive(Debug, Args)]
struct TufAssembleArgs {
    /// The tufaceous manifest path (relative to this crate's root)
    manifest_path: Utf8PathBuf,

    /// Allow non-semver artifact versions.
    #[clap(long)]
    allow_non_semver: bool,

    #[clap(
        long,
        // Use help here rather than a doc comment because rustdoc doesn't like
        // `<` and `>` in help messages.
        help = "The path to the output [default: repo-<system-version>.zip]"
    )]
    output: Option<Utf8PathBuf>,
}

#[derive(Debug, Args)]
struct LoadArgs {
    /// input file
    filename: Utf8PathBuf,

    /// id of inventory collection to use for sled details
    /// (may be omitted only if the file contains only one collection)
    collection_id: Option<CollectionUuid>,
}

#[derive(Debug, Args)]
struct LoadExampleArgs {
    /// Seed for the RNG that's used to generate the example system.
    ///
    /// If this is provided, the RNG is updated with this seed before the
    /// example system is generated. If it's not provided, the existing RNG is
    /// used.
    #[clap(long)]
    seed: Option<String>,

    /// The number of sleds in the example system.
    #[clap(short = 's', long, default_value_t = ExampleSystemBuilder::DEFAULT_N_SLEDS)]
    nsleds: usize,

    /// The number of disks per sled in the example system.
    #[clap(short = 'd', long, default_value_t = SledBuilder::DEFAULT_NPOOLS)]
    ndisks_per_sled: u8,

    /// Do not create entries for disks in the blueprint.
    #[clap(long)]
    no_disks_in_blueprint: bool,

    /// Set a 0-indexed sled's policy
    #[clap(long, value_name = "INDEX:POLICY")]
    sled_policy: Vec<LoadExampleSledPolicy>,
}

#[derive(Clone, Debug)]
struct LoadExampleSledPolicy {
    /// The index of the sled to set the policy for.
    index: usize,

    /// The policy to set.
    policy: SledPolicy,
}

impl FromStr for LoadExampleSledPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (index, policy) = s
            .split_once(':')
            .context("invalid format, expected <index>:<policy>")?;
        let index = index.parse().with_context(|| {
            format!("error parsing sled index `{index}` as a usize")
        })?;
        let policy = SledPolicyOpt::from_str(
            policy, /* ignore_case */ false,
        )
        .map_err(|_message| {
            // _message is just something like "invalid variant: <value>".
            // We choose to use our own message instead.
            anyhow!(
                "invalid sled policy `{policy}` (possible values: {})",
                SledPolicyOpt::value_variants().iter().join(", "),
            )
        })?;
        Ok(LoadExampleSledPolicy { index, policy: policy.into() })
    }
}

#[derive(Debug, Args)]
struct FileContentsArgs {
    /// input file
    filename: Utf8PathBuf,
}

#[derive(Debug, Args)]
struct SaveArgs {
    /// output file
    filename: Utf8PathBuf,
}

#[derive(Debug, Args)]
struct WipeArgs {
    /// What to wipe
    #[clap(subcommand)]
    command: WipeCommand,
}

#[derive(Debug, Subcommand)]
enum WipeCommand {
    /// Wipe everything
    All,
    /// Wipe the system
    System,
    /// Reset configuration to default
    Config,
    /// Reset RNG state
    Rng,
}

// Command handlers

fn cmd_silo_list(
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let mut s = String::new();
    for silo_name in sim.current_state().config().silo_names() {
        swriteln!(s, "{}", silo_name);
    }
    Ok(Some(s))
}

fn cmd_silo_add(
    sim: &mut ReconfiguratorSim,
    args: SiloAddRemoveArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let config = state.config_mut();
    config.add_silo(args.silo_name)?;
    sim.commit_and_bump("reconfigurator-cli silo-add".to_owned(), state);
    Ok(None)
}

fn cmd_silo_remove(
    sim: &mut ReconfiguratorSim,
    args: SiloAddRemoveArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let config = state.config_mut();
    config.remove_silo(args.silo_name)?;
    sim.commit_and_bump("reconfigurator-cli silo-remove".to_owned(), state);
    Ok(None)
}

fn cmd_sled_list(
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct Sled {
        id: SledUuid,
        serial: String,
        nzpools: usize,
        subnet: String,
    }

    let state = sim.current_state();
    let planning_input = state
        .system()
        .description()
        .to_planning_input_builder()
        .context("failed to generate planning input")?
        .build();
    let rows = planning_input.all_sleds(SledFilter::Commissioned).map(
        |(sled_id, sled_details)| Sled {
            id: sled_id,
            serial: sled_details.baseboard_id.serial_number.clone(),
            subnet: sled_details.resources.subnet.net().to_string(),
            nzpools: sled_details.resources.zpools.len(),
        },
    );
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_sled_add(
    sim: &mut ReconfiguratorSim,
    add: SledAddArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let sled_id = add.sled_id.unwrap_or_else(|| state.rng_mut().next_sled_id());
    let new_sled = SledBuilder::new()
        .id(sled_id)
        .npools(add.ndisks)
        .policy(add.policy.into());
    let system = state.system_mut();
    system.description_mut().sled(new_sled)?;
    // Figure out what serial number this sled was assigned.
    let added_sled = system
        .description()
        .get_sled(sled_id)
        .expect("we just added this sled");
    let serial = match added_sled.sp_state() {
        Some((_, sp_state)) => sp_state.serial_number.clone(),
        None => "(none)".to_owned(),
    };
    sim.commit_and_bump(
        format!("reconfigurator-cli sled-add: {sled_id} (serial: {serial})"),
        state,
    );

    Ok(Some(format!("added sled {} (serial: {})", sled_id, serial)))
}

fn cmd_sled_remove(
    sim: &mut ReconfiguratorSim,
    args: SledRemoveArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = args.sled_id.to_sled_id(system.description())?;
    system
        .description_mut()
        .sled_remove(sled_id)
        .context("failed to remove sled")?;
    sim.commit_and_bump(
        format!("reconfigurator-cli sled-remove: {sled_id}"),
        state,
    );
    Ok(Some(format!("removed sled {} from system", sled_id)))
}

fn cmd_sled_show(
    sim: &mut ReconfiguratorSim,
    args: SledArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let description = state.system().description();
    let sled_id = args.sled_id.to_sled_id(description)?;
    let stage0_version = description.sled_stage0_version(sled_id)?;
    let stage0_next_version = description.sled_stage0_next_version(sled_id)?;
    let sp_active_version = description.sled_sp_active_version(sled_id)?;
    let sp_inactive_version = description.sled_sp_inactive_version(sled_id)?;
    let rot_active_slot = description.sled_rot_active_slot(sled_id)?;
    let rot_slot_a_version = description.sled_rot_slot_a_version(sled_id)?;
    let rot_slot_b_version = description.sled_rot_slot_b_version(sled_id)?;
    let rot_persistent_boot_preference =
        description.sled_rot_persistent_boot_preference(sled_id)?;
    let rot_pending_persistent_boot_preference =
        description.sled_rot_pending_persistent_boot_preference(sled_id)?;
    let rot_transient_boot_preference =
        description.sled_rot_transient_boot_preference(sled_id)?;
    let planning_input = description
        .to_planning_input_builder()
        .context("failed to generate planning_input builder")?
        .build();
    let sled = planning_input.sled_lookup(args.filter, sled_id)?;
    let sled_resources = &sled.resources;
    let mut s = String::new();
    swriteln!(s, "sled {} ({}, {})", sled_id, sled.policy, sled.state);
    swriteln!(s, "serial {}", sled.baseboard_id.serial_number);
    swriteln!(s, "subnet {}", sled_resources.subnet.net());
    swriteln!(s, "SP active version: {:?}", sp_active_version);
    swriteln!(s, "SP inactive version: {:?}", sp_inactive_version);
    swriteln!(s, "RoT bootloader stage 0 version: {:?}", stage0_version);
    swriteln!(
        s,
        "RoT bootloader stage 0 next version: {:?}",
        stage0_next_version
    );
    swriteln!(s, "RoT active slot: {}", rot_active_slot);
    swriteln!(s, "RoT slot A version: {:?}", rot_slot_a_version);
    swriteln!(s, "RoT slot B version: {:?}", rot_slot_b_version);
    swriteln!(
        s,
        "RoT persistent boot preference: {}",
        rot_persistent_boot_preference
    );
    swriteln!(
        s,
        "RoT pending persistent boot preference: {:?}",
        rot_pending_persistent_boot_preference
    );
    swriteln!(
        s,
        "RoT transient boot preference: {:?}",
        rot_transient_boot_preference
    );
    swriteln!(s, "zpools ({}):", sled_resources.zpools.len());
    for (zpool, disk) in &sled_resources.zpools {
        swriteln!(s, "    {:?}", zpool);
        swriteln!(s, "    {:?}", disk);
    }
    Ok(Some(s))
}

fn cmd_sled_set(
    sim: &mut ReconfiguratorSim,
    args: SledSetArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = args.sled_id.to_sled_id(system.description())?;

    match args.command {
        SledSetCommand::Policy(SledSetPolicyArgs { policy }) => {
            system.description_mut().sled_set_policy(sled_id, policy.into())?;
            sim.commit_and_bump(
                format!(
                    "reconfigurator-cli sled-set policy: {} to {}",
                    sled_id, policy
                ),
                state,
            );
            Ok(Some(format!("set sled {sled_id} policy to {policy}")))
        }
        SledSetCommand::OmicronConfig(command) => {
            let resolved_id =
                system.resolve_blueprint_id(command.blueprint.into())?;
            let blueprint = system.get_blueprint(&resolved_id)?;
            let sled_cfg =
                blueprint.sleds.get(&sled_id).with_context(|| {
                    format!("sled id {sled_id} not found in blueprint")
                })?;
            let omicron_sled_cfg =
                sled_cfg.clone().into_in_service_sled_config();
            system
                .description_mut()
                .sled_set_omicron_config(sled_id, omicron_sled_cfg)?;
            sim.commit_and_bump(
                format!(
                    "reconfigurator-cli sled-set omicron-config: \
                     {sled_id} from {resolved_id}",
                ),
                state,
            );
            Ok(Some(format!(
                "set sled {sled_id} omicron config from {resolved_id}"
            )))
        }
        SledSetCommand::Visibility(command) => {
            let new = command.to_visibility();
            let prev = system
                .description_mut()
                .sled_set_inventory_visibility(sled_id, new)?;
            if prev == new {
                Ok(Some(format!(
                    "sled {sled_id} inventory visibility was already set to \
                     {new}, so no changes were performed",
                )))
            } else {
                sim.commit_and_bump(
                    format!(
                        "reconfigurator-cli sled-set inventory visibility: {} \
                         from {} to {}",
                        sled_id, prev, new,
                    ),
                    state,
                );
                Ok(Some(format!(
                    "set sled {sled_id} inventory visibility: {prev} -> {new}"
                )))
            }
        }
        SledSetCommand::MupdateOverride(SledSetMupdateOverrideArgs {
            source:
                SledMupdateOverrideSource { mupdate_override_id, with_error },
        }) => {
            let (desc, prev) = if with_error {
                let prev =
                    system.description_mut().sled_set_mupdate_override_error(
                        sled_id,
                        "reconfigurator-cli simulated mupdate-override error"
                            .to_owned(),
                    )?;
                ("error".to_owned(), prev)
            } else {
                let mupdate_override_id =
                    mupdate_override_id.expect("clap ensures that this is set");
                let prev = system.description_mut().sled_set_mupdate_override(
                    sled_id,
                    mupdate_override_id.into(),
                )?;
                let desc = match mupdate_override_id {
                    MupdateOverrideUuidOpt::Set(id) => id.to_string(),
                    MupdateOverrideUuidOpt::Unset => "unset".to_owned(),
                };
                (desc, prev)
            };

            let prev_desc = match prev {
                Ok(Some(id)) => id.to_string(),
                Ok(None) => "unset".to_owned(),
                Err(_) => "error".to_owned(),
            };

            sim.commit_and_bump(
                format!(
                    "reconfigurator-cli sled-set-mupdate-override: {}: {} -> {}",
                    sled_id, prev_desc, desc,
                ),
                state,
            );

            Ok(Some(format!(
                "set sled {} mupdate override: {} -> {}",
                sled_id, prev_desc, desc,
            )))
        }
    }
}

fn cmd_sled_update_install_dataset(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateInstallDatasetArgs,
) -> anyhow::Result<Option<String>> {
    let description = mupdate_source_to_description(sim, &args.source)?;

    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = args.sled_id.to_sled_id(system.description())?;
    system
        .description_mut()
        .sled_set_zone_manifest(sled_id, description.to_boot_inventory())?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-install-dataset: {}",
            description.message,
        ),
        state,
    );
    Ok(Some(format!(
        "sled {}: install dataset updated: {}",
        sled_id, description.message
    )))
}

fn cmd_sled_update_rot_bootloader(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateRotBootloaderArgs,
) -> anyhow::Result<Option<String>> {
    let mut labels = Vec::new();
    if let Some(stage0) = &args.stage0 {
        labels.push(format!("stage0 -> {}", stage0));
    }
    if let Some(stage0_next) = &args.stage0_next {
        labels.push(format!("stage0_next -> {}", stage0_next));
    }

    assert!(
        !labels.is_empty(),
        "clap configuration requires that at least one argument is specified"
    );

    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = args.sled_id.to_sled_id(system.description())?;
    system.description_mut().sled_update_rot_bootloader_versions(
        sled_id,
        args.stage0,
        args.stage0_next,
    )?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-rot-bootloader: {}: {}",
            sled_id,
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!(
        "set sled {} RoT bootloader versions: {}",
        sled_id,
        labels.join(", ")
    )))
}

fn cmd_sled_update_sp(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateSpArgs,
) -> anyhow::Result<Option<String>> {
    let mut labels = Vec::new();
    if let Some(active) = &args.active {
        labels.push(format!("active -> {}", active));
    }
    if let Some(inactive) = &args.inactive {
        labels.push(format!("inactive -> {}", inactive));
    }

    assert!(
        !labels.is_empty(),
        "clap configuration requires that at least one argument is specified"
    );

    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = args.sled_id.to_sled_id(system.description())?;
    system.description_mut().sled_update_sp_versions(
        sled_id,
        args.active,
        args.inactive,
    )?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-sp: {}: {}",
            sled_id,
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!("set sled {} SP versions: {}", sled_id, labels.join(", "))))
}

fn cmd_sled_update_rot(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateRotArgs,
) -> anyhow::Result<Option<String>> {
    let mut labels = Vec::new();

    if let Some(slot_a) = &args.slot_a {
        labels.push(format!("slot a -> {}", slot_a));
    }
    if let Some(slot_b) = &args.slot_b {
        labels.push(format!("slot b -> {}", slot_b));
    }
    if let Some(active_slot) = &args.active_slot {
        labels.push(format!("active slot -> {}", active_slot));
    }

    if let Some(persistent_boot_preference) = &args.persistent_boot_preference {
        labels.push(format!(
            "persistent boot preference -> {}",
            persistent_boot_preference
        ));
    }

    if let Some(pending_persistent_boot_preference) =
        &args.pending_persistent_boot_preference
    {
        labels.push(format!(
            "pending persistent boot preference -> {:?}",
            pending_persistent_boot_preference
        ));
    }
    if let Some(transient_boot_preference) = &args.transient_boot_preference {
        labels.push(format!(
            "transient boot preference -> {:?}",
            transient_boot_preference
        ));
    }

    assert!(
        !labels.is_empty(),
        "clap configuration requires that at least one argument is specified"
    );

    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = args.sled_id.to_sled_id(system.description())?;
    system.description_mut().sled_update_rot_versions(
        sled_id,
        RotStateOverrides {
            active_slot_override: args.active_slot,
            slot_a_version_override: args.slot_a,
            slot_b_version_override: args.slot_b,
            persistent_boot_preference_override: args
                .persistent_boot_preference,
            pending_persistent_boot_preference_override: args
                .pending_persistent_boot_preference,
            transient_boot_preference_override: args.transient_boot_preference,
        },
    )?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-rot: {sled_id}: {}",
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!("set sled {sled_id} RoT settings: {}", labels.join(", "))))
}

fn cmd_sled_update_host_phase_1(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateHostPhase1Args,
) -> anyhow::Result<Option<String>> {
    let SledUpdateHostPhase1Args { sled_id, active, slot_a, slot_b } = args;

    let mut labels = Vec::new();
    if let Some(active) = active {
        labels.push(format!("active -> {active:?}"));
    }
    if let Some(slot_a) = slot_a {
        labels.push(format!("A -> {slot_a}"));
    }
    if let Some(slot_b) = slot_b {
        labels.push(format!("B -> {slot_b}"));
    }
    if labels.is_empty() {
        bail!("sled-update-host-phase1 called with no changes");
    }

    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = sled_id.to_sled_id(system.description())?;
    system
        .description_mut()
        .sled_update_host_phase_1_artifacts(sled_id, active, slot_a, slot_b)?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-host-phase1: {sled_id}: {}",
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!(
        "set sled {} host phase 1 details: {}",
        sled_id,
        labels.join(", ")
    )))
}

fn cmd_sled_update_host_phase_2(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateHostPhase2Args,
) -> anyhow::Result<Option<String>> {
    let SledUpdateHostPhase2Args { sled_id, boot_disk, slot_a, slot_b } = args;

    let mut labels = Vec::new();
    if let Some(boot_disk) = boot_disk {
        labels.push(format!("boot_disk -> {boot_disk:?}"));
    }
    if let Some(slot_a) = slot_a {
        labels.push(format!("A -> {slot_a}"));
    }
    if let Some(slot_b) = slot_b {
        labels.push(format!("B -> {slot_b}"));
    }
    if labels.is_empty() {
        bail!("sled-update-host-phase2 called with no changes");
    }

    let mut state = sim.current_state().to_mut();
    let system = state.system_mut();
    let sled_id = sled_id.to_sled_id(system.description())?;
    system.description_mut().sled_update_host_phase_2_artifacts(
        sled_id, boot_disk, slot_a, slot_b,
    )?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-host-phase2: {sled_id}: {}",
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!(
        "set sled {} host phase 2 details: {}",
        sled_id,
        labels.join(", ")
    )))
}

fn cmd_inventory_list(
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct InventoryRow {
        id: CollectionUuid,
        nerrors: usize,
        time_done: String,
    }

    let state = sim.current_state();
    let rows = state.system().all_collections().map(|collection| {
        let id = collection.id;
        InventoryRow {
            id,
            nerrors: collection.errors.len(),
            time_done: humantime::format_rfc3339_millis(
                collection.time_done.into(),
            )
            .to_string(),
        }
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_inventory_generate(
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let builder = state.to_collection_builder()?;

    // The system carries around Omicron zones, which will make their way into
    // the inventory.
    let inventory = builder.build();

    let rv = format!(
        "generated inventory collection {} from configured sleds",
        inventory.id
    );
    state.system_mut().add_collection(inventory)?;
    sim.commit_and_bump(
        "reconfigurator-cli inventory-generate".to_owned(),
        state,
    );
    Ok(Some(rv))
}

fn cmd_inventory_show(
    sim: &mut ReconfiguratorSim,
    args: InventoryShowArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let system = state.system();
    let resolved = system.resolve_collection_id(args.collection_id.into())?;
    let collection = system.get_collection(&resolved)?;

    let mut display = collection.display();
    if let Some(filter) = &args.filter {
        display.apply_cli_filter(filter);
    }
    display.show_long_strings(args.show_long_strings);

    Ok(Some(display.to_string()))
}

fn cmd_blueprint_list(
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BlueprintRow {
        #[tabled(rename = "T")]
        is_target: &'static str,
        #[tabled(rename = "ENA")]
        enabled: &'static str,
        id: BlueprintUuid,
        parent: Cow<'static, str>,
        time_created: String,
    }

    let state = sim.current_state();

    let target_blueprint = state.system().target_blueprint();
    let mut rows = state.system().all_blueprints().collect::<Vec<_>>();
    rows.sort_unstable_by_key(|blueprint| blueprint.time_created);
    let rows = rows.into_iter().map(|blueprint| {
        let (is_target, enabled) = match target_blueprint {
            Some(t) if t.target_id == blueprint.id => {
                let enabled = if t.enabled { "yes" } else { "no" };
                ("*", enabled)
            }
            _ => ("", ""),
        };
        BlueprintRow {
            is_target,
            enabled,
            id: blueprint.id,
            parent: blueprint
                .parent_blueprint_id
                .map(|s| Cow::Owned(s.to_string()))
                .unwrap_or(Cow::Borrowed("<none>")),
            time_created: humantime::format_rfc3339_millis(
                blueprint.time_created.into(),
            )
            .to_string(),
        }
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_blueprint_blippy(
    sim: &mut ReconfiguratorSim,
    args: BlueprintArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let resolved_id =
        state.system().resolve_blueprint_id(args.blueprint_id.into())?;
    let blueprint = state.system().get_blueprint(&resolved_id)?;
    let planning_input = sim
        .planning_input(blueprint)
        .context("failed to construct planning input")?;
    let report = Blippy::new(&blueprint, &planning_input)
        .into_report(BlippyReportSortKey::Severity);
    Ok(Some(format!("{}", report.display())))
}

fn cmd_blueprint_plan(
    sim: &mut ReconfiguratorSim,
    args: BlueprintPlanArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let rng = state.rng_mut().next_planner_rng();
    let system = state.system_mut();

    let parent_blueprint_id =
        system.resolve_blueprint_id(args.parent_blueprint_id.into())?;
    let parent_blueprint = system.get_blueprint(&parent_blueprint_id)?;
    let collection = match args.collection_id {
        Some(collection_id) => {
            let resolved =
                system.resolve_collection_id(collection_id.into())?;
            system.get_collection(&resolved)?
        }
        None => {
            let mut all_collections_iter = system.all_collections();
            match all_collections_iter.len() {
                0 => bail!("cannot plan blueprint with no loaded collections"),
                1 => all_collections_iter.next().expect("iter length is 1"),
                _ => bail!(
                    "blueprint-plan: must specify collection ID (one of {:?})",
                    all_collections_iter.map(|c| c.id).join(", ")
                ),
            }
        }
    };

    let creator = "reconfigurator-sim";
    let planning_input = sim
        .planning_input(parent_blueprint)
        .context("failed to construct planning input")?;
    let planner = Planner::new_based_on(
        sim.log.clone(),
        parent_blueprint,
        &planning_input,
        creator,
        collection,
        rng,
    )
    .context("creating planner")?;

    let blueprint = planner.plan().context("generating blueprint")?;
    let rv = format!(
        "generated blueprint {} based on parent blueprint {}\n\
         blueprint source: {}",
        blueprint.id, parent_blueprint.id, blueprint.source,
    );
    system.add_blueprint(blueprint)?;

    sim.commit_and_bump("reconfigurator-cli blueprint-plan".to_owned(), state);

    Ok(Some(rv))
}

fn cmd_blueprint_edit(
    sim: &mut ReconfiguratorSim,
    args: BlueprintEditArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let rng = state.rng_mut().next_planner_rng();
    let system = state.system_mut();

    let resolved_id = system.resolve_blueprint_id(args.blueprint_id.into())?;
    let blueprint = system.get_blueprint(&resolved_id)?;
    let creator = args.creator.as_deref().unwrap_or("reconfigurator-cli");
    let planning_input = sim
        .planning_input(blueprint)
        .context("failed to create planning input")?;

    // TODO: We may want to do something other than just using the latest
    // collection -- add a way to specify which collection to use.
    let latest_collection = system
        .all_collections()
        .max_by_key(|c| c.time_started)
        .map(|c| c.clone())
        .unwrap_or_else(|| CollectionBuilder::new("sim").build());

    let mut builder = BlueprintBuilder::new_based_on(
        &sim.log,
        blueprint,
        &planning_input,
        creator,
        rng,
    )
    .context("creating blueprint builder")?;

    if let Some(comment) = args.comment {
        builder.comment(comment);
    }

    let label = match args.edit_command {
        BlueprintEditCommands::AddNexus {
            sled_id,
            image_source,
            nexus_generation,
        } => {
            let sled_id = sled_id.to_sled_id(system.description())?;
            let image_source = image_source_unwrap_or(
                image_source,
                &planning_input,
                ZoneKind::Nexus,
            )?;
            let external_ip = ExternalNetworkingAllocator::from_current_zones(
                &builder,
                planning_input.external_ip_policy(),
            )
            .context("failed to construct external networking allocator")?
            .for_new_nexus()
            .context("failed to pick an external IP for Nexus")?;
            builder
                .sled_add_zone_nexus(
                    sled_id,
                    image_source,
                    external_ip,
                    nexus_generation,
                )
                .context("failed to add Nexus zone")?;
            format!("added Nexus zone to sled {}", sled_id)
        }
        BlueprintEditCommands::AddCockroach { sled_id, image_source } => {
            let sled_id = sled_id.to_sled_id(system.description())?;
            let image_source = image_source_unwrap_or(
                image_source,
                &planning_input,
                ZoneKind::CockroachDb,
            )?;
            builder
                .sled_add_zone_cockroachdb(sled_id, image_source)
                .context("failed to add CockroachDB zone")?;
            format!("added CockroachDB zone to sled {}", sled_id)
        }
        BlueprintEditCommands::BumpNexusGeneration => {
            let current_generation = builder.nexus_generation();
            let current_max = blueprint
                .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
                .fold(
                    current_generation,
                    |current_max, (_sled_id, _zone_config, nexus_config)| {
                        std::cmp::max(
                            nexus_config.nexus_generation,
                            current_max,
                        )
                    },
                );
            ensure!(
                current_max > current_generation,
                "cannot bump blueprint generation (currently \
                 {current_generation}) past highest deployed Nexus \
                 generation (currently {current_max})",
            );
            let next = current_generation.next();
            builder.set_nexus_generation(next);
            format!("nexus generation: {current_generation} -> {next}")
        }
        BlueprintEditCommands::SetRemoveMupdateOverride { sled_id, value } => {
            let sled_id = sled_id.to_sled_id(system.description())?;
            builder
                .sled_set_remove_mupdate_override(sled_id, value.into())
                .context("failed to set remove_mupdate_override")?;
            match value {
                MupdateOverrideUuidOpt::Unset => {
                    "unset remove_mupdate_override".to_owned()
                }
                MupdateOverrideUuidOpt::Set(uuid) => {
                    format!("set remove_mupdate_override to {uuid}")
                }
            }
        }
        BlueprintEditCommands::SetTargetReleaseMinimumGeneration {
            generation,
        } => {
            builder
                .set_target_release_minimum_generation(
                    blueprint.target_release_minimum_generation,
                    generation,
                )
                .context("failed to set target release minimum generation")?;
            format!("set target release minimum generation to {generation}")
        }
        BlueprintEditCommands::SetZoneImage { zone_id, image_source } => {
            let sled_id = sled_with_zone(&builder, &zone_id)?;
            let source = BlueprintZoneImageSource::from(image_source);
            let rv = format!(
                "set sled {sled_id} zone {zone_id} image source to {source}\n\
                 warn: no validation is done on the requested image source"
            );
            builder
                .sled_set_zone_source(sled_id, zone_id, source)
                .context("failed to set image source")?;
            rv
        }
        BlueprintEditCommands::SetHostPhase2 {
            sled_id,
            slot,
            phase_2_source,
        } => {
            let sled_id = sled_id.to_sled_id(system.description())?;
            let source =
                BlueprintHostPhase2DesiredContents::from(phase_2_source);
            let rv = format!(
                "set sled {sled_id} host phase 2 slot {slot:?} source to \
                 {source}\n\
                 warn: no validation is done on the requested source"
            );
            builder
                .sled_set_host_phase_2_slot(sled_id, slot, source)
                .context("failed to set host phase 2 source")?;
            rv
        }
        BlueprintEditCommands::ExpungeZones { zone_ids } => {
            let mut rv = String::new();
            for zone_id in zone_ids {
                let sled_id = sled_with_zone(&builder, &zone_id)?;
                builder.sled_expunge_zone(sled_id, zone_id).with_context(
                    || format!("failed to expunge zone {zone_id}"),
                )?;
                swriteln!(rv, "expunged zone {zone_id} from sled {sled_id}");
            }
            rv
        }
        BlueprintEditCommands::MarkForCleanup { zone_id } => {
            let sled_id = sled_with_zone(&builder, &zone_id)?;
            builder
                .sled_mark_expunged_zone_ready_for_cleanup(sled_id, zone_id)
                .context("failed to mark zone ready for cleanup")?;
            format!("marked zone {zone_id} ready for cleanup")
        }
        BlueprintEditCommands::SetSpUpdate {
            serial,
            artifact_hash,
            version,
            component,
        } => {
            let (baseboard_id, sp) = latest_collection
                .sps
                .iter()
                .find(|(b, _)| b.serial_number == serial)
                .ok_or_else(|| {
                    anyhow!("unknown baseboard serial: {serial:?}")
                })?;

            let details = match component {
                SpUpdateComponent::Sp {
                    expected_active_version,
                    expected_inactive_version,
                } => PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
                    expected_active_version,
                    expected_inactive_version,
                }),
            };

            let artifact_version = ArtifactVersion::new(version)
                .context("parsing artifact version")?;

            let update = PendingMgsUpdate {
                baseboard_id: baseboard_id.clone(),
                sp_type: sp.sp_type,
                slot_id: sp.sp_slot,
                details,
                artifact_hash,
                artifact_version,
            };

            builder.pending_mgs_update_insert(update);
            format!(
                "configured update for serial {serial}\n\
                 warn: no validation is done on the requested artifact \
                 hash or version"
            )
        }
        BlueprintEditCommands::DeleteSpUpdate { serial } => {
            let baseboard_id = latest_collection
                .baseboards
                .iter()
                .find(|b| b.serial_number == serial)
                .ok_or_else(|| {
                    anyhow!("unknown baseboard serial: {serial:?}")
                })?;
            builder.pending_mgs_update_delete(baseboard_id);
            format!("deleted configured update for serial {serial}")
        }
        BlueprintEditCommands::Debug {
            command: BlueprintEditDebugCommands::RemoveSled { sled },
        } => {
            let sled_id = sled.to_sled_id(system.description())?;
            builder.debug_sled_remove(sled_id)?;
            format!("debug: removed sled {sled_id} from blueprint")
        }
        BlueprintEditCommands::Debug {
            command:
                BlueprintEditDebugCommands::ForceSledGenerationBump { sled },
        } => {
            let sled_id = sled.to_sled_id(system.description())?;
            builder.debug_sled_force_generation_bump(sled_id)?;
            format!("debug: forced sled {sled_id} generation bump")
        }
    };

    let mut new_blueprint =
        builder.build(BlueprintSource::ReconfiguratorCliEdit);

    // Normally `builder.build()` would construct the cockroach fingerprint
    // based on what we read from CRDB and put into the planning input, but
    // since we don't have a CRDB we had to make something up for our planning
    // input's CRDB fingerprint. In the absense of a better alternative, we'll
    // just copy our parent's CRDB fingerprint and carry it forward.
    new_blueprint
        .cockroachdb_fingerprint
        .clone_from(&blueprint.cockroachdb_fingerprint);

    let rv = format!(
        "blueprint {} created from {}: {}",
        new_blueprint.id, resolved_id, label
    );
    system.add_blueprint(new_blueprint)?;

    sim.commit_and_bump("reconfigurator-cli blueprint-edit".to_owned(), state);
    Ok(Some(rv))
}

fn sled_with_zone(
    builder: &BlueprintBuilder<'_>,
    zone_id: &OmicronZoneUuid,
) -> anyhow::Result<SledUuid> {
    let mut parent_sled_id = None;

    for sled_id in builder.sled_ids_with_zones() {
        if builder
            .current_sled_zones(sled_id, BlueprintZoneDisposition::any)
            .any(|z| z.id == *zone_id)
        {
            parent_sled_id = Some(sled_id);
            break;
        }
    }

    parent_sled_id
        .ok_or_else(|| anyhow!("could not find parent sled for zone {zone_id}"))
}

fn cmd_blueprint_show(
    sim: &mut ReconfiguratorSim,
    args: BlueprintArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let blueprint =
        state.system().resolve_and_get_blueprint(args.blueprint_id.into())?;
    Ok(Some(format!("{}", blueprint.display())))
}

fn cmd_blueprint_diff(
    sim: &mut ReconfiguratorSim,
    args: BlueprintDiffArgs,
) -> anyhow::Result<Option<String>> {
    let mut rv = String::new();
    let blueprint1_id = args.blueprint1_id;

    let state = sim.current_state();
    let blueprint =
        state.system().resolve_and_get_blueprint(blueprint1_id.into())?;
    let (blueprint1, blueprint2) = if let Some(blueprint2_arg) =
        args.blueprint2_id
    {
        // Two blueprint ids were provided.  Diff from the first to the second.
        let blueprint1 = blueprint;
        let blueprint2 =
            state.system().resolve_and_get_blueprint(blueprint2_arg.into())?;
        (blueprint1, blueprint2)
    } else if let Some(parent_id) = blueprint.parent_blueprint_id {
        // Only one blueprint id was provided.  Diff from that blueprint's
        // parent to the blueprint.
        let blueprint1 = state
            .system()
            .resolve_and_get_blueprint(BlueprintId::Id(parent_id))?;
        let blueprint2 = blueprint;
        (blueprint1, blueprint2)
    } else {
        bail!(
            "`blueprint2_id` was not specified and blueprint1 has no \
             parent blueprint"
        );
    };

    let sled_diff = blueprint2.diff_since_blueprint(&blueprint1);
    swriteln!(rv, "{}", sled_diff.display());

    // Diff'ing DNS is a little trickier.  First, compute what DNS should be for
    // each blueprint.  To do that we need to construct a list of sleds suitable
    // for the executor.
    let sleds_by_id = make_sleds_by_id(state.system().description())?;

    // It's tricky to figure out which active Nexus generation number to use
    // when diff'ing blueprints.  What's currently active might be wholly
    // different from what's here.  (Imagine generation 7 is active and these
    // blueprints are from Nexus generation 4.)  What's most likely useful is
    // picking the Nexus generation of the blueprint itself.
    let blueprint1_active_nexus_generation =
        blueprint_active_nexus_generation(&blueprint1);
    let blueprint2_active_nexus_generation =
        blueprint_active_nexus_generation(&blueprint2);
    let internal_dns_config1 = blueprint_internal_dns_config(
        &blueprint1,
        &sleds_by_id,
        blueprint1_active_nexus_generation,
        &Default::default(),
    )?;
    let internal_dns_config2 = blueprint_internal_dns_config(
        &blueprint2,
        &sleds_by_id,
        blueprint2_active_nexus_generation,
        &Default::default(),
    )?;
    let dns_diff = DnsDiff::new(&internal_dns_config1, &internal_dns_config2)
        .context("failed to assemble DNS diff")?;
    swriteln!(rv, "internal DNS:\n{}", dns_diff);

    let external_dns_zone_name = state.config().external_dns_zone_name();
    let external_dns_config1 = blueprint_external_dns_config(
        &blueprint1,
        state.config().silo_names(),
        external_dns_zone_name.to_owned(),
        blueprint1_active_nexus_generation,
    );
    let external_dns_config2 = blueprint_external_dns_config(
        &blueprint2,
        state.config().silo_names(),
        external_dns_zone_name.to_owned(),
        blueprint2_active_nexus_generation,
    );
    let dns_diff = DnsDiff::new(&external_dns_config1, &external_dns_config2)
        .context("failed to assemble external DNS diff")?;
    swriteln!(rv, "external DNS:\n{}", dns_diff);

    Ok(Some(rv))
}

fn make_sleds_by_id(
    system: &SystemDescription,
) -> Result<IdOrdMap<execution::Sled>, anyhow::Error> {
    let collection = system
        .to_collection_builder()
        .context(
            "unexpectedly failed to create collection for current set of sleds",
        )?
        .build();
    let sleds_by_id: IdOrdMap<_> = collection
        .sled_agents
        .iter()
        .map(|sa| {
            let sled = execution::Sled::new(
                sa.sled_id,
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                },
                sa.sled_agent_address,
                REPO_DEPOT_PORT,
                sa.sled_role,
            );
            sled
        })
        .collect();
    Ok(sleds_by_id)
}

fn cmd_blueprint_diff_dns(
    sim: &mut ReconfiguratorSim,
    args: BlueprintDiffDnsArgs,
) -> anyhow::Result<Option<String>> {
    let dns_group = args.dns_group;
    let dns_version = Generation::from(args.dns_version);
    let blueprint_id = args.blueprint_id;

    let state = sim.current_state();
    let blueprint =
        state.system().resolve_and_get_blueprint(blueprint_id.into())?;

    let existing_dns_config = match dns_group {
        CliDnsGroup::Internal => {
            state.system().get_internal_dns(dns_version)?
        }
        CliDnsGroup::External => {
            state.system().get_external_dns(dns_version)?
        }
    };

    let blueprint_active_generation =
        blueprint_active_nexus_generation(&blueprint);
    let blueprint_dns_zone = match dns_group {
        CliDnsGroup::Internal => {
            let sleds_by_id = make_sleds_by_id(state.system().description())?;
            blueprint_internal_dns_config(
                blueprint,
                &sleds_by_id,
                blueprint_active_generation,
                &Default::default(),
            )?
        }
        CliDnsGroup::External => blueprint_external_dns_config(
            blueprint,
            state.config().silo_names(),
            state.config().external_dns_zone_name().to_owned(),
            blueprint_active_generation,
        ),
    };

    let existing_dns_zone = existing_dns_config.sole_zone()?;
    let dns_diff = DnsDiff::new(&existing_dns_zone, &blueprint_dns_zone)
        .context("failed to assemble DNS diff")?;
    Ok(Some(dns_diff.to_string()))
}

// This command looks a lot like `omdb reconfigurator history`, but it differs
// in some ways that often don't matter but are worth knowing about:
//
// 1. It's reading the history of blueprints based on their parent ids.  `omdb
//    reconfigurator history` reads the `bp_target` table.  These are generally
//    equivalent, except that the `omdb` command sees entries for blueprints
//    being enabled and disabled and the times that that happened.  This command
//    doesn't know that.
//
// 2. Relatedly, this command prints the creation time of the blueprint, not
//    when it was made the target.  These are generally very close in time but
//    they're not the same thing.
fn cmd_blueprint_history(
    sim: &mut ReconfiguratorSim,
    args: BlueprintHistoryArgs,
) -> anyhow::Result<Option<String>> {
    let BlueprintHistoryArgs { limit, diff, blueprint_id } = args;

    let state = sim.current_state();
    let system = state.system();
    let resolved_id = system.resolve_blueprint_id(blueprint_id.into())?;
    let mut blueprint = system.get_blueprint(&resolved_id)?;

    // We want to print the output in logical order, but in order to construct
    // the output, we need to walk in reverse-logical order.  To do this, we'll
    // assemble the output as we go and then print it in reverse order.
    //
    // Although we're sort of printing a table, we use strings rather than a
    // table in case we're in `diff` mode.  In that case, we want to print
    // details with each "row" that don't go in the table itself.
    let mut entries = Vec::new();

    while entries.len() < limit {
        let mut entry = String::new();
        swriteln!(
            entry,
            "{} {} {}",
            humantime::format_rfc3339_millis(blueprint.time_created.into()),
            blueprint.id,
            blueprint.comment
        );
        let new_blueprint = blueprint;
        let Some(parent_blueprint_id) = &blueprint.parent_blueprint_id else {
            // We reached the initial blueprint.
            entries.push(entry);
            break;
        };

        blueprint = match system
            .resolve_and_get_blueprint(BlueprintId::Id(*parent_blueprint_id))
        {
            Ok(b) => b,
            Err(error) => {
                swriteln!(
                    entry,
                    "error walking back from blueprint {} to parent {}: {}",
                    blueprint.id,
                    parent_blueprint_id,
                    InlineErrorChain::new(&error)
                );
                entries.push(entry);
                break;
            }
        };

        if diff {
            let diff = new_blueprint.diff_since_blueprint(&blueprint);
            swriteln!(entry, "{}", diff.display());
        }

        entries.push(entry);
    }

    if entries.len() == limit {
        entries.push(String::from("... (earlier history omitted)\n"));
    }

    let mut output = String::new();
    swriteln!(output, "{:24} {:36}", "TIME", "BLUEPRINT");
    for entry in entries.iter().rev() {
        swrite!(output, "{entry}");
    }

    Ok(Some(output))
}

fn cmd_blueprint_save(
    sim: &mut ReconfiguratorSim,
    args: BlueprintSaveArgs,
) -> anyhow::Result<Option<String>> {
    let blueprint_id = args.blueprint_id;

    let state = sim.current_state();
    let resolved_id =
        state.system().resolve_blueprint_id(blueprint_id.into())?;
    let blueprint = state.system().get_blueprint(&resolved_id)?;

    let output_path = &args.filename;
    let output_str = serde_json::to_string_pretty(&blueprint)
        .context("serializing blueprint")?;
    std::fs::write(&output_path, &output_str)
        .with_context(|| format!("write {:?}", output_path))?;
    Ok(Some(format!("saved {} to {:?}", resolved_id, output_path)))
}

fn cmd_save(
    sim: &mut ReconfiguratorSim,
    args: SaveArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let saved = state.to_serializable()?;

    let output_path = &args.filename;
    let output_str =
        serde_json::to_string_pretty(&saved).context("serializing state")?;
    std::fs::write(&output_path, &output_str)
        .with_context(|| format!("write {:?}", output_path))?;
    Ok(Some(format!(
        "saved planning input, collections, and blueprints to {:?}",
        output_path
    )))
}

fn cmd_wipe(
    sim: &mut ReconfiguratorSim,
    args: WipeArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let output = match args.command {
        WipeCommand::All => {
            state.system_mut().wipe();
            state.config_mut().wipe();
            state.rng_mut().reset_state();
            format!(
                "- wiped system, reconfigurator-sim config, and RNG state\n
                 - reset seed to {}",
                state.rng_mut().seed()
            )
        }
        WipeCommand::System => {
            state.system_mut().wipe();
            "wiped system".to_string()
        }
        WipeCommand::Config => {
            state.config_mut().wipe();
            "wiped reconfigurator-sim config".to_string()
        }
        WipeCommand::Rng => {
            // Don't allow wiping the RNG state if the system is non-empty.
            // Wiping the RNG state is likely to cause duplicate IDs to be
            // generated.
            if !state.system_mut().is_empty() {
                bail!(
                    "cannot wipe RNG state with non-empty system: \
                     run `wipe system` first"
                );
            }
            state.rng_mut().reset_state();
            format!(
                "- wiped RNG state\n- reset seed to {}",
                state.rng_mut().seed()
            )
        }
    };

    sim.commit_and_bump(output.clone(), state);
    Ok(Some(output))
}

fn cmd_show(sim: &mut ReconfiguratorSim) -> anyhow::Result<Option<String>> {
    let mut s = String::new();
    let state = sim.current_state();

    swriteln!(
        s,
        "configured external DNS zone name: {}",
        state.config().external_dns_zone_name(),
    );
    swriteln!(
        s,
        "configured silo names: {}",
        state
            .config()
            .silo_names()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    swriteln!(
        s,
        "internal DNS generations: {}",
        state
            .system()
            .all_internal_dns()
            .map(|params| params.generation.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
    swriteln!(
        s,
        "external DNS generations: {}",
        state
            .system()
            .all_external_dns()
            .map(|params| params.generation.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
    swriteln!(
        s,
        "target number of Nexus instances: {}",
        state
            .config()
            .num_nexus()
            .map_or_else(|| "default".to_owned(), |n| n.to_string())
    );

    let target_release = state.system().description().target_release();
    match target_release.description() {
        TargetReleaseDescription::TufRepo(tuf_desc) => {
            swriteln!(
                s,
                "target release (generation {}): {} ({})",
                target_release.target_release_generation,
                tuf_desc.repo.system_version,
                tuf_desc.repo.file_name
            );
            for artifact in &tuf_desc.artifacts {
                swriteln!(
                    s,
                    "    artifact: {} {} ({} version {})",
                    artifact.hash,
                    artifact.id.kind,
                    artifact.id.name,
                    artifact.id.version
                );
            }
        }
        TargetReleaseDescription::Initial => {
            swriteln!(
                s,
                "target release (generation {}): unset",
                target_release.target_release_generation,
            );
        }
    }

    // Show nexus zone state information
    swriteln!(
        s,
        "active nexus zone generation: {}",
        state.config().active_nexus_zone_generation()
    );

    if let Some(zones) = state.config().explicit_active_nexus_zones() {
        swriteln!(
            s,
            "explicit active nexus zones ({}): {}",
            zones.len(),
            zones.iter().map(|z| z.to_string()).collect::<Vec<_>>().join(", ")
        );
    } else {
        swriteln!(s, "active nexus zones: inferred from generation");
    }

    if let Some(zones) = state.config().explicit_not_yet_nexus_zones() {
        swriteln!(
            s,
            "explicit not-yet nexus zones ({}): {}",
            zones.len(),
            zones.iter().map(|z| z.to_string()).collect::<Vec<_>>().join(", ")
        );
    } else {
        swriteln!(s, "not-yet nexus zones: inferred from generation");
    }

    swriteln!(s, "planner config:");
    // No need for swriteln! here because .display() adds its own newlines at
    // the end.
    swrite!(
        s,
        "{}",
        state.system().description().get_planner_config().display()
    );

    Ok(Some(s))
}

fn cmd_set(
    sim: &mut ReconfiguratorSim,
    args: SetArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let rv = match args {
        SetArgs::Seed { seed } => {
            // In this case, reset the RNG state to the provided seed.
            let rv = format!("new RNG seed: {seed}");
            state.rng_mut().set_seed(seed);
            rv
        }
        SetArgs::NumNexus { num_nexus } => {
            let rv = format!(
                "target number of Nexus zones: {:?} -> {}",
                state.config_mut().num_nexus(),
                num_nexus
            );
            state.config_mut().set_num_nexus(num_nexus);
            state
                .system_mut()
                .description_mut()
                .set_target_nexus_zone_count(usize::from(num_nexus));
            rv
        }
        SetArgs::ActiveNexusGen { gen } => {
            let rv =
                format!("will use active Nexus zones from generation {gen}");
            state.config_mut().set_active_nexus_zone_generation(gen);
            rv
        }
        SetArgs::NexusZones {
            active_inferred,
            active,
            not_yet_inferred,
            not_yet,
        } => {
            use std::collections::BTreeSet;
            let mut rv = String::new();
            if active_inferred {
                rv.push_str("inferring active nexus zones from generation");
                state.config_mut().set_explicit_active_nexus_zones(None);
            } else {
                let zone_set: BTreeSet<_> = active.into_iter().collect();
                let count = zone_set.len();
                rv.push_str(&format!(
                    "set {} explicit active Nexus zones",
                    count
                ));
                state
                    .config_mut()
                    .set_explicit_active_nexus_zones(Some(zone_set));
            }
            rv.push_str(", ");
            if not_yet_inferred {
                rv.push_str("inferring not-yet nexus zones from generation");
                state.config_mut().set_explicit_not_yet_nexus_zones(None);
            } else {
                let zone_set: BTreeSet<_> = not_yet.into_iter().collect();
                let count = zone_set.len();
                rv.push_str(&format!(
                    "set {} explicit not-yet Nexus zones",
                    count
                ));
                state
                    .config_mut()
                    .set_explicit_not_yet_nexus_zones(Some(zone_set));
            }
            rv
        }
        SetArgs::ExternalDnsZoneName { zone_name } => {
            let rv = format!(
                "external DNS zone name: {:?} -> {:?}",
                state.config_mut().external_dns_zone_name(),
                zone_name
            );
            state.config_mut().set_external_dns_zone_name(zone_name);
            rv
        }
        SetArgs::TargetRelease { filename } => {
            let description =
                extract_tuf_repo_description(&sim.log, &filename)?;
            state.system_mut().description_mut().set_target_release(
                TargetReleaseDescription::TufRepo(description),
            );
            format!("set target release based on {}", filename)
        }
        SetArgs::PlannerConfig(args) => {
            let current = state.system_mut().description().get_planner_config();
            if let Some(new) = args.planner_config.update_if_modified(&current)
            {
                state.system_mut().description_mut().set_planner_config(new);
                let diff = current.diff(&new);
                format!("planner config updated:\n{}", diff.display())
            } else {
                format!("no changes to planner config:\n{}", current.display())
            }
        }
        SetArgs::IgnoreImpossibleMgsUpdatesSince { since } => {
            state
                .system_mut()
                .description_mut()
                .set_ignore_impossible_mgs_updates_since(since.0);
            format!(
                "ignoring impossible MGS updates since {}",
                humantime::format_rfc3339_millis(since.0.into())
            )
        }
    };

    sim.commit_and_bump(format!("reconfigurator-cli set: {}", rv), state);
    Ok(Some(rv))
}

/// Converts a mupdate source to a TUF repo description.
fn mupdate_source_to_description(
    sim: &ReconfiguratorSim,
    source: &SledMupdateSource,
) -> anyhow::Result<SimTufRepoDescription> {
    let manifest_source = match source.mupdate_id {
        Some(mupdate_id) => {
            OmicronZoneManifestSource::Installinator { mupdate_id }
        }
        None => OmicronZoneManifestSource::SledAgent,
    };
    if let Some(repo_path) = &source.valid.from_repo {
        let description = extract_tuf_repo_description(&sim.log, repo_path)?;
        let mut sim_source = SimTufRepoSource::new(
            description,
            manifest_source,
            format!("from repo at {repo_path}"),
        )?;
        sim_source.simulate_zone_errors(&source.with_zone_error)?;
        Ok(SimTufRepoDescription::new(sim_source))
    } else if source.valid.to_target_release {
        let description = sim
            .current_state()
            .system()
            .description()
            .target_release()
            .description();
        match description {
            TargetReleaseDescription::Initial => {
                bail!(
                    "cannot mupdate zones without a target release \
                     (use `set target-release` or --from-repo)"
                )
            }
            TargetReleaseDescription::TufRepo(desc) => {
                let mut sim_source = SimTufRepoSource::new(
                    desc.clone(),
                    manifest_source,
                    "to target release".to_owned(),
                )?;
                sim_source.simulate_zone_errors(&source.with_zone_error)?;
                Ok(SimTufRepoDescription::new(sim_source))
            }
        }
    } else if source.with_manifest_error {
        Ok(SimTufRepoDescription::new_error(
            "simulated error obtaining zone manifest".to_owned(),
        ))
    } else {
        bail!("an update source must be specified")
    }
}

fn cmd_tuf_assemble(
    sim: &ReconfiguratorSim,
    args: TufAssembleArgs,
) -> anyhow::Result<Option<String>> {
    let manifest_path = if args.manifest_path.is_absolute() {
        args.manifest_path.clone()
    } else {
        // Use CARGO_MANIFEST_DIR to resolve relative paths.
        let dir = std::env::var("CARGO_MANIFEST_DIR").context(
            "CARGO_MANIFEST_DIR not set in environment \
             (are you running with `cargo run`?)",
        )?;
        let mut dir = Utf8PathBuf::from(dir);
        dir.push(&args.manifest_path);
        dir
    };

    // Obtain the system version from the manifest.
    let manifest =
        ArtifactManifest::from_path(&manifest_path).with_context(|| {
            format!("error parsing manifest from `{manifest_path}`")
        })?;

    let output_path = if let Some(output_path) = &args.output {
        output_path.clone()
    } else {
        // This is relative to the current directory.
        Utf8PathBuf::from(format!("repo-{}.zip", manifest.system_version))
    };

    tuf_assemble(
        &sim.log,
        &manifest_path,
        &output_path,
        args.allow_non_semver,
    )?;

    let rv = format!(
        "created {} for system version {}",
        output_path, manifest.system_version,
    );
    Ok(Some(rv))
}

fn read_file(
    input_path: &camino::Utf8Path,
) -> anyhow::Result<UnstableReconfiguratorState> {
    let file = std::fs::File::open(input_path)
        .with_context(|| format!("open {:?}", input_path))?;
    let bufread = std::io::BufReader::new(file);
    serde_json::from_reader(bufread)
        .with_context(|| format!("read {:?}", input_path))
}

fn cmd_load(
    sim: &mut ReconfiguratorSim,
    args: LoadArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    if !state.system_mut().is_empty() {
        bail!(
            "changes made to simulated system: run `wipe system` before \
              loading"
        );
    }

    let input_path = args.filename;
    let collection_id = args.collection_id;
    let loaded = read_file(&input_path)?;

    let result = state.load_serialized(loaded, collection_id)?;

    sim.commit_and_bump(
        format!("reconfigurator-sim: load {:?}", input_path),
        state,
    );

    let mut s = String::new();

    swriteln!(s, "loaded data from {:?}", input_path);

    if !result.warnings.is_empty() {
        swriteln!(s, "warnings:");
        for warning in result.warnings {
            swriteln!(s, "  {}", warning);
        }
    }

    swriteln!(s, "result:\n  system:");
    {
        let mut writer = IndentWriter::new("    ", &mut s);
        // It's assumed that the result.system Display impl always ends in a
        // newline, so we use `write!` instead of `writeln!`.
        write!(writer, "{}", result.system)?;
    }
    swriteln!(s, "  config:");
    {
        let mut writer = IndentWriter::new("    ", &mut s);
        // It's assumed that the result.config Display impl always ends in a
        // newline, so we use `write!` instead of `writeln!`.
        write!(writer, "{}", result.config)?;
    }

    Ok(Some(s))
}

fn cmd_load_example(
    sim: &mut ReconfiguratorSim,
    args: LoadExampleArgs,
) -> anyhow::Result<Option<String>> {
    let mut s = String::new();
    let mut state = sim.current_state().to_mut();
    if !state.system_mut().is_empty() {
        bail!(
            "changes made to simulated system: run `wipe system` before \
             loading"
        );
    }

    // Generate the example system.
    match args.seed {
        Some(seed) => {
            // In this case, reset the RNG state to the provided seed.
            swriteln!(s, "setting new RNG seed: {}", seed);
            state.rng_mut().set_seed(seed);
        }
        None => {
            // In this case, use the existing RNG state.
            swriteln!(
                s,
                "using existing RNG state (seed: {})",
                state.rng_mut().seed()
            );
        }
    };
    let rng = state.rng_mut().next_example_rng();

    let mut builder = ExampleSystemBuilder::new_with_rng(&sim.log, rng)
        .nsleds(args.nsleds)
        .ndisks_per_sled(args.ndisks_per_sled)
        .nexus_count(
            state
                .config_mut()
                .num_nexus()
                .map_or(NEXUS_REDUNDANCY, |n| n.into()),
        )
        .external_dns_count(3)
        .context("invalid external DNS zone count")?
        .create_disks_in_blueprint(!args.no_disks_in_blueprint);
    for sled_policy in args.sled_policy {
        builder = builder
            .with_sled_policy(sled_policy.index, sled_policy.policy)
            .context("setting sled policy")?;
    }

    let (example, blueprint) = builder.build();

    // Generate the internal and external DNS configs based on the blueprint.
    let sleds_by_id = make_sleds_by_id(&example.system)?;
    let blueprint_nexus_generation =
        blueprint_active_nexus_generation(&blueprint);
    let internal_dns = blueprint_internal_dns_config(
        &blueprint,
        &sleds_by_id,
        blueprint_nexus_generation,
        &Default::default(),
    )?;
    let external_dns_zone_name =
        state.config_mut().external_dns_zone_name().to_owned();
    let external_dns = blueprint_external_dns_config(
        &blueprint,
        state.config_mut().silo_names(),
        external_dns_zone_name,
        blueprint_nexus_generation,
    );

    let blueprint_id = blueprint.id;
    let collection_id = example.collection.id;

    state
        .system_mut()
        .load_example(example, blueprint, internal_dns, external_dns)
        .expect("already checked non-empty state above");
    sim.commit_and_bump("reconfigurator-cli load-example".to_owned(), state);

    Ok(Some(format!(
        "loaded example system with:\n\
         - collection: {collection_id}\n\
         - blueprint: {blueprint_id}",
    )))
}

fn cmd_file_contents(args: FileContentsArgs) -> anyhow::Result<Option<String>> {
    let loaded = read_file(&args.filename)?;

    let mut s = String::new();

    for (sled_id, sled_resources) in
        loaded.planning_input.all_sled_resources(SledFilter::Commissioned)
    {
        swriteln!(
            s,
            "sled: {} (subnet: {}, zpools: {})",
            sled_id,
            sled_resources.subnet.net(),
            sled_resources.zpools.len()
        );
    }

    for collection in loaded.collections {
        swriteln!(
            s,
            "collection: {} (errors: {}, completed at: {})",
            collection.id,
            collection.errors.len(),
            humantime::format_rfc3339_millis(collection.time_done.into())
                .to_string(),
        );
    }

    for blueprint in loaded.blueprints {
        swriteln!(
            s,
            "blueprint:  {} (created at: {})",
            blueprint.id,
            blueprint.time_created
        );
    }

    swriteln!(s, "internal DNS generations: {:?}", loaded.internal_dns.keys(),);
    swriteln!(s, "external DNS generations: {:?}", loaded.external_dns.keys(),);
    swriteln!(s, "silo names: {:?}", loaded.silo_names);
    swriteln!(
        s,
        "external DNS zone names: {}",
        loaded.external_dns_zone_names.join(", ")
    );

    Ok(Some(s))
}

/// Returns the "active Nexus generation" to use for a historical blueprint
/// (i.e., a blueprint that may not have been generated or executed against the
/// current simulated state).  This is used for `blueprint-diff`, for example,
/// which avoids assuming anything about the simulated state in comparing the
/// two blueprints.
///
/// In general, the active Nexus generation for a blueprint is not well-defined.
/// We cannot know what the active Nexus generation was at some point in the
/// past.  But we do know that it's one of these two values:
///
/// - `blueprint.nexus_generation - 1`, if this blueprint was created as part
///   of an upgrade, starting with the point where the Nexus handoff was
///   initiated (inclusive) and ending with the first blueprint after the
///   handoff (exclusive).  In most cases, this means that this is the single
///   blueprint during an upgrade that triggered the handoff.
/// - `blueprint.nexus_generation` otherwise (which includes all other
///   blueprints that are created during an upgrade and all blueprints created
///   outside of an upgrade).
///
/// This implementation always returns `blueprint.nexus_generation`.  In the
/// second case above, this is always correct.  In the first case, this is
/// basically equivalent to assuming that the Nexus handoff had happened
/// instantaneously when the blueprint was created.
fn blueprint_active_nexus_generation(blueprint: &Blueprint) -> Generation {
    blueprint.nexus_generation
}
