// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! developer REPL for driving blueprint planning

use anyhow::{Context, anyhow, bail};
use camino::Utf8PathBuf;
use clap::ValueEnum;
use clap::{Args, Parser, Subcommand};
use gateway_types::rot::RotSlot;
use indent_write::fmt::IndentWriter;
use internal_dns_types::diff::DnsDiff;
use itertools::Itertools;
use log_capture::LogCapture;
use nexus_inventory::CollectionBuilder;
use nexus_reconfigurator_blippy::Blippy;
use nexus_reconfigurator_blippy::BlippyReportSortKey;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::system::{SledBuilder, SystemDescription};
use nexus_reconfigurator_simulation::SimStateBuilder;
use nexus_reconfigurator_simulation::Simulator;
use nexus_reconfigurator_simulation::{BlueprintId, SimState};
use nexus_types::deployment::OmicronZoneNic;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::execution;
use nexus_types::deployment::execution::blueprint_external_dns_config;
use nexus_types::deployment::execution::blueprint_internal_dns_config;
use nexus_types::deployment::{Blueprint, UnstableReconfiguratorState};
use nexus_types::deployment::{BlueprintZoneDisposition, ExpectedVersion};
use nexus_types::deployment::{
    BlueprintZoneImageSource, PendingMgsUpdateDetails,
};
use nexus_types::deployment::{BlueprintZoneImageVersion, PendingMgsUpdate};
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_common::address::REPO_DEPOT_PORT;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Name;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_repl_utils::run_repl_from_file;
use omicron_repl_utils::run_repl_on_stdin;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::ReconfiguratorSimUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::VnicUuid;
use omicron_uuid_kinds::{BlueprintUuid, MupdateOverrideUuid};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::io::IsTerminal;
use std::num::ParseIntError;
use std::str::FromStr;
use swrite::{SWrite, swriteln};
use tabled::Tabled;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::ArtifactVersionError;
use update_common::artifacts::{ArtifactsWithPlan, ControlPlaneZonesMode};

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

        for (_, zone) in parent_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        {
            if let Some((external_ip, nic)) =
                zone.zone_type.external_networking()
            {
                builder
                    .add_omicron_zone_external_ip(zone.id, external_ip)
                    .context("adding omicron zone external IP")?;
                let nic = OmicronZoneNic {
                    // TODO-cleanup use `TypedUuid` everywhere
                    id: VnicUuid::from_untyped_uuid(nic.id),
                    mac: nic.mac,
                    ip: nic.ip,
                    slot: nic.slot,
                    primary: nic.primary,
                };
                builder
                    .add_omicron_zone_nic(zone.id, nic)
                    .context("adding omicron zone NIC")?;
            }
        }
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
        Commands::SledSetPolicy(args) => cmd_sled_set_policy(sim, args),
        Commands::SledUpdateRot(args) => cmd_sled_update_rot(sim, args),
        Commands::SledUpdateSp(args) => cmd_sled_update_sp(sim, args),
        Commands::SiloList => cmd_silo_list(sim),
        Commands::SiloAdd(args) => cmd_silo_add(sim, args),
        Commands::SiloRemove(args) => cmd_silo_remove(sim, args),
        Commands::InventoryList => cmd_inventory_list(sim),
        Commands::InventoryGenerate => cmd_inventory_generate(sim),
        Commands::BlueprintList => cmd_blueprint_list(sim),
        Commands::BlueprintBlippy(args) => cmd_blueprint_blippy(sim, args),
        Commands::BlueprintEdit(args) => cmd_blueprint_edit(sim, args),
        Commands::BlueprintPlan(args) => cmd_blueprint_plan(sim, args),
        Commands::BlueprintShow(args) => cmd_blueprint_show(sim, args),
        Commands::BlueprintDiff(args) => cmd_blueprint_diff(sim, args),
        Commands::BlueprintDiffDns(args) => cmd_blueprint_diff_dns(sim, args),
        Commands::BlueprintDiffInventory(args) => {
            cmd_blueprint_diff_inventory(sim, args)
        }
        Commands::BlueprintSave(args) => cmd_blueprint_save(sim, args),
        Commands::Show => cmd_show(sim),
        Commands::Set(args) => cmd_set(sim, args),
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
    /// set a sled's policy
    SledSetPolicy(SledSetPolicyArgs),
    /// simulate updating the sled's RoT versions
    SledUpdateRot(SledUpdateRotArgs),
    /// simulate updating the sled's SP versions
    SledUpdateSp(SledUpdateSpArgs),

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
    /// show differences between a blueprint and an inventory collection
    BlueprintDiffInventory(BlueprintDiffInventoryArgs),
    /// write one blueprint to a file
    BlueprintSave(BlueprintSaveArgs),

    /// show system properties
    Show,
    /// set system properties
    #[command(subcommand)]
    Set(SetArgs),

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
}

#[derive(Debug, Args)]
struct SledArgs {
    /// id of the sled
    sled_id: SledUuid,

    /// Filter to match sled ID against
    #[clap(short = 'F', long, value_enum, default_value_t = SledFilter::Commissioned)]
    filter: SledFilter,
}

#[derive(Debug, Args)]
struct SledSetPolicyArgs {
    /// id of the sled
    sled_id: SledUuid,

    /// The policy to set for the sled
    #[clap(value_enum)]
    policy: SledPolicyOpt,
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
struct SledUpdateSpArgs {
    /// id of the sled
    sled_id: SledUuid,

    /// sets the version reported for the SP active slot
    #[clap(long, required_unless_present_any = &["inactive"])]
    active: Option<ArtifactVersion>,

    /// sets the version reported for the SP inactive slot
    #[clap(long, required_unless_present_any = &["active"])]
    inactive: Option<ExpectedVersion>,
}

// TODO-K: Double check which of these need to be optional
#[derive(Debug, Args)]
struct SledUpdateRotArgs {
    /// id of the sled
    sled_id: SledUuid,

    /// whether we expect the "A" or "B" slot to be active
    #[clap(long)]
    active_slot: RotSlot,

    /// sets the version reported for the RoT slot a
    #[clap(long, required_unless_present_any = &["slot_b"])]
    slot_a: Option<ExpectedVersion>,

    /// sets the version reported for the RoT slot b
    #[clap(long, required_unless_present_any = &["slot_a"])]
    slot_b: Option<ExpectedVersion>,

    /// set the persistent boot preference written into the current
    /// authoritative CFPA page (ping or pong).
    /// Will default to the value of active_version when not set
    #[clap(long)]
    persistent_boot_preference: RotSlot,

    /// set the persistent boot preference written into the CFPA scratch
    /// page that will become the persistent boot preference in the authoritative
    /// CFPA page upon reboot, unless CFPA update of the authoritative page fails
    /// for some reason
    #[clap(long)]
    pending_persistent_boot_preference: Option<RotSlot>,

    /// override persistent preference selection for a single boot
    #[clap(long)]
    transient_boot_preference: Option<RotSlot>,
}

#[derive(Debug, Args)]
struct SledRemoveArgs {
    /// id of the sled
    sled_id: SledUuid,
}

#[derive(Debug, Args)]
struct SiloAddRemoveArgs {
    /// name of the silo
    silo_name: Name,
}

#[derive(Debug, Args)]
struct InventoryArgs {
    /// id of the inventory collection to use in planning
    collection_id: CollectionUuid,
}

#[derive(Debug, Args)]
struct BlueprintPlanArgs {
    /// id of the blueprint on which this one will be based, "latest", or
    /// "target"
    parent_blueprint_id: BlueprintIdOpt,
    /// id of the inventory collection to use in planning
    ///
    /// Must be provided unless there is only one collection in the loaded
    /// state.
    collection_id: Option<CollectionUuid>,
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
        sled_id: SledUuid,
    },
    /// add a CockroachDB instance to a particular sled
    AddCockroach { sled_id: SledUuid },
    /// set the image source for a zone
    SetZoneImage {
        /// id of zone whose image to set
        zone_id: OmicronZoneUuid,
        #[command(subcommand)]
        image_source: ImageSourceArgs,
    },
    /// set the remove_mupdate_override field for a sled
    SetRemoveMupdateOverride {
        /// sled to set the field on
        sled_id: SledUuid,

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
    ExpungeZone { zone_id: OmicronZoneUuid },
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
}

#[derive(Debug, Subcommand)]
enum BlueprintEditDebugCommands {
    /// remove a sled from the blueprint
    ///
    /// This bypasses expungement and decommissioning checks, and simply drops
    /// the sled from the blueprint.
    RemoveSled {
        /// the sled to remove
        sled: SledUuid,
    },

    /// Bump a sled's generation number, even if nothing else about the sled has
    /// changed.
    ForceSledGenerationBump {
        /// the sled to bump the sled-agent generation number of
        sled: SledUuid,
    },
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

impl From<BlueprintIdOpt> for BlueprintId {
    fn from(value: BlueprintIdOpt) -> Self {
        match value {
            BlueprintIdOpt::Latest => BlueprintId::Latest,
            BlueprintIdOpt::Target => BlueprintId::Target,
            BlueprintIdOpt::Id(id) => BlueprintId::Id(id),
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
        #[clap(value_parser = parse_blueprint_zone_image_version)]
        version: BlueprintZoneImageVersion,
        hash: ArtifactHash,
    },
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

fn parse_blueprint_zone_image_version(
    version: &str,
) -> Result<BlueprintZoneImageVersion, ArtifactVersionError> {
    // Treat the literal string "unknown" as an unknown version.
    if version == "unknown" {
        return Ok(BlueprintZoneImageVersion::Unknown);
    }

    Ok(BlueprintZoneImageVersion::Available {
        version: version.parse::<ArtifactVersion>()?,
    })
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
struct BlueprintDiffInventoryArgs {
    /// id of the inventory collection
    collection_id: CollectionUuid,
    /// id of the blueprint, "latest", or "target"
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
    /// system's external DNS zone name (suffix)
    ExternalDnsZoneName { zone_name: String },
    /// system target release
    TargetRelease {
        /// TUF repo containing release artifacts
        filename: Utf8PathBuf,
    },
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

    /// Do not create zones in the example system.
    #[clap(short = 'Z', long)]
    no_zones: bool,

    /// Do not create entries for disks in the blueprint.
    #[clap(long)]
    no_disks_in_blueprint: bool,
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
    let new_sled = SledBuilder::new().id(sled_id).npools(add.ndisks);
    state.system_mut().description_mut().sled(new_sled)?;
    sim.commit_and_bump(
        format!("reconfigurator-cli sled-add: {sled_id}"),
        state,
    );

    Ok(Some(format!("added sled {}", sled_id)))
}

fn cmd_sled_remove(
    sim: &mut ReconfiguratorSim,
    args: SledRemoveArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let sled_id = args.sled_id;
    state
        .system_mut()
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
    let sled_id = args.sled_id;
    let sp_active_version = description.sled_sp_active_version(sled_id)?;
    let sp_inactive_version = description.sled_sp_inactive_version(sled_id)?;
    let rot_active_slot = description.sled_rot_active_slot(sled_id)?;
    let rot_slot_a_version = description.sled_rot_slot_a_version(sled_id)?;
    let rot_slot_b_version = description.sled_rot_slot_b_version(sled_id)?;
    let planning_input = description
        .to_planning_input_builder()
        .context("failed to generate planning_input builder")?
        .build();
    let sled = planning_input.sled_lookup(args.filter, sled_id)?;
    let sled_resources = &sled.resources;
    let mut s = String::new();
    swriteln!(s, "sled {}", sled_id);
    swriteln!(s, "serial {}", sled.baseboard_id.serial_number);
    swriteln!(s, "subnet {}", sled_resources.subnet.net());
    swriteln!(s, "SP active version:   {:?}", sp_active_version);
    swriteln!(s, "SP inactive version: {:?}", sp_inactive_version);
    swriteln!(s, "RoT active slot: {}", rot_active_slot);
    // TODO-K: Include all other RoT settings?
    swriteln!(s, "RoT slot A version: {:?}", rot_slot_a_version);
    swriteln!(s, "RoT slot B version: {:?}", rot_slot_b_version);
    swriteln!(s, "zpools ({}):", sled_resources.zpools.len());
    for (zpool, disk) in &sled_resources.zpools {
        swriteln!(s, "    {:?}", zpool);
        swriteln!(s, "    {:?}", disk);
    }
    Ok(Some(s))
}

fn cmd_sled_set_policy(
    sim: &mut ReconfiguratorSim,
    args: SledSetPolicyArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    state
        .system_mut()
        .description_mut()
        .sled_set_policy(args.sled_id, args.policy.into())?;
    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-set-policy: {} to {}",
            args.sled_id, args.policy,
        ),
        state,
    );
    Ok(Some(format!("set sled {} policy to {}", args.sled_id, args.policy)))
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
    state.system_mut().description_mut().sled_update_sp_versions(
        args.sled_id,
        args.active,
        args.inactive,
    )?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-sp: {}: {}",
            args.sled_id,
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!(
        "set sled {} SP versions: {}",
        args.sled_id,
        labels.join(", ")
    )))
}

fn cmd_sled_update_rot(
    sim: &mut ReconfiguratorSim,
    args: SledUpdateRotArgs,
) -> anyhow::Result<Option<String>> {
    let mut labels = Vec::new();

    labels.push(format!("active slot -> {}", &args.active_slot));
    if let Some(slot_a) = &args.slot_a {
        labels.push(format!("slot a -> {}", slot_a));
    }
    if let Some(slot_b) = &args.slot_b {
        labels.push(format!("slot b -> {}", slot_b));
    }
    // TODO-K: Do I need these settings as well?
    labels.push(format!(
        "persistent boot preference -> {}",
        &args.persistent_boot_preference
    ));
    if let Some(pending_persistent_boot_preference) =
        &args.pending_persistent_boot_preference
    {
        labels.push(format!(
            "pending persistent boot preference -> {}",
            pending_persistent_boot_preference
        ));
    }
    labels.push(format!(
        "pending persistent boot preference -> {}",
        &args.persistent_boot_preference
    ));
    if let Some(transient_boot_preference) = &args.transient_boot_preference {
        labels.push(format!(
            "transient boot preference -> {}",
            transient_boot_preference
        ));
    }

    assert!(
        !labels.is_empty(),
        "clap configuration requires that at least one argument is specified"
    );

    let mut state = sim.current_state().to_mut();
    state.system_mut().description_mut().sled_update_rot_versions(
        args.sled_id,
        args.slot_a,
        args.slot_b,
    )?;

    sim.commit_and_bump(
        format!(
            "reconfigurator-cli sled-update-rot: {}: {}",
            args.sled_id,
            labels.join(", "),
        ),
        state,
    );

    Ok(Some(format!(
        // TODO-K: Is "RoT settings" what I want here?
        "set sled {} RoT settings: {}",
        args.sled_id,
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
    let report =
        Blippy::new(&blueprint).into_report(BlippyReportSortKey::Severity);
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
    let collection_id = args.collection_id;
    let parent_blueprint = system.get_blueprint(&parent_blueprint_id)?;
    let collection = match collection_id {
        Some(collection_id) => system.get_collection(collection_id)?,
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
    )
    .context("creating planner")?
    .with_rng(rng);

    let blueprint = planner.plan().context("generating blueprint")?;
    let rv = format!(
        "generated blueprint {} based on parent blueprint {}",
        blueprint.id, parent_blueprint.id,
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
        &latest_collection,
        creator,
    )
    .context("creating blueprint builder")?;
    builder.set_rng(rng);

    if let Some(comment) = args.comment {
        builder.comment(comment);
    }

    let label = match args.edit_command {
        BlueprintEditCommands::AddNexus { sled_id } => {
            builder
                .sled_add_zone_nexus(sled_id)
                .context("failed to add Nexus zone")?;
            format!("added Nexus zone to sled {}", sled_id)
        }
        BlueprintEditCommands::AddCockroach { sled_id } => {
            builder
                .sled_add_zone_cockroachdb(sled_id)
                .context("failed to add CockroachDB zone")?;
            format!("added CockroachDB zone to sled {}", sled_id)
        }
        BlueprintEditCommands::SetRemoveMupdateOverride { sled_id, value } => {
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
        BlueprintEditCommands::ExpungeZone { zone_id } => {
            let sled_id = sled_with_zone(&builder, &zone_id)?;
            builder
                .sled_expunge_zone(sled_id, zone_id)
                .context("failed to expunge zone")?;
            format!("expunged zone {zone_id} from sled {sled_id}")
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
                } => PendingMgsUpdateDetails::Sp {
                    expected_active_version,
                    expected_inactive_version,
                },
            };

            let artifact_version = ArtifactVersion::new(version)
                .context("parsing artifact version")?;

            let update = PendingMgsUpdate {
                baseboard_id: baseboard_id.clone(),
                sp_type: sp.sp_type,
                slot_id: u32::from(sp.sp_slot),
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
            builder.debug_sled_remove(sled)?;
            format!("debug: removed sled {sled} from blueprint")
        }
        BlueprintEditCommands::Debug {
            command:
                BlueprintEditDebugCommands::ForceSledGenerationBump { sled },
        } => {
            builder.debug_sled_force_generation_bump(sled)?;
            format!("debug: forced sled {sled} generation bump")
        }
    };

    let mut new_blueprint = builder.build();

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
    let internal_dns_config1 = blueprint_internal_dns_config(
        &blueprint1,
        &sleds_by_id,
        &Default::default(),
    )?;
    let internal_dns_config2 = blueprint_internal_dns_config(
        &blueprint2,
        &sleds_by_id,
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
    );
    let external_dns_config2 = blueprint_external_dns_config(
        &blueprint2,
        state.config().silo_names(),
        external_dns_zone_name.to_owned(),
    );
    let dns_diff = DnsDiff::new(&external_dns_config1, &external_dns_config2)
        .context("failed to assemble external DNS diff")?;
    swriteln!(rv, "external DNS:\n{}", dns_diff);

    Ok(Some(rv))
}

fn make_sleds_by_id(
    system: &SystemDescription,
) -> Result<BTreeMap<SledUuid, execution::Sled>, anyhow::Error> {
    let collection = system
        .to_collection_builder()
        .context(
            "unexpectedly failed to create collection for current set of sleds",
        )?
        .build();
    let sleds_by_id: BTreeMap<_, _> = collection
        .sled_agents
        .iter()
        .map(|(sled_id, sled_agent_info)| {
            let sled = execution::Sled::new(
                *sled_id,
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                },
                sled_agent_info.sled_agent_address,
                REPO_DEPOT_PORT,
                sled_agent_info.sled_role,
            );
            (*sled_id, sled)
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

    let blueprint_dns_zone = match dns_group {
        CliDnsGroup::Internal => {
            let sleds_by_id = make_sleds_by_id(state.system().description())?;
            blueprint_internal_dns_config(
                blueprint,
                &sleds_by_id,
                &Default::default(),
            )?
        }
        CliDnsGroup::External => blueprint_external_dns_config(
            blueprint,
            state.config().silo_names(),
            state.config().external_dns_zone_name().to_owned(),
        ),
    };

    let existing_dns_zone = existing_dns_config.sole_zone()?;
    let dns_diff = DnsDiff::new(&existing_dns_zone, &blueprint_dns_zone)
        .context("failed to assemble DNS diff")?;
    Ok(Some(dns_diff.to_string()))
}

fn cmd_blueprint_diff_inventory(
    sim: &mut ReconfiguratorSim,
    args: BlueprintDiffInventoryArgs,
) -> anyhow::Result<Option<String>> {
    let collection_id = args.collection_id;
    let blueprint_id = args.blueprint_id;

    let state = sim.current_state();
    let _collection = state.system().get_collection(collection_id)?;
    let _blueprint =
        state.system().resolve_and_get_blueprint(blueprint_id.into())?;
    // See https://github.com/oxidecomputer/omicron/issues/7242
    // let diff = blueprint.diff_since_collection(&collection);
    // Ok(Some(diff.display().to_string()))
    bail!("Not Implemented")
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
    match target_release {
        Some(tuf_desc) => {
            swriteln!(
                s,
                "target release: {} ({})",
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
        None => {
            swriteln!(s, "target release: unset");
        }
    }

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
                .target_nexus_zone_count(usize::from(num_nexus));
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
            let file = std::fs::File::open(&filename)
                .with_context(|| format!("open {:?}", filename))?;
            let buf = std::io::BufReader::new(file);
            let rt = tokio::runtime::Runtime::new()
                .context("creating tokio runtime")?;
            // We're not using the repo hash here.  Make one up.
            let repo_hash = ArtifactHash([0; 32]);
            let artifacts_with_plan = rt.block_on(async {
                ArtifactsWithPlan::from_zip(
                    buf,
                    None,
                    repo_hash,
                    ControlPlaneZonesMode::Split,
                    &sim.log,
                )
                .await
                .with_context(|| format!("unpacking {:?}", filename))
            })?;
            let description = artifacts_with_plan.description().clone();
            drop(artifacts_with_plan);
            state
                .system_mut()
                .description_mut()
                .set_target_release(Some(description));
            format!("set target release based on {}", filename)
        }
    };

    sim.commit_and_bump(format!("reconfigurator-cli set: {}", rv), state);
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

    let (example, blueprint) =
        ExampleSystemBuilder::new_with_rng(&sim.log, rng)
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
            .create_zones(!args.no_zones)
            .create_disks_in_blueprint(!args.no_disks_in_blueprint)
            .build();

    // Generate the internal and external DNS configs based on the blueprint.
    let sleds_by_id = make_sleds_by_id(&example.system)?;
    let internal_dns = blueprint_internal_dns_config(
        &blueprint,
        &sleds_by_id,
        &Default::default(),
    )?;
    let external_dns_zone_name =
        state.config_mut().external_dns_zone_name().to_owned();
    let external_dns = blueprint_external_dns_config(
        &blueprint,
        state.config_mut().silo_names(),
        external_dns_zone_name,
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
