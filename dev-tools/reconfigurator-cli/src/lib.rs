// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! developer REPL for driving blueprint planning

use anyhow::{Context, anyhow, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::ValueEnum;
use clap::{Args, Parser, Subcommand};
use iddqd::IdOrdMap;
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
use nexus_reconfigurator_planning::system::{
    SledBuilder, SledInventoryVisibility, SystemDescription,
};
use nexus_reconfigurator_simulation::{BlueprintId, CollectionId, SimState};
use nexus_reconfigurator_simulation::{SimStateBuilder, SimTufRepoSource};
use nexus_reconfigurator_simulation::{SimTufRepoDescription, Simulator};
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::execution;
use nexus_types::deployment::execution::blueprint_external_dns_config;
use nexus_types::deployment::execution::blueprint_internal_dns_config;
use nexus_types::deployment::{Blueprint, UnstableReconfiguratorState};
use nexus_types::deployment::{BlueprintArtifactVersion, PendingMgsUpdate};
use nexus_types::deployment::{BlueprintZoneDisposition, ExpectedVersion};
use nexus_types::deployment::{
    BlueprintZoneImageSource, PendingMgsUpdateDetails,
};
use nexus_types::deployment::{OmicronZoneNic, TargetReleaseDescription};
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_common::address::REPO_DEPOT_PORT;
use omicron_common::api::external::Name;
use omicron_common::api::external::{Generation, TufRepoDescription};
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
use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt::{self, Write};
use std::io::IsTerminal;
use std::num::ParseIntError;
use std::str::FromStr;
use swrite::{SWrite, swriteln};
use tabled::Tabled;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::ArtifactVersionError;
use tufaceous_lib::assemble::ArtifactManifest;
use update_common::artifacts::{
    ArtifactsWithPlan, ControlPlaneZonesMode, VerificationMode,
};

mod log_capture;

/// The default key for TUF repository generation.
///
/// This was randomly generated through a tufaceous invocation.
pub static DEFAULT_TUFACEOUS_KEY: &str = "ed25519:\
MFECAQEwBQYDK2VwBCIEIJ9CnAhwk8PPt1x8icu\
z9c12PdfCRHJpoUkuqJmIZ8GbgSEAbNGMpsHK5_w32\
qwYdZH_BeVssmKzQlFsnPuaiHx2hy0=";

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
        Commands::SledSet(args) => cmd_sled_set(sim, args),
        Commands::SledUpdateInstallDataset(args) => {
            cmd_sled_update_install_dataset(sim, args)
        }
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
    #[clap(flatten)]
    Visibility(SledSetVisibilityCommand),
}

#[derive(Debug, Args)]
struct SledSetPolicyArgs {
    /// the policy to set
    #[clap(value_enum)]
    policy: SledPolicyOpt,
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
struct InventoryArgs {
    /// id of the inventory collection to use in planning
    collection_id: CollectionUuid,
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
        "a" | "A" | "0" => Ok(M2Slot::A),
        "b" | "B" | "1" => Ok(M2Slot::B),
        _ => bail!("invalid slot `{slot}` (expected `a` or `b`)"),
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

    /// Do not create zones in the example system.
    #[clap(short = 'Z', long)]
    no_zones: bool,

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
    let sp_active_version = description.sled_sp_active_version(sled_id)?;
    let sp_inactive_version = description.sled_sp_inactive_version(sled_id)?;
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
    swriteln!(s, "SP active version:   {:?}", sp_active_version);
    swriteln!(s, "SP inactive version: {:?}", sp_inactive_version);
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
        BlueprintEditCommands::AddNexus { sled_id, image_source } => {
            let sled_id = sled_id.to_sled_id(system.description())?;
            let image_source = image_source_unwrap_or(
                image_source,
                &planning_input,
                ZoneKind::Nexus,
            )?;
            builder
                .sled_add_zone_nexus(sled_id, image_source)
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
            let description =
                extract_tuf_repo_description(&sim.log, &filename)?;
            state.system_mut().description_mut().set_target_release(
                TargetReleaseDescription::TufRepo(description),
            );
            format!("set target release based on {}", filename)
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

fn extract_tuf_repo_description(
    log: &slog::Logger,
    filename: &Utf8Path,
) -> anyhow::Result<TufRepoDescription> {
    let file = std::fs::File::open(filename)
        .with_context(|| format!("open {:?}", filename))?;
    let buf = std::io::BufReader::new(file);
    let rt =
        tokio::runtime::Runtime::new().context("creating tokio runtime")?;
    let repo_hash = ArtifactHash([0; 32]);
    let artifacts_with_plan = rt.block_on(async {
        ArtifactsWithPlan::from_zip(
            buf,
            None,
            repo_hash,
            ControlPlaneZonesMode::Split,
            VerificationMode::BlindlyTrustAnything,
            log,
        )
        .await
        .with_context(|| format!("unpacking {:?}", filename))
    })?;
    let description = artifacts_with_plan.description().clone();
    Ok(description)
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

    if output_path.exists() {
        bail!("output path `{output_path}` already exists");
    }

    // Just use a fixed key for now.
    //
    // In the future we may want to test changing the TUF key.
    let mut tufaceous_args = vec![
        "tufaceous",
        "--key",
        DEFAULT_TUFACEOUS_KEY,
        "assemble",
        manifest_path.as_str(),
        output_path.as_str(),
    ];
    if args.allow_non_semver {
        tufaceous_args.push("--allow-non-semver");
    }
    let args = tufaceous::Args::try_parse_from(tufaceous_args)
        .expect("args are valid so this shouldn't fail");
    let rt =
        tokio::runtime::Runtime::new().context("creating tokio runtime")?;
    rt.block_on(async move { args.exec(&sim.log).await })
        .context("error executing tufaceous assemble")?;

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
        .create_zones(!args.no_zones)
        .create_disks_in_blueprint(!args.no_disks_in_blueprint);
    for sled_policy in args.sled_policy {
        builder = builder
            .with_sled_policy(sled_policy.index, sled_policy.policy)
            .context("setting sled policy")?;
    }

    let (example, blueprint) = builder.build();

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
