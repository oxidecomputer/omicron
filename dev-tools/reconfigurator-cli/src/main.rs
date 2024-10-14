// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! developer REPL for driving blueprint planning

use anyhow::{anyhow, bail, Context};
use camino::Utf8PathBuf;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::ValueEnum;
use clap::{Args, Parser, Subcommand};
use internal_dns_types::diff::DnsDiff;
use nexus_inventory::CollectionBuilder;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::blueprint_builder::EnsureMultiple;
use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::system::{SledBuilder, SystemDescription};
use nexus_reconfigurator_simulation::MutableSimState;
use nexus_reconfigurator_simulation::SimState;
use nexus_reconfigurator_simulation::Simulator;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::execution;
use nexus_types::deployment::execution::blueprint_external_dns_config;
use nexus_types::deployment::execution::blueprint_internal_dns_config;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::OmicronZoneNic;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::{Blueprint, UnstableReconfiguratorState};
use omicron_common::api::external::Generation;
use omicron_common::api::external::Name;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::ReconfiguratorSimUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::VnicUuid;
use reedline::{Reedline, Signal};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::BufRead;
use swrite::{swriteln, SWrite};
use tabled::Tabled;
use uuid::Uuid;

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

    fn commit_and_bump(&mut self, description: String, state: MutableSimState) {
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

        for (_, zone) in
            parent_blueprint.all_omicron_zones(BlueprintZoneFilter::All)
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
struct CmdReconfiguratorSim {
    input_file: Option<Utf8PathBuf>,

    /// The RNG seed to initialize the simulator with.
    #[clap(long)]
    seed: Option<String>,
}

// REPL implementation

fn main() -> anyhow::Result<()> {
    let cmd = CmdReconfiguratorSim::parse();

    let log = dropshot::ConfigLogging::StderrTerminal {
        level: dropshot::ConfigLoggingLevel::Info,
    }
    .to_logger("reconfigurator-sim")
    .context("creating logger")?;

    let seed_provided = cmd.seed.is_some();
    let mut sim = ReconfiguratorSim::new(log, cmd.seed);
    if seed_provided {
        println!("using provided RNG seed: {}", sim.sim.initial_seed());
    } else {
        println!("generated RNG seed: {}", sim.sim.initial_seed());
    }

    if let Some(input_file) = cmd.input_file {
        let file = std::fs::File::open(&input_file)
            .with_context(|| format!("open {:?}", &input_file))?;
        let bufread = std::io::BufReader::new(file);
        for maybe_buffer in bufread.lines() {
            let buffer = maybe_buffer
                .with_context(|| format!("read {:?}", &input_file))?;
            println!("> {}", buffer);
            match process_entry(&mut sim, buffer) {
                LoopResult::Continue => (),
                LoopResult::Bail(error) => return Err(error),
            }
            println!("");
        }
    } else {
        let mut ed = Reedline::create();
        let prompt = reedline::DefaultPrompt::new(
            reedline::DefaultPromptSegment::Empty,
            reedline::DefaultPromptSegment::Empty,
        );
        loop {
            match ed.read_line(&prompt) {
                Ok(Signal::Success(buffer)) => {
                    match process_entry(&mut sim, buffer) {
                        LoopResult::Continue => (),
                        LoopResult::Bail(error) => return Err(error),
                    }
                }
                Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => break,
                Err(error) => {
                    bail!("reconfigurator-cli: unexpected error: {:#}", error);
                }
            }
        }
    }

    Ok(())
}

/// Describes next steps after evaluating one "line" of user input
///
/// This could just be `Result`, but it's easy to misuse that here because
/// _commands_ might fail all the time without needing to bail out of the REPL.
/// We use a separate type for clarity about what success/failure actually
/// means.
enum LoopResult {
    /// Show the prompt and accept another command
    Continue,

    /// Exit the REPL with a fatal error
    Bail(anyhow::Error),
}

/// Processes one "line" of user input.
fn process_entry(sim: &mut ReconfiguratorSim, entry: String) -> LoopResult {
    // If no input was provided, take another lap (print the prompt and accept
    // another line).  This gets handled specially because otherwise clap would
    // treat this as a usage error and print a help message, which isn't what we
    // want here.
    if entry.trim().is_empty() {
        return LoopResult::Continue;
    }

    // Parse the line of input as a REPL command.
    //
    // Using `split_whitespace()` like this is going to be a problem if we ever
    // want to support arguments with whitespace in them (using quotes).  But
    // it's good enough for now.
    let parts = entry.split_whitespace();
    let parsed_command = TopLevelArgs::command()
        .multicall(true)
        .try_get_matches_from(parts)
        .and_then(|matches| TopLevelArgs::from_arg_matches(&matches));
    let command = match parsed_command {
        Err(error) => {
            // We failed to parse the command.  Print the error.
            return match error.print() {
                // Assuming that worked, just take another lap.
                Ok(_) => LoopResult::Continue,
                // If we failed to even print the error, that itself is a fatal
                // error.
                Err(error) => LoopResult::Bail(
                    anyhow!(error).context("printing previous error"),
                ),
            };
        }
        Ok(TopLevelArgs { command }) => command,
    };

    // Dispatch to the command's handler.
    let cmd_result = match command {
        Commands::SledList => cmd_sled_list(sim),
        Commands::SledAdd(args) => cmd_sled_add(sim, args),
        Commands::SledShow(args) => cmd_sled_show(sim, args),
        Commands::SiloList => cmd_silo_list(sim),
        Commands::SiloAdd(args) => cmd_silo_add(sim, args),
        Commands::SiloRemove(args) => cmd_silo_remove(sim, args),
        Commands::InventoryList => cmd_inventory_list(sim),
        Commands::InventoryGenerate => cmd_inventory_generate(sim),
        Commands::BlueprintList => cmd_blueprint_list(sim),
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

    match cmd_result {
        Err(error) => println!("error: {:#}", error),
        Ok(Some(s)) => println!("{}", s),
        Ok(None) => (),
    }

    LoopResult::Continue
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
    /// show details about one sled
    SledShow(SledArgs),

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
    /// id of the blueprint on which this one will be based
    parent_blueprint_id: Uuid,
    /// id of the inventory collection to use in planning
    collection_id: CollectionUuid,
}

#[derive(Debug, Args)]
struct BlueprintEditArgs {
    /// id of the blueprint to edit
    blueprint_id: Uuid,
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
    /// expunge a particular zone from a particular sled
    ExpungeZone { sled_id: SledUuid, zone_id: OmicronZoneUuid },
}

#[derive(Debug, Args)]
struct BlueprintArgs {
    /// id of the blueprint
    blueprint_id: Uuid,
}

#[derive(Debug, Args)]
struct BlueprintDiffDnsArgs {
    /// DNS group (internal or external)
    dns_group: CliDnsGroup,
    /// DNS version to diff against
    dns_version: u32,
    /// id of the blueprint
    blueprint_id: Uuid,
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
    /// id of the blueprint
    blueprint_id: Uuid,
}

#[derive(Debug, Args)]
struct BlueprintSaveArgs {
    /// id of the blueprint
    blueprint_id: Uuid,
    /// output file
    filename: Utf8PathBuf,
}

#[derive(Debug, Args)]
struct BlueprintDiffArgs {
    /// id of the first blueprint
    blueprint1_id: Uuid,
    /// id of the second blueprint
    blueprint2_id: Uuid,
}

#[derive(Debug, Subcommand)]
enum SetArgs {
    /// target number of Nexus instances (for planning)
    NumNexus { num_nexus: u16 },
    /// system's external DNS zone name (suffix)
    ExternalDnsZoneName { zone_name: String },
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
    let rows = planning_input.all_sled_resources(SledFilter::Commissioned).map(
        |(sled_id, sled_resources)| Sled {
            id: sled_id,
            subnet: sled_resources.subnet.net().to_string(),
            nzpools: sled_resources.zpools.len(),
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
    let new_sled = SledBuilder::new().id(sled_id);
    state.system_mut().description_mut().sled(new_sled)?;
    sim.commit_and_bump(
        format!("reconfigurator-cli sled-add: {sled_id}"),
        state,
    );

    // TODO: we should show the sled ID here
    Ok(Some("added sled".to_owned()))
}

fn cmd_sled_show(
    sim: &mut ReconfiguratorSim,
    args: SledArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let planning_input = state
        .system()
        .description()
        .to_planning_input_builder()
        .context("failed to generate planning_input builder")?
        .build();
    let sled_id = args.sled_id;
    let sled_resources =
        &planning_input.sled_lookup(args.filter, sled_id)?.resources;
    let mut s = String::new();
    swriteln!(s, "sled {}", sled_id);
    swriteln!(s, "subnet {}", sled_resources.subnet.net());
    swriteln!(s, "zpools ({}):", sled_resources.zpools.len());
    for (zpool, disk) in &sled_resources.zpools {
        swriteln!(s, "    {:?}", zpool);
        swriteln!(s, "    ↳ {:?}", disk);
    }
    Ok(Some(s))
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
        id: Uuid,
        parent: Cow<'static, str>,
        time_created: String,
    }

    let state = sim.current_state();

    let mut rows = state.system().all_blueprints().collect::<Vec<_>>();
    rows.sort_unstable_by_key(|blueprint| blueprint.time_created);
    let rows = rows.into_iter().map(|blueprint| BlueprintRow {
        id: blueprint.id,
        parent: blueprint
            .parent_blueprint_id
            .map(|s| Cow::Owned(s.to_string()))
            .unwrap_or(Cow::Borrowed("<none>")),
        time_created: humantime::format_rfc3339_millis(
            blueprint.time_created.into(),
        )
        .to_string(),
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_blueprint_plan(
    sim: &mut ReconfiguratorSim,
    args: BlueprintPlanArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let rng = state.rng_mut().next_blueprint_rng();
    let system = state.system_mut();

    let parent_blueprint_id = args.parent_blueprint_id;
    let collection_id = args.collection_id;
    let parent_blueprint = system.get_blueprint(parent_blueprint_id)?;
    let collection = system.get_collection(collection_id)?;

    let creator = "reconfigurator-sim";
    let planning_input = sim.planning_input(parent_blueprint)?;
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
        blueprint.id, parent_blueprint_id,
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
    let rng = state.rng_mut().next_blueprint_rng();
    let system = state.system_mut();

    let blueprint_id = args.blueprint_id;
    let blueprint = system.get_blueprint(blueprint_id)?;
    let creator = args.creator.as_deref().unwrap_or("reconfigurator-cli");
    let planning_input = sim.planning_input(blueprint)?;

    // TODO: We may want to do something other than just using the latest
    // collection? Using timestamps like this is somewhat dubious, especially
    // in the presence of merged data from cmd_load.
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
            let current = builder
                .sled_num_running_zones_of_kind(sled_id, ZoneKind::Nexus);
            let added = builder
                .sled_ensure_zone_multiple_nexus(sled_id, current + 1)
                .context("failed to add Nexus zone")?;
            assert_matches::assert_matches!(
                added,
                EnsureMultiple::Changed { added: 1, removed: 0 }
            );
            format!("added Nexus zone to sled {}", sled_id)
        }
        BlueprintEditCommands::AddCockroach { sled_id } => {
            let current = builder
                .sled_num_running_zones_of_kind(sled_id, ZoneKind::CockroachDb);
            let added = builder
                .sled_ensure_zone_multiple_cockroachdb(sled_id, current + 1)
                .context("failed to add CockroachDB zone")?;
            assert_matches::assert_matches!(
                added,
                EnsureMultiple::Changed { added: 1, removed: 0 }
            );
            format!("added CockroachDB zone to sled {}", sled_id)
        }
        BlueprintEditCommands::ExpungeZone { sled_id, zone_id } => {
            builder
                .sled_expunge_zone(sled_id, zone_id)
                .context("failed to expunge zone")?;
            format!("expunged zone {zone_id} from sled {sled_id}")
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
        "blueprint {} created from blueprint {}: {}",
        new_blueprint.id, blueprint_id, label
    );
    system.add_blueprint(new_blueprint)?;

    sim.commit_and_bump("reconfigurator-cli blueprint-edit".to_owned(), state);
    Ok(Some(rv))
}

fn cmd_blueprint_show(
    sim: &mut ReconfiguratorSim,
    args: BlueprintArgs,
) -> anyhow::Result<Option<String>> {
    let state = sim.current_state();
    let blueprint = state.system().get_blueprint(args.blueprint_id)?;
    Ok(Some(format!("{}", blueprint.display())))
}

fn cmd_blueprint_diff(
    sim: &mut ReconfiguratorSim,
    args: BlueprintDiffArgs,
) -> anyhow::Result<Option<String>> {
    let mut rv = String::new();
    let blueprint1_id = args.blueprint1_id;
    let blueprint2_id = args.blueprint2_id;

    let state = sim.current_state();
    let blueprint1 = state.system().get_blueprint(blueprint1_id)?;
    let blueprint2 = state.system().get_blueprint(blueprint2_id)?;

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
                sled_agent_info.sled_agent_address,
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
    let blueprint = state.system().get_blueprint(blueprint_id)?;

    let existing_dns_config = match dns_group {
        CliDnsGroup::Internal => state.system().get_internal_dns(dns_version),
        CliDnsGroup::External => state.system().get_external_dns(dns_version),
    }
    .ok_or_else(|| {
        anyhow!("no such {:?} DNS version: {}", dns_group, dns_version)
    })?;

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
    let collection = state.system().get_collection(collection_id)?;
    let blueprint = state.system().get_blueprint(blueprint_id)?;
    let diff = blueprint.diff_since_collection(&collection);
    Ok(Some(diff.display().to_string()))
}

fn cmd_blueprint_save(
    sim: &mut ReconfiguratorSim,
    args: BlueprintSaveArgs,
) -> anyhow::Result<Option<String>> {
    let blueprint_id = args.blueprint_id;

    let state = sim.current_state();
    let blueprint = state.system().get_blueprint(blueprint_id)?;

    let output_path = &args.filename;
    let output_str = serde_json::to_string_pretty(&blueprint)
        .context("serializing blueprint")?;
    std::fs::write(&output_path, &output_str)
        .with_context(|| format!("write {:?}", output_path))?;
    Ok(Some(format!("saved blueprint {} to {:?}", blueprint_id, output_path)))
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
    do_print_properties(&mut s, state);
    swriteln!(
        s,
        "target number of Nexus instances: {}",
        state
            .config()
            .num_nexus()
            .map_or_else(|| "default".to_owned(), |n| n.to_string())
    );
    Ok(Some(s))
}

// TODO: consider moving this to a method on `SimState`.
fn do_print_properties(s: &mut String, state: &SimState) {
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
}

fn cmd_set(
    sim: &mut ReconfiguratorSim,
    args: SetArgs,
) -> anyhow::Result<Option<String>> {
    let mut state = sim.current_state().to_mut();
    let rv = match args {
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
    let input_path = args.filename;
    let collection_id = args.collection_id;
    let loaded = read_file(&input_path)?;

    let mut state = sim.current_state().to_mut();
    let result = state.merge_serializable(loaded, collection_id)?;

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

    if !result.notices.is_empty() {
        swriteln!(s, "notices:");
        for notice in result.notices {
            swriteln!(s, "  {}", notice);
        }
    }

    do_print_properties(&mut s, sim.current_state());
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
             loading an example system"
        );
    }

    // Generate the example system.
    match args.seed {
        Some(seed) => {
            // In this case, reset the RNG state to the provided seed.
            swriteln!(s, "setting new RNG seed: {}", seed,);
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
