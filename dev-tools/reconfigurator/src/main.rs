// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, bail, ensure, Context};
use nexus_deployment::blueprint_builder::BlueprintBuilder;
use nexus_deployment::planner::Planner;
use nexus_deployment::system::{
    SledBuilder, SledHwInventory, SystemDescription,
};
use nexus_types::deployment::{Blueprint, UnstableReconfiguratorState};
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use reedline_repl_rs::clap::{Arg, ArgMatches, Command};
use reedline_repl_rs::Repl;
use swrite::{swriteln, SWrite};
use tabled::Tabled;
use uuid::Uuid;

// Current status:
// - starting to flesh out the basic CRUD stuff.  it kind of works.  it's hard
//   to really see it because I haven't fleshed out "show".  save/load works.
// - I think it's finally time to switch to using the derive version of clap +
//   reedline directly.  This is in my way right now because I'm getting back
//   errors that I cannot see.  See item below.
// - in a bit of a dead end in the demo flow because SystemDescription does
//   not put any zones onto anything, and build-blueprint-from-inventory
//   requires that there are zones.  options here include:
//   - create a function to create an initial Blueprint from a *blank* inventory
//     (or maybe a SystemBuilder? since RSS won't have an inventory) that does
//     what RSS would do (maybe even have RSS use this?)
//   - add support to SystemDescription for putting zones onto things
//     (not sure this is worth doing)
//   - build support for loading *saved* stuff from existing systems and only
//     support that (nope)
// - flesh out:
//   - "inventory show"
//   - "blueprint show"
//   - "inventory-diff-zones"
//   - "blueprint-diff-zones"
//   - "blueprint-diff-zones-from-inventory"
// - after I've done that, I should be able to walk through a pretty nice demo
//   of the form:
//   - configure some sleds
//   - generate inventory
//   - generate a "rack setup" blueprint over those sleds
//     - show it
//     - diff inventory -> blueprint
//
// At this point I'll want to be able to walk through multiple steps.  To do
// this, I need a way to generate an inventory from the result of _executing_ a
// blueprint.  I could:
//
// - call the real deployment code against simulated stuff
//   (sled agents + database)
//   This is appealing but in the limit may require quite a lot of Nexus to
//   work.  But can you even create a fake datastore outside the
//   nexus-db-queries crate?  If so, how much does this stuff assume stuff has
//   been set up by a real Nexus?  I'm fairly sure that DNS does assume this.
//   Plus, the sled info is stored in the database, so that means the sled
//   addresses would need to all be "localhost" instead of the more
//   realistic-looking addresses.
// - implement dry-run deployment code
//   - we have a basic idea how this might work
//     - dns: can directly report what it would do
//     - zones: can report what requests it would make
//     - external IP/NIC stuff: ??
//   - but then we also need a way to fake up an inventory from the result
//   - seems like this would require the alternate approach of implementing
//     methods on SystemBuilder to specify zones?  or would we use an
//     InventoryBuilder directly?
//
// With this in place, though, the demo could proceed:
//
// - generate inventory after initial setup
// - add sled
// - generate "add sled" blueprint
// - execute
// - regenerate inventory
// - repeat for multi-step process
//
// We could have a similar flow where we start not by generating a "rack setup"
// blueprint but by starting with an inventory that has zones in it.  (We might
// _get_ that through the above simulation.)
//
// At some point we can add:
//
// - omdb: read from database, save to same format of file
//
// Then we can see what would happen when we make changes to a production system
// by saving its state and loading it here.
//
// XXX-dap reedline-repl-rs nits
// - cannot turn off the date banner
// - cannot use structopt version of command definitions
// - when it prints errors, it doesn't print messages from causes, which is
//   terrible when it comes to anyhow
// - commands in help are not listed in alphabetical order nor the order in
//   which I added them

#[derive(Debug)]
struct ReconfiguratorSim {
    system: SystemDescription,
    collections: Vec<Collection>,
    blueprints: Vec<Blueprint>,
    log: slog::Logger,
}

fn main() -> anyhow::Result<()> {
    let log = dropshot::ConfigLogging::StderrTerminal {
        level: dropshot::ConfigLoggingLevel::Debug,
    }
    .to_logger("reconfigurator-sim")
    .context("creating logger")?;
    let sim = ReconfiguratorSim {
        system: SystemDescription::new(),
        collections: vec![],
        blueprints: vec![],
        log,
    };
    let mut repl = Repl::new(sim)
        .with_name("reconfigurator-sim")
        .with_description("simulate blueprint planning and execution")
        .with_partial_completions(false)
        .with_command(
            Command::new("sled-list").about("list sleds"),
            cmd_sled_list,
        )
        .with_command(
            Command::new("sled-add").about("add a new sled"),
            cmd_sled_add,
        )
        .with_command(
            Command::new("sled-show")
                .about("show details about a sled")
                .arg(Arg::new("sled_id").required(true)),
            cmd_sled_show,
        )
        .with_command(
            Command::new("inventory-list")
                .about("lists all inventory collections"),
            cmd_inventory_list,
        )
        .with_command(
            Command::new("inventory-generate").about(
                "generates an inventory collection from the configured \
                    sleds",
            ),
            cmd_inventory_generate,
        )
        .with_command(
            Command::new("blueprint-list").about("lists all blueprints"),
            cmd_blueprint_list,
        )
        .with_command(
            Command::new("blueprint-from-inventory")
                .about("generate an initial blueprint from inventory")
                .arg(Arg::new("collection_id").required(true)),
            cmd_blueprint_from_inventory,
        )
        .with_command(
            Command::new("blueprint-plan")
                .about("run planner to generate a new blueprint")
                .arg(Arg::new("parent_blueprint_id").required(true))
                .arg(Arg::new("collection_id").required(true)),
            cmd_blueprint_plan,
        )
        .with_command(
            Command::new("save")
                .about("save all state to a file")
                .arg(Arg::new("filename").required(true)),
            cmd_save,
        )
        .with_command(
            Command::new("load")
                .about("load state from a file")
                .arg(Arg::new("filename").required(true))
                .arg(Arg::new("collection_id")),
            cmd_load,
        )
        .with_command(
            Command::new("file-contents")
                .about("show the contents of a given file")
                .arg(Arg::new("filename").required(true)),
            cmd_file_contents,
        );

    repl.run().context("unexpected failure")
}

fn cmd_sled_list(
    _args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct Sled {
        id: Uuid,
        nzpools: usize,
        subnet: String,
    }

    let policy = sim.system.to_policy().context("failed to generate policy")?;
    let rows = policy.sleds.iter().map(|(sled_id, sled_resources)| Sled {
        id: *sled_id,
        subnet: sled_resources.subnet.net().to_string(),
        nzpools: sled_resources.zpools.len(),
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_sled_add(
    _args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let mut sled = SledBuilder::new();
    let _ = sim.system.sled(sled).context("adding sled")?;
    Ok(Some(format!("added sled")))
}

fn cmd_sled_show(
    args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let policy = sim.system.to_policy().context("failed to generate policy")?;
    let sled_id: Uuid = args
        .get_one::<String>("sled_id")
        .ok_or_else(|| anyhow!("missing sled_id"))?
        .parse()
        .context("sled_id")?;
    let sled_resources = policy
        .sleds
        .get(&sled_id)
        .ok_or_else(|| anyhow!("no sled with id {:?}", sled_id))?;
    let mut s = String::new();
    swriteln!(s, "sled {}", sled_id);
    swriteln!(s, "subnet {}", sled_resources.subnet.net());
    swriteln!(s, "zpools ({}):", sled_resources.zpools.len());
    for z in &sled_resources.zpools {
        swriteln!(s, "    {:?}", z);
    }
    Ok(Some(s))
}

fn cmd_inventory_list(
    _args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct InventoryRow {
        id: Uuid,
    }

    let rows = sim
        .collections
        .iter()
        .map(|collection| InventoryRow { id: collection.id });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_inventory_generate(
    _args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let inventory =
        sim.system.to_collection().context("generating inventory")?;
    let rv = format!(
        "generated inventory collection {} from configured sleds",
        inventory.id
    );
    sim.collections.push(inventory);
    Ok(Some(rv))
}

fn cmd_blueprint_list(
    _args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BlueprintRow {
        id: Uuid,
    }

    let rows = sim
        .blueprints
        .iter()
        .map(|blueprint| BlueprintRow { id: blueprint.id });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    Ok(Some(table))
}

fn cmd_blueprint_from_inventory(
    args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let collection_id: Uuid = args
        .get_one::<String>("collection_id")
        .ok_or_else(|| anyhow!("missing collection_id"))?
        .parse()
        .context("collection_id")?;
    let collection = sim
        .collections
        .iter()
        .find(|c| c.id == collection_id)
        .ok_or_else(|| anyhow!("no such collection: {}", collection_id))?;
    let dns_version = Generation::new();
    let policy = sim.system.to_policy().context("generating policy")?;
    let creator = "reconfigurator-sim";
    let blueprint = BlueprintBuilder::build_initial_from_collection(
        collection,
        dns_version,
        &policy,
        creator,
    )
    .context("building collection")?;
    let rv = format!(
        "generated blueprint {} from inventory collection {}",
        blueprint.id, collection_id
    );
    sim.blueprints.push(blueprint);
    Ok(Some(rv))
}

fn cmd_blueprint_plan(
    args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let parent_blueprint_id: Uuid = args
        .get_one::<String>("parent_blueprint_id")
        .ok_or_else(|| anyhow!("missing parent_blueprint_id"))?
        .parse()
        .context("parent_blueprint_id")?;
    let collection_id: Uuid = args
        .get_one::<String>("collection_id")
        .ok_or_else(|| anyhow!("missing collection_id"))?
        .parse()
        .context("collection_id")?;
    let parent_blueprint = sim
        .blueprints
        .iter()
        .find(|b| b.id == parent_blueprint_id)
        .ok_or_else(|| anyhow!("no such blueprint: {}", parent_blueprint_id))?;
    let collection = sim
        .collections
        .iter()
        .find(|c| c.id == collection_id)
        .ok_or_else(|| anyhow!("no such collection: {}", collection_id))?;
    let dns_version = Generation::new();
    let policy = sim.system.to_policy().context("generating policy")?;
    let creator = "reconfigurator-sim";
    let planner = Planner::new_based_on(
        sim.log.clone(),
        parent_blueprint,
        dns_version,
        &policy,
        creator,
        collection,
    )
    .context("creating planner")?;
    let blueprint = planner.plan().context("generating blueprint")?;
    let rv = format!(
        "generated blueprint {} based on parent blueprint {}",
        blueprint.id, parent_blueprint_id,
    );
    sim.blueprints.push(blueprint);
    Ok(Some(rv))
}

fn cmd_save(
    args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let policy = sim.system.to_policy().context("creating policy")?;
    let saved = UnstableReconfiguratorState {
        policy,
        collections: sim.collections.clone(),
        blueprints: sim.blueprints.clone(),
    };

    let output_path_str: &String = args
        .get_one::<String>("filename")
        .ok_or_else(|| anyhow!("missing filename"))?;
    let output_path = camino::Utf8Path::new(output_path_str);

    // Check up front if the output path exists so that we don't clobber it.
    // This is not perfect because there's a time-of-check-to-time-of-use race,
    // but it seems better than nothing.
    match std::fs::metadata(&output_path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            bail!("stat {:?}: {:#}", output_path, e);
        }
        Ok(_) => {
            bail!("error: file {:?} already exists", output_path);
        }
    };

    let output_path_basename = output_path
        .file_name()
        .ok_or_else(|| anyhow!("unsupported path (no filename part)"))?;
    let tmppath =
        output_path.with_file_name(format!("{}.tmp", output_path_basename));
    let tmpfile = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmppath)
        .with_context(|| format!("open {:?}", tmppath))?;
    serde_json::to_writer_pretty(&tmpfile, &saved)
        .with_context(|| format!("writing to {:?}", tmppath))
        .unwrap_or_else(|e| panic!("{:#}", e));
    std::fs::rename(&tmppath, &output_path)
        .with_context(|| format!("mv {:?} {:?}", &tmppath, &output_path))?;
    Ok(Some(format!(
        "saved policy, collections, and blueprints to {:?}",
        output_path
    )))
}

fn read_file(
    input_path: &camino::Utf8Path,
) -> anyhow::Result<UnstableReconfiguratorState> {
    let file = std::fs::File::open(input_path)
        .with_context(|| format!("open {:?}", input_path))?;
    serde_json::from_reader(file)
        .with_context(|| format!("read {:?}", input_path))
}

fn cmd_load(
    args: ArgMatches,
    sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let input_path_str: &String = args
        .get_one::<String>("filename")
        .ok_or_else(|| anyhow!("missing filename"))?;
    let input_path = camino::Utf8Path::new(input_path_str);
    let collection_id = args
        .get_one::<String>("collection_id")
        .map(|s| s.parse::<Uuid>())
        .transpose()
        .context("parsing \"collection_id\" as uuid")?;
    let loaded = read_file(input_path)?;

    let mut s = String::new();

    let collection_id = match collection_id {
        Some(s) => s,
        None => {
            ensure!(
                loaded.collections.len() == 1,
                "no collection_id specified and file contains {} collections",
                loaded.collections.len()
            );
            loaded.collections[0].id
        }
    };

    swriteln!(
        s,
        "using collection {} as source of sled inventory data",
        collection_id
    );
    let primary_collection =
        loaded.collections.iter().find(|c| c.id == collection_id).ok_or_else(
            || {
                anyhow!(
                    "collection {} not found in file {:?}",
                    collection_id,
                    input_path
                )
            },
        )?;

    let current_policy = sim.system.to_policy().context("generating policy")?;
    for (sled_id, sled_resources) in loaded.policy.sleds {
        if current_policy.sleds.contains_key(&sled_id) {
            swriteln!(
                s,
                "saved sled {}: skipped (one with \
                the same id is already loaded)",
                sled_id
            );
            continue;
        }

        let Some(inventory_sled_agent) =
            primary_collection.sled_agents.get(&sled_id)
        else {
            swriteln!(
                s,
                "error: saved sled {}: no inventory found for sled agent in \
                collection {}",
                sled_id,
                collection_id
            );
            continue;
        };

        let inventory_sp = match &inventory_sled_agent.baseboard_id {
            Some(baseboard_id) => {
                let inv_sp = primary_collection
                    .sps
                    .get(baseboard_id)
                    .ok_or_else(|| {
                        anyhow!(
                            "error: saved sled {}: missing SP inventory",
                            sled_id
                        )
                    })?;
                let inv_rot = primary_collection
                    .rots
                    .get(baseboard_id)
                    .ok_or_else(|| {
                        anyhow!(
                            "error: saved sled {}: missing RoT inventory",
                            sled_id
                        )
                    })?;
                Some(SledHwInventory { baseboard_id, sp: inv_sp, rot: inv_rot })
            }
            None => None,
        };

        let result = sim.system.sled_full(
            sled_id,
            sled_resources,
            inventory_sp,
            inventory_sled_agent,
        );

        match result {
            Ok(_) => swriteln!(s, "saved sled {}: loaded", sled_id),
            Err(error) => {
                swriteln!(s, "error: saved sled {}: {:#}", sled_id, error)
            }
        };
    }

    // XXX-dap O(n^2)
    for collection in loaded.collections {
        if sim.collections.iter().any(|c| c.id == collection.id) {
            swriteln!(
                s,
                "saved collection {}: skipped (one with the \
                same id is already loaded)",
                collection.id
            );
        } else {
            swriteln!(s, "saved collection {}: loaded", collection.id);
            sim.collections.push(collection);
        }
    }

    // XXX-dap O(n^2)
    for blueprint in loaded.blueprints {
        if sim.blueprints.iter().any(|b| b.id == blueprint.id) {
            swriteln!(
                s,
                "saved blueprint {}: skipped (one with the \
                same id is already loaded)",
                blueprint.id
            );
        } else {
            swriteln!(s, "saved blueprint {}: loaded", blueprint.id);
            sim.blueprints.push(blueprint);
        }
    }

    swriteln!(s, "loaded data from {:?}", input_path);
    Ok(Some(s))
}

fn cmd_file_contents(
    args: ArgMatches,
    _sim: &mut ReconfiguratorSim,
) -> anyhow::Result<Option<String>> {
    let input_path_str: &String = args
        .get_one::<String>("filename")
        .ok_or_else(|| anyhow!("missing filename"))?;
    let input_path = camino::Utf8Path::new(input_path_str);
    let loaded = read_file(input_path)?;

    let mut s = String::new();

    for (sled_id, sled_resources) in loaded.policy.sleds {
        swriteln!(
            s,
            "sled: {} (subnet: {}, zpools: {})",
            sled_id,
            sled_resources.subnet.net(),
            sled_resources.zpools.len()
        );
    }

    for collection in loaded.collections {
        swriteln!(s, "collection: {}", collection.id);
    }

    for blueprint in loaded.blueprints {
        swriteln!(s, "blueprint:  {}", blueprint.id);
    }

    Ok(Some(s))
}
