// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, Context};
use nexus_deployment::blueprint_builder::BlueprintBuilder;
use nexus_deployment::planner::Planner;
use nexus_deployment::synthetic::{SledBuilder, SyntheticSystemBuilder};
use nexus_types::deployment::Blueprint;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use reedline_repl_rs::clap::{Arg, ArgMatches, Command};
use reedline_repl_rs::Repl;
use swrite::{swriteln, SWrite};
use tabled::Tabled;
use uuid::Uuid;

// Current status:
// - starting to flesh out the basic CRUD stuff.  it kind of works.  it's hard
//   to really see it because I haven't fleshed out "show".
// - I think it's finally time to switch to using the derive version of clap +
//   reedline directly.  This is in my way right now because I'm getting back
//   errors that I cannot see.  See item below.
// - in a bit of a dead end in the demo flow because SyntheticSystemBuilder does
//   not put any zones onto anything, and build-blueprint-from-inventory
//   requires that there are zones.  options here include:
//   - create a function to create an initial Blueprint from a *blank* inventory
//     (or maybe a SystemBuilder? since RSS won't have an inventory) that does
//     what RSS would do (maybe even have RSS use this?)
//   - add support to SyntheticSystemBuilder for putting zones onto things
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
//   work.  Can you even create a fake datastore outside the nexus-db-queries
//   crate?  If so, how much does this stuff assume stuff has been set up by a
//   real Nexus?
// - implement dry-run deployment code
//   - we have a basic idea how this might work
//   - but then we also need a way to fake up an inventory from the result
//   - seems like this would require the alternate approach of implementing
//     methods on SystemBuilder to specify zones
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
// XXX-dap reedline-repl-rs nits
// - cannot turn off the date banner
// - cannot use structopt version of command definitions
// - when it prints errors, it doesn't print messages from causes, which is
//   terrible when it comes to anyhow
// - commands in help are not listed in alphabetical order nor the order in
//   which I added them

#[derive(Debug)]
struct ReconfiguratorSim {
    system: SyntheticSystemBuilder,
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
        system: SyntheticSystemBuilder::new(),
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
