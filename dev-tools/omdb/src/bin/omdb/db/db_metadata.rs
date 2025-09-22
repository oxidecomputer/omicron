// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db db_metadata` subcommands

use super::display_option_blank;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use nexus_db_model::DbMetadataNexusState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use std::collections::BTreeMap;
use tabled::Tabled;

#[derive(Debug, Args, Clone)]
pub struct DbMetadataArgs {
    #[command(subcommand)]
    pub command: DbMetadataCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum DbMetadataCommands {
    #[clap(alias = "ls-nexus")]
    ListNexus,
}

// DB Metadata

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct DbMetadataNexusRow {
    id: OmicronZoneUuid,
    #[tabled(display_with = "display_option_blank")]
    last_drained_blueprint: Option<BlueprintUuid>,

    // Identifies the state we observe in the database
    state: String,

    // Identifies the state this Nexus is trying to achieve, based on the target
    // blueprint, if it's different from the current state
    #[tabled(display_with = "display_option_blank")]
    transitioning_to: Option<String>,
}

fn get_intended_nexus_state(
    bp_nexus_generation: Generation,
    bp_nexus_generation_by_zone: &BTreeMap<OmicronZoneUuid, Generation>,
    id: OmicronZoneUuid,
) -> Option<DbMetadataNexusState> {
    let Some(gen) = bp_nexus_generation_by_zone.get(&id) else {
        return None;
    };

    Some(if *gen < bp_nexus_generation {
        // This Nexus is either quiescing, or has already quiesced
        DbMetadataNexusState::Quiesced
    } else if *gen == bp_nexus_generation {
        // This Nexus is either active, or will become active once
        // the prior generation has quiesced
        DbMetadataNexusState::Active
    } else {
        // This Nexus is not ready to be run yet
        DbMetadataNexusState::NotYet
    })
}

fn get_nexus_state_transition(
    observed: DbMetadataNexusState,
    intended: Option<DbMetadataNexusState>,
) -> Option<String> {
    match (observed, intended) {
        (observed, Some(intended)) if observed == intended => None,
        (_, Some(intended)) => Some(intended.to_string()),
        (_, None) => Some("Unknown".to_string()),
    }
}

async fn get_db_metadata_nexus_rows(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> Result<Vec<DbMetadataNexusRow>, anyhow::Error> {
    let states = vec![
        DbMetadataNexusState::Active,
        DbMetadataNexusState::NotYet,
        DbMetadataNexusState::Quiesced,
    ];

    let nexus_generation_by_zone = blueprint
        .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
        .map(|(_, zone, nexus_zone)| (zone.id, nexus_zone.nexus_generation))
        .collect::<BTreeMap<_, _>>();

    Ok(datastore
        .get_db_metadata_nexus_in_state(opctx, states)
        .await?
        .into_iter()
        .map(|db_metadata_nexus| {
            let id = db_metadata_nexus.nexus_id();
            let last_drained_blueprint =
                db_metadata_nexus.last_drained_blueprint_id();
            let state = db_metadata_nexus.state().to_string();
            let intended_state = get_intended_nexus_state(
                blueprint.nexus_generation,
                &nexus_generation_by_zone,
                id,
            );

            let transitioning_to = get_nexus_state_transition(
                db_metadata_nexus.state(),
                intended_state,
            );

            DbMetadataNexusRow {
                id,
                last_drained_blueprint,
                state,
                transitioning_to,
            }
        })
        .collect())
}

pub async fn cmd_db_metadata_list_nexus(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let (_, current_target_blueprint) = datastore
        .blueprint_target_get_current_full(opctx)
        .await
        .context("loading current target blueprint")?;
    println!(
        "Target Blueprint {} @ nexus_generation: {}",
        current_target_blueprint.id, current_target_blueprint.nexus_generation
    );

    let rows: Vec<_> =
        get_db_metadata_nexus_rows(opctx, datastore, &current_target_blueprint)
            .await?;
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);

    Ok(())
}
