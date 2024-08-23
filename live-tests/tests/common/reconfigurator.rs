// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers common to Reconfigurator tests

use anyhow::{ensure, Context};
use nexus_client::types::BlueprintTargetSet;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_types::deployment::{Blueprint, PlanningInput};
use slog::{debug, info};

pub async fn blueprint_edit_current_target(
    log: &slog::Logger,
    planning_input: &PlanningInput,
    nexus: &nexus_client::Client,
    edit_fn: &dyn Fn(&mut BlueprintBuilder) -> Result<(), anyhow::Error>,
) -> Result<(Blueprint, Blueprint), anyhow::Error> {
    // Fetch the current target configuration.
    info!(log, "editing current target blueprint");
    let target_blueprint = nexus
        .blueprint_target_view()
        .await
        .context("fetch current target config")?
        .into_inner();
    debug!(log, "found current target blueprint";
        "blueprint_id" => %target_blueprint.target_id
    );
    ensure!(
        target_blueprint.enabled,
        "refusing to modify a system with target blueprint disabled"
    );

    // Fetch the actual blueprint.
    let blueprint1 = nexus
        .blueprint_view(&target_blueprint.target_id)
        .await
        .context("fetch current target blueprint")?
        .into_inner();
    debug!(log, "fetched current target blueprint";
        "blueprint_id" => %target_blueprint.target_id
    );

    // Make a new builder based on that blueprint and use `edit_fn` to edit it.
    let mut builder = BlueprintBuilder::new_based_on(
        log,
        &blueprint1,
        &planning_input,
        "test-suite",
    )
    .context("creating BlueprintBuilder")?;

    edit_fn(&mut builder)?;

    // Assemble the new blueprint, import it, and make it the new target.
    let blueprint2 = builder.build();
    info!(log, "assembled new blueprint based on target";
        "current_target_id" => %target_blueprint.target_id,
        "new_blueprint_id" => %blueprint2.id,
    );
    nexus
        .blueprint_import(&blueprint2)
        .await
        .context("importing new blueprint")?;
    debug!(log, "imported new blueprint";
        "blueprint_id" => %blueprint2.id,
    );
    nexus
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: true,
            target_id: blueprint2.id,
        })
        .await
        .expect("setting new target");
    info!(log, "finished editing target blueprint";
        "old_target_id" => %blueprint1.id,
        "new_target_id" => %blueprint2.id,
    );

    Ok((blueprint1, blueprint2))
}
