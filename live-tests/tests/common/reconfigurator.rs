// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers common to Reconfigurator tests

use anyhow::{Context, ensure};
use nexus_client::types::BlueprintTargetSet;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_types::deployment::{Blueprint, PlanningInput};
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::GenericUuid;
use slog::{debug, info};

/// Modify the system by editing the current target blueprint
///
/// More precisely, this function:
///
/// - fetches the current target blueprint
/// - creates a new BlueprintBuilder based on it
/// - invokes the caller's `edit_fn`, which may modify the builder however it
///   likes
/// - generates a new blueprint (thus based on the current target)
/// - uploads the new blueprint
/// - sets the new blueprint as the current target
/// - enables the new blueprint
///
/// ## Errors
///
/// This function fails if the current target blueprint is not already enabled.
/// That's because a disabled target blueprint means somebody doesn't want
/// Reconfigurator running or doesn't want it using that blueprint.  We don't
/// want the test to inadvertently override that behavior.  In a typical use
/// case, a developer enables the initial target blueprint before running these
/// tests and then doesn't need to think about it again for the lifetime of
/// their test environment.
pub async fn blueprint_edit_current_target(
    log: &slog::Logger,
    planning_input: &PlanningInput,
    collection: &Collection,
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
        .blueprint_view(target_blueprint.target_id.as_untyped_uuid())
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
        &collection,
        "test-suite",
        PlannerRng::from_entropy(),
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
