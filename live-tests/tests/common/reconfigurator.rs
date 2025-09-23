// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers common to Reconfigurator tests

use anyhow::{Context, anyhow, bail, ensure};
use nexus_client::types::{BackgroundTasksActivateRequest, BlueprintTargetSet};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_types::deployment::{Blueprint, BlueprintSource, PlanningInput};
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use slog::{debug, info};
use slog_error_chain::InlineErrorChain;
use std::time::Duration;

/// Return the current target blueprint
///
/// Also validates that it's enabled.  If an operator has disabled execution, we
/// don't want to proceed with tests.
pub async fn blueprint_load_target_enabled(
    log: &slog::Logger,
    nexus: &nexus_client::Client,
) -> Result<Blueprint, anyhow::Error> {
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
        "refusing to operate on a system with target blueprint disabled"
    );

    let blueprint = nexus
        .blueprint_view(target_blueprint.target_id.as_untyped_uuid())
        .await
        .context("fetch current target blueprint")?
        .into_inner();
    debug!(log, "fetched current target blueprint";
        "blueprint_id" => %target_blueprint.target_id
    );
    Ok(blueprint)
}

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
    let blueprint1 = blueprint_load_target_enabled(log, nexus).await?;

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
    let blueprint2 = builder.build(BlueprintSource::Test);
    info!(log, "assembled new blueprint based on target";
        "current_target_id" => %blueprint1.id,
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

/// Checks whether the given blueprint's sled configurations appear to be
/// propagated to all sleds.
///
/// If so, returns the inventory collection so that the caller can check
/// additional details if wanted.  If not or if we failed to determine the
/// answer, returns an error.
pub async fn blueprint_sled_configs_propagated(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> Result<Collection, anyhow::Error> {
    let log = &opctx.log;
    let latest_collection = datastore
        .inventory_get_latest_collection(opctx)
        .await
        .context("fetching latest collection")?
        .ok_or_else(|| anyhow!("have no inventory collections"))?;
    debug!(log, "got inventory"; "id" => %latest_collection.id);
    for (sled_id, sled_config) in &blueprint.sleds {
        if sled_config.state != SledState::Active {
            continue;
        }

        let agent = latest_collection
            .sled_agents
            .get(sled_id)
            .ok_or_else(|| anyhow!("sled {sled_id}: missing inventory"))?;
        let reconciled_config = &agent
            .last_reconciliation
            .as_ref()
            .ok_or_else(|| {
                anyhow!("sled {sled_id}: missing last_reconciliation")
            })?
            .last_reconciled_config;
        if reconciled_config.generation < sled_config.sled_agent_generation {
            bail!(
                "sled {sled_id}: last reconciled generation {}, waiting for {}",
                reconciled_config.generation,
                sled_config.sled_agent_generation
            );
        }
    }

    Ok(latest_collection)
}

/// Waits for the given blueprint's sled configurations to appear to be
/// propagated to all sleds.
///
/// Returns the inventory collection so that the caller can check additional
/// details if wanted.
pub async fn blueprint_wait_sled_configs_propagated(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus: &nexus_client::Client,
    timeout: Duration,
) -> Result<Collection, anyhow::Error> {
    wait_for_condition(
        || async {
            match blueprint_sled_configs_propagated(opctx, datastore, blueprint)
                .await
            {
                Ok(collection) => Ok(collection),
                Err(error) => {
                    debug!(
                        opctx.log,
                        "blueprint_wait_sled_configs_propagated";
                        InlineErrorChain::new(&*error)
                    );

                    // Activate the inventory collector.
                    info!(opctx.log, "activating inventory collector");
                    nexus
                        .bgtask_activate(&BackgroundTasksActivateRequest {
                            bgtask_names: vec![String::from(
                                "inventory_collection",
                            )],
                        })
                        .await
                        .expect("activating inventory background task");

                    // We don't use the variant of `CondCheckError` that carries
                    // a permanent error, so we need to put a type here.  We
                    // want the resulting error to impl `ToString`, so we need a
                    // type that impls that.  We pick `String`.
                    Err(CondCheckError::<String>::NotYet)
                }
            }
        },
        &Duration::from_millis(3000),
        &timeout,
    )
    .await
    .map_err(|error| {
        anyhow!(
            "waiting for blueprint {}'s sled configs to be propagated: {error}",
            blueprint.id
        )
    })
}
