// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_reconfigurator_planning` crate-level docs for background.

use anyhow::{anyhow, Context};
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::execution::*;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::views::SledState;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_zones::DeployZonesDone;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use update_engine::merge_anyhow_list;
use update_engine::StepSuccess;
use update_engine::StepWarning;

mod clickhouse;
mod cockroachdb;
mod datasets;
mod dns;
mod omicron_physical_disks;
mod omicron_zones;
mod sagas;
mod sled_state;
#[cfg(test)]
mod test_utils;

/// The result of calling [`realize_blueprint`] or
/// [`realize_blueprint_with_overrides`].
#[derive(Debug)]
#[must_use = "the output of realize_blueprint should probably be used"]
pub struct RealizeBlueprintOutput {
    /// Whether any sagas need to be reassigned to a new Nexus.
    pub needs_saga_recovery: bool,
}

/// Make one attempt to realize the given blueprint, meaning to take actions to
/// alter the real system to match the blueprint
///
/// The assumption is that callers are running this periodically or in a loop to
/// deal with transient errors or changes in the underlying system state.
pub async fn realize_blueprint(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    nexus_id: OmicronZoneUuid,
    sender: mpsc::Sender<Event>,
) -> Result<RealizeBlueprintOutput, anyhow::Error> {
    realize_blueprint_with_overrides(
        opctx,
        datastore,
        resolver,
        blueprint,
        nexus_id,
        &Default::default(),
        sender,
    )
    .await
}

pub async fn realize_blueprint_with_overrides(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    nexus_id: OmicronZoneUuid,
    overrides: &Overridables,
    sender: mpsc::Sender<Event>,
) -> Result<RealizeBlueprintOutput, anyhow::Error> {
    let opctx = opctx.child(BTreeMap::from([(
        "comment".to_string(),
        blueprint.comment.clone(),
    )]));

    info!(
        opctx.log,
        "attempting to realize blueprint";
        "blueprint_id" => %blueprint.id
    );

    let engine = UpdateEngine::new(&opctx.log, sender);

    register_zone_external_networking_step(
        &engine.for_component(ExecutionComponent::ExternalNetworking),
        &opctx,
        datastore,
        blueprint,
    );

    let sled_list = register_sled_list_step(
        &engine.for_component(ExecutionComponent::SledList),
        &opctx,
        datastore,
    )
    .into_shared();

    register_deploy_disks_step(
        &engine.for_component(ExecutionComponent::PhysicalDisks),
        &opctx,
        blueprint,
        sled_list.clone(),
    );

    register_deploy_datasets_step(
        &engine.for_component(ExecutionComponent::Datasets),
        &opctx,
        blueprint,
        sled_list.clone(),
    );

    let deploy_zones_done = register_deploy_zones_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        blueprint,
        sled_list.clone(),
    );

    register_plumb_firewall_rules_step(
        &engine.for_component(ExecutionComponent::FirewallRules),
        &opctx,
        datastore,
    );

    register_dns_records_step(
        &engine.for_component(ExecutionComponent::Dns),
        &opctx,
        datastore,
        blueprint,
        nexus_id,
        overrides,
        sled_list.clone(),
    );

    let deploy_zones_done = register_cleanup_expunged_zones_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        datastore,
        resolver,
        blueprint,
        deploy_zones_done,
    );

    register_decommission_sleds_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        datastore,
        blueprint,
    );

    register_decommission_disks_step(
        &engine.for_component(ExecutionComponent::PhysicalDisks),
        &opctx,
        datastore,
        blueprint,
    );

    register_deploy_clickhouse_cluster_nodes_step(
        &engine.for_component(ExecutionComponent::Clickhouse),
        &opctx,
        blueprint,
    );

    register_deploy_clickhouse_single_node_step(
        &engine.for_component(ExecutionComponent::Clickhouse),
        &opctx,
        blueprint,
    );

    let deploy_zones_done = register_support_bundle_failure_step(
        &engine.for_component(ExecutionComponent::SupportBundles),
        &opctx,
        datastore,
        blueprint,
        nexus_id,
        deploy_zones_done,
    );

    let reassign_saga_output = register_reassign_sagas_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        datastore,
        blueprint,
        nexus_id,
        deploy_zones_done,
    );

    let register_cockroach_output = register_cockroachdb_settings_step(
        &engine.for_component(ExecutionComponent::Cockroach),
        &opctx,
        datastore,
        blueprint,
    );

    let output = register_finalize_step(
        &engine.for_component(ExecutionComponent::Cockroach),
        reassign_saga_output,
        register_cockroach_output,
    );

    // All steps are registered, so execute the engine.
    let result = engine.execute().await?;

    Ok(output.into_value(result.token()).await)
}

// Convert a `Result<(), anyhow::Error>` nto a `StepResult` containing either a
// `StepSuccess` or `StepWarning` and wrap it in `Result::Ok`.
//
// This is necessary because we never want to return an error from execution.
// Doing so stops execution at the errored step and prevents other independent
// steps after the errored step from executing.
fn result_to_step_result(
    res: Result<(), anyhow::Error>,
) -> Result<StepResult<(), ReconfiguratorExecutionSpec>, anyhow::Error> {
    match res {
        Ok(_) => Ok(StepSuccess::new(()).build()),
        Err(e) => Ok(StepWarning::new((), e.to_string()).build()),
    }
}

/// We explicitly don't continue with execution after this step. It only talks
/// to CRDB, and if nexus cannot talk to CRDB, we probably shouldn't continue.
fn register_zone_external_networking_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) {
    // Deallocate external networking resources for non-externally-reachable
    // zones first. This will allow external networking resource allocation to
    // succeed if we are swapping an external IP between two zones (e.g., moving
    // a specific external IP from an old external DNS zone to a new one).
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Ensure external networking resources",
            move |_cx| async move {
                datastore
                    .blueprint_ensure_external_networking_resources(
                        &opctx, blueprint,
                    )
                    .await
                    .map_err(|err| anyhow!(err))?;

                StepSuccess::new(()).into()
            },
        )
        .register();
}

fn register_support_bundle_failure_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
    nexus_id: OmicronZoneUuid,
    deploy_zones_done: StepHandle<DeployZonesDone>,
) -> StepHandle<DeployZonesDone> {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Mark support bundles as failed if they rely on an expunged disk or sled",
            move |cx| async move {
                let done = deploy_zones_done.into_value(cx.token()).await;
                datastore
                    .support_bundle_fail_expunged(
                        &opctx, blueprint, nexus_id
                    )
                    .await
                    .map_err(|err| anyhow!(err))?;

                StepSuccess::new(done).into()
            },
        )
        .register()
}

fn register_sled_list_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
) -> StepHandle<Arc<BTreeMap<SledUuid, Sled>>> {
    registrar
        .new_step(
            ExecutionStepId::Fetch,
            "Fetch sled list",
            move |_cx| async move {
                let sleds_by_id: BTreeMap<SledUuid, _> = datastore
                    .sled_list_all_batched(&opctx, SledFilter::InService)
                    .await
                    .context("listing all sleds")?
                    .into_iter()
                    .map(|db_sled| {
                        (
                            SledUuid::from_untyped_uuid(db_sled.id()),
                            db_sled.into(),
                        )
                    })
                    .collect();

                StepSuccess::new(Arc::new(sleds_by_id)).into()
            },
        )
        .register()
}

fn register_deploy_disks_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
    sleds: SharedStepHandle<Arc<BTreeMap<SledUuid, Sled>>>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy physical disks",
            move |cx| async move {
                let sleds_by_id = sleds.into_value(cx.token()).await;
                let res = omicron_physical_disks::deploy_disks(
                    &opctx,
                    &sleds_by_id,
                    &blueprint.blueprint_disks,
                )
                .await
                .map_err(merge_anyhow_list);
                result_to_step_result(res)
            },
        )
        .register();
}

fn register_deploy_datasets_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
    sleds: SharedStepHandle<Arc<BTreeMap<SledUuid, Sled>>>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy datasets",
            move |cx| async move {
                let sleds_by_id = sleds.into_value(cx.token()).await;
                let res = datasets::deploy_datasets(
                    &opctx,
                    &sleds_by_id,
                    &blueprint.blueprint_datasets,
                )
                .await
                .map_err(merge_anyhow_list);
                result_to_step_result(res)
            },
        )
        .register();
}

fn register_deploy_zones_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
    sleds: SharedStepHandle<Arc<BTreeMap<SledUuid, Sled>>>,
) -> StepHandle<DeployZonesDone> {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy Omicron zones",
            move |cx| async move {
                let sleds_by_id = sleds.into_value(cx.token()).await;
                let done = omicron_zones::deploy_zones(
                    &opctx,
                    &sleds_by_id,
                    &blueprint.blueprint_zones,
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(done).into()
            },
        )
        .register()
}

fn register_plumb_firewall_rules_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
) {
    // After deploying omicron zones, we may need to refresh OPTE service
    // firewall rules. This is an idempotent operation, so we don't attempt to
    // optimize out calling it in unnecessary cases, although it is only needed
    // in cases where we've changed the set of services on one or more sleds.
    //
    // TODO-cleanup: We should trigger the `service-firewall-rules` RPW here
    // instead of doing this ourselves.
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Plumb service firewall rules",
            move |_cx| async move {
                let res = nexus_networking::plumb_service_firewall_rules(
                    datastore,
                    &opctx,
                    &[],
                    &opctx,
                    &opctx.log,
                )
                .await
                .context("failed to plumb service firewall rules to sleds");
                result_to_step_result(res)
            },
        )
        .register();
}

fn register_dns_records_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
    nexus_id: OmicronZoneUuid,
    overrides: &'a Overridables,
    sleds: SharedStepHandle<Arc<BTreeMap<SledUuid, Sled>>>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy DNS records",
            move |cx| async move {
                let sleds_by_id = sleds.into_value(cx.token()).await;

                let res = dns::deploy_dns(
                    &opctx,
                    datastore,
                    nexus_id.to_string(),
                    blueprint,
                    &sleds_by_id,
                    overrides,
                )
                .await
                .map_err(|e| anyhow!("{}", InlineErrorChain::new(&e)));
                result_to_step_result(res)
            },
        )
        .register();
}

fn register_cleanup_expunged_zones_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    resolver: &'a Resolver,
    blueprint: &'a Blueprint,
    deploy_zones_done: StepHandle<DeployZonesDone>,
) -> StepHandle<DeployZonesDone> {
    registrar
        .new_step(
            ExecutionStepId::Remove,
            "Cleanup expunged zones",
            move |cx| async move {
                let done = deploy_zones_done.into_value(cx.token()).await;
                omicron_zones::clean_up_expunged_zones(
                    &opctx,
                    datastore,
                    resolver,
                    blueprint.all_omicron_zones(BlueprintZoneFilter::Expunged),
                    &done,
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(done).into()
            },
        )
        .register()
}

fn register_decommission_sleds_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) {
    registrar
        .new_step(
            ExecutionStepId::Remove,
            "Decommission sleds",
            move |_cx| async move {
                let res = sled_state::decommission_sleds(
                    &opctx,
                    datastore,
                    blueprint
                        .sled_state
                        .iter()
                        .filter(|&(_, &state)| {
                            state == SledState::Decommissioned
                        })
                        .map(|(&sled_id, _)| sled_id),
                )
                .await
                .map_err(merge_anyhow_list);
                result_to_step_result(res)
            },
        )
        .register();
}

fn register_decommission_disks_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) {
    registrar
        .new_step(
            ExecutionStepId::Remove,
            "Decommission expunged disks",
            move |_cx| async move {
                let res = omicron_physical_disks::decommission_expunged_disks(
                    &opctx,
                    datastore,
                    blueprint
                        .all_omicron_disks(BlueprintPhysicalDiskDisposition::is_ready_for_cleanup)
                        .map(|(sled_id, config)| (sled_id, config.id)),
                )
                .await
                .map_err(merge_anyhow_list);
                result_to_step_result(res)
            },
        )
        .register();
}

fn register_deploy_clickhouse_cluster_nodes_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy clickhouse cluster nodes",
            move |_cx| async move {
                if let Some(clickhouse_cluster_config) =
                    &blueprint.clickhouse_cluster_config
                {
                    let res = clickhouse::deploy_nodes(
                        &opctx,
                        &blueprint.blueprint_zones,
                        &clickhouse_cluster_config,
                    )
                    .await
                    .map_err(merge_anyhow_list);
                    return result_to_step_result(res);
                }

                StepSuccess::new(()).into()
            },
        )
        .register();
}

fn register_deploy_clickhouse_single_node_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy single-node clickhouse cluster",
            move |_cx| async move {
                let res = clickhouse::deploy_single_node(
                    &opctx,
                    &blueprint.blueprint_zones,
                )
                .await;
                result_to_step_result(res)
            },
        )
        .register();
}

#[derive(Debug)]
struct ReassignSagaOutput {
    needs_saga_recovery: bool,
    error: Option<anyhow::Error>,
}

fn register_reassign_sagas_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
    nexus_id: OmicronZoneUuid,
    deploy_zones_done: StepHandle<DeployZonesDone>,
) -> StepHandle<ReassignSagaOutput> {
    // For this and subsequent steps, we'll assume that any errors that we
    // encounter do *not* require stopping execution.  We'll just accumulate
    // them and return them all at the end.
    //
    // TODO We should probably do this with more of the errors above, too.
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Reassign sagas",
            move |cx| async move {
                let done = deploy_zones_done.into_value(cx.token()).await;

                // For any expunged Nexus zones, re-assign in-progress sagas to
                // some other Nexus.  If this fails for some reason, it doesn't
                // affect anything else.
                let sec_id = nexus_db_model::SecId::from(nexus_id);
                let reassigned = sagas::reassign_sagas_from_expunged(
                    &opctx, datastore, blueprint, sec_id, &done,
                )
                .await
                .context("failed to re-assign sagas");
                match reassigned {
                    Ok(needs_saga_recovery) => {
                        let output = ReassignSagaOutput {
                            needs_saga_recovery,
                            error: None,
                        };
                        StepSuccess::new(output).into()
                    }
                    Err(error) => {
                        // We treat errors as non-fatal here, but we still want
                        // to log them. It's okay to just log the message here
                        // without the chain of sources, since we collect the
                        // full chain in the last step
                        // (`register_finalize_step`).
                        let message = error.to_string();
                        let output = ReassignSagaOutput {
                            needs_saga_recovery: false,
                            error: Some(error),
                        };
                        StepWarning::new(output, message).into()
                    }
                }
            },
        )
        .register()
}

fn register_cockroachdb_settings_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) -> StepHandle<Option<anyhow::Error>> {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Ensure CockroachDB settings",
            move |_cx| async move {
                if let Err(error) =
                    cockroachdb::ensure_settings(&opctx, datastore, blueprint)
                        .await
                {
                    // We treat errors as non-fatal here, but we still want to
                    // log them. It's okay to just log the message here without
                    // the chain of sources, since we collect the full chain in
                    // the last step (`register_finalize_step`).
                    let message = error.to_string();
                    StepWarning::new(Some(error), message).into()
                } else {
                    StepSuccess::new(None).into()
                }
            },
        )
        .register()
}

fn register_finalize_step(
    registrar: &ComponentRegistrar<'_, '_>,
    reassign_saga_output: StepHandle<ReassignSagaOutput>,
    register_cockroach_output: StepHandle<Option<anyhow::Error>>,
) -> StepHandle<RealizeBlueprintOutput> {
    registrar
        .new_step(
            ExecutionStepId::Finalize,
            "Finalize and check for errors",
            move |cx| async move {
                let reassign_saga_output =
                    reassign_saga_output.into_value(cx.token()).await;
                let register_cockroach_output =
                    register_cockroach_output.into_value(cx.token()).await;

                let mut errors = Vec::new();
                if let Some(error) = register_cockroach_output {
                    errors.push(error);
                }
                if let Some(error) = reassign_saga_output.error {
                    errors.push(error);
                }

                if errors.is_empty() {
                    StepSuccess::new(RealizeBlueprintOutput {
                        needs_saga_recovery: reassign_saga_output
                            .needs_saga_recovery,
                    })
                    .into()
                } else {
                    Err(merge_anyhow_list(errors))
                }
            },
        )
        .register()
}
