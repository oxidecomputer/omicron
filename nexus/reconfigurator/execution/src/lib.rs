// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_reconfigurator_planning` crate-level docs for background.

use anyhow::{Context, anyhow};
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::execution::*;
use nexus_types::external_api::views::SledState;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use update_engine::StepSuccess;
use update_engine::StepWarning;
use update_engine::merge_anyhow_list;

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

    register_deploy_zones_step(
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

    register_cleanup_expunged_zones_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        datastore,
        resolver,
        blueprint,
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

    register_support_bundle_failure_step(
        &engine.for_component(ExecutionComponent::SupportBundles),
        &opctx,
        datastore,
        blueprint,
        nexus_id,
    );

    let reassign_saga_output = register_reassign_sagas_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        datastore,
        blueprint,
        nexus_id,
    );

    register_cockroachdb_settings_step(
        &engine.for_component(ExecutionComponent::Cockroach),
        &opctx,
        datastore,
        blueprint,
    );

    // All steps are registered, so execute the engine.
    let result = engine.execute().await?;

    let needs_saga_recovery =
        reassign_saga_output.into_value(result.token()).await;

    Ok(RealizeBlueprintOutput { needs_saga_recovery })
}

// Convert a `Result<(), anyhow::Error>` into a `StepResult` containing either a
// `StepSuccess` or `StepWarning`.
//
// Most steps use this to avoid stopping execution at the errored step, which
// would prevent other independent steps after the errored step from executing.
fn map_err_to_step_warning(
    res: Result<(), anyhow::Error>,
) -> StepResult<(), ReconfiguratorExecutionSpec> {
    match res {
        Ok(_) => StepSuccess::new(()).build(),
        Err(e) => StepWarning::new((), format!("{e:#}")).build(),
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
) {
    registrar
        .new_step(
            ExecutionStepId::Cleanup,
            "Mark support bundles as failed if they rely on \
             an expunged disk or sled",
            move |_cx| async move {
                let res = match datastore
                    .support_bundle_fail_expunged(&opctx, blueprint, nexus_id)
                    .await
                {
                    Ok(report) => StepSuccess::new(())
                        .with_message(format!(
                            "support bundle expunge report: {report:?}"
                        ))
                        .build(),
                    Err(err) => StepWarning::new((), err.to_string()).build(),
                };
                Ok(res)
            },
        )
        .register();
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
                    blueprint
                        .sleds
                        .iter()
                        .map(|(sled_id, sled)| (*sled_id, &sled.disks_config)),
                )
                .await
                .map_err(merge_anyhow_list);
                Ok(map_err_to_step_warning(res))
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
                    blueprint.sleds.iter().map(|(sled_id, sled)| {
                        (*sled_id, &sled.datasets_config)
                    }),
                )
                .await
                .map_err(merge_anyhow_list);
                Ok(map_err_to_step_warning(res))
            },
        )
        .register();
}

fn register_deploy_zones_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
    sleds: SharedStepHandle<Arc<BTreeMap<SledUuid, Sled>>>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy Omicron zones",
            move |cx| async move {
                let sleds_by_id = sleds.into_value(cx.token()).await;
                let res = omicron_zones::deploy_zones(
                    &opctx,
                    &sleds_by_id,
                    blueprint
                        .sleds
                        .iter()
                        .map(|(sled_id, sled)| (*sled_id, &sled.zones_config)),
                )
                .await
                .map_err(merge_anyhow_list);
                Ok(map_err_to_step_warning(res))
            },
        )
        .register();
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
                Ok(map_err_to_step_warning(res))
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
                Ok(map_err_to_step_warning(res))
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
) {
    registrar
        .new_step(
            ExecutionStepId::Cleanup,
            "Cleanup expunged zones",
            move |_cx| async move {
                let res = omicron_zones::clean_up_expunged_zones(
                    &opctx,
                    datastore,
                    resolver,
                    blueprint.all_omicron_zones(
                        BlueprintZoneDisposition::is_ready_for_cleanup,
                    ),
                )
                .await
                .map_err(merge_anyhow_list);
                Ok(map_err_to_step_warning(res))
            },
        )
        .register();
}

fn register_decommission_sleds_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) {
    registrar
        .new_step(
            ExecutionStepId::Cleanup,
            "Decommission sleds",
            move |_cx| async move {
                let res = sled_state::decommission_sleds(
                    &opctx,
                    datastore,
                    blueprint
                        .sleds
                        .iter()
                        .filter(|(_, sled)| {
                            sled.state == SledState::Decommissioned
                        })
                        .map(|(&sled_id, _)| sled_id),
                )
                .await
                .map_err(merge_anyhow_list);
                Ok(map_err_to_step_warning(res))
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
            ExecutionStepId::Cleanup,
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
                Ok(map_err_to_step_warning(res))
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
                        blueprint
                            .all_omicron_zones(BlueprintZoneDisposition::any),
                        &clickhouse_cluster_config,
                    )
                    .await
                    .map_err(merge_anyhow_list);
                    return Ok(map_err_to_step_warning(res));
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
                    blueprint
                        .all_omicron_zones(
                            BlueprintZoneDisposition::is_in_service,
                        )
                        .filter(|(_, z)| z.zone_type.is_clickhouse()),
                )
                .await;
                Ok(map_err_to_step_warning(res))
            },
        )
        .register();
}

// Returns a boolean indicating whether saga recovery is needed.
fn register_reassign_sagas_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
    nexus_id: OmicronZoneUuid,
) -> StepHandle<bool> {
    registrar
        .new_step(
            ExecutionStepId::Cleanup,
            "Reassign sagas",
            move |_cx| async move {
                // For any expunged Nexus zones, re-assign in-progress sagas to
                // some other Nexus.  If this fails for some reason, it doesn't
                // affect anything else.
                let sec_id = nexus_db_model::SecId::from(nexus_id);
                let reassigned = sagas::reassign_sagas_from_expunged(
                    &opctx, datastore, blueprint, sec_id,
                )
                .await
                .context("failed to re-assign sagas");
                match reassigned {
                    Ok(needs_saga_recovery) => {
                        Ok(StepSuccess::new(needs_saga_recovery).build())
                    }
                    Err(error) => {
                        Ok(StepWarning::new(false, error.to_string()).build())
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
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Ensure CockroachDB settings",
            move |_cx| async move {
                let res =
                    cockroachdb::ensure_settings(&opctx, datastore, blueprint)
                        .await;
                Ok(map_err_to_step_warning(res))
            },
        )
        .register();
}
