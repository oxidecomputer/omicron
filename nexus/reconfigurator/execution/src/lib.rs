// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_reconfigurator_planning` crate-level docs for background.

use anyhow::{Context, anyhow};
use iddqd::IdOrdMap;
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::execution::StepSkipped;
use nexus_types::deployment::execution::overridables;
use nexus_types::deployment::execution::{
    ComponentRegistrar, Event, ExecutionComponent, ExecutionStepId,
    Overridables, ReconfiguratorExecutionSpec, SharedStepHandle, Sled,
    StepHandle, StepResult, UpdateEngine,
};
use nexus_types::quiesce::SagaQuiesceHandle;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use update_engine::StepSuccess;
use update_engine::StepWarning;
use update_engine::merge_anyhow_list;

mod clickhouse;
mod cockroachdb;
mod database;
mod dns;
mod omicron_physical_disks;
mod omicron_sled_config;
mod omicron_zones;
mod sagas;
mod sled_state;
#[cfg(test)]
mod test_utils;

/// Encapsulates arguments used for [`realize_blueprint`]
///
/// You probably want to start with `RequiredRealizeArgs` and use the
/// builder-like methods to tweak it as required.
pub struct RealizeArgs<'a> {
    pub opctx: &'a OpContext,
    pub datastore: &'a DataStore,
    pub resolver: &'a Resolver,
    pub blueprint: &'a Blueprint,
    pub nexus_id: Option<OmicronZoneUuid>,
    pub creator: OmicronZoneUuid,
    pub sender: mpsc::Sender<Event>,
    pub overrides: Option<&'a Overridables>,
    pub mgs_updates: watch::Sender<PendingMgsUpdates>,
    pub saga_quiesce: SagaQuiesceHandle,
}

impl<'a> RealizeArgs<'a> {
    /// See [`Overridables`]
    ///
    /// If not specified, no overrides are in place during blueprint execution.
    pub fn with_overrides(
        mut self,
        overrides: &'a Overridables,
    ) -> RealizeArgs<'a> {
        self.overrides = Some(overrides);
        self
    }

    /// Specifies the `creator` field used when Nexus creates various kinds of
    /// database records
    ///
    /// This generally matches the current Nexus's id.  It's generally only used
    /// for debugging.
    pub fn with_creator(mut self, creator: OmicronZoneUuid) -> RealizeArgs<'a> {
        self.creator = creator;
        self
    }

    /// Specifies that we're running as a real Nexus with id `nexus_id`
    ///
    /// This enables some steps of blueprint execution that currently assume
    /// it's running a valid Nexus.
    pub fn as_nexus(mut self, nexus_id: OmicronZoneUuid) -> RealizeArgs<'a> {
        self.nexus_id = Some(nexus_id);
        self
    }
}

/// Encapsulates all of the required arguments for [`realize_blueprint`]
///
/// You'll need to convert this to a [`RealizeArgs`] to use it with
/// `realize_blueprint()`.  You can also use the builder-like `with_*` methods
/// to tweak this as you do so.
pub struct RequiredRealizeArgs<'a> {
    pub opctx: &'a OpContext,
    pub datastore: &'a DataStore,
    pub resolver: &'a Resolver,
    pub creator: OmicronZoneUuid,
    pub blueprint: &'a Blueprint,
    pub sender: mpsc::Sender<Event>,
    pub mgs_updates: watch::Sender<PendingMgsUpdates>,
    pub saga_quiesce: SagaQuiesceHandle,
}

impl<'a> From<RequiredRealizeArgs<'a>> for RealizeArgs<'a> {
    fn from(value: RequiredRealizeArgs<'a>) -> Self {
        RealizeArgs {
            opctx: value.opctx,
            datastore: value.datastore,
            resolver: value.resolver,
            blueprint: value.blueprint,
            creator: value.creator,
            nexus_id: None,
            sender: value.sender,
            overrides: None,
            mgs_updates: value.mgs_updates,
            saga_quiesce: value.saga_quiesce,
        }
    }
}

impl<'a> RequiredRealizeArgs<'a> {
    /// See [`RealizeArgs::with_overrides()`]
    pub fn with_overrides(
        self,
        overrides: &'a Overridables,
    ) -> RealizeArgs<'a> {
        RealizeArgs::from(self).with_overrides(overrides)
    }

    /// See [`RealizeArgs::as_nexus()`]
    pub fn as_nexus(self, nexus_id: OmicronZoneUuid) -> RealizeArgs<'a> {
        RealizeArgs::from(self).as_nexus(nexus_id)
    }
}

/// The result of calling [`realize_blueprint`].
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
    exec_ctx: RealizeArgs<'_>,
) -> Result<RealizeBlueprintOutput, anyhow::Error> {
    let RealizeArgs {
        opctx,
        datastore,
        resolver,
        blueprint,
        nexus_id,
        creator,
        sender,
        overrides,
        mgs_updates,
        saga_quiesce,
    } = exec_ctx;

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

    register_deploy_db_metadata_nexus_records_step(
        &engine.for_component(ExecutionComponent::DeployNexusRecords),
        &opctx,
        datastore,
        blueprint,
    );

    register_deploy_sled_configs_step(
        &engine.for_component(ExecutionComponent::SledAgent),
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
        creator,
        overrides.unwrap_or(&*overridables::DEFAULT),
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
        saga_quiesce,
    );

    register_cockroachdb_settings_step(
        &engine.for_component(ExecutionComponent::Cockroach),
        &opctx,
        datastore,
        blueprint,
    );

    register_mgs_update_step(
        &engine.for_component(ExecutionComponent::MgsUpdates),
        blueprint,
        mgs_updates,
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
            async move |_cx| {
                datastore
                    .blueprint_ensure_external_networking_resources(
                        opctx, blueprint,
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
    nexus_id: Option<OmicronZoneUuid>,
) {
    registrar
        .new_step(
            ExecutionStepId::Cleanup,
            "Mark support bundles as failed if they rely on \
             an expunged disk or sled",
            async move |_cx| {
                let Some(nexus_id) = nexus_id else {
                    return StepSkipped::new((), "not running as Nexus").into();
                };

                let res = match datastore
                    .support_bundle_fail_expunged(opctx, blueprint, nexus_id)
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
) -> StepHandle<Arc<IdOrdMap<Sled>>> {
    registrar
        .new_step(ExecutionStepId::Fetch, "Fetch sled list", async move |_cx| {
            let sleds_by_id: IdOrdMap<_> = datastore
                .sled_list_all_batched(opctx, SledFilter::InService)
                .await
                .context("listing all sleds")?
                .into_iter()
                .map(|db_sled| db_sled.into())
                .collect();

            StepSuccess::new(Arc::new(sleds_by_id)).into()
        })
        .register()
}

fn register_deploy_db_metadata_nexus_records_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Ensure db_metadata_nexus_state records exist",
            async move |_cx| match database::deploy_db_metadata_nexus_records(
                opctx, &datastore, &blueprint,
            )
            .await
            {
                Ok(()) => StepSuccess::new(()).into(),
                Err(err) => StepWarning::new(
                    (),
                    err.context("ensuring db_metadata_nexus_state").to_string(),
                )
                .into(),
            },
        )
        .register();
}

fn register_deploy_sled_configs_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    blueprint: &'a Blueprint,
    sleds: SharedStepHandle<Arc<IdOrdMap<Sled>>>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy sled configs",
            async move |cx| {
                let sleds_by_id = sleds.into_value(cx.token()).await;
                let res = omicron_sled_config::deploy_sled_configs(
                    opctx,
                    &sleds_by_id,
                    &blueprint.sleds,
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
            async move |_cx| {
                let res = nexus_networking::plumb_service_firewall_rules(
                    datastore,
                    opctx,
                    &[],
                    opctx,
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
    creator: OmicronZoneUuid,
    overrides: &'a Overridables,
    sleds: SharedStepHandle<Arc<IdOrdMap<Sled>>>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy DNS records",
            async move |cx| {
                let sleds_by_id = sleds.into_value(cx.token()).await;

                let res = dns::deploy_dns(
                    opctx,
                    datastore,
                    creator.to_string(),
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
            async move |_cx| {
                let res = omicron_zones::clean_up_expunged_zones(
                    opctx, datastore, resolver, blueprint,
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
            async move |_cx| {
                let res =
                    sled_state::decommission_sleds(opctx, datastore, blueprint)
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
            async move |_cx| {
                let res = omicron_physical_disks::decommission_expunged_disks(
                    opctx, datastore, blueprint,
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
            async move |_cx| {
                if let Some(clickhouse_cluster_config) =
                    &blueprint.clickhouse_cluster_config
                {
                    let res = clickhouse::deploy_nodes(
                        opctx,
                        blueprint,
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
            async move |_cx| {
                let res =
                    clickhouse::deploy_single_node(opctx, blueprint).await;
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
    nexus_id: Option<OmicronZoneUuid>,
    saga_quiesce: SagaQuiesceHandle,
) -> StepHandle<bool> {
    registrar
        .new_step(
            ExecutionStepId::Cleanup,
            "Reassign sagas",
            async move |_cx| {
                let Some(nexus_id) = nexus_id else {
                    return StepSkipped::new(false, "not running as Nexus")
                        .into();
                };

                // Re-assign sagas.
                Ok(saga_quiesce
                    .reassign_sagas(async || {
                        // For any expunged Nexus zones, re-assign in-progress
                        // sagas to some other Nexus.  If this fails for some
                        // reason, it doesn't affect anything else.
                        let sec_id = nexus_db_model::SecId::from(nexus_id);
                        let reassigned = sagas::reassign_sagas_from_expunged(
                            opctx, datastore, blueprint, sec_id,
                        )
                        .await
                        .context("failed to re-assign sagas");
                        match reassigned {
                            Ok(needs_saga_recovery) => (
                                StepSuccess::new(needs_saga_recovery).build(),
                                needs_saga_recovery,
                            ),
                            Err(error) => {
                                // It's possible that we failed after having
                                // re-assigned sagas in the database.
                                let maybe_reassigned = true;
                                (
                                    StepWarning::new(false, error.to_string())
                                        .build(),
                                    maybe_reassigned,
                                )
                            }
                        }
                    })
                    .await)
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
            async move |_cx| {
                let res =
                    cockroachdb::ensure_settings(opctx, datastore, blueprint)
                        .await;
                Ok(map_err_to_step_warning(res))
            },
        )
        .register();
}

fn register_mgs_update_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    blueprint: &'a Blueprint,
    sender: watch::Sender<PendingMgsUpdates>,
) {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Kick off MGS-managed updates",
            move |_cx| async move {
                let result =
                    sender.send(blueprint.pending_mgs_updates.clone()).context(
                        "failed to send to MgsUpdateDriver on watch channel",
                    );
                Ok(map_err_to_step_warning(result))
            },
        )
        .register();
}
