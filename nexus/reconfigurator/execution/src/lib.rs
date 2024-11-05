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
use nexus_types::deployment::BlueprintDatasetFilter;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::views::SledState;
use nexus_types::identity::Asset;
use omicron_physical_disks::DeployDisksDone;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
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

    let deploy_disks_done = register_deploy_disks_step(
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

    register_dataset_records_step(
        &engine.for_component(ExecutionComponent::DatasetRecords),
        &opctx,
        datastore,
        blueprint,
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

    register_decommission_expunged_disks_step(
        &engine.for_component(ExecutionComponent::PhysicalDisks),
        &opctx,
        datastore,
        deploy_disks_done,
    );

    register_deploy_clickhouse_cluster_nodes_step(
        &engine.for_component(ExecutionComponent::Clickhouse),
        &opctx,
        blueprint,
    );

    let reassign_saga_output = register_reassign_sagas_step(
        &engine.for_component(ExecutionComponent::OmicronZones),
        &opctx,
        datastore,
        blueprint,
        nexus_id,
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
) -> StepHandle<DeployDisksDone> {
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Deploy physical disks",
            move |cx| async move {
                let sleds_by_id = sleds.into_value(cx.token()).await;
                let done = omicron_physical_disks::deploy_disks(
                    &opctx,
                    &sleds_by_id,
                    &blueprint.blueprint_disks,
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(done).into()
            },
        )
        .register()
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
                datasets::deploy_datasets(
                    &opctx,
                    &sleds_by_id,
                    &blueprint.blueprint_datasets,
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(()).into()
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
                omicron_zones::deploy_zones(
                    &opctx,
                    &sleds_by_id,
                    &blueprint.blueprint_zones,
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(()).into()
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
                nexus_networking::plumb_service_firewall_rules(
                    datastore,
                    &opctx,
                    &[],
                    &opctx,
                    &opctx.log,
                )
                .await
                .context("failed to plumb service firewall rules to sleds")?;

                StepSuccess::new(()).into()
            },
        )
        .register();
}

fn register_dataset_records_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    blueprint: &'a Blueprint,
) {
    let bp_id = BlueprintUuid::from_untyped_uuid(blueprint.id);
    registrar
        .new_step(
            ExecutionStepId::Ensure,
            "Ensure dataset records",
            move |_cx| async move {
                datasets::ensure_dataset_records_exist(
                    &opctx,
                    datastore,
                    bp_id,
                    blueprint.all_omicron_datasets(BlueprintDatasetFilter::All),
                )
                .await?;

                StepSuccess::new(()).into()
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

                dns::deploy_dns(
                    &opctx,
                    datastore,
                    nexus_id.to_string(),
                    blueprint,
                    &sleds_by_id,
                    overrides,
                )
                .await
                .map_err(|e| anyhow!("{}", InlineErrorChain::new(&e)))?;

                StepSuccess::new(()).into()
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
            ExecutionStepId::Remove,
            "Cleanup expunged zones",
            move |_cx| async move {
                omicron_zones::clean_up_expunged_zones(
                    &opctx,
                    datastore,
                    resolver,
                    blueprint.all_omicron_zones(BlueprintZoneFilter::Expunged),
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(()).into()
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
            ExecutionStepId::Remove,
            "Decommission sleds",
            move |_cx| async move {
                sled_state::decommission_sleds(
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
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(()).into()
            },
        )
        .register();
}

fn register_decommission_expunged_disks_step<'a>(
    registrar: &ComponentRegistrar<'_, 'a>,
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    deploy_disks_done: StepHandle<DeployDisksDone>,
) {
    // This depends on the "deploy_disks" call earlier -- disk expungement is a
    // statement of policy, but we need to be assured that the Sled Agent has
    // stopped using that disk before we can mark its state as decommissioned.
    registrar
        .new_step(
            ExecutionStepId::Remove,
            "Decommission expunged disks",
            move |cx| async move {
                let done = deploy_disks_done.into_value(cx.token()).await;
                omicron_physical_disks::decommission_expunged_disks(
                    &opctx, datastore, done,
                )
                .await
                .map_err(merge_anyhow_list)?;

                StepSuccess::new(()).into()
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
                    clickhouse::deploy_nodes(
                        &opctx,
                        &blueprint.blueprint_zones,
                        &clickhouse_cluster_config,
                    )
                    .await
                    .map_err(merge_anyhow_list)?;
                }

                StepSuccess::new(()).into()
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

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::Generation;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_model::Zpool;
    use omicron_common::api::external::Error;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    // Helper function to insert sled records from an initial blueprint. Some
    // tests expect to be able to realize the the blueprint created from an
    // initial collection, and ensuring the zones' datasets exist requires first
    // inserting the sled and zpool records.
    pub(crate) async fn insert_sled_records(
        datastore: &DataStore,
        blueprint: &Blueprint,
    ) {
        let rack_id = Uuid::new_v4();
        let mut sleds_inserted = BTreeSet::new();

        for sled_id in blueprint.blueprint_zones.keys().copied() {
            if sleds_inserted.insert(sled_id) {
                let sled = SledUpdate::new(
                    sled_id.into_untyped_uuid(),
                    "[::1]:0".parse().unwrap(),
                    SledBaseboard {
                        serial_number: format!("test-{sled_id}"),
                        part_number: "test-sled".to_string(),
                        revision: 0,
                    },
                    SledSystemHardware {
                        is_scrimlet: false,
                        usable_hardware_threads: 128,
                        usable_physical_ram: (64 << 30).try_into().unwrap(),
                        reservoir_size: (16 << 30).try_into().unwrap(),
                    },
                    rack_id,
                    Generation::new(),
                );
                datastore
                    .sled_upsert(sled)
                    .await
                    .expect("failed to upsert sled");
            }
        }
    }

    // Helper function to insert zpool records from an initial blueprint. Some
    // tests expect to be able to realize the the blueprint created from an
    // initial collection, and ensuring the zones' datasets exist requires first
    // inserting the sled and zpool records.
    pub(crate) async fn create_disks_for_zones_using_datasets(
        datastore: &DataStore,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) {
        let mut pool_inserted = BTreeSet::new();

        for (sled_id, config) in
            blueprint.all_omicron_zones(BlueprintZoneFilter::All)
        {
            let Some(dataset) = config.zone_type.durable_dataset() else {
                continue;
            };

            let physical_disk_id = Uuid::new_v4();
            let pool_id = dataset.dataset.pool_name.id();

            let disk = PhysicalDisk::new(
                physical_disk_id,
                String::from("Oxide"),
                format!("PhysDisk of {}", pool_id),
                String::from("FakeDisk"),
                PhysicalDiskKind::U2,
                sled_id.into_untyped_uuid(),
            );
            match datastore.physical_disk_insert(&opctx, disk.clone()).await {
                Ok(_) | Err(Error::ObjectAlreadyExists { .. }) => (),
                Err(e) => panic!("failed to upsert physical disk: {e}"),
            }

            if pool_inserted.insert(pool_id) {
                let zpool = Zpool::new(
                    pool_id.into_untyped_uuid(),
                    sled_id.into_untyped_uuid(),
                    physical_disk_id,
                );
                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("failed to upsert zpool");
            }
        }
    }
}
