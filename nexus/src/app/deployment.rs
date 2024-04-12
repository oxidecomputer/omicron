// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use nexus_db_model::DnsGroup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_common::address::NEXUS_REDUNDANCY;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::CollectionUuid;
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    planning_input: PlanningInput,
    creator: String,
    inventory: Option<Collection>,
}

impl super::Nexus {
    pub async fn blueprint_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BlueprintMetadata> {
        self.db_datastore.blueprints_list(opctx, pagparams).await
    }

    pub async fn blueprint_view(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> LookupResult<Blueprint> {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_read(opctx, &blueprint).await
    }

    pub async fn blueprint_delete(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> DeleteResult {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_delete(opctx, &blueprint).await
    }

    pub async fn blueprint_target_view(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<BlueprintTarget>, Error> {
        self.db_datastore.blueprint_target_get_current(opctx).await
    }

    pub async fn blueprint_target_set(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current(opctx, new_target)
            .await?;

        // We have a new target: trigger the background task to load this
        // blueprint.
        self.background_tasks
            .activate(&self.background_tasks.task_blueprint_loader);

        Ok(new_target)
    }

    pub async fn blueprint_target_set_enabled(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current_enabled(opctx, new_target)
            .await?;

        // We don't know whether this actually changed the enabled bit; activate
        // the background task to load this blueprint which does know.
        self.background_tasks
            .activate(&self.background_tasks.task_blueprint_loader);

        Ok(new_target)
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        let creator = self.id.to_string();
        let datastore = self.datastore();

        let sled_rows = datastore.sled_list_all_batched(opctx).await?;
        let zpool_rows =
            datastore.zpool_list_all_external_batched(opctx).await?;
        let ip_pool_range_rows = {
            let (authz_service_ip_pool, _) =
                datastore.ip_pools_service_lookup(opctx).await?;
            datastore
                .ip_pool_list_ranges_batched(opctx, &authz_service_ip_pool)
                .await?
        };
        let external_ip_rows =
            datastore.external_ip_list_service_all_batched(opctx).await?;
        let service_nic_rows = datastore
            .service_network_interfaces_all_list_batched(opctx)
            .await?;

        let internal_dns_version = datastore
            .dns_group_latest_version(opctx, DnsGroup::Internal)
            .await
            .internal_context(
                "fetching internal DNS version for blueprint planning",
            )?
            .version;
        let external_dns_version = datastore
            .dns_group_latest_version(opctx, DnsGroup::External)
            .await
            .internal_context(
                "fetching external DNS version for blueprint planning",
            )?
            .version;

        let planning_input = PlanningInputFromDb {
            sled_rows: &sled_rows,
            zpool_rows: &zpool_rows,
            ip_pool_range_rows: &ip_pool_range_rows,
            external_ip_rows: &external_ip_rows,
            service_nic_rows: &service_nic_rows,
            target_nexus_zone_count: NEXUS_REDUNDANCY,
            log: &opctx.log,
            internal_dns_version,
            external_dns_version,
        }
        .build()?;

        // The choice of which inventory collection to use here is not
        // necessarily trivial.  Inventory collections may be incomplete due to
        // transient (or even persistent) errors.  It's not yet clear what
        // general criteria we'll want to use in picking a collection here.  But
        // as of this writing, this is only used for one specific case, which is
        // to implement a gate that prevents the planner from provisioning
        // non-NTP zones on a sled unless we know there's an NTP zone already on
        // that sled.  For that purpose, it's okay if this collection is
        // incomplete due to a transient error -- that would just prevent
        // forward progress in the planner until the next time we try this.
        // (Critically, it won't cause the planner to do anything wrong.)
        let inventory = datastore
            .inventory_get_latest_collection(opctx)
            .await
            .internal_context(
                "fetching latest inventory collection for blueprint planner",
            )?;

        Ok(PlanningContext { planning_input, creator, inventory })
    }

    async fn blueprint_add(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        self.db_datastore.blueprint_insert(opctx, blueprint).await
    }

    pub async fn blueprint_generate_from_collection(
        &self,
        opctx: &OpContext,
        collection_id: CollectionUuid,
    ) -> CreateResult<Blueprint> {
        let collection = self
            .datastore()
            .inventory_collection_read(opctx, collection_id)
            .await?;
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let blueprint = BlueprintBuilder::build_initial_from_collection(
            &collection,
            planning_context.planning_input.internal_dns_version(),
            planning_context.planning_input.external_dns_version(),
            planning_context.planning_input.all_sled_ids(SledFilter::All),
            &planning_context.creator,
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error generating initial blueprint from collection {}: {}",
                collection_id,
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, &blueprint).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_create_regenerate(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        let maybe_target =
            self.db_datastore.blueprint_target_get_current_full(opctx).await?;
        let Some((_, parent_blueprint)) = maybe_target else {
            return Err(Error::conflict(
                "cannot regenerate blueprint without existing target",
            ));
        };

        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &parent_blueprint,
            &planning_context.planning_input,
            &planning_context.creator,
            &inventory,
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error creating blueprint planner: {error:#}",
            ))
        })?;
        let blueprint = planner.plan().map_err(|error| {
            Error::internal_error(&format!(
                "error generating blueprint: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, &blueprint).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_import(
        &self,
        opctx: &OpContext,
        blueprint: Blueprint,
    ) -> Result<(), Error> {
        let _ = self.blueprint_add(&opctx, &blueprint).await?;
        Ok(())
    }
}
