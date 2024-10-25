// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sleds, and the hardware and services within them.

use crate::external_api::params;
use crate::internal_api::params::{
    PhysicalDiskPutRequest, SledAgentInfo, ZpoolPutRequest,
};
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::views::PhysicalDiskPolicy;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_uuid_kinds::{GenericUuid, SledUuid};
use sled_agent_client::Client as SledAgentClient;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    // Sleds
    pub fn sled_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        sled_id: &Uuid,
    ) -> LookupResult<lookup::Sled<'a>> {
        nexus_networking::sled_lookup(&self.db_datastore, opctx, *sled_id)
    }

    // TODO-robustness we should have a limit on how many sled agents there can
    // be (for graceful degradation at large scale).
    //
    // TODO-multisled: This should not use the rack_id for the given nexus,
    // unless the DNS lookups at sled-agent are only for rack-local nexuses.
    pub(crate) async fn upsert_sled(
        &self,
        _opctx: &OpContext,
        id: Uuid,
        info: SledAgentInfo,
    ) -> Result<(), Error> {
        info!(self.log, "registered sled agent"; "sled_uuid" => id.to_string());

        let is_scrimlet = match info.role {
            SledRole::Gimlet => false,
            SledRole::Scrimlet => true,
        };

        let sled = db::model::SledUpdate::new(
            id,
            info.sa_address,
            db::model::SledBaseboard {
                serial_number: info.baseboard.serial,
                part_number: info.baseboard.part,
                revision: info.baseboard.revision,
            },
            db::model::SledSystemHardware {
                is_scrimlet,
                usable_hardware_threads: info.usable_hardware_threads,
                usable_physical_ram: info.usable_physical_ram.into(),
                reservoir_size: info.reservoir_size.into(),
            },
            self.rack_id,
            info.generation.into(),
        );
        let (_, was_modified) = self.db_datastore.sled_upsert(sled).await?;

        // If a new sled-agent just came online we want to trigger inventory
        // collection.
        //
        // This will allow us to learn about disks so that they can be added to
        // the control plane.
        if was_modified {
            self.activate_inventory_collection();
        }

        Ok(())
    }

    /// Mark a sled as expunged
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    ///
    /// This is idempotent, and it returns the old policy of the sled.
    pub(crate) async fn sled_expunge(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
    ) -> Result<SledPolicy, Error> {
        let sled_lookup = self.sled_lookup(opctx, &sled_id)?;
        let (authz_sled,) =
            sled_lookup.lookup_for(authz::Action::Modify).await?;
        let prev_policy = self
            .db_datastore
            .sled_set_policy_to_expunged(opctx, &authz_sled)
            .await?;

        // The instance-watcher background task is responsible for marking any
        // VMMs running on `Expunged` sleds as `Failed`, so that their instances
        // can transition to `Failed` and be deleted or restarted. Let's go
        // ahead and activate it now so that those instances don't need to wait
        // for the next periodic activation before they can be cleaned up.
        self.background_tasks.task_instance_watcher.activate();

        Ok(prev_policy)
    }

    pub(crate) async fn sled_request_firewall_rules(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        info!(self.log, "requesting firewall rules"; "sled_uuid" => id.to_string());
        self.plumb_service_firewall_rules(opctx, &[id]).await?;
        Ok(())
    }

    pub(crate) async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Sled> {
        self.db_datastore
            .sled_list(&opctx, pagparams, SledFilter::InService)
            .await
    }

    pub async fn sled_client(
        &self,
        id: &SledUuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        // TODO: We should consider injecting connection pooling here,
        // but for now, connections to sled agents are constructed
        // on an "as requested" basis.
        //
        // Frankly, returning an "Arc" here without a connection pool is a
        // little silly; it's not actually used if each client connection exists
        // as a one-shot.
        let client = nexus_networking::sled_client(
            &self.db_datastore,
            &self.opctx_alloc,
            id.into_untyped_uuid(),
            &self.log,
        )
        .await?;
        Ok(Arc::new(client))
    }

    pub(crate) async fn reserve_on_random_sled(
        &self,
        resource_id: Uuid,
        resource_kind: db::model::SledResourceKind,
        resources: db::model::Resources,
        constraints: db::model::SledReservationConstraints,
    ) -> Result<db::model::SledResource, Error> {
        self.db_datastore
            .sled_reservation_create(
                &self.opctx_alloc,
                resource_id,
                resource_kind,
                resources,
                constraints,
            )
            .await
    }

    pub(crate) async fn delete_sled_reservation(
        &self,
        resource_id: Uuid,
    ) -> Result<(), Error> {
        self.db_datastore
            .sled_reservation_delete(&self.opctx_alloc, resource_id)
            .await
    }

    /// Returns the old provision policy.
    pub(crate) async fn sled_set_provision_policy(
        &self,
        opctx: &OpContext,
        sled_lookup: &lookup::Sled<'_>,
        new_policy: SledProvisionPolicy,
    ) -> Result<SledProvisionPolicy, Error> {
        let (authz_sled,) =
            sled_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .sled_set_provision_policy(opctx, &authz_sled, new_policy)
            .await
    }

    // Physical disks

    pub fn physical_disk_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        disk_selector: &params::PhysicalDiskPath,
    ) -> Result<lookup::PhysicalDisk<'a>, Error> {
        Ok(lookup::LookupPath::new(&opctx, &self.db_datastore)
            .physical_disk(disk_selector.disk_id))
    }

    pub(crate) async fn sled_list_physical_disks(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDisk> {
        self.db_datastore
            .sled_list_physical_disks(&opctx, sled_id, pagparams)
            .await
    }

    pub(crate) async fn physical_disk_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDisk> {
        self.db_datastore
            .physical_disk_list(&opctx, pagparams, DiskFilter::InService)
            .await
    }

    /// Upserts a physical disk into the database, updating it if it already exists.
    pub(crate) async fn upsert_physical_disk(
        &self,
        opctx: &OpContext,
        request: PhysicalDiskPutRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "upserting physical disk";
            "physical_disk_id" => %request.id,
            "sled_id" => %request.sled_id,
            "vendor" => %request.vendor,
            "serial" => %request.serial,
            "model" => %request.model,
        );
        let disk = db::model::PhysicalDisk::new(
            request.id,
            request.vendor,
            request.serial,
            request.model,
            request.variant.into(),
            request.sled_id,
        );
        self.db_datastore.physical_disk_insert(&opctx, disk).await?;
        Ok(())
    }

    /// Mark a physical disk as expunged
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    pub(crate) async fn physical_disk_expunge(
        &self,
        opctx: &OpContext,
        disk: params::PhysicalDiskPath,
    ) -> Result<(), Error> {
        let physical_disk_lookup = self.physical_disk_lookup(opctx, &disk)?;
        let (authz_disk,) =
            physical_disk_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .physical_disk_update_policy(
                opctx,
                authz_disk.id(),
                PhysicalDiskPolicy::Expunged.into(),
            )
            .await
    }

    // Zpools (contained within sleds)

    /// Upserts a Zpool into the database, updating it if it already exists.
    pub(crate) async fn upsert_zpool(
        &self,
        opctx: &OpContext,
        request: ZpoolPutRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "upserting zpool";
            "sled_id" => %request.sled_id,
            "zpool_id" => %request.id,
            "physical_disk_id" => %request.physical_disk_id,
        );

        let zpool = db::model::Zpool::new(
            request.id,
            request.sled_id,
            request.physical_disk_id,
        );
        self.db_datastore.zpool_insert(&opctx, zpool).await?;
        Ok(())
    }

    // Datasets (contained within zpools)

    /// Upserts a crucible dataset into the database, updating it if it already exists.
    pub(crate) async fn upsert_crucible_dataset(
        &self,
        id: Uuid,
        zpool_id: Uuid,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        info!(
            self.log,
            "upserting dataset";
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => id.to_string(),
            "address" => address.to_string()
        );
        let kind = DatasetKind::Crucible;
        let dataset =
            db::model::Dataset::new(id, zpool_id, Some(address), kind);
        self.db_datastore.dataset_upsert(dataset).await?;
        Ok(())
    }

    /// Ensure firewall rules for internal services get reflected on all the relevant sleds.
    pub(crate) async fn plumb_service_firewall_rules(
        &self,
        opctx: &OpContext,
        sleds_filter: &[Uuid],
    ) -> Result<(), Error> {
        nexus_networking::plumb_service_firewall_rules(
            &self.db_datastore,
            opctx,
            sleds_filter,
            &self.opctx_alloc,
            &self.log,
        )
        .await
    }
}
