// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sleds, and the hardware and services within them.

use crate::internal_api::params::{
    PhysicalDiskDeleteRequest, PhysicalDiskPutRequest, SledAgentInfo, SledRole,
    ZpoolPutRequest,
};
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::DatasetKind;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
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
        self.db_datastore.sled_upsert(sled).await?;
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
        self.db_datastore.sled_set_policy_to_expunged(opctx, &authz_sled).await
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
        self.db_datastore.sled_list(&opctx, pagparams).await
    }

    pub async fn sled_client(
        &self,
        id: &Uuid,
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
            *id,
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
        self.db_datastore.physical_disk_list(&opctx, pagparams).await
    }

    /// Upserts a physical disk into the database, updating it if it already exists.
    pub(crate) async fn upsert_physical_disk(
        &self,
        opctx: &OpContext,
        request: PhysicalDiskPutRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "upserting physical disk";
            "sled_id" => request.sled_id.to_string(),
            "vendor" => request.vendor.to_string(),
            "serial" => request.serial.to_string(),
            "model" => request.model.to_string()
        );
        let disk = db::model::PhysicalDisk::new(
            request.vendor,
            request.serial,
            request.model,
            request.variant.into(),
            request.sled_id,
        );
        self.db_datastore.physical_disk_upsert(&opctx, disk).await?;
        Ok(())
    }

    /// Removes a physical disk from the database.
    ///
    /// TODO: Remove Zpools and datasets contained within this disk.
    pub(crate) async fn delete_physical_disk(
        &self,
        opctx: &OpContext,
        request: PhysicalDiskDeleteRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "deleting physical disk";
            "sled_id" => request.sled_id.to_string(),
            "vendor" => request.vendor.to_string(),
            "serial" => request.serial.to_string(),
            "model" => request.model.to_string()
        );
        self.db_datastore
            .physical_disk_delete(
                &opctx,
                request.vendor,
                request.serial,
                request.model,
                request.sled_id,
            )
            .await?;
        Ok(())
    }

    // Zpools (contained within sleds)

    /// Upserts a Zpool into the database, updating it if it already exists.
    pub(crate) async fn upsert_zpool(
        &self,
        opctx: &OpContext,
        id: Uuid,
        sled_id: Uuid,
        info: ZpoolPutRequest,
    ) -> Result<(), Error> {
        info!(self.log, "upserting zpool"; "sled_id" => sled_id.to_string(), "zpool_id" => id.to_string());

        let (_authz_disk, db_disk) =
            LookupPath::new(&opctx, &self.db_datastore)
                .physical_disk(
                    &info.disk_vendor,
                    &info.disk_serial,
                    &info.disk_model,
                )
                .fetch()
                .await?;
        let zpool = db::model::Zpool::new(id, sled_id, db_disk.uuid());
        self.db_datastore.zpool_upsert(zpool).await?;
        Ok(())
    }

    // Datasets (contained within zpools)

    /// Upserts a dataset into the database, updating it if it already exists.
    pub(crate) async fn upsert_dataset(
        &self,
        id: Uuid,
        zpool_id: Uuid,
        address: SocketAddrV6,
        kind: DatasetKind,
    ) -> Result<(), Error> {
        info!(self.log, "upserting dataset"; "zpool_id" => zpool_id.to_string(), "dataset_id" => id.to_string(), "address" => address.to_string());
        let dataset = db::model::Dataset::new(id, zpool_id, address, kind);
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
