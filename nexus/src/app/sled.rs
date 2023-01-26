// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sleds, and the hardware and services within them.

use crate::context::OpContext;
use crate::db;
use crate::db::identity::Asset;
use crate::db::lookup::LookupPath;
use crate::db::model::DatasetKind;
use crate::db::model::ServiceKind;
use crate::internal_api::params::{
    PhysicalDiskDeleteRequest, PhysicalDiskPutRequest, SledAgentStartupInfo, SledRole, ZpoolPutRequest,
};
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use sled_agent_client::Client as SledAgentClient;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    // Sleds

    // TODO-robustness we should have a limit on how many sled agents there can
    // be (for graceful degradation at large scale).
    pub async fn upsert_sled(
        &self,
        id: Uuid,
        info: SledAgentStartupInfo,
    ) -> Result<(), Error> {
        info!(self.log, "registered sled agent"; "sled_uuid" => id.to_string());

        let is_scrimlet = match info.role {
            SledRole::Gimlet => false,
            SledRole::Scrimlet => true,
        };

        let sled = db::model::Sled::new(
            id,
            info.sa_address,
            is_scrimlet,
            self.rack_id,
        );
        self.db_datastore.sled_upsert(sled).await?;
        Ok(())
    }

    pub async fn sleds_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Sled> {
        self.db_datastore.sled_list(&opctx, pagparams).await
    }

    pub async fn sled_lookup(
        &self,
        opctx: &OpContext,
        sled_id: &Uuid,
    ) -> LookupResult<db::model::Sled> {
        let (.., db_sled) = LookupPath::new(opctx, &self.db_datastore)
            .sled_id(*sled_id)
            .fetch()
            .await?;
        Ok(db_sled)
    }

    pub async fn sled_client(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        // TODO: We should consider injecting connection pooling here,
        // but for now, connections to sled agents are constructed
        // on an "as requested" basis.
        //
        // Franky, returning an "Arc" here without a connection pool is a little
        // silly; it's not actually used if each client connection exists as a
        // one-shot.
        let sled = self.sled_lookup(&self.opctx_alloc, id).await?;

        let log = self.log.new(o!("SledAgent" => id.clone().to_string()));
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();
        Ok(Arc::new(SledAgentClient::new_with_client(
            &format!("http://{}", sled.address()),
            client,
            log,
        )))
    }

    pub async fn random_sled_id(&self) -> Result<Option<Uuid>, Error> {
        Ok(self
            .db_datastore
            .random_sled(&self.opctx_alloc)
            .await?
            .map(|sled| sled.id()))
    }

    // Physical disks

    /// Upserts a physical disk into the database, updating it if it already exists.
    pub async fn upsert_physical_disk(
        &self,
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
            request.total_size,
        );
        self.db_datastore.physical_disk_upsert(disk).await?;
        Ok(())
    }

    /// Upserts a physical disk into the database, updating it if it already exists.
    pub async fn delete_physical_disk(
        &self,
        request: PhysicalDiskDeleteRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "deleting physical disk";
            "vendor" => request.vendor.to_string(),
            "serial" => request.serial.to_string(),
            "model" => request.model.to_string()
        );
        self.db_datastore.physical_disk_delete(
            request.vendor,
            request.serial,
            request.model,
        ).await?;
        Ok(())
    }

    // Zpools (contained within sleds)

    /// Upserts a Zpool into the database, updating it if it already exists.
    pub async fn upsert_zpool(
        &self,
        id: Uuid,
        sled_id: Uuid,
        info: ZpoolPutRequest,
    ) -> Result<(), Error> {
        info!(self.log, "upserting zpool"; "sled_id" => sled_id.to_string(), "zpool_id" => id.to_string());
        let zpool = db::model::Zpool::new(id, sled_id, &info);
        self.db_datastore.zpool_upsert(zpool).await?;
        Ok(())
    }

    // Datasets (contained within zpools)

    /// Upserts a dataset into the database, updating it if it already exists.
    pub async fn upsert_dataset(
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

    // Services

    /// Upserts a Service into the database, updating it if it already exists.
    pub async fn upsert_service(
        &self,
        opctx: &OpContext,
        id: Uuid,
        sled_id: Uuid,
        address: Ipv6Addr,
        kind: ServiceKind,
    ) -> Result<(), Error> {
        info!(
            self.log,
            "upserting service";
            "sled_id" => sled_id.to_string(),
            "service_id" => id.to_string(),
            "address" => address.to_string(),
        );
        let service = db::model::Service::new(id, sled_id, address, kind);
        self.db_datastore.service_upsert(opctx, service).await?;
        Ok(())
    }
}
