// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sleds, and the hardware and services within them.

use crate::authz;
use crate::db;
use crate::db::identity::Asset;
use crate::db::lookup::LookupPath;
use crate::db::model::DatasetKind;
use crate::db::model::ServiceKind;
use crate::internal_api::params::{
    PhysicalDiskDeleteRequest, PhysicalDiskPutRequest, SledAgentStartupInfo,
    SledRole, ZpoolPutRequest,
};
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use sled_agent_client::types::SetVirtualNetworkInterfaceHost;
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
            info.baseboard.identifier,
            info.baseboard.model,
            info.baseboard.revision,
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

    pub async fn sled_list_physical_disks(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDisk> {
        self.db_datastore
            .sled_list_physical_disks(&opctx, sled_id, pagparams)
            .await
    }

    pub async fn physical_disk_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDisk> {
        self.db_datastore.physical_disk_list(&opctx, pagparams).await
    }

    /// Upserts a physical disk into the database, updating it if it already exists.
    pub async fn upsert_physical_disk(
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

    /// Upserts a physical disk into the database, updating it if it already exists.
    pub async fn delete_physical_disk(
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

    // OPTE V2P mappings

    /// Ensure that the necessary v2p mappings for an instance are created
    pub async fn create_instance_v2p_mappings(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<(), Error> {
        // For every sled that isn't the sled this instance was allocated to, create
        // a virtual to physical mapping for each of this instance's NICs.
        //
        // TODO this is a first-pass naive approach. A more optimized approach would
        // be to see what other instances share the VPC this instance is in (or more
        // generally, what instances should have connectivity to this one), see what
        // sleds those are allocated to, and only create a v2p mapping for those
        // sleds. However, this naive approach has the advantage of not requiring
        // additional work: v2p mappings have to be bidirectional in order for both
        // instances's packets to make a round trip. After this function executes, a
        // v2p mapping to this instance will exist on every sled, and when another
        // instance is allocated to a random sled, that sled will already contain a
        // mapping to this instance.
        //
        // TODO-correctness OPTE currently will block instances in different VPCs
        // from connecting to each other. If it ever stops doing this, this naive
        // approach will create v2p mappings that shouldn't exist.

        let (.., authz_instance, db_instance) =
            LookupPath::new(&opctx, &self.db_datastore)
                .instance_id(instance_id)
                .fetch_for(authz::Action::Read)
                .await?;

        let instance_nics = self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?;

        // Lookup the physical host IP of the sled hosting this instance
        let instance_sled_id = db_instance.runtime().sled_id;
        let physical_host_ip: std::net::IpAddr =
            (*self.sled_lookup(&self.opctx_alloc, &instance_sled_id).await?.ip)
                .into();

        let mut last_sled_id: Option<Uuid> = None;
        loop {
            let pagparams = DataPageParams {
                marker: last_sled_id.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: std::num::NonZeroU32::new(10).unwrap(),
            };

            let sleds_page =
                self.sleds_list(&self.opctx_alloc, &pagparams).await?;

            for sled in &sleds_page {
                // set_v2p not required for sled instance was allocated to, OPTE
                // currently does that automatically
                if sled.id() == instance_sled_id {
                    continue;
                }

                let client = self.sled_client(&sled.id()).await?;

                for nic in &instance_nics {
                    // This function is idempotent: calling the set_v2p ioctl with
                    // the same information is a no-op.
                    client
                        .set_v2p(
                            &nic.id,
                            &SetVirtualNetworkInterfaceHost {
                                virtual_ip: nic.ip,
                                virtual_mac: nic.mac.clone(),
                                physical_host_ip,
                                vni: nic.vni.clone(),
                            },
                        )
                        .await?;
                }
            }

            if sleds_page.len() < 10 {
                break;
            }

            if let Some(last) = sleds_page.last() {
                last_sled_id = Some(last.id());
            }
        }

        Ok(())
    }

    /// Ensure that the necessary v2p mappings for an instance are deleted
    pub async fn delete_instance_v2p_mappings(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<(), Error> {
        // For every sled that isn't the sled this instance was allocated to, delete
        // the virtual to physical mapping for each of this instance's NICs. If
        // there isn't a V2P mapping, del_v2p should be a no-op.
        let (.., authz_instance, db_instance) =
            LookupPath::new(&opctx, &self.db_datastore)
                .instance_id(instance_id)
                .fetch_for(authz::Action::Read)
                .await?;

        let instance_nics = self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?;

        // Lookup the physical host IP of the sled hosting this instance
        let instance_sled_id = db_instance.runtime().sled_id;
        let physical_host_ip: std::net::IpAddr =
            (*self.sled_lookup(&self.opctx_alloc, &instance_sled_id).await?.ip)
                .into();

        let mut last_sled_id: Option<Uuid> = None;

        loop {
            let pagparams = DataPageParams {
                marker: last_sled_id.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: std::num::NonZeroU32::new(10).unwrap(),
            };

            let sleds_page =
                self.sleds_list(&self.opctx_alloc, &pagparams).await?;

            for sled in &sleds_page {
                // del_v2p not required for sled instance was allocated to, OPTE
                // currently does that automatically
                if sled.id() == instance_sled_id {
                    continue;
                }

                let client = self.sled_client(&sled.id()).await?;

                for nic in &instance_nics {
                    // This function is idempotent: calling the set_v2p ioctl with
                    // the same information is a no-op.
                    client
                        .del_v2p(
                            &nic.id,
                            &SetVirtualNetworkInterfaceHost {
                                virtual_ip: nic.ip,
                                virtual_mac: nic.mac.clone(),
                                physical_host_ip,
                                vni: nic.vni.clone(),
                            },
                        )
                        .await?;
                }
            }

            if sleds_page.len() < 10 {
                break;
            }

            if let Some(last) = sleds_page.last() {
                last_sled_id = Some(last.id());
            }
        }

        Ok(())
    }
}
