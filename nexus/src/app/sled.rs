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
use std::net::SocketAddrV6;
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
            db::model::SledBaseboard {
                serial_number: info.baseboard.identifier,
                part_number: info.baseboard.model,
                revision: info.baseboard.revision,
            },
            db::model::SledSystemHardware {
                is_scrimlet,
                usable_hardware_threads: info.usable_hardware_threads,
                usable_physical_ram: info.usable_physical_ram.into(),
            },
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

    pub async fn reserve_on_random_sled(
        &self,
        resource_id: Uuid,
        resource_kind: db::model::SledResourceKind,
        resources: db::model::Resources,
    ) -> Result<db::model::SledResource, Error> {
        self.db_datastore
            .sled_reservation_create(
                &self.opctx_alloc,
                resource_id,
                resource_kind,
                resources,
            )
            .await
    }

    pub async fn delete_sled_reservation(
        &self,
        resource_id: Uuid,
    ) -> Result<(), Error> {
        self.db_datastore
            .sled_reservation_delete(&self.opctx_alloc, resource_id)
            .await
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

    /// Removes a physical disk from the database.
    ///
    /// TODO: Remove Zpools and datasets contained within this disk.
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
        let zpool = db::model::Zpool::new(
            id,
            sled_id,
            db_disk.uuid(),
            info.size.into(),
        );
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
        address: SocketAddrV6,
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

        if kind == ServiceKind::ExternalDNSConfig {
            self.background_tasks.activate(&self.task_external_dns_servers);
        } else if kind == ServiceKind::InternalDNSConfig {
            self.background_tasks.activate(&self.task_internal_dns_servers);
        }

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
        // For the mappings to be correct, a few invariants must hold:
        //
        // - mappings must be set whenever an instance's sled changes (eg.
        //   during instance creation, migration, stop + start)
        //
        // - an instances' sled must not change while its corresponding mappings
        //   are being created
        //
        // - the same mapping creation must be broadcast to all sleds
        //
        // A more targeted approach would be to see what other instances share
        // the VPC this instance is in (or more generally, what instances should
        // have connectivity to this one), see what sleds those are allocated
        // to, and only create V2P mappings for those sleds.
        //
        // There's additional work with this approach:
        //
        // - it means that delete calls are required as well as set calls,
        //   meaning that now the ordering of those matters (this may also
        //   necessitate a generation number for V2P mappings)
        //
        // - V2P mappings have to be bidirectional in order for both instances's
        //   packets to make a round trip. This isn't a problem with the
        //   broadcast approach because one of the sides will exist already, but
        //   it is something to orchestrate with a more targeted approach.
        //
        // TODO-correctness Default firewall rules currently will block
        // instances in different VPCs from connecting to each other. If it ever
        // stops doing this, the broadcast approach will create V2P mappings
        // that shouldn't exist.

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
        let physical_host_ip =
            *self.sled_lookup(&self.opctx_alloc, &instance_sled_id).await?.ip;

        let mut last_sled_id: Option<Uuid> = None;
        loop {
            let pagparams = DataPageParams {
                marker: last_sled_id.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: std::num::NonZeroU32::new(10).unwrap(),
            };

            let sleds_page =
                self.sleds_list(&self.opctx_alloc, &pagparams).await?;
            let mut join_handles =
                Vec::with_capacity(sleds_page.len() * instance_nics.len());

            for sled in &sleds_page {
                // set_v2p not required for sled instance was allocated to, OPTE
                // currently does that automatically
                if sled.id() == instance_sled_id {
                    continue;
                }

                for nic in &instance_nics {
                    let client = self.sled_client(&sled.id()).await?;
                    let nic_id = nic.id;
                    let mapping = SetVirtualNetworkInterfaceHost {
                        virtual_ip: nic.ip,
                        virtual_mac: nic.mac.clone(),
                        physical_host_ip,
                        vni: nic.vni.clone(),
                    };

                    // This function is idempotent: calling the set_v2p ioctl with
                    // the same information is a no-op.
                    join_handles.push(tokio::spawn(futures::future::lazy(
                        move |_ctx| async move {
                            client.set_v2p(&nic_id, &mapping).await
                        },
                    )));
                }
            }

            // Concurrently run each future to completion, but return the last
            // error seen.
            let mut error = None;
            for join_handle in join_handles {
                let result = join_handle
                    .await
                    .map_err(|e| Error::internal_error(&e.to_string()))?
                    .await;

                if result.is_err() {
                    error!(self.log, "{:?}", result);
                    error = Some(result);
                }
            }
            if let Some(e) = error {
                return e.map(|_| ()).map_err(|e| e.into());
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
        let physical_host_ip =
            *self.sled_lookup(&self.opctx_alloc, &instance_sled_id).await?.ip;

        let mut last_sled_id: Option<Uuid> = None;

        loop {
            let pagparams = DataPageParams {
                marker: last_sled_id.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: std::num::NonZeroU32::new(10).unwrap(),
            };

            let sleds_page =
                self.sleds_list(&self.opctx_alloc, &pagparams).await?;
            let mut join_handles =
                Vec::with_capacity(sleds_page.len() * instance_nics.len());

            for sled in &sleds_page {
                // del_v2p not required for sled instance was allocated to, OPTE
                // currently does that automatically
                if sled.id() == instance_sled_id {
                    continue;
                }

                for nic in &instance_nics {
                    let client = self.sled_client(&sled.id()).await?;
                    let nic_id = nic.id;
                    let mapping = SetVirtualNetworkInterfaceHost {
                        virtual_ip: nic.ip,
                        virtual_mac: nic.mac.clone(),
                        physical_host_ip,
                        vni: nic.vni.clone(),
                    };

                    // This function is idempotent: calling the set_v2p ioctl with
                    // the same information is a no-op.
                    join_handles.push(tokio::spawn(futures::future::lazy(
                        move |_ctx| async move {
                            client.del_v2p(&nic_id, &mapping).await
                        },
                    )));
                }
            }

            // Concurrently run each future to completion, but return the last
            // error seen.
            let mut error = None;
            for join_handle in join_handles {
                let result = join_handle
                    .await
                    .map_err(|e| Error::internal_error(&e.to_string()))?
                    .await;

                if result.is_err() {
                    error!(self.log, "{:?}", result);
                    error = Some(result);
                }
            }
            if let Some(e) = error {
                return e.map(|_| ()).map_err(|e| e.into());
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
