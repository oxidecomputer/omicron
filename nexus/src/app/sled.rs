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
use crate::internal_api::params::ZpoolPutRequest;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use sled_agent_client::Client as SledAgentClient;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    // Sleds

    // TODO-robustness we should have a limit on how many sled agents there can
    // be (for graceful degradation at large scale).
    pub async fn upsert_sled(
        &self,
        id: Uuid,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        info!(self.log, "registered sled agent"; "sled_uuid" => id.to_string());
        let sled = db::model::Sled::new(id, address, self.rack_id);
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

    // TODO-design This interface should not exist.  See
    // SagaContext::alloc_server().
    pub async fn sled_allocate(&self) -> Result<Uuid, Error> {
        // We need an OpContext to query the database.  Normally we'd use
        // one from the current operation, usually a saga action or API call.
        // In this case, though, the caller may not have permissions to access
        // the sleds in the system.  We're really doing this as Nexus itself,
        // operating on behalf of the caller.
        let opctx = &self.opctx_alloc;

        // TODO: replace this with a real allocation policy.
        //
        // This implementation always assigns the first sled (by ID order).
        let pagparams = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(1).unwrap(),
        };
        let sleds = self.db_datastore.sled_list(&opctx, &pagparams).await?;

        sleds
            .first()
            .ok_or_else(|| Error::ServiceUnavailable {
                internal_message: String::from(
                    "no sleds available for new Instance",
                ),
            })
            .map(|s| s.id())
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
        address: SocketAddr,
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
